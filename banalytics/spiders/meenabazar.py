import json
from string import ascii_lowercase
from urllib.parse import urljoin

import scrapy
from scrapy.http.request.json_request import JsonRequest
from scrapy.http.response import Response

from banalytics.kinds import MeenabazarItemKinds
from banalytics.utils import preprocess_item


class MeenabazarSpider(scrapy.Spider):
    name = "meenabazar"
    allowed_domains = ["meenabazardev.com"]
    start_urls = []

    def __init__(self, name: str | None = None, **kwargs):
        super().__init__(name, **kwargs)

        self.subunits: set[int] = set()
        self.delivery_area_query_queue: set[str] = set(
            [a + b for a in ascii_lowercase for b in ascii_lowercase]
        )

    def start_requests(self):
        for i in list(self.delivery_area_query_queue):
            yield JsonRequest(
                "https://meenabazardev.com/api/front/areas/search",
                callback=self.parse_delivery_area,
                data={"q": i},
                cb_kwargs={"letter": i},
            )

    def parse_delivery_area(self, response: Response, letter: str):
        for item in response.json()["data"]:  # type: ignore
            # NOTE: They refer branches as "Subunits".
            # Not all branches are available online.
            subunit_id = item["SubUnitId"]
            if subunit_id not in self.subunits:
                self.subunits.add(subunit_id)
                yield response.follow(
                    f"/api/front/store/picup/name?SubUnitId={subunit_id}",
                    self.parse_subunit_name,
                )
            yield preprocess_item(item, MeenabazarItemKinds.DELIVERY_AREA)

        self.delivery_area_query_queue.remove(letter)
        if not self.delivery_area_query_queue:
            # Start parsing the categories AFTER we have
            # fetched all the subunits or branch outlets.
            yield scrapy.Request(
                "https://meenabazardev.com/api/front/nav/categories/list",
                callback=self.parse_categories,
            )

    def parse_categories(self, response: Response):
        for item in response.json()["data"]:  # type: ignore
            category_id = item["ItemCategoryId"]
            category_slug = item["CategorySlug"]

            for subunit in self.subunits:
                yield JsonRequest(
                    urljoin(
                        response.url,
                        f"/api/front/product/category/{category_slug}",
                    ),
                    callback=self.parse_listing,
                    data={
                        "BrandId": [],
                        "CategoryId": [category_id],
                        "NoOfItem": 20,
                        "SearchSlug": category_slug,
                        "SearchType": "C",
                        "StartSl": 1,
                        "SubCategoryId": [],
                        "SubUnitId": subunit,
                        "ThumbSize": "lg",
                    },
                )
            yield preprocess_item(item, MeenabazarItemKinds.CATEGORY)

    def parse_listing(self, response: Response):
        data = json.loads(response.request.body)  # type: ignore
        items = response.json()["data"]["Category"]  # type: ignore
        if not items:
            return

        for item in items:
            yield preprocess_item(item, MeenabazarItemKinds.LISTING)

        old_start_sl = data["StartSl"]
        data["StartSl"] += data["NoOfItem"]
        if len(items) < data["NoOfItem"]:
            return

        if old_start_sl == 1:
            data["NoOfItem"] = items[0]["TotalItem"] - data["NoOfItem"]

        yield JsonRequest(
            response.url,
            callback=self.parse_listing,
            data=data,
        )

    def parse_subunit_name(self, response: Response):
        yield preprocess_item(response.json()["data"], MeenabazarItemKinds.BRANCH)  # type: ignore
