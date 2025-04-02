import json

import scrapy
from scrapy.http.request.json_request import JsonRequest
from scrapy.http.response import Response

from banalytics.kinds import ItemKind
from banalytics.utils import preprocess_item


class ChaldalSpider(scrapy.Spider):
    name = "chaldal"
    allowed_domains = ["chaldal.com"]
    start_urls = ["https://chaldal.com"]
    api_key = ""

    def parse(self, response: Response):
        window_service_state_script = response.css("body > script::text")[0]
        json_payload = window_service_state_script.extract().replace(
            "window.__serviceState = ", "", 1
        )

        payload = json.loads(json_payload)

        shop_metadata = payload["LogicService"]["globalConstants"][0]
        yield preprocess_item(shop_metadata, ItemKind.Chaldal_SHOP_METADATA)

        categories = payload["CategoryService"]["categories"]["1"]  # "1" is the storeId
        yield preprocess_item(categories, ItemKind.Chaldal_CATEGORIES)

        brands = payload["RouterService"]["manufacturerRoutes"]["1"]
        yield preprocess_item(brands, ItemKind.Chaldal_BRANDS)

        idx = response.body.find(b"apiKey")
        self.api_key = response.body[idx + 15 : idx + 79].decode()

        for category in categories:
            if not category["ContainsProducts"]:
                continue
            for area in shop_metadata["Areas"].values():
                data = {
                    "apiKey": self.api_key,
                    "storeId": 1,
                    "warehouseId": area["WarehouseId"],
                    "pageSize": 250,
                    "currentPageIndex": 0,
                    "metropolitanAreaId": area["MetropolitanAreaId"],
                    "query": "",
                    "productVariantId": -1,
                    "bundleId": {"case": "None"},
                    "canSeeOutOfStock": "false",
                    "filters": ["categories%3D" + str(category["Id"])],
                    "shouldShowAlternateProductsForAllOutOfStock": {
                        "case": "Some",
                        "fields": [True],
                    },
                    "customerGuid": {"case": "None"},
                    "deliveryAreaId": {"case": "None"},
                    "shouldShowCategoryBasedRecommendations": {"case": "None"},
                }
                yield JsonRequest(
                    "https://catalog.chaldal.com/searchPersonalized",
                    callback=self.parse_listings,
                    data=data,
                    cb_kwargs={
                        "warehouse": area["WarehouseId"],
                        "metropolitan": area["MetropolitanAreaId"],
                    },
                )

    def parse_listings(self, response: Response, warehouse: str, metropolitan: str):
        payload = response.json()  # type: ignore
        if payload["page"] < payload["nbPages"] and response.request is not None:
            request_payload = json.loads(response.request.body.decode())
            request_payload["currentPageIndex"] += 1

            yield JsonRequest(
                "https://catalog.chaldal.com/searchPersonalized",
                callback=self.parse_listings,
                data=request_payload,
                cb_kwargs={
                    "warehouse": warehouse,
                    "metropolitan": metropolitan,
                },
            )

        for hit in payload["hits"]:
            yield preprocess_item(
                hit | {"warehouse": warehouse, "metropolitan": metropolitan},
                ItemKind.Chaldal_LISTING,
            )
