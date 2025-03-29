# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
# from itemadapter import ItemAdapter


from scrapy.exceptions import DropItem

from banalytics.kinds import MeenabazarItemKinds
from banalytics.utils import BananlyticsModel


class BanalyticsPipeline:
    def process_item(self, item, _):
        return item


class UniqueMeenabazar:
    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item: BananlyticsModel, _):
        match item.kind:
            case MeenabazarItemKinds.DELIVERY_AREA:
                key = "AreaId"
            case MeenabazarItemKinds.CATEGORY:
                key = "ItemCategoryId"
            case MeenabazarItemKinds.LISTING:
                key = "ItemId"
            case MeenabazarItemKinds.BRANCH:
                key = "SubUnitId"
            case _:
                assert False, "unhandeled type"

        key = key + ":" + str(item.payload.get(key, ""))
        if key in self.ids_seen:
            raise DropItem()

        self.ids_seen.add(key)

        return item
