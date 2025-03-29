# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
# from itemadapter import ItemAdapter


from scrapy.exceptions import DropItem

from banalytics.utils import BananlyticsModel


class BanalyticsPipeline:
    def process_item(self, item, _):
        return item


class Unique:
    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item: BananlyticsModel, _):
        if item.unique_key is None:
            return item

        if item.unique_key in self.ids_seen:
            raise DropItem()

        self.ids_seen.add(item.unique_key)

        return item
