from datetime import datetime
from typing import Any
from urllib.parse import urlencode

from pydantic import BaseModel

from banalytics.kinds import ItemKind


# Scope is basically a namespace. For example an outlet for meenabazar.
# Not all outlets might have the same item, hence we need it.
class BananlyticsModel(BaseModel):
    payload: Any
    date: datetime
    kind: ItemKind


def preprocess_item(
    item: dict,
    item_type: ItemKind,
) -> BananlyticsModel:
    return BananlyticsModel(
        date=datetime.now(),
        kind=item_type,
        payload=item,
    )


def should_skip_deduplication(item: BananlyticsModel) -> bool:
    return item.kind in (
        ItemKind.Meenabazar_DELIVERY_AREAS,
        ItemKind.Meenabazar_CATEGORIES,
        ItemKind.Chaldal_CATEGORIES,
        ItemKind.Chaldal_SHOP_METADATA,
    )


def get_item_unique_key(item: BananlyticsModel) -> str:
    match item.kind:

        case ItemKind.Meenabazar_DELIVERY_AREAS:
            keys = ["AreaId"]
        case ItemKind.Meenabazar_CATEGORIES:
            keys = ["ItemCategoryId"]
        case ItemKind.Meenabazar_LISTING:
            keys = ["subunit", "ItemId"]
        case ItemKind.Meenabazar_BRANCH:
            keys = ["SubUnitId"]

        case ItemKind.Chaldal_LISTING:
            keys = ["warehouse", "objectID"]

        case _:
            assert False, "unhandeled type"

    return urlencode({key: item.payload[key] for key in keys})
