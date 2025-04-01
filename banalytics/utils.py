import hashlib
from datetime import datetime
from typing import Any
from urllib.parse import urlencode

from pydantic import BaseModel

from banalytics.kinds import ItemKind


# Scope is basically a namespace. For example an outlet for meenabazar.
# Not all outlets might have the same item, hence we need it.
class BananlyticsModel(BaseModel):
    payload: Any
    run_id: str
    date: datetime
    kind: ItemKind
    unique_key: str | None


def overwrite_fields(
    item: dict,
    kind: ItemKind,
) -> dict:
    if kind == ItemKind.Chaldal_LISTING:
        elts = []
        for elt in item["productAvailabilityForSelectedWarehouse"]:
            d = {}
            for k, v in elt.items():
                if isinstance(v, dict):
                    v["UnixTimeMilliseconds"] = 0
                d[k] = v
            elts.append(d)
        item["productAvailabilityForSelectedWarehouse"] = elts
    return item


run_id = hashlib.sha1(datetime.now().isoformat().encode()).hexdigest()[:7]


def preprocess_item(
    item: dict,
    kind: ItemKind,
) -> BananlyticsModel:
    unique = None
    if not should_skip_deduplication(kind):
        match kind:

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

        unique = urlencode({key: item[key] for key in keys})

    return BananlyticsModel(
        unique_key=unique,
        run_id=run_id,
        date=datetime.now(),
        kind=kind,
        payload=item,
    )


def should_skip_deduplication(kind: ItemKind) -> bool:
    return kind in (
        ItemKind.Meenabazar_DELIVERY_AREAS,
        ItemKind.Meenabazar_CATEGORIES,
        ItemKind.Chaldal_CATEGORIES,
        ItemKind.Chaldal_SHOP_METADATA,
    )
