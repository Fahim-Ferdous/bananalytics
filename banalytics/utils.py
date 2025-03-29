from datetime import datetime
from typing import Any

from pydantic import BaseModel

from banalytics.kinds import ItemKind


class BananlyticsModel(BaseModel):
    payload: Any
    date: datetime
    kind: ItemKind


def preprocess_item(item: dict, item_type: ItemKind) -> BananlyticsModel:
    return BananlyticsModel(date=datetime.now(), kind=item_type, payload=item)
