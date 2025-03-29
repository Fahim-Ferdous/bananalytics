from datetime import datetime
from typing import Any

from pydantic import BaseModel

from banalytics.kinds import KindOfItem


class BananlyticsModel(BaseModel):
    payload: Any
    date: datetime
    kind: KindOfItem


def preprocess_item(item: dict, item_type: KindOfItem) -> BananlyticsModel:
    return BananlyticsModel(date=datetime.now(), kind=item_type, payload=item)
