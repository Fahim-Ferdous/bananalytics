from enum import StrEnum, auto


class KindOfItem(StrEnum):
    pass


class MeenabazarItemKinds(KindOfItem):
    DELIVERY_AREA = auto()
    CATEGORY = auto()
    LISTING = auto()
    BRANCH = auto()
