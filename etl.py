import logging
import os
from argparse import ArgumentParser
from collections import defaultdict
from datetime import datetime
from os import path
from typing import TextIO

import psycopg
from psycopg.errors import UniqueViolation
from pydantic import BaseModel

from banalytics.kinds import ItemKind
from banalytics.utils import BananlyticsModel

parser = ArgumentParser(
    epilog="Load scraped results to database.",
)

parser.add_argument(
    "files",
    nargs="+",
    help="Load these files. Must containing jsonline serialized Banalytics objects.",
)

parser.add_argument("--dsn", help="database connection string")


def get_quantity_and_unit(text: str) -> tuple[float, str]:
    """
    >>> get_quantity_and_unit("KG")
    (1.0, 'kg')
    >>> get_quantity_and_unit("1000g")
    (1000.0, 'g')
    >>> get_quantity_and_unit("1.1kg")
    (1.1, 'kg')
    >>> get_quantity_and_unit("250")
    (250.0, '')
    >>> get_quantity_and_unit("250 Gm")
    (250.0, 'gm')
    >>> get_quantity_and_unit("500 Gram ±")
    (500.0, 'gram')
    >>> get_quantity_and_unit("Each")
    (1.0, 'each')
    """
    s = 0
    for c in text:
        if c.isdigit() or c == ".":
            s += 1
        else:
            break

    qty = text[:s]
    unit = text[s:]

    if "±" in unit:
        unit = unit.replace("±", "")

    return (float(1 if qty == "" else qty), unit.strip().lower())


class Row(BaseModel):
    id: str
    name: str

    quantity: float
    unit: str

    price: float
    sale_price: float

    unique_key: str
    date: datetime


def load(file: TextIO) -> tuple[dict, list[Row]]:
    metadata = defaultdict(list)
    rows: list[Row] = []
    for line in file:
        model = BananlyticsModel.model_validate_json(line)
        d = model.payload

        item_id = vendor = ""
        row: Row | None = None
        match model.kind:
            case ItemKind.Chaldal_LISTING:
                item_id = str(d["objectID"])
                vendor = "chaldal"

                sub_text = d["subText"]
                qty, unit = get_quantity_and_unit(sub_text)

                assert model.unique_key is not None
                row = Row(
                    id=item_id,
                    name=d["nameWithoutSubText"],
                    quantity=qty,
                    unit=unit,
                    price=d["mrp"],
                    sale_price=d["price"],
                    unique_key=model.unique_key,
                    date=model.date,
                )
            case ItemKind.Chaldal_CATEGORIES:
                metadata["categories"] = d
            case ItemKind.Chaldal_BRANDS:
                metadata["brands"] = d
            case ItemKind.Chaldal_SHOP_METADATA:
                metadata["shop_metadata"] = d
            case ItemKind.Meenabazar_LISTING:
                item_id = str(d["ItemId"])
                vendor = "meenabazar"

                qty, unit = get_quantity_and_unit(d["Unit"])

                assert model.unique_key is not None
                row = Row(
                    id=item_id,
                    name=d["ItemDisplayName"],
                    quantity=qty,
                    unit=unit,
                    price=d["UnitSalesPrice"],
                    sale_price=d["DiscountSalesPrice"],
                    unique_key=model.unique_key,
                    date=model.date,
                )
            case ItemKind.Meenabazar_DELIVERY_AREA:
                metadata["deliver_areas"].append(d)
            case ItemKind.Meenabazar_CATEGORIES:
                metadata["categories"] = d
            case ItemKind.Meenabazar_BRANCH:
                metadata["branches"].append(d)

            case _ as kind:
                raise TypeError("Idk what kinda type this is", kind)

        if row is not None:
            if row.price == 0:
                logging.warning(
                    "skipping item %s from vendor %s (price = 0).", item_id, vendor
                )
                continue
            rows.append(row)
    return metadata, rows


class Run(BaseModel):
    run_id: str
    vendor: str
    metadata: dict
    started_at: datetime
    ended_at: datetime | None

    @classmethod
    def from_filename(cls, filename: str, metadata: dict) -> "Run":
        value_str = path.splitext(path.basename(filename))[0]
        vendor, started, ended, run_id = value_str.split("_")
        return cls(
            run_id=run_id,
            vendor=vendor,
            started_at=datetime.strptime(started, "%Y%m%d%H%M%S"),
            ended_at=datetime.strptime(ended, "%Y%m%d%H%M%S"),
            metadata=metadata,
        )


def insert_everything(conn: psycopg.Connection, run: Run, rows: list[Row]):
    with conn.cursor() as cur:
        try:
            result = cur.execute(
                """insert into runs(run_id, started_at, ended_at, vendor) values(%s, %s, %s, %s) returning id""",
                (run.run_id, run.started_at, run.ended_at, run.vendor),
            )
        except UniqueViolation:
            logging.error("run id %s is probably already processed", run.run_id)
            conn.rollback()
            return

        fetched = result.fetchone()

        assert fetched is not None
        run_id = fetched[0]

        values = (
            [
                r.id,
                r.name,
                r.quantity,
                r.unit,
                r.price,
                r.sale_price,
                r.unique_key,
                r.date,
                run_id,
            ]
            for r in rows
        )
        cur.executemany(
            """insert into datapoints(item_id, name, quantity, unit, price, sale_price, unique_key, fetched_at, run_id)
                values(%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            values,
        )
        conn.commit()


def filesize_nice(size: int | float):
    for unit in ("", "Ki", "Mi"):
        if abs(size) < 1024.0:
            return f"{size:3.1f}{unit}B"
        size /= 1024.0
    return f"{size:.1f}GiB"


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    namespace = parser.parse_args()

    conn = psycopg.connect(namespace.dsn)

    for filename in namespace.files:
        read_started = datetime.now()
        with open(filename) as f:
            metadata, rows = load(f)
            print(metadata)
        read_ended = datetime.now()
        logging.info(
            "read file %s (%s) in %s",
            filename,
            filesize_nice(path.getsize(filename)),
            read_ended - read_started,
        )

        run = Run.from_filename(filename, metadata)
        insert_everything(conn, run, rows)
        logging.info(
            "inserted %s data rows in %s", len(rows), datetime.now() - read_ended
        )
