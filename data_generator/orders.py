"""Synthetic orders and order items, delivered in daily batches.

Each day becomes its own folder in the landing zone, the way many source
systems export one file drop per day:

    data/landing/orders/batch_date=2026-11-01/orders.json        (JSON lines)
    data/landing/order_items/batch_date=2026-11-01/order_items.csv

Real source data is never clean, so a small share of records is made dirty on
purpose. The bronze and silver layers are built to catch exactly these cases:

    duplicate       same order sent twice, the second copy with a later status
    null_customer   customer_id missing
    orphan_customer customer_id that does not exist in customers.csv
    bad_timestamp   order_ts in a non-ISO format (e.g. 18/11/2026 14:05)
    messy_status    status with stray spaces and capitals ("  Shipped ")
    bad_quantity    order item with zero or negative quantity
    corrupt_line    a line that is not valid JSON at all

Usage (run reference_data first):
    python -m data_generator.orders --start-date 2026-11-01 --days 3 --orders-per-day 200
"""

from __future__ import annotations

import argparse
import json
import random
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

from data_generator.reference_data import read_csv, write_csv
from lakehouse.utils.config import load_config
from lakehouse.utils.logger import get_logger

logger = get_logger(__name__)

ORDER_ITEM_COLUMNS = ["order_id", "line_number", "product_id", "quantity",
                      "unit_price", "discount_pct"]
STATUSES = ["placed", "shipped", "delivered", "cancelled", "returned"]
# How an order moves forward when a duplicate (later) copy is sent.
_NEXT_STATUS = {"placed": "shipped", "shipped": "delivered", "delivered": "returned"}
PAYMENT_METHODS = ["card", "card", "card", "paypal", "gift_card"]
DIRTY_KINDS = ["duplicate", "null_customer", "orphan_customer", "bad_timestamp",
               "messy_status", "bad_quantity", "corrupt_line"]


@dataclass
class DailyBatch:
    """Everything generated for one batch date."""

    batch_date: date
    orders: list[dict] = field(default_factory=list)
    order_items: list[dict] = field(default_factory=list)
    corrupt_lines: list[str] = field(default_factory=list)
    # How many of each kind of dirty record was injected (for logging/tests).
    dirty_counts: Counter = field(default_factory=Counter)


def generate_daily_batch(batch_date: date, customer_ids: list[str], products: list[dict],
                         orders_per_day: int, dirty_rate: float = 0.05,
                         seed: int = 42) -> DailyBatch:
    """Generate one day of orders and order items.

    `dirty_rate` is the chance that any one order gets a defect. The seed is
    combined with the date so each day is different but still reproducible.
    """
    if orders_per_day < 1:
        raise ValueError("orders_per_day must be at least 1")
    if not 0 <= dirty_rate <= 1:
        raise ValueError("dirty_rate must be between 0 and 1")
    if not customer_ids or not products:
        raise ValueError("customers and products are required; run reference_data first")

    rng = random.Random(f"{seed}-{batch_date.isoformat()}")
    batch = DailyBatch(batch_date=batch_date)
    day_start = datetime.combine(batch_date, datetime.min.time())

    for i in range(1, orders_per_day + 1):
        order_ts = day_start + timedelta(seconds=rng.randint(0, 86_399))
        order = {
            # The date is in the ID so IDs stay unique across days.
            "order_id": f"O{batch_date:%Y%m%d}-{i:05d}",
            "customer_id": rng.choice(customer_ids),
            "order_ts": order_ts.isoformat(),
            "status": rng.choices(STATUSES, weights=[50, 25, 15, 6, 4])[0],
            "payment_method": rng.choice(PAYMENT_METHODS),
            "currency": "USD",
            # Source systems usually stamp each change; silver uses this
            # column to decide which duplicate copy is the newest.
            "updated_at": order_ts.isoformat(),
        }
        items = _make_items(order["order_id"], products, rng)
        # Append first, so a duplicate copy added by a defect lands after the
        # original, like a re-sent record would in a real export.
        batch.orders.append(order)
        batch.order_items.extend(items)

        if rng.random() < dirty_rate:
            kind = rng.choice(DIRTY_KINDS)
            batch.dirty_counts[kind] += 1
            _apply_defect(kind, order, items, batch, rng)

    return batch


def _make_items(order_id: str, products: list[dict], rng: random.Random) -> list[dict]:
    """Return 1-4 line items with distinct products for one order."""
    chosen = rng.sample(products, k=min(rng.randint(1, 4), len(products)))
    return [
        {
            "order_id": order_id,
            "line_number": line,
            "product_id": product["product_id"],
            "quantity": rng.choices([1, 2, 3, 5], weights=[70, 20, 7, 3])[0],
            # Price is copied at order time: if the catalogue price changes
            # later, historical orders must keep the price the customer paid.
            "unit_price": product["unit_price"],
            "discount_pct": rng.choice([0, 0, 0, 5, 10, 20]),
        }
        for line, product in enumerate(chosen, start=1)
    ]


def _apply_defect(kind: str, order: dict, items: list[dict], batch: DailyBatch,
                  rng: random.Random) -> None:
    """Mutate the order (or its items) in place to inject one kind of defect."""
    if kind == "duplicate":
        # Re-send the same order later the same day with a newer status. This
        # is the classic case for "keep the latest version" deduplication.
        updated = datetime.fromisoformat(order["updated_at"]) + timedelta(hours=rng.randint(1, 6))
        copy = dict(order, status=_NEXT_STATUS.get(order["status"], order["status"]),
                    updated_at=updated.isoformat())
        batch.orders.append(copy)
    elif kind == "null_customer":
        order["customer_id"] = None
    elif kind == "orphan_customer":
        order["customer_id"] = "C99999"
    elif kind == "bad_timestamp":
        order["order_ts"] = datetime.fromisoformat(order["order_ts"]).strftime("%d/%m/%Y %H:%M")
    elif kind == "messy_status":
        order["status"] = f"  {order['status'].title()} "
    elif kind == "bad_quantity":
        items[0]["quantity"] = rng.choice([0, -1, -3])
    elif kind == "corrupt_line":
        # A truncated line, as if the export was cut off mid-write.
        batch.corrupt_lines.append(json.dumps(order)[: rng.randint(10, 40)])
    else:
        raise ValueError(f"Unknown defect kind: {kind}")


def write_batch(batch: DailyBatch, landing_path: Path) -> tuple[Path, Path]:
    """Write one batch to its batch_date folders. Rewriting a date replaces it."""
    folder = f"batch_date={batch.batch_date.isoformat()}"
    orders_file = landing_path / "orders" / folder / "orders.json"
    orders_file.parent.mkdir(parents=True, exist_ok=True)
    with orders_file.open("w", encoding="utf-8") as handle:
        for order in batch.orders:
            handle.write(json.dumps(order) + "\n")
        for line in batch.corrupt_lines:
            handle.write(line + "\n")

    items_file = write_csv(batch.order_items, ORDER_ITEM_COLUMNS,
                           landing_path / "order_items" / folder / "order_items.csv")
    return orders_file, items_file


def main(argv: list[str] | None = None) -> None:
    config = load_config()
    parser = argparse.ArgumentParser(description="Generate daily order batches.")
    parser.add_argument("--start-date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--days", type=int, default=1)
    parser.add_argument("--orders-per-day", type=int, default=200)
    parser.add_argument("--dirty-rate", type=float, default=0.05)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--landing", type=Path, default=config.landing_path,
                        help="landing folder (default: <data_root>/landing)")
    args = parser.parse_args(argv)

    customers_file = args.landing / "customers" / "customers.csv"
    products_file = args.landing / "products" / "products.csv"
    if not customers_file.exists() or not products_file.exists():
        raise SystemExit("Reference data not found. Run: python -m data_generator.reference_data")
    customer_ids = [row["customer_id"] for row in read_csv(customers_file)]
    products = read_csv(products_file)

    for offset in range(args.days):
        batch_date = args.start_date + timedelta(days=offset)
        batch = generate_daily_batch(batch_date, customer_ids, products,
                                     args.orders_per_day, args.dirty_rate, args.seed)
        orders_file, _ = write_batch(batch, args.landing)
        logger.info("%s: %d order rows, %d item rows, defects %s -> %s",
                    batch_date, len(batch.orders), len(batch.order_items),
                    dict(batch.dirty_counts), orders_file.parent)


if __name__ == "__main__":
    main()
