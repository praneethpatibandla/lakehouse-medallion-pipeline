import json
from datetime import date, datetime

import pytest

from data_generator.orders import (
    DIRTY_KINDS,
    generate_daily_batch,
    main,
    write_batch,
)
from data_generator.reference_data import generate_products, read_csv
from data_generator.reference_data import main as generate_reference_data

CUSTOMERS = [f"C{i:05d}" for i in range(1, 21)]
PRODUCTS = generate_products(10)
DAY = date(2026, 11, 1)


def test_clean_batch_has_valid_records():
    batch = generate_daily_batch(DAY, CUSTOMERS, PRODUCTS, orders_per_day=30, dirty_rate=0)
    assert len(batch.orders) == 30
    assert not batch.dirty_counts and not batch.corrupt_lines
    assert len({o["order_id"] for o in batch.orders}) == 30
    for order in batch.orders:
        assert order["customer_id"] in CUSTOMERS
        assert datetime.fromisoformat(order["order_ts"]).date() == DAY
    order_ids = {o["order_id"] for o in batch.orders}
    assert all(item["order_id"] in order_ids for item in batch.order_items)
    assert all(item["quantity"] > 0 for item in batch.order_items)


def test_same_seed_and_date_is_reproducible():
    first = generate_daily_batch(DAY, CUSTOMERS, PRODUCTS, 10, seed=1)
    second = generate_daily_batch(DAY, CUSTOMERS, PRODUCTS, 10, seed=1)
    other_day = generate_daily_batch(date(2026, 11, 2), CUSTOMERS, PRODUCTS, 10, seed=1)
    assert first.orders == second.orders
    assert first.orders != other_day.orders


def test_full_dirty_rate_injects_every_kind():
    batch = generate_daily_batch(DAY, CUSTOMERS, PRODUCTS, orders_per_day=300, dirty_rate=1)
    assert sum(batch.dirty_counts.values()) == 300
    assert set(batch.dirty_counts) == set(DIRTY_KINDS)


def test_duplicates_are_newer_copies():
    batch = generate_daily_batch(DAY, CUSTOMERS, PRODUCTS, orders_per_day=300, dirty_rate=1)
    by_id = {}
    for order in batch.orders:
        by_id.setdefault(order["order_id"], []).append(order)
    dupes = [copies for copies in by_id.values() if len(copies) > 1]
    assert len(dupes) == batch.dirty_counts["duplicate"]
    for original, copy in dupes:
        assert copy["updated_at"] > original["updated_at"]


@pytest.mark.parametrize("kwargs", [
    {"orders_per_day": 0},
    {"orders_per_day": 5, "dirty_rate": 1.5},
])
def test_invalid_arguments_are_rejected(kwargs):
    with pytest.raises(ValueError):
        generate_daily_batch(DAY, CUSTOMERS, PRODUCTS, **kwargs)


def test_write_batch_layout(tmp_path):
    batch = generate_daily_batch(DAY, CUSTOMERS, PRODUCTS, orders_per_day=200, dirty_rate=0.3)
    orders_file, items_file = write_batch(batch, tmp_path)
    assert orders_file.parent.name == "batch_date=2026-11-01"

    lines = orders_file.read_text().splitlines()
    assert len(lines) == len(batch.orders) + len(batch.corrupt_lines)
    parsed = 0
    for line in lines:
        try:
            json.loads(line)
            parsed += 1
        except json.JSONDecodeError:
            pass
    assert parsed == len(batch.orders)
    assert len(read_csv(items_file)) == len(batch.order_items)


def test_main_needs_reference_data(tmp_path):
    with pytest.raises(SystemExit):
        main(["--landing", str(tmp_path)])


def test_main_writes_one_folder_per_day(tmp_path):
    generate_reference_data(["--customers", "5", "--products", "5", "--output", str(tmp_path)])
    main(["--landing", str(tmp_path), "--start-date", "2026-11-01", "--days", "3",
          "--orders-per-day", "4"])
    folders = sorted(p.name for p in (tmp_path / "orders").iterdir())
    assert folders == ["batch_date=2026-11-01", "batch_date=2026-11-02", "batch_date=2026-11-03"]
