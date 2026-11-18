"""Synthetic reference data: customers and products.

These are the "master data" of the fake e-commerce shop. Orders generated later
point at these customer and product IDs, so this module must run first.

Only the Python standard library is used, and every function takes a seed, so
the same command always produces exactly the same files. Reproducible test data
makes bugs reproducible too.

Usage:
    python -m data_generator.reference_data --customers 500 --products 100
"""

from __future__ import annotations

import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path

from lakehouse.utils.config import load_config
from lakehouse.utils.logger import get_logger

logger = get_logger(__name__)

CUSTOMER_COLUMNS = [
    "customer_id", "first_name", "last_name", "city", "state",
    "country", "segment", "signup_date",
]
PRODUCT_COLUMNS = [
    "product_id", "product_name", "category", "brand", "unit_price", "is_active",
]

_FIRST_NAMES = [
    "Aarav", "Maya", "Liam", "Sofia", "Noah", "Priya", "Ethan", "Zara",
    "Lucas", "Ananya", "Mia", "Omar", "Chloe", "Ravi", "Emma", "Diego",
]
_LAST_NAMES = [
    "Sharma", "Smith", "Garcia", "Nguyen", "Patel", "Johnson", "Kim",
    "Lopez", "Brown", "Reddy", "Martin", "Silva", "Chen", "Davis",
]
# (city, state) pairs so the two columns always agree with each other.
_CITIES = [
    ("Portland", "OR"), ("Seattle", "WA"), ("Austin", "TX"), ("Denver", "CO"),
    ("Chicago", "IL"), ("Boston", "MA"), ("San Jose", "CA"), ("Atlanta", "GA"),
]
_SEGMENTS = ["consumer", "consumer", "consumer", "small_business", "corporate"]

# category -> (example product nouns, price range in dollars)
_CATALOG = {
    "electronics": (["Headphones", "Keyboard", "Monitor", "Charger", "Speaker"], (15, 400)),
    "home": (["Lamp", "Blanket", "Mug Set", "Cutting Board", "Vase"], (8, 120)),
    "sports": (["Yoga Mat", "Water Bottle", "Dumbbell", "Jump Rope", "Backpack"], (10, 150)),
    "books": (["Cookbook", "Novel", "Travel Guide", "Notebook", "Comic"], (5, 45)),
    "beauty": (["Face Cream", "Shampoo", "Lip Balm", "Perfume", "Sunscreen"], (4, 90)),
}
_BRANDS = ["Northwind", "Contoso", "Fabrikam", "Tailspin", "Litware", "Adatum"]


def generate_customers(count: int, seed: int = 42, start: date = date(2023, 1, 1)) -> list[dict]:
    """Return `count` customer rows with IDs C00001, C00002, ...

    Signup dates are spread over roughly two years after `start`, which later
    lets the gold layer compute customer tenure.
    """
    if count < 1:
        raise ValueError("count must be at least 1")
    rng = random.Random(seed)
    customers = []
    for i in range(1, count + 1):
        city, state = rng.choice(_CITIES)
        customers.append({
            "customer_id": f"C{i:05d}",
            "first_name": rng.choice(_FIRST_NAMES),
            "last_name": rng.choice(_LAST_NAMES),
            "city": city,
            "state": state,
            "country": "US",
            "segment": rng.choice(_SEGMENTS),
            "signup_date": (start + timedelta(days=rng.randint(0, 730))).isoformat(),
        })
    return customers


def generate_products(count: int, seed: int = 42) -> list[dict]:
    """Return `count` product rows with IDs P0001, P0002, ...

    Prices are rounded to cents and written as strings ("19.99") so the CSV
    holds an exact decimal value, not a float like 19.990000000000002.
    About 1 in 10 products is inactive (discontinued) but still exists, because
    old orders can still reference it.
    """
    if count < 1:
        raise ValueError("count must be at least 1")
    rng = random.Random(seed + 1)  # different stream from customers
    categories = sorted(_CATALOG)
    products = []
    for i in range(1, count + 1):
        category = categories[(i - 1) % len(categories)]  # spread evenly
        nouns, (low, high) = _CATALOG[category]
        brand = rng.choice(_BRANDS)
        products.append({
            "product_id": f"P{i:04d}",
            "product_name": f"{brand} {rng.choice(nouns)}",
            "category": category,
            "brand": brand,
            "unit_price": f"{rng.uniform(low, high):.2f}",
            "is_active": "false" if rng.random() < 0.1 else "true",
        })
    return products


def write_csv(rows: list[dict], columns: list[str], path: Path) -> Path:
    """Write rows to a CSV file with a header, creating parent folders."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    return path


def read_csv(path: Path) -> list[dict]:
    """Read a CSV written by `write_csv` back into a list of dicts (all strings)."""
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def main(argv: list[str] | None = None) -> None:
    config = load_config()
    parser = argparse.ArgumentParser(description="Generate customers and products CSVs.")
    parser.add_argument("--customers", type=int, default=500)
    parser.add_argument("--products", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=config.landing_path,
                        help="landing folder (default: <data_root>/landing)")
    args = parser.parse_args(argv)

    customers_file = write_csv(generate_customers(args.customers, args.seed),
                               CUSTOMER_COLUMNS, args.output / "customers" / "customers.csv")
    products_file = write_csv(generate_products(args.products, args.seed),
                              PRODUCT_COLUMNS, args.output / "products" / "products.csv")
    logger.info("Wrote %d customers to %s", args.customers, customers_file)
    logger.info("Wrote %d products to %s", args.products, products_file)


if __name__ == "__main__":
    main()
