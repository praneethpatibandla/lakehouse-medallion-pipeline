from datetime import date

import pytest

from data_generator.reference_data import (
    CUSTOMER_COLUMNS,
    PRODUCT_COLUMNS,
    generate_customers,
    generate_products,
    main,
    read_csv,
    write_csv,
)


def test_customers_have_unique_ids_and_all_columns():
    customers = generate_customers(50)
    assert len(customers) == 50
    assert len({c["customer_id"] for c in customers}) == 50
    assert customers[0]["customer_id"] == "C00001"
    assert all(set(c) == set(CUSTOMER_COLUMNS) for c in customers)


def test_signup_dates_are_valid_iso_dates():
    for customer in generate_customers(20):
        date.fromisoformat(customer["signup_date"])  # raises if malformed


def test_same_seed_gives_same_data():
    assert generate_customers(10, seed=7) == generate_customers(10, seed=7)
    assert generate_products(10, seed=7) == generate_products(10, seed=7)
    assert generate_customers(10, seed=7) != generate_customers(10, seed=8)


def test_products_have_two_decimal_positive_prices():
    products = generate_products(40)
    assert products[0]["product_id"] == "P0001"
    for product in products:
        assert set(product) == set(PRODUCT_COLUMNS)
        whole, cents = product["unit_price"].split(".")
        assert len(cents) == 2 and float(product["unit_price"]) > 0
        assert product["is_active"] in {"true", "false"}


@pytest.mark.parametrize("func", [generate_customers, generate_products])
def test_count_must_be_positive(func):
    with pytest.raises(ValueError):
        func(0)


def test_csv_round_trip(tmp_path):
    rows = generate_products(5)
    path = write_csv(rows, PRODUCT_COLUMNS, tmp_path / "nested" / "products.csv")
    assert read_csv(path) == rows


def test_main_writes_both_files(tmp_path):
    main(["--customers", "3", "--products", "4", "--output", str(tmp_path)])
    assert len(read_csv(tmp_path / "customers" / "customers.csv")) == 3
    assert len(read_csv(tmp_path / "products" / "products.csv")) == 4
