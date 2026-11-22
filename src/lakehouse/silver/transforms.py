"""Cleaning and standardisation transforms for the silver layer.

Every function here takes a DataFrame (or column name) and returns a new one,
with no reads, writes or global state. That makes each step easy to unit test
with a tiny hand-made DataFrame, and lets the silver jobs (steps 09-11) chain
them with `DataFrame.transform`:

    clean = bronze_orders.transform(clean_orders)

Casting never fails the job. Values that cannot be converted become NULL, and
the row gets a label in `_dq_issues` saying what was wrong, so bad records can
be counted, filtered or reported instead of crashing the pipeline or quietly
disappearing.

`try_cast` / `try_to_timestamp` are used instead of `cast` / `to_timestamp`
because with ANSI mode on (the default on Databricks serverless) the plain
versions raise an error on the first bad value instead of returning NULL.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

# Target types per source. Columns not listed stay strings. Money is DECIMAL,
# never DOUBLE: floats cannot store most cents exactly.
TARGET_TYPES: dict[str, dict[str, str]] = {
    "customers": {"signup_date": "date"},
    "products": {"unit_price": "decimal(10,2)", "is_active": "boolean"},
    "orders": {"order_ts": "timestamp", "updated_at": "timestamp"},
    "order_items": {"line_number": "int", "quantity": "int",
                    "unit_price": "decimal(10,2)", "discount_pct": "decimal(5,2)"},
}

# Timestamp layouts seen in the source, tried in order. The first is ISO-8601
# (what the source is supposed to send); the second is the day-first format
# that leaks in from one upstream system. Spark datetime patterns, not Python's.
TIMESTAMP_FORMATS = ("yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ss", "dd/MM/yyyy HH:mm")

VALID_ORDER_STATUSES = ("placed", "shipped", "delivered", "cancelled", "returned")

DQ_COLUMN = "_dq_issues"


# --------------------------------------------------------------------------
# Column-level helpers
# --------------------------------------------------------------------------

def clean_string(name: str) -> "Column":
    """Trim whitespace and turn empty strings into NULL.

    "" and "   " mean "no value" in CSV exports; treating them as NULL lets
    one `isNull()` check catch every kind of missing value.
    """
    from pyspark.sql import functions as F

    trimmed = F.trim(F.col(name))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def parse_timestamp(name: str, formats: tuple[str, ...] = TIMESTAMP_FORMATS) -> "Column":
    """Parse a string column with the first format that works, else NULL."""
    from pyspark.sql import functions as F

    value = clean_string(name)
    return F.coalesce(*[F.try_to_timestamp(value, F.lit(fmt)) for fmt in formats])


def safe_cast(name: str, data_type: str) -> "Column":
    """Cast a string column to `data_type`, returning NULL instead of failing."""
    from pyspark.sql import functions as F

    if data_type == "timestamp":
        return parse_timestamp(name)
    if data_type == "date":
        return F.to_date(parse_timestamp(name, ("yyyy-MM-dd",) + TIMESTAMP_FORMATS))
    # try_cast has no Python Column method in Spark 3.5, so use the SQL form.
    # The column is cleaned first so " 12 " still casts to 12.
    return F.expr(f"try_cast(nullif(trim(`{name}`), '') AS {data_type})")


# --------------------------------------------------------------------------
# DataFrame-level transforms
# --------------------------------------------------------------------------

def cast_columns(df: "DataFrame", types: dict[str, str]) -> "DataFrame":
    """Cast the given columns; every other string column is just trimmed."""
    from pyspark.sql.types import StringType

    out = df
    for field in df.schema.fields:
        if field.name in types:
            out = out.withColumn(field.name, safe_cast(field.name, types[field.name]))
        elif isinstance(field.dataType, StringType) and not field.name.startswith("_"):
            # Pipeline metadata (underscore columns) is left exactly as is.
            out = out.withColumn(field.name, clean_string(field.name))
    return out


def standardize_case(df: "DataFrame", lower: tuple[str, ...] = (),
                     upper: tuple[str, ...] = ()) -> "DataFrame":
    """Lower- or upper-case code columns, so "Shipped" and "shipped" match."""
    from pyspark.sql import functions as F

    for name in lower:
        df = df.withColumn(name, F.lower(F.col(name)))
    for name in upper:
        df = df.withColumn(name, F.upper(F.col(name)))
    return df


def add_dq_issues(df: "DataFrame", rules: dict[str, "Column"]) -> "DataFrame":
    """Add `_dq_issues`: the names of every rule whose condition is true.

    An empty array means the row passed. Flagging instead of filtering keeps
    the decision ("drop it, fix it, or keep it") in the job that uses the
    data, and makes the issues countable: explode `_dq_issues` and group.
    """
    from pyspark.sql import functions as F

    flags = [F.when(condition, F.lit(name)) for name, condition in rules.items()]
    # A rule that evaluates to NULL (e.g. `quantity <= 0` when quantity is
    # NULL) counts as a pass; missing values get their own explicit rules.
    return df.withColumn(DQ_COLUMN, F.filter(F.array(*flags), lambda x: x.isNotNull()))


def _failed_cast(name: str) -> "Column":
    """True when the raw value was present but the cast produced NULL."""
    from pyspark.sql import functions as F

    return clean_string(name).isNotNull() & F.col(f"_typed_{name}").isNull()


def _cast_with_failure_rules(df: "DataFrame", types: dict[str, str]) -> "DataFrame":
    """Cast columns and build one `invalid_<column>` rule per cast column.

    The typed value is computed into a temporary column first, so the rule
    can compare it with the original string before the original is replaced.
    """
    from pyspark.sql import functions as F

    typed = df
    for name, data_type in types.items():
        typed = typed.withColumn(f"_typed_{name}", safe_cast(name, data_type))
    rules = {f"invalid_{name}": _failed_cast(name) for name in types}
    typed = add_dq_issues(typed, rules)
    for name in types:
        typed = typed.withColumn(name, F.col(f"_typed_{name}")).drop(f"_typed_{name}")
    # Trim the remaining string columns (the cast ones are no longer strings).
    return cast_columns(typed, {})


def append_dq_issues(df: "DataFrame", rules: dict[str, "Column"]) -> "DataFrame":
    """Add more rule names to an existing `_dq_issues` column.

    Used by the cleaners below and by silver jobs that add checks needing
    other tables (e.g. "unknown product" in order items, step 11).
    """
    from pyspark.sql import functions as F

    df = add_dq_issues(df.withColumnRenamed(DQ_COLUMN, "_dq_previous"), rules)
    return (df.withColumn(DQ_COLUMN, F.concat(F.col("_dq_previous"), F.col(DQ_COLUMN)))
              .drop("_dq_previous"))


# --------------------------------------------------------------------------
# One cleaning function per source
# --------------------------------------------------------------------------

def clean_orders(df: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    out = _cast_with_failure_rules(df, TARGET_TYPES["orders"])
    out = standardize_case(out, lower=("status", "payment_method"),
                           upper=("customer_id", "currency"))
    return append_dq_issues(out, {
        "missing_customer_id": F.col("customer_id").isNull(),
        # coalesce: a NULL status is unknown too, not a silent pass.
        "unknown_status": F.coalesce(~F.col("status").isin(*VALID_ORDER_STATUSES), F.lit(True)),
    })


def clean_order_items(df: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    out = _cast_with_failure_rules(df, TARGET_TYPES["order_items"])
    out = standardize_case(out, upper=("product_id",))
    return append_dq_issues(out, {
        "non_positive_quantity": F.col("quantity") <= 0,
        "discount_out_of_range": (F.col("discount_pct") < 0) | (F.col("discount_pct") > 100),
    })


def clean_customers(df: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    out = _cast_with_failure_rules(df, TARGET_TYPES["customers"])
    out = (out.withColumn("first_name", F.initcap("first_name"))
              .withColumn("last_name", F.initcap("last_name")))
    out = standardize_case(out, lower=("segment",), upper=("customer_id", "state", "country"))
    return append_dq_issues(out, {"missing_customer_id": F.col("customer_id").isNull()})


def clean_products(df: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    out = _cast_with_failure_rules(df, TARGET_TYPES["products"])
    out = standardize_case(out, lower=("category",), upper=("product_id",))
    return append_dq_issues(out, {"non_positive_price": F.col("unit_price") <= 0})


CLEANERS = {
    "customers": clean_customers,
    "products": clean_products,
    "orders": clean_orders,
    "order_items": clean_order_items,
}
