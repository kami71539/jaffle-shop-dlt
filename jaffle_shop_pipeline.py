
import os
import time

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator


# ============================================================
# PERFORMANCE OPTIMIZATION
# ============================================================

# 1. Worker tuning
os.environ["EXTRACT__WORKERS"] = "3"
os.environ["NORMALIZE__WORKERS"] = "2"
os.environ["LOAD__WORKERS"] = "5"

# 2. Buffer control
os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = "10000"

# 3. File rotation
os.environ["NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS"] = "5000"


# ============================================================
# API CLIENT
# ============================================================

client = RESTClient(
    base_url="https://jaffle-shop.scalevector.ai/api/v1"
)


# ============================================================
# 4. CHUNKING
# ============================================================

def get_pages(endpoint):
    """
    Fetch the Jaffle Shop API page-by-page.

    Each API page is yielded as one chunk.
    The API does not return a total page count, so pagination
    stops when an empty page is returned.
    """

    for page in client.paginate(
        endpoint,
        paginator=PageNumberPaginator(
            base_page=1,
            page_param="page",
            total_path=None,
            stop_after_empty_page=True,
        ),
    ):
        if page:
            yield page


# ============================================================
# 5. PARALLEL RESOURCES
# ============================================================

@dlt.resource(
    table_name="customers",
    write_disposition="replace",
    primary_key="id",
    parallelized=True,
)
def customers():
    yield from get_pages("/customers")


@dlt.resource(
    table_name="orders",
    write_disposition="replace",
    primary_key="id",
    parallelized=True,
)
def orders():
    yield from get_pages("/orders")


@dlt.resource(
    table_name="products",
    write_disposition="replace",
    primary_key="sku",
    parallelized=True,
)
def products():
    yield from get_pages("/products")


# ============================================================
# SOURCE
# ============================================================

@dlt.source
def jaffle_shop():
    return (
        customers,
        orders,
        products,
    )


# ============================================================
# DUCKDB PIPELINE
# ============================================================

pipeline = dlt.pipeline(
    pipeline_name="jaffle_shop_final",
    destination="duckdb",
    dataset_name="jaffle_data",
)


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    print("=" * 65)
    print("JAFFLE SHOP - OPTIMIZED dlt PIPELINE")
    print("=" * 65)

    print("\nPerformance configuration:")
    print("  Chunking:           API pages")
    print("  Parallel resources: enabled")
    print("  Extract workers:    3")
    print("  Normalize workers:  2")
    print("  Load workers:       5")
    print("  Buffer size:        10,000 items")
    print("  File rotation:      5,000 items")

    print("\nRunning pipeline...")

    start = time.perf_counter()

    load_info = pipeline.run(jaffle_shop())

    elapsed = time.perf_counter() - start

    print("\n" + "=" * 65)
    print("PIPELINE COMPLETE")
    print("=" * 65)

    print(f"\nExecution time: {elapsed:.2f} seconds")

    print("\nLoad information:")
    print(load_info)

    # ========================================================
    # VERIFY CORRECTNESS
    # ========================================================

    print("\n" + "=" * 65)
    print("DATA VERIFICATION")
    print("=" * 65)

    with pipeline.sql_client() as sql_client:

        for table in ["customers", "orders", "products"]:

            result = sql_client.execute_sql(
                f'SELECT COUNT(*) FROM "{table}"'
            )

            print(f"{table}: {result[0][0]:,} rows")

    print("\nSUCCESS - Pipeline completed successfully!")
