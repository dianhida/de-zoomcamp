"""dlt pipeline to ingest taxi data from the Zoomcamp REST API."""

import requests

import dlt


BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"
PAGE_SIZE = 1000


@dlt.resource(name="taxi_trips")
def taxi_trips():
    """Yield pages of taxi trips from the API until an empty page is returned."""
    page = 1
    while True:
        response = requests.get(
            BASE_URL,
            params={"limit": PAGE_SIZE, "page": page},
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()

        # API returns a raw array; stop when it's empty
        if not data:
            break

        # yield the whole page (list of dicts)
        yield data

        page += 1


pipeline = dlt.pipeline(
    pipeline_name='taxi_pipeline',
    destination='duckdb',
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working pipeline.
    refresh="drop_sources",
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(taxi_trips())
    print(load_info)  # noqa: T201
