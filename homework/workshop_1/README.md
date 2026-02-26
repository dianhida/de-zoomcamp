# This README is created by AI coding Agent

## Overview - Taxi dlt Workshop

This workshop builds a small `dlt` pipeline that ingests **NYC taxi trip data** from a public REST API into **DuckDB**, and then answers a few analytical questions using SQL.

I implemented and debugged the pipeline with the help of an **AI coding agent (Cursor / GPT-based assistant)**, which:
- **Inspected** the API responses and pagination behavior
- **Suggested** the correct `dlt` configuration and a custom resource
- **Generated** the SQL queries used to answer the homework questions

## Project Structure

- **Pipeline code**: `taxi-pipeline/taxi_pipeline.py`
- **Destination database**: `taxi-pipeline/taxi_pipeline.duckdb`
- **dlt configuration**: `taxi-pipeline/.dlt/config.toml`, `taxi-pipeline/.dlt/secrets.toml`

The pipeline is defined as a simple Python script that:
- **Calls** the Zoomcamp taxi API with pagination (`limit` + `page`)
- **Yields** each page of results as a list of dictionaries
- **Loads** all data into a DuckDB table via `dlt.pipeline(..., destination="duckdb")`

### Running the Pipeline

From the `taxi-pipeline` folder:

```bash
python taxi_pipeline.py
```

This will:
- Extract all pages from the API until an **empty page** is returned
- Load the data into DuckDB, into schema `taxi_pipeline_dataset`
- Create the main table `taxi_trips` plus dlt metadata tables

## How the Answers Were Computed

All questions were answered by running **SQL queries against DuckDB** on the fully loaded `taxi_trips` table.

To open DuckDB from the `taxi-pipeline` directory:

```bash
duckdb taxi_pipeline.duckdb
```

### Question 1 – Dataset start and end date

> What is the start date and end date of the dataset?

I queried the **minimum and maximum pickup/dropoff timestamps**:

```sql
select
  min(trip_pickup_date_time)  as min_pickup,
  max(trip_pickup_date_time)  as max_pickup,
  min(trip_dropoff_date_time) as min_dropoff,
  max(trip_dropoff_date_time) as max_dropoff
from taxi_pipeline_dataset.taxi_trips;
```

Result (dates only):
- **Start date**: `2009-06-01`
- **End date**: `2009-07-01`

So the correct option is **2009-06-01 to 2009-07-01**.

### Question 2 – Proportion of credit card trips

> What proportion of trips are paid with credit card?

I first computed the **total number of trips** and the **number of credit-card trips**:

```sql
select
  count(*) as total_trips,
  sum(case when payment_type = 'Credit' then 1 else 0 end) as credit_trips
from taxi_pipeline_dataset.taxi_trips;
```

Result:
- `total_trips = 10000`
- `credit_trips = 2666`

Then I computed the percentage:

\[
\text{proportion} = \frac{2666}{10000} \times 100 = 26.66\%
\]

So the correct option is **26.66%**.

### Question 3 – Total tip amount

> What is the total amount of money generated in tips?

I summed the `tip_amt` column:

```sql
select
  sum(tip_amt) as total_tips
from taxi_pipeline_dataset.taxi_trips;
```

Result:

- `total_tips = 6063.41`

So the correct option is **$6,063.41**.

## Notes on AI Assistance

- **Pipeline design**: The AI agent helped identify that the Zoomcamp API returns a **raw JSON array** and that the generic `rest_api_source` helper was not appropriate, so we switched to a **custom `@dlt.resource` using `requests`** with page-number pagination.
- **Debugging**: The agent guided the switch from incorrect `offset` pagination to correct `page`-based pagination, and explained why early runs appeared to extract millions of rows without creating tables.
- **SQL queries**: The AI also suggested the DuckDB queries above to compute:
  - Dataset date range
  - Proportion of credit card payments
  - Total tips

All final results were **verified directly in DuckDB** against `taxi_pipeline.duckdb`.

