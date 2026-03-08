## Module 6 PySpark project

This folder uses the shared environment at `zoomcamp_cohort/6-batch/.venv` via the local symlink `venv`.

### Run

From the repo root:

```bash
PYTHONPATH=zoomcamp_cohort/homework/module_6/src \
  zoomcamp_cohort/homework/module_6/venv/bin/python \
  zoomcamp_cohort/homework/module_6/run_metrics.py
```

### Inputs / outputs

- **input**: `yellow_tripdata_2025-11.parquet` (in this folder)
- **output**: `output/yellow_2025_11_p4/` (created by the script)


## Homework – Questions and answers

In this homework we use what we learned about Spark in practice.

**Data source (Yellow 2025-11):**

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
```

To install PySpark, follow the [official guide](https://spark.apache.org/docs/latest/api/python/getting_started/install.html).

---

### Question 1: Install Spark and PySpark

- Install Spark, run PySpark, create a local Spark session, then run `spark.version`.
- **What's the output?**

**Answer:** `3.5.1`

(From `print_spark_version(spark)` in this project: Spark version 3.5.1.)

---

### Question 2: Yellow November 2025

- Read the November 2025 Yellow data into a Spark DataFrame.
- Repartition the DataFrame to 4 partitions and save it as Parquet.
- **What is the average size of the Parquet files (`.parquet` extension) that were created (in MB)?** Choose the closest option.

  - 6 MB  
  - **25 MB**  
  - 75 MB  
  - 100 MB  

**Answer:** **25 MB**

(Run result: average size ≈ 24.4 MB; 25 MB is the closest option.)

---

### Question 3: Count records

- **How many taxi trips were there on the 15th of November?**  
  Consider only trips that **started** on the 15th of November.

  - 62,610  
  - 102,340  
  - **162,604**  
  - 225,768  

**Answer:** **162,604**

(From `count_records_on_date(df, pickup_date="2025-11-15")`.)

---

### Question 4: Longest trip

- **What is the length of the longest trip in the dataset, in hours?**

  - 22.7  
  - 58.2  
  - **90.6**  
  - 134.5  

**Answer:** **90.6**

(From `longest_trip_hours(df)` ≈ 90.65 hours.)

---

### Question 5: User Interface

- **On which local port does Spark’s UI (application dashboard) run?**

  - 80  
  - 443  
  - **4040**  
  - 8080  

**Answer:** **4040**

(Default port for the Spark application UI.)

---

### Question 6: Least frequent pickup location zone

- Load the zone lookup data (e.g. into a temp view in Spark):

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

- Using the zone lookup and the Yellow November 2025 data, **what is the name of the LEAST frequent pickup location Zone?**

  - Governor's Island/Ellis Island/Liberty Island  
  - **Arden Heights**  
  - Rikers Island  
  - Jamaica Bay  

*(If multiple answers are correct, select any.)*

**Answer:** **Arden Heights**

(From `least_frequent_pickup_zone_name(yellow_df, zones_df)` in this project.)