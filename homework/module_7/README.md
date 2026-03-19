## Module 7 Streaming project

## Homework – Questions and answers

In this homework we use what we learned about Kafka in practice.

**Data source (Yellow 2025-11):**

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet
```

---

### Question 1: Redpandas

- Redpandas, docker, pyflink workshop, and run `rpk version` from the docker container.
- **What's the output?**

**Answer:** Rpk version: `v25.3.9`
---

### Question 2: Producer time spent

Honestly, after many trials, I only found **5 seconds** duration for the producer duration. The nearest value is 10 seconds, So I'll choose that option.
**Answer:** **10 seconds** 

(Run result: 5 seconds; 10 seconds is the closest option.)

---

### Question 3: Count records with trips_distance > 5

```sql
select count(*) from processed_events where trip_distance > 5;
```

**Answer:** **8,506**
---

### Question 4: Longest streak trip

Refers to: **q4_job.py**
```sql CREATE TABLE
postgres=# SELECT PULocationID, num_trips
FROM processed_events_aggregated
ORDER BY num_trips DESC
LIMIT 3;
```
```bash
 pulocationid | num_trips 
--------------+-----------
           74 |        15
           74 |        14
           74 |        13
(3 rows)
```
**Answer:** **74**

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