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

### Question 5: Max Trips 

```sql
postgres=# SELECT PULocationID, num_trips     
FROM session_events_aggregated
ORDER BY num_trips DESC
LIMIT 5;
```
```bash
 pulocationid | num_trips 
--------------+-----------
           74 |        82
           74 |        76
           74 |        72
           74 |        72
           74 |        72
(5 rows)
```

**Answer:** **81**

(Closest answer as I found always 82.)

---

### Question 6: Largest tip
```sql
SELECT window_start, total_tip
FROM hourly_tips
ORDER BY total_tip DESC
LIMIT 1;
```
```bash
    window_start     |     total_tip     
---------------------+-------------------
 2025-10-16 18:00:00 | 524.9599999999999
(1 row)
```

**Answer:** **2025-10-16 18:00:00**
