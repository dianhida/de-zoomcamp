# Homework solution - Module 5

## Overview
This document contains solutions for the homework exercises in Module 5 of the Data Engineering Zoomcamp course.

## Number 1: Bruin components
.bruin.yml → config environment & connections
pipeline/pipeline.yml → pipeline definition
pipeline/assets/... → all assets (sql, python, seed)

Answer: .bruin.yml and pipeline/ with pipeline.yml and assets/

## Number 2: Interval
materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

Answer: time_interval


## Number 3: Default value parameter
Variable type is array, thus the parameter sent should be array too.
Answer: bruin run --var 'taxi_types=["yellow"]'


## Number 4: Experimental downstream
Answer: bruin run ingestion/trips.py --downstream

## Number 5: Restriction check
Answer: name: not_null

## Number 6: Dependency:
Also accessible through terminal - lineage
Answer: bruin lineage

## Number 7: Build from scratch
Answer: --full-refresh
