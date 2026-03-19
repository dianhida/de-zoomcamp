CREATE TABLE processed_events (
    PULocationID INTEGER,
    DOLocationID INTEGER,
    trip_distance DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    passenger_count DOUBLE PRECISION,
    pickup_datetime VARCHAR,
    dropoff_datetime VARCHAR
);


CREATE TABLE session_events_aggregated ( 
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    PULocationID INT,
    num_trips BIGINT,
    PRIMARY KEY (window_start, window_end, PULocationID)
);

DROP TABLE IF EXISTS hourly_tips;

CREATE TABLE hourly_tips (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    total_tip DOUBLE PRECISION,
    PRIMARY KEY (window_start, window_end)
);