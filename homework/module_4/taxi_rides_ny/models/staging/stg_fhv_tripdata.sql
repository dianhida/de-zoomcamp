with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        dispatching_base_num,
        pickup_datetime,
        dropOff_datetime as dropoff_datetime,
        PUlocationID as pulocationid,
        DOlocationID as dolocationid
    from source
    -- Filter out records with not null dispatch base num (data quality requirement)
    where dispatching_base_num is not null
)

select *
from renamed
