create or update  table "USER"."PUBLIC"."covid_epidemiology_stats"
as (
    select
        _airbyte_emitted_at,
        (current_timestamp at time zone 'utc')::timestamp as _airbyte_normalized_at,

        cast(jsonb_extract_path_text("_airbyte_data",'key') as varchar) as "key",
        cast(jsonb_extract_path_text("_airbyte_data",'date') as varchar) as "date",
        cast(jsonb_extract_path_text("_airbyte_data",'new_tested') as float) as new_tested,
        cast(jsonb_extract_path_text("_airbyte_data",'new_deceased') as float) as new_deceased,
        cast(jsonb_extract_path_text("_airbyte_data",'total_tested') as float) as total_tested,
        cast(jsonb_extract_path_text("_airbyte_data",'new_confirmed') as float) as new_confirmed,
        cast(jsonb_extract_path_text("_airbyte_data",'new_recovered') as float) as new_recovered,
        cast(jsonb_extract_path_text("_airbyte_data",'total_deceased') as float) as total_deceased,
        cast(jsonb_extract_path_text("_airbyte_data",'total_confirmed') as float) as total_confirmed,
        cast(jsonb_extract_path_text("_airbyte_data",'total_recovered') as float) as total_recovered
    from "USER".PUBLIC._AIRBYTE_RAW_COVID_EPIDEMIOLOGY
);

create or update view "USER"."PUBLIC"."covid_epidemiology_stats" as (
    with parse_json_cte as (
        select
            _airbyte_emitted_at,

            cast(jsonb_extract_path_text("_airbyte_data",'key') as varchar) as id,
            cast(jsonb_extract_path_text("_airbyte_data",'date') as varchar) as updated_at,
            cast(jsonb_extract_path_text("_airbyte_data",'new_tested') as float) as new_tested,
            cast(jsonb_extract_path_text("_airbyte_data",'new_deceased') as float) as new_deceased,
            cast(jsonb_extract_path_text("_airbyte_data",'total_tested') as float) as total_tested,
            cast(jsonb_extract_path_text("_airbyte_data",'new_confirmed') as float) as new_confirmed,
            cast(jsonb_extract_path_text("_airbyte_data",'new_recovered') as float) as new_recovered,
            cast(jsonb_extract_path_text("_airbyte_data",'total_deceased') as float) as total_deceased,
            cast(jsonb_extract_path_text("_airbyte_data",'total_confirmed') as float) as total_confirmed,
            cast(jsonb_extract_path_text("_airbyte_data",'total_recovered') as float) as total_recovered
        from "USER".PUBLIC._AIRBYTE_RAW_COVID_EPIDEMIOLOGY
    ),
    cte as (
        select
            *,
            row_number() over (
                partition by id
                order by updated_at desc
            ) as row_num
        from parse_json_cte
    )
    select
        substring(id, 1, 2) as id, -- Probably not the right way to identify the primary key in this dataset...
        updated_at,
        _airbyte_emitted_at,

        case when new_tested = 'NaN' then 0 else cast(new_tested as integer) end as new_tested,
        case when new_deceased = 'NaN' then 0 else cast(new_deceased as integer) end as new_deceased,
        case when total_tested = 'NaN' then 0 else cast(total_tested as integer) end as total_tested,
        case when new_confirmed = 'NaN' then 0 else cast(new_confirmed as integer) end as new_confirmed,
        case when new_recovered = 'NaN' then 0 else cast(new_recovered as integer) end as new_recovered,
        case when total_deceased = 'NaN' then 0 else cast(total_deceased as integer) end as total_deceased,
        case when total_confirmed = 'NaN' then 0 else cast(total_confirmed as integer) end as total_confirmed,
        case when total_recovered = 'NaN' then 0 else cast(total_recovered as integer) end as total_recovered
    from cte
    where row_num = 1
);