
{{
    config(
        materialized='table',
        post_hook=["
            grant usage on schema {{target.schema}} to group biusers;
            grant select on table {{target.schema}}.bireport to group biusers;
        "]   
    )
}}


select * from {{ref('joins')}}