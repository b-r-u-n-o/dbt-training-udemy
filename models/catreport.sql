-- config 
-- onde apontamos os parâmetros para materialização
-- e etc
{{
    config(
        materialized='ephemeral'
    )
}}

select * from {{ref('joins')}}

-- {{source('sources', 'categories')}}