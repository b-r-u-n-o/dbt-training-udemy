with features as (
    select 
        date_part_year(current_date) - date_part_year(birth_date) age,
        date_part_year(current_date) - date_part_year(hire_date) lenghtofservice,  
        (first_name || ' ' || last_name) fullname,
        *
    from
        {{source('sources','employees')}}
)
select * from features