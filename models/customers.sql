with remark as (
    select 
        *,
        first_value(customer_id)
        over(partition by company_name, contact_name
            order by company_name
            rows between unbounded preceding 
            and unbounded following) as result
    from 
        {{source('sources', 'customers')}}
),
drop_duplicates as (
    select
        distinct result
    from 
        remark    
),
final as (
    select 
        *
    from
       remark c
    where 
        exists (
                select 
                    1 
                from 
                    drop_duplicates d
                where
                    c.customer_id = d.result)
)
select * from final
-- select count(*) from drop_duplicates