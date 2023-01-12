with prod as (
    select 
        ct.category_name,
        sp.company_name suppliers,
        pr.product_name,
        pr.unit_price,
        pr.product_id
    from 
        {{source('sources', 'products')}} pr
    left join
        {{source('sources', 'suppliers')}} sp 
        using(supplier_id)
    left join 
        {{source('sources', 'categories')}} ct
        using(category_id)
),
orderdetail as (
    select
        pd.*,
        od.order_id,
        od.quantity,
        od.discount
    from 
        {{ref('orderdetails')}} od
    left join
        prod pd
        using(product_id)
),
orders as (
    select
        od.order_date,
        od.order_id,
        cs.company_name customer,
        ep.fullname employee,
        ep.age,
        ep.lenghtofservice
    from 
        {{source('sources', 'orders')}} od
    left join
        {{ref('customers')}} cs using(customer_id)
    left join
        {{ref('employees')}} ep using(employee_id)
    left join 
        {{source('sources', 'shippers')}} sh on od.ship_via = sh.shipper_id
),
final as (
    select
        od.*,
        ord.customer,
        ord.employee,
        ord.age,
        ord.lenghtofservice
    from 
        orderdetail od
    left join
        orders ord using(order_id) 
)

select * from final