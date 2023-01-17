
select 
    *
from
    {{source('sources', 'shippers')}} sh
left join
    {{ref('shippers_emails')}} se using (shipper_id)