-- testa se a deduplicação deu certo
select
    company_name,
    contact_name,
    count(*) qtd
from
    {{ref('customers')}}
group by
    1,2
having
    qtd > 1