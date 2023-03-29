select
--from raw_orders
orderid,
orderdate,
shipdate,
shipmode,
o.customerid,
o.productid,
ordersellingprice,
ordercostprice,
--from raw_customer
customername,
segment,
country,
--from raw_product
category,
productname,
subcategory,
ordersellingprice - ordercostprice as orderprofit,
{{ markup('ordersellingprice', 'ordercostprice' ) }} as markup
from {{ ref('raw_orders') }} as o
left join {{ ref('raw_customers') }} as c
on o.customerid = c.customerid
left join {{ ref('raw_product') }} as p
on o.productid = p.productid
