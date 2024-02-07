select od.order_id idpedido, od.quantity quantidade, p.product_name produto,
od.unit_price preco_pedido, p.unit_price preco_tabela, p.unit_price - od.unit_price diferenca, od.discount
from order_details od, products p where od.unit_price < p.unit_price
and od.product_id = p.product_id order by 6 desc

select e.first_name + ' ' + e.last_name nome, sum(od.unit_price * od.quantity - od.discount) total
from order_details od
inner join orders o on (o.order_id = od.order_id)
inner join employees e on (e.employee_id = o.employee_id)
where DATE_PART(year, o.order_date) = 2022
group by nome
order by total desc

select p.product_name, p.unit_price from products p order by p.unit_price desc limit 10

with for2020 as (
    select sup.supplier_id id, sup.company_name fornecedor, sum(od.unit_price * od.quantity) total_2020 from order_details od
    inner join products p on (p.product_id = od.product_id)
    inner join suppliers sup on (sup.supplier_id = p.supplier_id)
    inner join orders o on (o.order_id = od.order_id)
    where DATE_PART(year, o.order_date) = 2020
    group by id, fornecedor
), for2021 as (
    select sup.supplier_id id, sup.company_name fornecedor, sum(od.unit_price * od.quantity) total_2021 from order_details od
    inner join products p on (p.product_id = od.product_id)
    inner join suppliers sup on (sup.supplier_id = p.supplier_id)
    inner join orders o on (o.order_id = od.order_id)
    where DATE_PART(year, o.order_date) = 2021
    group by id, fornecedor
), ambos as (
    select for2021.id, for2020.fornecedor, total_2021 - total_2020 resultado from for2020
    inner join for2021 on (for2021.id = for2020.id)
    order by resultado desc
)
select * from ambos

with result as (
  select  c.category_name categoria, DATE_PART(year, o.order_date) ano, 
  sum(od.unit_price * od.quantity - od.discount) total ,
  row_number() over (partition by ano order by ano,total desc) as res 
  from categories c
  inner join products p on (c.category_id = p.category_id)
  inner join order_details od on (p.product_id = od.product_id)
  inner join orders o on (o.order_id = od.order_id)
  group by categoria, ano
),
filtro as (
  select * from result where res <=5
)
  select categoria, ano, total from filtro