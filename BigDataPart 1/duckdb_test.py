import duckdb

duckdb.sql("create table customers as select * from 'data/customers.csv'")
duckdb.sql("create table products as select * from 'data/products.csv'")
duckdb.sql("create table orders as select * from 'data/orders.csv'")

#print(duckdb.sql("select order_id, amount from orders order by amount desc limit 5").fetchall())
print(duckdb.sql("select customer_id, sum(amount) as total_spent from orders group by customer_id order by total_spent desc limit 5").fetchall())

'''
select o.order_id,
    c.first_name,
    p.name,
    o.amount,
    o.order_date
from orders o
join customers c on o.customer_id = c.customer_id
join products p on o.product_id = p.product_id
order by order_date desc;
'''
