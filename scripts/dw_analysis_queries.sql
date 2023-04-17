-- 1. Query the top 5 sales by product


with s1 as
(select productid,title, sum(currentprice) as sumprice  from sales_fact fc inner join product_desc pd on fc.productid = pd.id group by productid, title)
select productid, title, max(sumprice) as topsale from s1 group by productid, title order by max(sumprice) desc limit 5



-- 2. Query the top 5 sales by category agrupation
with s1 as (
select ct.name, sum(currentprice) as sumprice  from sales_fact fc inner join category ct on fc.categoryid = ct.name group by ct.name 
) select name , max(sumprice) as topsale from s1 group by name order by max(sumprice) desc limit 5


-- 3. Query the least 5 sales by category agrupation
with s1 as (
select ct.name, sum(currentprice) as sumprice  from sales_fact fc inner join category ct on fc.categoryid = ct.name group by ct.name 
) select name , max(sumprice) as topsale from s1 group by name order by max(sumprice) limit 5 

-- 4. Query the top 5 sales by title and subtitle agrupation
with s1 as (
select title, subtitle , sum(currentprice) as sumprice from sales_fact fc inner join product_desc pd on fc.productid = pd.id group by title , subtitle
)
select title, subtitle, max(sumprice) from s1 group by title , subtitle  order by max(sumprice) desc limit 5

-- 5. Query the top 3 products that has greatest sales by category
with s1 as
(select fc.productid, ct.name, sum(currentprice) as sumprice  from sales_fact fc 
    inner join product_desc pd on fc.productid = pd.id 
    inner join category ct on fc.categoryid = ct.name 
    group by fc.productid, ct.name
)
select productid, name, max(sumprice) as topsale from s1 group by productid, name order by max(sumprice) desc limit 3