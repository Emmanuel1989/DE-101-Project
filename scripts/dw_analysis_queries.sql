-- 1. Query the top 5 sales by product


select productid,title, sum(currentprice) as sumprice  from sales_fact fc 
    inner join product_desc pd on fc.productid = pd.id 
    group by productid, title 
    order by sumprice desc limit 5


-- 2. Query the top 5 sales by category agrupation
select ct.name, sum(currentprice) as sumprice  from sales_fact fc 
    inner join category ct on fc.categoryid = ct.name 
    group by ct.name 
    order by sumprice desc limit 5


-- 3. Query the least 5 sales by category agrupation
select ct.name, sum(currentprice) as sumprice  from sales_fact fc
    inner join category ct on fc.categoryid = ct.name 
    group by ct.name 
    order by sumprice limit 5

-- 4. Query the top 5 sales by title and subtitle agrupation
select title, subtitle , sum(currentprice) as sumprice from sales_fact fc 
    inner join product_desc pd on fc.productid = pd.id 
    group by title , subtitle 
    order by sumprice desc limit 5

-- 5. Query the top 3 products that has greatest sales by category
select fc.productid, ct.name, sum(currentprice) as sumprice  from sales_fact fc 
    inner join product_desc pd on fc.productid = pd.id 
    inner join category ct on fc.categoryid = ct.name 
    group by fc.productid, ct.name
    order by sumprice desc limit 3
