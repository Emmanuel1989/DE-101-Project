/**
    Store proc to insert from staging table into fact and dimnesion nike sales tables
*/
CREATE OR REPLACE PROCEDURE insert_fact_dim_nike_sales()
returns varchar 
LANGUAGE SQL
AS
BEGIN
    /**
    **** SQL Insert Statements
    **/
    insert into category (name, type)  select distinct category , type from nike_sales;

    insert into color_desc( colorId ,
        colorNum ,
        color_description ,
        color_bestseller,
        color_discount ,
        color_memberexclusive
    ) select distinct color_ID, colornum, color_description, color_bestseller, color_discount, color_memberexclusive from nike_sales;

    insert into product_desc (Id, title, subtitle ) select distinct productid, title, subtitle from nike_sales;

    insert into sales_fact (productid, colorId, categoryId, currency, fullprice, currentPrice, sale)
        select productid, color_id, category, currency, fullprice, currentprice, sale from nike_sales;
    return 'success';    
END;

