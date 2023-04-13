create table category(
 name varchar,
 type varchar,
 primary key (name, type)
)

create table color_desc(
 colorId varchar,
 colorNum number,
 color_description varchar,
 color_bestseller varchar,
 color_discount varchar,
 color_memberexclusive varchar,
 primary key(colorId)
)
drop table product_desc
create table product_desc (
 Id varchar,
 title varchar,
 subtitle varchar,
 PRIMARY KEY (Id)
)

create table sales_fact(
     id number autoincrement start 1 increment 1,
     productId varchar,
     colorId varchar,
     categoryId varchar,
     currency varchar,
     fullprice number,
     currentPrice number,
     sale boolean,
     PRIMARY KEY(id)

)