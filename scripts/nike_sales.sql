create table nike_sales(
 id number autoincrement start 1 increment 1,
 productId varchar,
 colorNum number,
 title varchar,
 subtitle varchar,
 category varchar,
 type varchar,
 currency varchar,
 fullprice number, 
 currentPrice float,
 sale BOOLEAN,
 color_ID varchar, 
 color_Description varchar,
 color_BestSeller varchar,
 color_Discount varchar,
 color_MemberExclusive varchar
)