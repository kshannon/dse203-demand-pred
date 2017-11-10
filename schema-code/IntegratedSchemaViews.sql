ALTER table calendar
add column season varchar(10) NULL;

ALTER table calendar
add column holidayseason varchar(25) NULL;

select * from calendar;

update calendar 
set season = case when month in (3,4,5) then 'Spring'
                   when month in (6,7,8) then 'Summer'
                   when month in (9,10,11) then 'Fall'
                   when month in (12,1,2) then 'Winter'
              end,
     holidayseason = case when month = 12 then 'Christmas'
                     else null
                     end;
 
DROP VIEW IF EXISTS IS_SALES;
CREATE VIEW IS_SALES AS
SELECT o.orderid,
  o.orderdate,
  o.customerid,
  c.householdid,
  c.gender,
  o.city,
  o.state,
  o.zipcode,
  ol.productid,
  p.asin,
  p.nodeid,
  p.isinstock,
  p.fullprice,
  ol.unitprice,
  ol.numunits,
  ol.totalprice,
  o.campaignid,
  cp.channel campaignchannel,
  cp.discount campaigndiscount,
  cp.freeshppingflag campaignfreeshippingflag
FROM orders o
INNER JOIN orderlines ol
ON o.orderid = ol.orderid
INNER JOIN products p
ON ol.productid = p.productid
LEFT OUTER JOIN customers c
ON o.customerid = c.customerid
LEFT OUTER JOIN campaigns cp
ON o.campaignid = cp.campaignid;
  
DROP VIEW IF EXISTS IS_SALES_BY_MONTH;  
CREATE VIEW IS_SALES_BY_MONTH 
AS
SELECT s.nodeid,
  c.month,
  c.year,
  c.season,
  SUM(s.numunits) total_sales_volume,
  ROUND(AVG(CAST(s.numunits AS NUMERIC)),4) avg_sales_volume,
  SUM(CAST(s.totalprice AS NUMERIC)) total_sales_price,
  ROUND(AVG(CAST(s.totalprice AS NUMERIC)),2) avg_sales_price
FROM IS_SALES s
INNER JOIN calendar c
ON s.orderdate = c.date
INNER JOIN
  (SELECT MAX(dom) tot_days_in_month,
    MONTH,
    YEAR
  FROM calendar
  GROUP BY MONTH,
    YEAR
  ) tdm
ON tdm.month = c.month
AND tdm.year = c.year
GROUP BY s.nodeid,
  c.MONTH,
  c.YEAR,
  c.season;
  
create table demandpredictions
(demandid integer primary key,
nodeid integer,
month integer,
year integer,
season varchar(10),
predictedsales integer);