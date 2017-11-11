CREATE VIEW monthly_sales AS
select EXTRACT(MONTH from o.billdate) as mon, p.nodeid as category, sum(o.numunits) as books_sold
from orderlines as o, products as p 
where o.productid = p.productid AND o.totalprice > 0::money
group by p.nodeid, EXTRACT(MONTH from billdate)
order by p.nodeid

-- Q1
SELECT category, sum(books_sold) AS num_sold FROM monthly_sales 
WHERE mon = 11 or mon = 12 
GROUP BY category 
ORDER BY num_sold DESC 
LIMIT 3
 
-- Q2
-- Based on to Q3 ML model with a specific set of NodeId's e.g. (matchin Education) 

-- Q3
-- The ML model will predict on Demand
-- The features needed for this Questions are : 

-- date_agg_month, inventory_sold_ratio, dollar_sold_ratio, volume_moved, product_rating_average, 
-- product_rating_delta, total_sales, contains_sold_out_product
-- ,large_inventory_drop,is_pos_sentiment
-- ,is_neg_sentiment,is_neutral_sentiment,count_of_nodeIDs,is_in_campaign 

-- Q4
-- Spring 
SELECT s.category, round(avg(s.change_in_sales_from_last_month)) AS sale_trend
FROM
(
SELECT category, mon, count(mon) OVER (PARTITION BY category) as num_months, 
books_sold - lag(books_sold,1) over (PARTITION BY category ORDER BY mon) as change_in_sales_from_last_month
FROM monthly_sales
WHERE mon >= 2 AND mon <= 5
GROUP BY category, mon, books_sold
) AS s
WHERE s.num_months = (5-2) AND s.mon > 2 AND s.mon <= 5
GROUP BY s.category
ORDER BY sale_trend ASC

-- Winter 

SELECT s.category, round(avg(s.change_in_sales_from_last_month)) AS sale_trend
FROM
(
SELECT category, mon, count(mon) OVER (PARTITION BY category) as num_months, 
books_sold - lag(books_sold,1) over (PARTITION BY category ORDER BY mon) as change_in_sales_from_last_month
FROM monthly_sales
WHERE mon >= 8 AND mon <= 12
GROUP BY category, mon, books_sold
) AS s
WHERE s.num_months = (12-8) AND s.mon > 8 AND s.mon <= 12
GROUP BY s.category
ORDER BY sale_trend ASC

-- q5
CREATE VIEW yearly_sales AS
select EXTRACT(YEAR from o.billdate) as yr,
  p.nodeid as category, sum(o.numunits) as books_sold
from orderlines as o, products as p 
where o.productid = p.productid AND o.totalprice > 0::money
group by p.nodeid , EXTRACT(YEAR from o.billdate)
order by p.nodeid 

-- Threshold number of sold books=4 (for example)
SELECT category FROM yearly_sales 
where books_sold < 4 AND (yr < 2017 AND yr > 2012)

