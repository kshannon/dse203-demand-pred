import psycopg2
from sqlalchemy import create_engine, text
import json
#import pandas as pd
from json import loads
#import pysolr
#from textblob import TextBlob as tb
import requests

host="132.249.238.27"
dbname='bookstore_dp'
user='student'
password='123456'

pg_connstring='postgresql+psycopg2://{0}:{1}@{2}:5432/{3}'.format(user, password, host, dbname)
astx_host="45.79.91.219" # not open or not running in ucsd
solr_host="45.79.91.219" # not open or not running in ucsd


def Stats(stat_func, col_pair, table_pair, key_pair):
    engine = create_engine(pg_connstring)
    conn = engine.connect()

    sql = """
    select %s(a.%s::int, b.%s::int)
    from %s a, %s b
    where a.%s = b.%s
    """ %(stat_func, col_pair[0], col_pair[1], table_pair[0], table_pair[1], key_pair[0], key_pair[1])

    stmt = text(sql)
    result = conn.execute(stmt)

    conn.close()
    return result.fetchone()[0] #str(result)

def Correlation(col_pair, table_pair, key_pair):
    stat = Stats("corr", col_pair, table_pair, key_pair)
    return json.dumps({"correlation" : stat})

def Covariance(col_pair, table_pair, key_pair):
    """Determine the covariance coefficient between two columns."""

    stat = Stats("covar_samp", col_pair, table_pair, key_pair)
    return json.dumps({"covariance" : stat})

def Histogram(table, groupby, count):

    engine = create_engine(pg_connstring)
    conn = engine.connect()

    sql = """
    SELECT %s AS Group, count(%s) AS Count
    FROM %s
    Group by %s
    Order by count(%s) DESC
    """ % (groupby, count, table, groupby, count)

    stmt = text(sql)

    results = conn.execute(stmt)

    l = []

    for result in results:
        d = {'Group': result[0], 'Count': result[1]}
        l.append(d)

    theresult_json = json.dumps(l)

    conn.close()

    return theresult_json

def getNodeIds(category_list):

    _where = ' OR '.join(['user.category.nested.nested.level_2 = "{0}"'.format(x) for x in category_list])

    sql="""
    use bookstore_dp;

    select user.nodeID
    from ClassificationInfo user
    WHERE {0}
    """.format(_where)

    # where  user.category.nested.nested.level_2 = "Education & Reference";

    ads = AsterixDataSource(host=astx_host)
    jsonobj = ads.execute(sql)

    return jsonobj

def HighestMonthlySalesByCategory(category_list, limit):

    def convertToIn(_jlist):
        m = ','.join(["'{0}'".format(str(x["nodeID"])) for x in _jlist])
        return '({0})'.format(m)

    engine = create_engine(pg_connstring)

    conn = engine.connect()

    _jlist = getNodeIds(category_list)

    # print(_jlist)

    _inStr = convertToIn(_jlist)

    #
    sql = """
       SELECT mon, sum(books_sold) AS num_sold
       FROM
         (     select EXTRACT(MONTH from o.billdate) as mon, p.nodeid as category, count(o.orderid) as books_sold
               from orderlines as o, products as p
               where o.productid = p.productid AND o.totalprice > 0::money
               group by p.productid, EXTRACT(MONTH from billdate)
               order by p.productid
         ) monthlysales
       WHERE category IN {0}
       GROUP By mon
       ORDER BY num_sold DESC
       LIMIT {1}
       """.format(_inStr, limit)
    #
    # print(sql)

    stmt = text(sql)

    results = conn.execute(stmt)

    l = []

    for result in results:
        d = {'mon': int(result[0]), 'num_sold' : int(result[1])}
        l.append(d)

    theresult_json = json.dumps(l)

    conn.close()

    return (theresult_json)

def TopCategories(num_categories, months):
    # return jsonify({ "n" : num_categories, "m" : months})

    engine = create_engine(pg_connstring)
    conn = engine.connect()


    sql = """
    SELECT category, sum(books_sold) AS num_sold
    FROM
      (     select EXTRACT(MONTH from o.billdate) as mon, p.nodeid as category, count(o.orderid) as books_sold
            from orderlines as o, products as p
            where o.productid = p.productid AND o.totalprice > 0::money
            group by p.nodeid, EXTRACT(MONTH from billdate)
            order by p.nodeid
      ) monthlysales
      WHERE mon in ({0})
    GROUP BY category
    ORDER BY num_sold DESC
    LIMIT ({1})
    """.format(months,num_categories)

    stmt = text(sql)

    results = conn.execute(stmt)

    l = []

    for result in results:
        d = {'category': result[0], 'num_sold': float(result[1])}
        l.append(d)

    theresult_json = json.dumps(l)

    conn.close()
    return (theresult_json)

def Discontinue_Stocking(threshold, startyear, endyear):
    # return jsonify({ "n" : num_categories, "m" : months})

    engine = create_engine(pg_connstring)
    conn = engine.connect()

    sql = """
    SELECT category
    FROM (
          select EXTRACT(YEAR from o.billdate) as yr,
          p.nodeid as category, sum(o.numunits) as books_sold
          from orderlines as o, products as p
          where o.productid = p.productid AND o.totalprice > 0::money
          group by p.nodeid , EXTRACT(YEAR from o.billdate)
          order by p.nodeid
          ) yearly_sales

    where books_sold < {0} AND (yr < {2} AND yr >= {1})

    """.format(threshold,startyear, endyear)

    stmt = text(sql)

    results = conn.execute(stmt)

    l = []

    for result in results:
        d = {'category': result[0]}
        l.append(d)

    theresult_json = json.dumps(l)

    conn.close()
    return (theresult_json)

def Downware_Sales(season):
    # return jsonify({ "n" : num_categories, "m" : months})
    seasons = {'spring':(3,4,5),
                'summer':(6,7,8),
                'fall':(9,10,11),
                'winter':(12,1,2)
                }
    seasontrend = seasons.get((season.lower()))

    engine = create_engine(pg_connstring)
    conn = engine.connect()

    sql = """
    SELECT s.category, round(avg(s.change_in_sales_from_last_month)) AS sale_trend
        FROM
        (
        SELECT category, mon, count(mon) OVER (PARTITION BY category) as num_months,
        books_sold - lag(books_sold,1) over (PARTITION BY category ORDER BY mon) as change_in_sales_from_last_month
        FROM(			select EXTRACT(MONTH from o.billdate) as mon, p.nodeid as category, count(o.orderid) as books_sold
                    from orderlines as o, products as p
                    where o.productid = p.productid AND o.totalprice > 0::money
                    group by p.nodeid, EXTRACT(MONTH from billdate)
                    order by p.nodeid
        )  monthly_sales
        WHERE mon in {0}
        GROUP BY category, mon, books_sold
        ) AS s
        WHERE s.num_months = {1}
        AND s.mon in {0}
        GROUP BY s.category
        having round(avg(s.change_in_sales_from_last_month)) < 0
        ORDER BY sale_trend ASC


    """.format(seasontrend,len(seasontrend))
    stmt = text(sql)

    results = conn.execute(stmt)

    l = []

    for result in results:
        d = {'category': result[0],'SaleTrend': result[1]}
        l.append(d)

    theresult_json = json.dumps(l)

    conn.close()
    # return (results)
    # print(theresult_json)
    return (theresult_json)

class AsterixDataSource():

    def __init__(self, host="localhost", port=19002):
        self.headers = {'Content-type': 'application/x-www-form-urlencoded'}
        self.host = host
        self.port = port

    def execute(self, _sql):
        connstr = 'http://{0}:{1}/query/service'.format(self.host, self.port)

        response = requests.post(connstr, data=_sql, headers=self.headers)
        jsonobj = json.loads(response.text.replace("{ ,", "{ "))

        if jsonobj['status'] == 'success':
            return jsonobj['results']
        else:
            return json.dumps("[]")


# pip install psycopg2
# pip install pysolr
# pip install textblob

if __name__ == "__main__":

    list='Education & Reference'
    category_list = list.split(",")

    print("Print Only Node Ids based on Category List: \n")

    node_ids = getNodeIds(category_list)

    print(node_ids)




