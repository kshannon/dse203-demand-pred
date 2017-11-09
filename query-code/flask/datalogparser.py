import pandas as pd
import psycopg2 as pg
import types
import re
import numpy as np

# Given a datalog query of form - "Ans(numunits, firstname, billdate), orders.orderid > 1000, orders.numunits > 1"
# this method extracts the column names from it
def getColumns(datalog):
    return [col.strip() for col in datalog[(datalog.index("(")+1): datalog.index(")")].split(",")]

# Given a datalog query of form - "Ans(numunits, firstname, billdate), orders.orderid > 1000, orders.numunits > 1"
# this method extracts the where conditions from it
def getWhereConditions(datalog):
    rawString = datalog.split("),")[1]
    conds = [cond.strip() for cond in rawString.split(",")]
    return conds

# Given a result as a list, this method converts it to the expected format of
# ['Ans(A,B,C)', 'Ans(a,b,c)']
def prettyPrintResult(res):
    result_list = []
    for r in res:
        current = "Ans("
        for ele in r:
            current += str(ele) + ","
        current = current[:-1] 
        current += ")"
        result_list.append(current)
    return result_list

def getWhereConditionForSelectQuery(conds):
    print conds
    if not conds or len(conds) == 0:
        return
    where_condition = "" + conds[0]
    if len(conds) == 1:
        return where_condition
    islimit = re.compile(r'\LIMIT ', flags=re.IGNORECASE)
    for cond in conds[1:]:
        if islimit.match(cond):
            where_condition += " " + cond
        else:
            where_condition += " AND " + cond + " "
    return where_condition

def getFullyQualifiedColumnNames(q):
    col_names = {}
    for val in q:
        table_name = val[:val.index("(")]
        columns = getColumns(val)
        for col in columns:
            if col in col_names:
                table = col_names.get(col)
                table += ";"+table_name
                col_names[col] = table
            else:
                col_names[col] = table_name
    return col_names

def getQueryColsFromFQCols(column_names, cols_to_return):
    return_str = ""
    for col in cols_to_return:
        if col in column_names:
            table_name = column_names[col]
            if table_name.find(";") == -1:
                return_str += table_name + "." + col + ", "
        else:
            raise Exception('Oh. Something bad happened!')
    return return_str[:-2]

def getTableNames(q):
    return [i[:i.index("(")].strip() for i in q]

def getQueryTableNames(table_names):
    return_str = ""
    for table_name in table_names:
        return_str += table_name + ", "
    return return_str[:-2]

def getJoinConditions(column_names):
    return_str = ""
    for col in column_names:
        if column_names[col].find(";") != -1:
            tables = column_names[col].split(";")
            base_str = tables[0].strip() + "." + col + "="
            for table in tables[1:]:
                return_str += (base_str + table.strip() + "." + col + " AND ")
    return return_str[:-5]

def buildSelectQuery(q):
    datalog = q[0]                               #"Ans(numunits, firstname, billdate), orderid > x, numunits > 1"
    cols_to_return = getColumns(datalog)         #numunits, firstname, billdate
    q = q[1:]                                    #everything except datalog
    
    conds   = [i for i in q if isinstance(i, list)]
    # conds includes the following:
    # [['orders.orderid > 1000', 'orders.numunits > 1']]
    
    selects = [i for i in q if not isinstance(i, list)]
    # selects includes the following:
    # 'orders(numunits, customerid, orderid)',
    #'customers(firstname, customerid)',
    # 'orderlines(billdate, orderid)'
    
    column_names = getFullyQualifiedColumnNames(selects) # Dictionary of {numunits:orders, customerid:orders;customers} etc.
    table_names = getTableNames(selects) # [orders, customers, orderlines]
    
    print ("Join Conditions:" + getJoinConditions(column_names))
    print ("Where Conditions:" + getWhereConditionForSelectQuery(conds[0]))
    
    return "SELECT " + getQueryColsFromFQCols(column_names, cols_to_return) + \
           " FROM " + getQueryTableNames(table_names) + \
           " WHERE " + getJoinConditions(column_names) + \
           " AND " + getWhereConditionForSelectQuery(conds[0]) + ";"

def getDatalogResult(q):
    #if connection.closed:
    #    connection = pg.connect("dbname=SQLBook user=postgres password=password")
    cur = connection.cursor()
    select_query = buildSelectQuery(q)
    print (select_query)
    cur.execute(select_query)
    res = cur.fetchall()
    return prettyPrintResult(res)

# Datalog queries
def parseDatalog(datalog):
    
    q = datalog.split(':-')
    head = q[0].strip()
    rightside = q[1]
    parsed = {'Ans':head}
    pat = re.compile(r'\w*\(.*\)')
    m = pat.findall(rightside)
    parsed['predicates']=m
    for j in m:
        rightside = rightside.replace(j,'')

    conditions = [s.strip() for s in rightside.split(',') if len(s.strip()) > 0 ]
    print (len(conditions))
    if len(conditions) > 0:
        parsed['conditions']=conditions
    return parsed

def buildQuery(datalog):
    parsed= parseDatalog(datalog)
    q = [parsed['Ans']]
    q.extend(parsed['predicates'])
    q.append(parsed['conditions'])
    return buildSelectQuery(q)