from flask import Flask
import psycopg2
from datetime import datetime, date, time
from sqlalchemy import create_engine
import json
import requests
import pandas as pd
import numpy as np
import wrap as w

# To setup:
# - might have used pip and not conda to install
# - my python version: Python 2.7.9 :: Anaconda custom (x86_64)

# place this python file the desired newly created directory
# NOTE: adjust postgres username, password, db name appropriately

# > conda install flask
# > export FLASK_APP=dataint.py
# > flask run


app = Flask(__name__)
#@app.route('/hello')
#def hello_world():
#    return 'Hello, World!

#
# This code only account for successful use cases
#
    
@app.route('/orders')
def orders():
    # NOTE: Username, password, database
    df = w.pgdbWrap('''select * from orders limit 5''')
    return df.to_json(orient='records')

@app.route('/reviews')
def reviews():
    df = w.asterixDBWrap("TinySocial","SELECT VALUE r FROM reviews r LIMIT 5 ;")
    results = df.to_json(orient='records')
    return results

@app.route('/sampleorderlines')
def sampleorderlines():
    
    # These could be run in parallel.
    # Or, could only look up in asterix the nodeid that are part of the sampling
    
    # 1) Get sampled orderlines
    sql = '''
    SELECT o.*, p.nodeid, p.asin 
    FROM orderlines o TABLESAMPLE BERNOULLI(1) REPEATABLE(200)
    inner join products p on o.productid = p.productid
    '''

    orderlines_df = w.pgdbWrap(sql)
    # creating a temporary column to hold the integer value of nodeid for joining/merging
    orderlines_df['nodeid'] = orderlines_df['nodeid'].apply(np.int64)

    # 2) get classificatin data. this could be cached as we are using it alot
    classification_df = w.asterixDBWrap("TinySocial",
            '''SELECT r.nodeID, r.classification
            FROM ClassificationInfo r;''')
    o_df  = pd.merge(orderlines_df, 
                     classification_df[['classification','nodeID']], 
                     left_on='nodeid', right_on='nodeID')
    #o_df.drop('_nodeid')

    results = o_df.to_json(orient='records')
    return results

@app.route('/reviewer')
def reviewerid():
    d = {'q': '*:*', 'wt': 'json', 'fq': "reviewerID:\'AH2L9G3DQHHAJ\'"} 
    d_res1=solrWrap('bookstore',d)
    return d_res1

if __name__ == '__main__':
    app.run(threaded=True)
