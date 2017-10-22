from flask import Flask
import psycopg2
from datetime import datetime, date, time
from sqlalchemy import create_engine
import json
import requests

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
    engine = create_engine('postgresql://postgres:password@localhost:5433/SQLBook')
    conn = engine.connect()
    result = conn.execute('''with t as (
        select * 
        from orders
        limit 5 )
        select to_json(array_agg(t)) from t''')
    
    r = result.fetchone()
    conn.close()
    return json.dumps(r[0])

@app.route('/reviews')
def reviews():
    statement = '''USE TinySocial;
                SELECT VALUE r
                FROM reviews r
                LIMIT 5 ;'''
    payload = {
                'statement': statement
            }

    a_response = requests.post('http://localhost:19002/query/service', data = payload)
    print a_response.status_code
    q = a_response.json()
    return json.dumps(q) ;

if __name__ == '__main__':
    app.run(threaded=True)
