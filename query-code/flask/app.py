from flask import Flask, render_template, request
import logging
import psycopg2
from datetime import datetime, date, time
from sqlalchemy import create_engine
import json
import requests
import pandas as pd
import numpy as np
import wrapper as w
import datalogparser as dlg

from logging.handlers import RotatingFileHandler
app = Flask(__name__)

environment = 'local'

@app.route("/")
def show():
    #query = request.form['query']    
    #processed_text = text.upper()
    #return processed_text
    #app.logger.info('query %s', query) 
    return render_template('queryform.html')

@app.route("/process", methods = ['POST','GET'])
def process():
    query = request.form['query']
    dataframeH = None
    sql = None
    app.logger.info('query [%s]', query) 
    print('query [%s]', query) 
    
    if query:
        pgs = w.postgresWrapper(env=environment)
        df = pgs.get_dataframe(datalog=query)
        sql = pgs.get_query()
        dataframeH = df.style.set_table_attributes('class="u-full-width"') \
         .render()
    
    return render_template('queryform.html', query=query,dataframeH=dataframeH,sql=sql)


if __name__ == '__main__':
    app.run(debug=True)
    handler = RotatingFileHandler('flask.log', maxBytes=10000, backupCount=1)
    handler.setLevel(logging.DEBUG)
    app.logger.addHandler(handler)
    app.run()
