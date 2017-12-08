import pysolr
import pandas as pd
import numpy as np
import json
import requests
from datetime import datetime, date, time
from sqlalchemy import create_engine

def solrWrap(core,params):
    query_string='http://localhost:8983/solr/'+core+'/select?'
    for key in params:
        query_string=query_string+key+'='+params[key]+'&'
        print (query_string)
    solrcon = pysolr.Solr(query_string, timeout=10)
    results = solrcon.search('*:*')
    print (results)
    #docs=pd.DataFrame(results.docs)
    
    return results
    
def asterixDBWrap(dverse, query):
    statement = 'USE '+dverse+';'+query
    payload = {'statement': statement}
    a_response = requests.post('http://localhost:19002/query/service', data = payload)
    print "---------\n"
    print (a_response.status_code)
    print (a_response)
    print "---------\n"

    q = a_response.json()
    
    return pd.DataFrame(q['results'])
    
def pgdbWrap(qString):
    engine = create_engine('postgresql://postgres:password@localhost:5432/SQLBook')
    df = pd.read_sql(qString, engine)
    engine.dispose()
    
    return df