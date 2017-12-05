from abc import ABCMeta, abstractmethod
import pysolr
import pandas as pd
import numpy as np
import json
import requests
#import datalogparser as dlg
from sqlgenerator import SqlGenerator
from datetime import datetime, date, time
from sqlalchemy import create_engine

class wrapper(object):
    """Basic wrapper for a specific database
    given a datalog expression it translate into an executeable statement
    """
    __metaclass__ = ABCMeta
    
    @abstractmethod
    def get_datalog(self):
        pass
    
    @abstractmethod
    def get_query(self):
        pass
     
    @abstractmethod
    def get_dataframe(self):
        pass
    
    @abstractmethod
    def get_json(self):
        pass


class postgresWrapper(wrapper):
    
    def __init__(self, env='remote',url=None):
        self.environment = env
        self.url = url
        self.engine = None
        self.datalog = None
        self.query = None
        
        #print "postgresConstructor"
        if env == 'local':
            self.url = 'postgresql://postgres:password@localhost:5432/SQLBook' 
        elif env == 'remote':
            self.url = 'postgresql://student:123456@132.249.238.27:5432/bookstore_dp'
        else:
            if not self.url:
                raise Exception("if environment != 'local' or 'remote' must set sqlalchemy style url")
            self.url = url
        
  
    def get_query(self):
        return self.query
    
    def get_datalog(self):
        return self.datalog
    
    def set_environment(self,env=None,url=None):
        if env:
            self.environment = env
        if self.environment == 'local':
            self.url = 'postgresql://postgres:password@localhost:5432/SQLBook' 
        elif env == 'remote':
            self.url = 'postgresql://student:123456@132.249.238.27:5432/bookstore_dp'
        else:
            if not self.url:
                raise Exception("if environment != 'local' or 'remote' must set sqlachemy style url")
            self.url = url
          
    def connect(self):
        #print ("url:" + self.url)
        self.engine = create_engine(self.url)

    def disconnect(self):
        if self.engine:
            self.engine.dispose()
     
    def get_dataframe(self,datalog=None,query=None):
        #print datalog
        if datalog:
            sqlgen = SqlGenerator("postgres")
            query = sqlgen.sqlGenerator(datalog)
        if not query:
            raise Exception("datalog parameter or postgres SQL query must be provided")
        self.datalog = datalog
        self.query = query
        #self.connect()
        print "Postgres SQL: " + self.query
        self.engine = create_engine(self.url)
        df = pd.read_sql(query + " LIMIT 500", self.engine)
        #self.disconnect()
        self.engine.dispose()
        return df
    
    def get_json(self,datalog=None,query=None):
        df = self.get_dataframe(datalog,query)
        return df.to_json()

class asterixWrapper(wrapper):
    
    def __init__(self, datalog=None, env='remote',url=None,dverse='TinySocial'):
        self.environment = env
        self.url = url
        self.datalog = datalog
        self.engine = None
        self.dverse = dverse
        self.query = None
        
        #print "asterix constructor"
        #print "***" + env + "***"
        if env == 'local':
            self.url = 'http://localhost:19002/query/service'
            self.dverse = "TinySocial"
        elif env == 'remote':
            self.url = 'http://132.249.238.32:19002/query/service'
            self.dverse = "bookstore_dp"
        else:
            if not self.url:
                raise Exception("if environment != 'local' or 'remote' must set asterix style url")
            self.url = url 
            
    def get_query(self):
        return self.query
  
    def set_datalog(self,datalog):
        self.datalog = datalog
    
    def get_datalog(self):
        return self.datalog
        
    def set_dverse(self,dverse):
        self.dverse = dverse
    
    def set_environment(self, env=None):
        if env:
            self.environment = env
        if self.environment == 'local':
            self.url = 'http://localhost:19002/query/service'
            self.dverse = "TinySocial"
        elif env == 'remote':
            self.url = 'http://132.249.238.32:19002/query/service'
            self.dverse = "bookstore_dp"
     
    def get_dataframe(self,datalog=None,query=None):
        q = self.get_json(datalog=datalog,query=query)
        return pd.DataFrame(q)

    def get_json(self,datalog=None,query=None):
        #print self.dverse
        if query:
            self.query = query
        if not self.dverse:
            raise Exception("dverse must be set for asterix")
        if not self.url:
            raise Exception("api url for must be set")
        if datalog:
            sqlgen = SqlGenerator("asterix")
            self.query = sqlgen.sqlGenerator(datalog)
        print "Asterix SQL++:" + self.query
        statement = 'USE '+self.dverse+';\n'+self.query + " LIMIT 500"
        payload = {'statement': statement}
        
        #print payload
        
        a_response = requests.post(self.url, data = payload)
        
        if a_response.status_code != 200:
            raise Exception("status code "+a_response.status_code+":" + a_response.status)
        
        return a_response.json()['results']
