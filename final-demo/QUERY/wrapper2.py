from abc import ABCMeta, abstractmethod
import pysolr
import pandas as pd
import json
import requests
from sqlgenerator import SqlGenerator
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
    
    def __init__(self, env='remote',url=None,tablelogger=None):
        self.environment = env
        self.url = url
        self.engine = None
        self.datalog = None
        self.query = None
        self.logger=tablelogger
        
        #print "postgresConstructor"
        if env == 'local':
            self.url = 'postgresql://postgres:password@localhost:5432/SQLBook' 
        elif env == 'remote':
            self.url = 'postgresql://student:123456@132.249.238.27:5432/bookstore_dp'
        else:
            if not self.url:
                raise Exception("if environment != 'local' or 'remote' must set sqlalchemy style url")
            self.url = url

    def logit (self,data):
        if self.logger:
            self.logger(data)
        else:
            print(data)

  
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
            sqlgen = SqlGenerator(sqltype="postgres",tablelogger=self.logger)
            query = sqlgen.sqlGenerator(datalog)
        if not query:
            raise Exception("datalog parameter or postgres SQL query must be provided")
        self.datalog = datalog
        self.query = query
        #self.connect()
        self.logit("Postgres SQL: " + self.query)
        self.engine = create_engine(self.url)
        df = pd.read_sql(query, self.engine)
        self.logit("Finished")
        #self.disconnect()
        self.engine.dispose()
        return df
    
    def get_json(self,datalog=None,query=None):
        df = self.get_dataframe(datalog,query)
        return df.to_json()

class asterixWrapper(wrapper):
    
    def __init__(self, datalog=None, env='remote',url=None,dverse='TinySocial',tablelogger=None):
        self.environment = env
        self.url = url
        self.datalog = datalog
        self.engine = None
        self.dverse = dverse
        self.query = None
        self.logger=tablelogger
        
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

    def logit(self, data):
        if self.logger:
            self.logger(data)
        else:
            print(data)
    
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
        self.logit("finished")
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
            sqlgen = SqlGenerator(sqltype="asterix",tablelogger=self.logger)
            self.query = sqlgen.sqlGenerator(datalog)
        self.logit( "Asterix SQL++:" + self.query)
        statement = 'USE '+self.dverse+';\n'+self.query
        payload = {'statement': statement}
        
        #print payload
        
        a_response = requests.post(self.url, data = payload)
        
        if a_response.status_code != 200:
            raise Exception("status code "+str(a_response.status_code)+":" + a_response.status)
        
        return a_response.json()['results']
