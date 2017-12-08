from textblob import TextBlob as tb
import re
import json
import Datalog_Parsing as dp
import wrapper2 as wp
import wrapperfactory as wf
import pandas as pd
from Mediator import Mediator
import sentiment
import math
import os
from table_logger import TableLogger


class QPE():
    def __init__(self):
        """Just to intialize the class,can be use for optimizations """
        self.logger = TableLogger(columns='data', rownum=False, time_delta=True, timestamp=False,
                                  colwidth={'data':150,'rownum':3,'time_delta':6})
        self.ast = wf.wrapperFactory("asterixWrapper", env='remote',tablelogger=self.logger)
        self.pgget = wf.wrapperFactory('postgresWrapper', env='remote',tablelogger=self.logger)
        self.unfold = Mediator()
        self.asinfile = "asincache.pkl"
        self.asin_df = None
        self.sourcedict = None
        with open('sourcedictionary.json', 'r') as f:
            sourcedict = json.load(f)
        self.sourcedict = sourcedict
        self.datalog_parsed = None
        self.logger("QPE initialized")

    def logit(self, data):
        if self.logger:
            self.logger(data)
        else:
            print(data)

    def dbPostgres(self):
        return self.pgget

    def dbAsterix(self):
        return self.ast

    #def combine_data_sources(self, df1, col1, df2, col2):
    def combine_data_sources(self, df1, df2):

        common = list(set(df1.columns).intersection(set(df2.columns)))
        self.logit("combining dataframes by:" + str(common))
        df_new = pd.merge(left=df1, right=df2, how='inner', left_on=common, right_on=common)
        self.logit("done merging")
        return df_new

    def translation(self, query):
        r = dp.processDatalog(query)
        year = None
        month = None
        nodeids = None
        df = None

        conditionlist = self.conditionParser(r['single_parts'][0]['conditions'][0])
        for c in conditionlist:
            if c['condition']['lhs'].lower() == 'yr':
                year = int(c['condition']['rhs'])
            if c['condition']['lhs'].lower() == 'mn':
                month = int(c['condition']['rhs'])
            if c['condition']['lhs'].lower() == 'nodeid':
                nodeids = [int(id) for id in c['condition']['rhs'].split(',')]
        ##print "year:" + str(year)
        ##print "month:" + str(month)
        ##print "nodes:" + str(nodeids)
        ##print nodeids
        #df = sentiment.extract_ml_features_multisource_for_month(nodeids, month, year)
        self.logit ("running sentiment analysis")
        if nodeids and month and year:
            df = sentiment.extract_ml_features_multisource_for_month(nodeids, month, year)
        elif nodeids and not year and not month:
            df = sentiment.extract_ml_features_multisource(nodeids)
        else:
            self.logit("ERROR: sentiment not called with support qualifiers")
            raise Exception("sentiment not called with support qualifiers")
        self.logit ("completed sentiment analysis")
        '''
        for i in range(df.shape[0]):
            str1 = str(df.reviewtext[i])
            blob = tb(str1)
            polarity_measure.append(blob.sentiment.polarity)
        se = pd.Series(polarity_measure)
        df['Sentiment_polarity'] = se.values
        '''
        #print "SENTIMENT"

        ##print df.columns
        return df

    def pgsql(self, sql1):
        df_sql = self.dbPostgres().get_dataframe(query=sql1)
        return df_sql

    def check_cache(self, fname):
        # check if file is X hours old
        if os.path.isfile(fname) == True:
            return True
        else:
            return False

    def create_cache(self, name):
        # df_asin_nodeid = pd.DataFrame()
        if name == "asinmap":
            ##print "creating asinmap cache"
            pdb = self.dbPostgres()
            df_asin_nodeid = pdb.get_dataframe("Ans(asin,nodeid) :- S1.products(asin,nodeid)")
            self.asin_df = df_asin_nodeid

            ##print self.asin_df.head()
            df_asin_nodeid.to_pickle(self.asinfile)
            ##print self.asin_df.columns
            return self.asin_df
        self.logit ("[" + str(name) + "] is unknown cache to create")
        raise Exception("[" + str(name) + "] is unknown cache to create")

        # create asin_nodeid_cache
        # create asin_nodeid_month_sentmental_analysis cache

    def unfold_datalog(self,datalog):
        return self.unfold.unfold_datalog(datalog)

    def get_cache(self, name):
        # opendataframe in memory

        # if old reload

        if name == "asinmap":
            if self.asin_df is not None:
                ##print "asin data already in memory"
                ##print self.asin_df.columns
                return self.asin_df
            elif self.check_cache(self.asinfile):
                ##print "loading cached asin data"
                ##print self.asin_df.columns
                self.asin_df = pd.read_pickle(self.asinfile)
                return self.asin_df
            else:
                return self.create_cache(name)
        raise Exception("[" + str(name) + "] is unknown cache to retrieve")

    # function to figure out the order of the unfolded Query  
    def query_order(self, unfolded_query):
        pattern = 'S\d\.'
        m = re.findall(pattern, unfolded_query)
        return m

    def hasTranslation(self, unfolded_query):
        pattern = 'S2.mlview'
        m = re.findall(pattern, unfolded_query)
        if m:
            return True
        return False

    def hasTranslation_ML(self, unfolded_query,pattern):
        m = re.findall(pattern, unfolded_query)
        if m:
            return True
        return False

    def get_dataframe(self,datalog):
        self.logit("Original Datalog:" + datalog)
        unfolded_datalog, parsed_tree = self.unfold_datalog(datalog)
        df = self.get_unfolded_dataframe(unfolded_datalog,parsed_tree)
        return df


    def get_unfolded_dataframe(self, unfolded_query, datalog_parsed=None):
        self.logit ("Unfolded Query Received:" + unfolded_query)
        if not datalog_parsed:
            datalog_parsed = dp.processDatalog(unfolded_query)
        self.datalog_parsed = datalog_parsed
        answer_columns = None
        if datalog_parsed:
            answer_columns = datalog_parsed['single_parts'][-1]['head.parsed']['atoms']
        # these could eventually be aliases that would require resolution
        if len(list(set(self.query_order(unfolded_query)))) > 1:
            r = dp.processDatalog(unfolded_query)
            if self.hasTranslation_ML(unfolded_query,'S2.mlview') and self.hasTranslation_ML(unfolded_query,'S1.mv_ml_features'):
                return self.translation(unfolded_query)
            elif len(r['single_parts'][0]['conditions']) == 0:
                listofqueries = self.break_unfolded_query_without_condition(unfolded_query)
            else:
                listofqueries = self.break_unfolded_query_with_condition(unfolded_query)
            df_list = self.call_queries_in_order(listofqueries)

            pairlen = int(math.floor(len(df_list) / 2.) * 2)
            combining = [[df_list[i],df_list[i + 1]] for i in range(pairlen - 1)]
            df_combined = None
            for dfpair in combining:
                if df_combined is None:
                    df_combined = self.combine_data_sources (dfpair[0],dfpair[1])
                else:
                    self.combine_data_sources(df_combined,dfpair[1])
            return df_combined
            # translation
        else:
            # no transformation or translation when single source
            Y = list(set(self.query_order(unfolded_query)))
            if Y[0] == 'S1.':
                pgget = self.dbPostgres()
                pg_df = pgget.get_dataframe(unfolded_query)
                if answer_columns:
                    pg_df = pg_df[answer_columns]
                return pg_df
            elif Y[0] == 'S2.':
                ast = self.dbAsterix()
                ast_df = ast.get_dataframe(unfolded_query)
                if answer_columns:
                    ast_df = ast_df[answer_columns]
                return ast_df
            else:
                raise Exception("bad source of '" + Y[0] + "' expecting S1,S2")
        # maybe throw error
        return None

    def break_unfolded_query_without_condition(self, query2):
        r = dp.processDatalog(query2)
        temp_list = []
        for i in range(len(self.query_order(query2))):
            # #print r['single_parts'][0]['body.parsed'][i]['atoms']
            temp = r['single_parts'][0]['head'] + ':-' + r['single_parts'][0]['body'][i]
            temp_list.append(temp)
        return temp_list

    def break_unfolded_query_with_condition(self, query2):
        r = dp.processDatalog(query2)
        temp_list = []
        for i in range(len((self.query_order(query2)))):
            ##print r['single_parts'][0]['body.parsed'][i]['atoms']
            temp = r['single_parts'][0]['head'] + ':-' + r['single_parts'][0]['body'][i]
            temp_cond = []
            for k in range(len(r['single_parts'][0]['conditions.parsed'])):
                if r['single_parts'][0]['conditions.parsed'][k]['condition']['lhs'] in \
                        r['single_parts'][0]['body.parsed'][i]['atoms']:
                    temp_cond.append(r['single_parts'][0]['conditions'][k])
            for j in temp_cond:
                temp = temp + ',' + j
            temp_list.append(temp)
        return temp_list

    def call_queries_in_order(self, listq, polarity=True):
        # under development
        # running based upon order
        # #print "call_queries_in_order"
        List_df = []
        for query in listq:
            ##print query
            ##print self.query_order(query)
            Z = list(set(self.query_order(query)))
            if Z[0] == 'S1.':
                pgget = self.dbPostgres()
                pg_df = pgget.get_dataframe(query)
                #print pg_df.head()
                List_df.append(pg_df)
            elif Z[0] == 'S2.':
                if self.hasTranslation(query):
                    ast_df = self.translation(query)
                else:
                    ast = self.dbAsterix()
                    ast_df = ast.get_dataframe(query)
                #print ast_df.head()
                List_df.append(ast_df)
        return List_df

    def conditionParser(self,condition):

        valid_qualifiers = '<|>|<=|>=|=|!=|in|is\snot'

        condlist = []
        ##print condition
        expressions = ['(\w+)(%s)(\w+)' % (valid_qualifiers), '(\w+)[%|\s](in)[%|\s]\((.*)\)']
        for pattern in expressions:
            matching = re.findall(pattern, condition)
            parsed = None
            if matching:
                ##print matching
                for m in list(matching):
                    ##print m
                    if m and len(m) == 3:
                        # created with "condition" to distinquish from AND, OR, IN, NOT IS, IS NULL... operators
                        parsed = {'condition': {'lhs': m[0], 'qualifier': m[1], 'rhs': m[2]}}
                        condlist.append(parsed)

        return condlist



'''
    def reviews(self, cat_list):
        #print ("Extracting ML review features from Asterix")
        # first get the asin's for the categories from postgres db

        #df1 = self.get_cache("asinmap")
        #print "FIRST DATAFRAME 1"
        #print df1.columns
        #print df1.dtypes
        #print df1.head()
        #print df1['nodeid'].head()
        #print "df1 nodeid is of type" + str(type(df1['nodeid']))
        #print "catlist is of type" + str(type(cat_list))
        include =  df1['nodeid'].isin(cat_list)
        df1 = df1[include]
        #print "DATAFRAME 1"
        #print df1.columns
        #print df1.dtypes
        #print df1.head()
        # get the ASIN list to pass to asterix
        asinlist = df1['asin'].astype(str).tolist()
        reviewquery = "Ans(asin,yr,mn,reviewtext) :- S2.reviews(asin,yr,mn,reviewtext)"
        if cat_list:
            asinlist = df1['asin'].astype(str).tolist()
            if asinlist:
                reviewquery = "%s, asin in (%s)" % (reviewquery, ','.join(asinlist))

        #print reviewquery
        astdb = self.dbAsterix()
        df2 = astdb.get_dataframe(reviewquery)
        #print astdb.get_query()

        #print "DATAFRAME 2"
        #print df2.dtypes
        #print df2.head()
        df3 = pd.merge(df1, df2, on=['asin']).sort_values(['nodeid'], ascending=[True]).fillna(0)
        #print "DATAFRAME 3"
        #print df3.dtypes
        #print df3.head()
        # convert the text into sentimental polarity
        df4 = append_sentip_columns(df3)
        #print "DATAFRAME 4"
        #print df4.dtypes
        #print df4.head()

        # append the ml feature columns (prev month, prev 3months etc) and aggregate the data on nodeid
        df5 = agg_review_data(df4)
        # populate ml feature columns

        #print "DATAFRAME 5"
        #print df5.dtypes
        #print df5.head()
        df6 = populate_pm_columns(df5, cat_list)
        df7 = df6[df6.nodeid != 0].sort_values(['nodeid', 'yr', 'mn'], ascending=[True, True, True]).reset_index(
            drop=True)
        #print "DATAFRAME 7"
        #print df7.dtypes
        #print df7.head()
        return df7
'''
