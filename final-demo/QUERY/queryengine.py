import numpy as np
import psycopg2
from textblob import TextBlob as tb
import re
import json
import random
import string
import Datalog_Parsing as dp
import wrapper2 as wp
import wrapperfactory as wf
import pandas as pd
from sqlgenerator import SqlGenerator
from Mediator import Mediator
import os


# function append columns with sentimental polarity and review count
# calculating sentiment polarity, the values ranges from -1 to 1
def compute_sentimental_polarity(r_text):
    str1 = str(r_text).encode('ascii')
    blob = tb(str1)
    return blob.sentiment.polarity


# function append columns with sentimental polarity and review count
def append_sentip_columns(df):
    print ("Computing sentimental polarity")
    df1 = df.copy()
    df1["senti_polarity"] = [compute_sentimental_polarity(df1.loc[idx, 'reviewtext']) for idx in
                             range(len(df1))]
    df1["review_count"] = 1
    # drop the reviewtext column with raw review texts after we compute sentimental polarity
    df1 = df1.drop('reviewtext', 1)
    return df1


# function to aggregate on nodeId after computing sentimental polarity
def agg_review_data(df):
    # list of ML features related to review count/rating and sentimental polarity
    col = ['pm_avgsntp', 'p3m_avgsntp', 'p12m_avgsntp']
    #aggr_map = {'senti_polarity': ['mean'], 'review_rating': ['mean'], 'review_count': ['sum']}  # aggregator
    aggr_map = {'senti_polarity': ['mean']}  # aggregator
    dfs = df.groupby(['nodeid', 'yr', 'mn'], as_index=False).agg(aggr_map)
    dfs['nodeid'] = pd.to_numeric(dfs['nodeid'], errors='coerce')
    # drop to single column index
    dfs.columns = dfs.columns.droplevel(level=1)
    # add ml columns and initialize them to zero
    for l in col:
        dfs[l] = 0.0
    return dfs


# Function to compute pm, p3m and p12m values for review count, rating and sentimental polarity
def compute_pm_values(df):
    df2 = df.copy()
    # populate previous month columns for reviews, rating and sentimental polarity
    for i in range(1, (len(df2))):
        df2.loc[i, 'pm_avgsntp'] = df2.loc[i - 1, 'senti_polarity']

    # cmpute the averag of prev 3 months for reviews, rating and sentimental polarity
    for i in range(3, (len(df2))):
        val2 = 0
        for j in range(1, 4):
            val2 = val2 + df2.loc[i - j, 'senti_polarity']
        df2.loc[i, 'p3m_avgsntp'] = val2 / 3.0

    # cmpute the averag of prev 12 months for reviews, rating and sentimental polarity
    for i in range(12, (len(df2))):
        val2 = 0
        for j in range(1, 13):
            val2 = val2 + df2.loc[i - j, 'senti_polarity']
        df2.loc[i, 'p12m_avgsntp'] = val2 / 12.0
    return df2


# Function to compute pm, p3m and p12m values for review count, rating and sentimental polarity for a list of node-ids
def populate_pm_columns(df, cat_list):
    # build a dataframe that icludes all months/years
    print("Computing ML review features")
    rg = pd.date_range(str(df['yr'].min()), str(df['yr'].max() + 1), freq="M")
    df_base = pd.DataFrame(rg, columns=['dt'])
    df_base['yr'] = df_base['dt'].dt.year
    df_base['mn'] = df_base['dt'].dt.month
    df_base = df_base.drop('dt', 1)

    # compute the pm_avgsntp, p3m_avgsntp, p12m_sntp features for one nodeid at a time
    for idx, val in enumerate(cat_list):
        df2 = pd.merge(df_base, df[df.nodeid == val], on=['yr', 'mn'], how='left').fillna(0).astype(float)
        df3 = compute_pm_values(df2)
        # df3.loc[(df3.iloc[:, 6:9] != 0).any(1), 'nodeid'] = val
        df3['nodeid'] = val
        if idx == 0:
            # print df2.head(20)
            df_final = df3.copy()
        else:
            df_final = pd.concat([df_final, df3], ignore_index=True)

    # drop unwanted columns here
    drop_col = ['review_rating', 'review_count', 'senti_polarity']
    df_final = df_final.drop(drop_col, axis=1)
    return df_final


class QPE():
    def __init__(self):
        """Just to intialize the class,can be use for optimizations """
        self.ast = wf.wrapperFactory("asterixWrapper", env='remote')
        self.pgget = wf.wrapperFactory('postgresWrapper', env='remote')
        self.unfold = Mediator()
        self.asinfile = "asincache.pkl"
        self.asin_df = None
        self.sourcedict = None
        with open('sourcedictionary.json', 'r') as f:
            sourcedict = json.load(f)
        self.sourcedict = sourcedict
        self.datalog_parsed = None

    def dbPostgres(self):
        return self.pgget

    def dbAsterix(self):
        return self.ast

    def combine_data_sources(self, df1, col1, df2, col2):
        ''' to left-join the 2 dataframes coming from different DB sources 
        df1 DB source on the left 
        df2 DB source on the right
        col1 is joining column on left 
        col2 is joing column on right '''
        df_new = pd.merge(left=df1, right=df2, how='left', left_on=col1, right_on=col2)
        return df_new

    def translation(self, df):
        polarity_measure = []
        for i in range(df.shape[0]):
            str1 = str(df.reviewtext[i])
            blob = tb(str1)
            polarity_measure.append(blob.sentiment.polarity)
        se = pd.Series(polarity_measure)
        df['Sentiment_polarity'] = se.values
        return df

    def pgsql(self, sql1):
        df_sql = self.dbPostgres().get_dataframe(query=sql1)
        return df_sql

    def check_cache(self, fname):
        # check if file is X hours old
        if os.path.isfile(fname) == True:
            print True
        else:
            print False

    def create_cache(self, name):
        # df_asin_nodeid = pd.DataFrame()
        if name == "asinmap":
            print "creating asinmap cache"
            pdb = self.dbPostgres()
            df_asin_nodeid = pdb.get_dataframe("Ans(asin,nodeid) :- S1.products(asin,nodeid)")
            self.asin_df = df_asin_nodeid

            print self.asin_df.head()
            df_asin_nodeid.to_pickle(self.asinfile)
            print self.asin_df.columns
            return self.asin_df

        raise Exception("[" + str(name) + "] is unknown cache to create")

        # create asin_nodeid_cache
        # create asin_nodeid_month_sentmental_analysis cache

    def unfold_datalog(self,datalog):
        return self.unfold.unfold_datalog(datalog)

    def get_cache(self, name):
        # opendataframe in memory
        if name == "asinmap":
            if self.asin_df is None:
                print "asin data already in memory"
                print self.asin_df.columns
                return self.asin_df
            elif self.check_cache(self.asinfile):
                print "loading cached asin data"
                print self.asin_df.columns
                self.asin_df = pd.read_pickle(self.asinfile)
                return self.asin_df
            else:
                return self.create_cache(name)
        raise Exception("[" + str(name) + "] is unknown cache to retrieve")

    # function to figure out the order of the unfolded Query  
    def query_order(self, unfolded_query):
        pattern = 'S\d\.'
        return re.findall(pattern, unfolded_query)

    def get_dataframe(self,datalog):
        unfolded_datalog, parsed_tree = self.unfold_datalog(datalog)
        df = self.get_unfolded_dataframe(unfolded_datalog,parsed_tree)
        return df


    def get_unfolded_dataframe(self, unfolded_query, datalog_parsed=None):
        print "GOT UNFOLDED:" + unfolded_query
        self.datalog_parsed = datalog_parsed
        answer_columns = None
        if datalog_parsed:
            answer_columns = datalog_parsed['single_parts'][-1]['head.parsed']['atoms']
        # these could eventually be aliases that would require resolution
        if len(list(set(self.query_order(unfolded_query)))) > 1:
            r = dp.processDatalog(unfolded_query)
            if len(r['single_parts'][0]['conditions']) == 0:
                listofqueries = self.break_unfolded_query_without_condition(unfolded_query)
            else:
                listofqueries = self.break_unfolded_query_with_condition(unfolded_query)
            # call_queries_in_order; this also does translation
            # return listofqueries
            df_list = self.call_queries_in_order(listofqueries)
        # combiner (by common columns)
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
                    ast_df = ast_df[columns]
                return ast_df
            else:
                raise Exception("bad source of '" + Y[0] + "' expecting S1,S2")
        # maybe throw error
        return None

    def break_unfolded_query_without_condition(self, query2):
        r = dp.processDatalog(query2)
        temp_list = []
        for i in range(len(self.query_order(query2))):
            # print r['single_parts'][0]['body.parsed'][i]['atoms']
            temp = r['single_parts'][0]['head'] + ':-' + r['single_parts'][0]['body'][i]
            temp_list.append(temp)
        return temp_list

    def break_unfolded_query_with_condition(self, query2):
        r = dp.processDatalog(query2)
        temp_list = []
        for i in range(len((self.query_order(query2)))):
            # print r['single_parts'][0]['body.parsed'][i]['atoms']
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
        print "1) call_queries_in_order"
        print listq
        print "2) call_queries_in_order"
        List_df = []
        for i in listq:
            Z = list(set(self.query_order(i)))
            if Z[0] == 'S1.':
                pgget = self.dbPostgres()
                pg_df = pgget.get_dataframe(i)
                List_df.append(pg_df)
            elif Z[0] == 'S2.':
                ast = self.dbAsterix()
                ast_df = ast.get_dataframe(i)
                if polarity == True:
                    ast_df = self.translation(ast_df)
                List_df.append(ast_df)
        return List_df




    def reviews(self, cat_list):
        print ("Extracting ML review features from Asterix")
        # first get the asin's for the categories from postgres db

        df1 = self.get_cache("asinmap")
        if cat_list:
            df1 = df1[df1['nodeid'].isin(cat_list)]
        print "DATAFRAME 1"
        print df1.head()
        # get the ASIN list to pass to asterix
        asinlist = df1['asin'].astype(str).tolist()
        reviewquery = "Ans(asin,yr,mn,reviewtext) :- S2.reviews(asin,yr,mn,reviewtext)"
        if cat_list:
            asinlist = df1['asin'].astype(str).tolist()
            if asinlist:
                reviewquery = "%s, asin in (%s)" % (reviewquery, ','.join(asinlist))

        print reviewquery
        astdb = self.dbAsterix()
        df2 = astdb.get_dataframe(reviewquery)
        print astdb.get_query()

        print "DATAFRAME 2"
        print df2.head()
        df3 = pd.merge(df1, df2, on=['asin']).sort_values(['nodeid'], ascending=[True]).fillna(0)
        print "DATAFRAME 3"
        print df3.head()
        # convert the text into sentimental polarity
        df4 = append_sentip_columns(df3)

        # append the ml feature columns (prev month, prev 3months etc) and aggregate the data on nodeid
        df5 = agg_review_data(df4)
        # populate ml feature columns

        print "DATAFRAME 5"
        print df5.head()
        df6 = populate_pm_columns(df5, cat_list)
        df7 = df6[df6.nodeid != 0].sort_values(['nodeid', 'yr', 'mn'], ascending=[True, True, True]).reset_index(
            drop=True)
        print "DATAFRAME 7"
        print df7.head()
        return df7
