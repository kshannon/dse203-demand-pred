from __future__ import division, unicode_literals
import pysolr
import pandas as pd
import math
from textblob import TextBlob as tb

import json
import requests
from datetime import datetime, date, time
from sqlalchemy import create_engine

#super categories
# each super category is a list of node-ids
'''
super_cat1 = [2950,3179,3188,3214,3263,3939,171114,465288,132561011,377550011,6133990011,6343223011,7976022011,8622832011,10806600011]

super_cat2 = [ 2672,2946,3122,3131,3167,3185,3200,3215,3262,3279,3610,3612,3616,3981,3987,4134,4676,5322,13682,688868,720028,882340,13998731,15375251,16244041,16244061,713347011,3564978011,3564986011,7009088011,8883852011,8883853011,8883961011,8951155011,8951160011,8951165011,8951173011,8951174011,8951191011 ]

super_cat3 = [1002,1007,2963,3094,3291,3527,4263,4278,282838,8883838011,8951153011,9432890011,9432902011 ]

super_cat4 = [2945, 3220, 4296, 282840, 8883864011, 8883963011, 8944264011, 9432900011]
'''



# In[3]:


#################################
# wrapper functions
#################################

def solrWrap(core,params):
    query_string='http://localhost:8983/solr/'+core+'/select?'
    for key in params:
        query_string=query_string+key+'='+params[key]+'&'
        #print (query_string)
    solrcon = pysolr.Solr(query_string, timeout=10)
    results = solrcon.search('*:*')
    docs=pd.DataFrame(results.docs)
    
    return docs
    
def asterixDBWrapper(dverse, query):
    statement = 'USE '+dverse+';'+query
    payload = {'statement': statement}
    a_response = requests.post('http://132.249.238.32:19002/query/service', data = payload)
    #print "---------\n"
    #print (a_response.status_code)
    #print (a_response)
    #print "---------\n"
    q = a_response.json()
    #print q
    
    return pd.DataFrame(q['results'])
    

def postgresWrapper(qr):
    url = 'postgresql://student:123456@132.249.238.27:5432/bookstore_dp'
    #def connect(self):
    #print ("url:" + url)
    engine = create_engine(url)    
#def get_dataframe(self,datalog=None,query=None):

    df = pd.read_sql(qr, engine)
    engine.dispose()
    return df


# In[4]:


#function to extract the list of views from pgdb
def get_views_from_pgdb():
    #print ("extracting view names from pgdb")
    #q = "SELECT * FROM mv_mlview WHERE nodeid in ( %s )"%(st)
    q = "SELECT * FROM pg_views WHERE schemaname NOT IN('information_schema', 'pg_catalog')"

    df = postgresWrapper(q)
    return df


# In[5]:


#function to extract the list of views from pgdb
def get_mviews_from_pgdb():
    #print ("extracting view names from pgdb")
    #q = "SELECT * FROM mv_mlview WHERE nodeid in ( %s )"%(st)
    q = "SELECT * FROM pg_matviews;"

    df = postgresWrapper(q)
    return df


# In[6]:


#function to extract the list of views from pgdb
def get_view_definition_from_pgdb(vw):
    #print ("extracting %s definition from pgdb"%(vw))
    q = "SELECT * FROM %s LIMIT 1"%(vw)

    df = postgresWrapper(q)
    return df


# In[7]:



#################################
# Function to extract reviews from Asterix DB for a given list of ASINs
#################################
def extract_reviews_from_asterix(asin_list):
    #print("Extracting reviews from Asterix DB")
    #convert the list of ASINs to string
    st = str(asin_list).strip('[]')
    dverse ='bookstore_dp'
    #query ="SELECT VALUE r FROM reviews r WHERE asin in [%s];"%st
    query = """SELECT  r.asin as asin, get_month(datetime_from_unix_time_in_secs(bigint(r.unixReviewTime)))  as mn, 
      get_year(datetime_from_unix_time_in_secs(bigint(r.unixReviewTime))) as yr, r.reviewText as reviewtext,
      float(r.overall) as review_rating 
       FROM  reviews r 
       WHERE asin in [%s]; """%(st)
    statement = 'USE '+dverse+';'+query
    #print(statement)
    d2 = asterixDBWrapper(dverse, query)
    return d2


# In[ ]:





# In[8]:



#function to extract sales features from ML view in postgres
def extract_ml_features_from_pgdb(category_list):
    #print ("extracting one ml features from pgdb")
    st = str(category_list).strip('[u]')
    #q = "SELECT * FROM mv_mlview WHERE nodeid in ( %s )"%(st)
    q = "SELECT * FROM mv_ml_features WHERE nodeid in ( %s )"%(st)

    df = postgresWrapper(q)
    return df
    


# In[9]:



#function to extract nodeids and asin values from postgres
def extract_asin_pgdb(category_list):
    #print ("extracting ASINs from pgdb")
    #convert list of integers to list of strings as nodeid is string in pgdb
    cat = [str(i) for i in category_list]
    st = str(cat).strip('[]')
    #build query to extract node-id and asin
    q = """SELECT CAST(p.nodeid as FLOAT), p.asin
           FROM products p
           WHERE p.nodeid in ( %s ) 
           ORDER BY p.nodeid """%(st)
    df = postgresWrapper(q)
    return df


# In[10]:


# calculating sentiment polarity, the values ranges from -1 to 1
def compute_sentimental_polarity(r_text):
    str1 = str(r_text).encode('ascii')
    blob=tb(str1)
    return blob.sentiment.polarity


# In[11]:



#function append columns with sentimental polarity and review count
def append_sentip_columns(df):
    #print ("Computing sentimental polarity")
    df1= df.copy()
    df1["senti_polarity"] = [compute_sentimental_polarity(df1.loc[idx, 'reviewtext']) for idx in range(len(df1))]
    df1["review_count"] = 1
    #drop the reviewtext column with raw review texts after we compute sentimental polarity
    df1 = df1.drop('reviewtext', 1)
    return df1


# In[12]:


#function to aggregate on nodeId after computing sentimental polarity 
def agg_review_data(df):
    # list of ML features related to review count/rating and sentimental polarity
    col = ['pm_avgsntp', 'p3m_avgsntp', 'p12m_avgsntp' ]
    aggr_map = {'senti_polarity':['mean'], 'review_rating':['mean'], 'review_count':['sum']}    #aggregator
    dfs=df.groupby(['nodeid', 'yr', 'mn'],as_index=False).agg(aggr_map)
    dfs['nodeid'] = pd.to_numeric(dfs['nodeid'], errors='coerce')
    #drop to single column index   
    dfs.columns = dfs.columns.droplevel(level=1)
    #add ml columns and initialize them to zero
    for l in col:
        dfs[l] = 0.0
    return dfs


# In[13]:


#Function to compute pm, p3m and p12m values for review count, rating and sentimental polarity
def compute_pm_values(df):
    df2 = df.copy()
    #populate previous month columns for reviews, rating and sentimental polarity
    for i in range(1, (len(df2))):
        df2.loc[i, 'pm_avgsntp'] = df2.loc[i-1, 'senti_polarity']

    #cmpute the averag of prev 3 months for reviews, rating and sentimental polarity
    for i in range(3, (len(df2))):
        val2=0
        for j in range(1,4):
            val2 = val2 + df2.loc[i-j, 'senti_polarity']
        df2.loc[i, 'p3m_avgsntp'] = val2/3.0

    #cmpute the averag of prev 12 months for reviews, rating and sentimental polarity
    for i in range(12, (len(df2))):
        val2=0
        for j in range(1,13):
            val2 = val2 + df2.loc[i-j, 'senti_polarity']
        df2.loc[i, 'p12m_avgsntp'] = val2/12.0
    return df2


# In[14]:



#Function to compute pm, p3m and p12m values for review count, rating and sentimental polarity for a list of node-ids
def populate_pm_columns(df, cat_list):
    #build a dataframe that icludes all months/years
    #print("Computing ML review features")
    rg = pd.date_range(str(df['yr'].min()), str(df['yr'].max()+1), freq="M")
    df_base = pd.DataFrame(rg, columns=['dt'])
    df_base['yr'] = df_base['dt'].dt.year
    df_base['mn'] = df_base['dt'].dt.month
    df_base = df_base.drop('dt', 1)
    
    #compute the pm_avgsntp, p3m_avgsntp, p12m_sntp features for one nodeid at a time
    for idx, val in enumerate(cat_list):    
        df2 = pd.merge(df_base, df[df.nodeid == val], on=['yr', 'mn'], how='left').fillna(0).astype(float)
        df3 = compute_pm_values(df2)
        #df3.loc[(df3.iloc[:, 6:9] != 0).any(1), 'nodeid'] = val
        df3['nodeid'] = val
        if idx == 0:
            #print df2.head(20)
            df_final= df3.copy()
        else:
            df_final = pd.concat([df_final, df3], ignore_index=True)
#         print idx, val
#         print df2.shape
#         print df3.shape
#         print df_final.shape
    #drop unwanted columns here
    drop_col = ['review_rating', 'review_count', 'senti_polarity']
    df_final = df_final.drop(drop_col, axis=1)
    return df_final
   


# In[15]:



#function to extract review text and rating info from postgres
def extract_reviews_from_pgdb(category_list):
    #print("Extracting reviews from pgdb")
    #convert list of integers to list of strings as nodeid is string in pgdb
    cat = [str(i) for i in category_list]
    st = str(cat).strip('[]')
    #get review text for sentimental analysis
    q = """SELECT p.nodeid, p.asin, c.month as mn, c.year as yr, r.reviewtext, r.overall as review_rating 
           FROM products p, calendar c, reviews r 
           WHERE r.reviewtime = c.date AND p.asin = r.asin AND p.nodeid in ( %s 
           ) ORDER BY p.nodeid,  c.year, c.month """%(st)
    df = postgresWrapper(q)
    return df


# In[16]:



#Function to extract ML review features from postgres db
def extract_ml_review_features_from_pgdb(cat_list):
    #print ("Extracting ML review features from pgdb")
    #extract the reviews from postgres db
    df1 = extract_reviews_from_pgdb(cat_list)
    #convert the text into sentimental polarity
    df2 = append_sentip_columns(df1)
    
    #append the ml feature columns (prev month, prev 3months etc) and aggregate the data on nodeid
    df3 = agg_review_data(df2)
    #populate ml feature columns
    df4 = populate_pm_columns(df3, cat_list)
    df5 = df4[df4.nodeid != 0].sort_values(['nodeid', 'yr', 'mn'], ascending=[True, True, True]).reset_index(drop=True)
    return df5
    


# In[17]:



#Function to extract ML review features from Asterix db
def extract_ml_review_features_from_asterix(cat_list):
    #print ("Extracting ML review features from Asterix")
    #first get the asin's for the categories from postgres db
    df1= extract_asin_pgdb(cat_list)
    #print df1.head()
    #get the ASIN list to pass to asterix
    asinlist = df1['asin'].astype(str).tolist()
    df2 = extract_reviews_from_asterix(asinlist)
    df3 = pd.merge(df1, df2, on= ['asin']).sort_values(['nodeid'], ascending=[True]).fillna(0)
    #convert the text into sentimental polarity
    df4 = append_sentip_columns(df3)
    
    #append the ml feature columns (prev month, prev 3months etc) and aggregate the data on nodeid
    df5 = agg_review_data(df4)
    #populate ml feature columns
    df6 = populate_pm_columns(df5, cat_list)
    df7 = df6[df6.nodeid != 0].sort_values(['nodeid', 'yr', 'mn'], ascending=[True, True, True]).reset_index(drop=True)
    return df7 


# In[18]:



#Function to extract ML features from postgres and review ml features from asterix
def extract_ml_features_multisource(cat_list):
    #print ("Extracting ML features from Multisource")

    #extract the ml feature list from pdgb
    pgd1= extract_ml_features_from_pgdb(cat_list)
    
    #extract the ml feature list from asterix
    asd1 = extract_ml_review_features_from_asterix(cat_list)
    
    #merge the data
    df_sc = pd.merge(pgd1, asd1, on= ['yr', 'mn','nodeid'], how='left').sort_values(['nodeid', 'yr', 'mn'], ascending=[True, True, True]).fillna(0)
    return df_sc


# In[19]:



#Function to extract ML features and review features from single source(pgdb)
def extract_ml_features_singlesource(cat_list):
    #print ("Extracting ML features from Single source")

    #extract the ml feature list from pdgb
    pgd1= extract_ml_features_from_pgdb(cat_list)
    
    #extract the ml feature list from asterix
    pgd2 = extract_ml_review_features_from_pgdb(cat_list)
    
    #merge the data
    df_sc = pd.merge(pgd1, pgd2, on= ['yr', 'mn','nodeid'], how='left').sort_values(['nodeid', 'yr', 'mn'], ascending=[True, True, True]).fillna(0)
    return df_sc


# In[20]:


#Function to extract ML features for given month and year and a list of node-ids for prediction task
def extract_ml_features_multisource_for_month(category_list, month, year):
    sdf = extract_ml_features_multisource(category_list)
    sdf2 = sdf[(sdf.yr == year) & (sdf.mn == month)]
    return sdf2


# In[30]:


#%%time
#cat = [4134]
#year = 2014
#month = 7
#sdf = extract_ml_features_multisource_for_month(cat, month, year)
#sdf


# In[ ]:





# In[22]:


# # %%time
# # Extract features for training task (use single source for both sales and reviews)
# df1= extract_ml_features_singlesource(super_cat1)
# print df1.shape
# df1.head()


# In[23]:


# %%time
# sdf1= extract_ml_features_multisource(super_cat1)
# # sdf2= extract_ml_features_multisource(super_cat2)
# # sdf3= extract_ml_features_multisource(super_cat3)
# # sdf4= extract_ml_features_multisource(super_cat4)

# # print sdf1.shape, sdf2.shape, sdf3.shape, sdf4.shape
# sdf1.head()


# In[24]:


# %%time
# sdf1= extract_ml_features_multisource(super_cat1)
# sdf2= extract_ml_features_multisource(super_cat2)
# sdf3= extract_ml_features_multisource(super_cat3)
# sdf4= extract_ml_features_multisource(super_cat4)

# print sdf1.shape, sdf2.shape, sdf3.shape, sdf4.shape