
# coding: utf-8

# # Model predictions
# 
# This will use the regression models to predict the number of books sold in a given month for a given nodeID/nodeIDs.

import numpy as np
import pandas as pd
import pickle

# ## Supercategory code
# 
# EDA team will provide a way to find nodeIDs based on a free text search of the categories. So, for example, if we wanted to search for all categories that match "computer science", then the API would come back with a list of nodeIDs that match that description at any level of category.


# Here are the supercategories for the top 75 nodeIDs as determined by EDA
# http://localhost:8888/notebooks/Documents/DSE203/EDA%20Supercategories.ipynb
df_supercat = pd.read_csv('supercategories.csv')
df_supercat = df_supercat.drop(df_supercat.columns[0], axis=1)
df_supercat['Supercategory'] += 1  # Add 1 so that supercategories are indexed from 1 not 0.


def findSuperCats(nodeIDs):
    
    return df_supercat[df_supercat['NodeID'].isin(nodeIDs)]
   

def predict_model(nodeId_lst):
    

    clf = {}
    clf[1] = pickle.load(open('pickle_model1.pkl', 'rb'))
    clf[2] = pickle.load(open('pickle_model2.pkl', 'rb'))
    clf[3] = pickle.load(open('pickle_model3.pkl', 'rb'))
    clf[4] = pickle.load(open('pickle_model4.pkl', 'rb'))

    # ## Load the data for these nodeIDs
    # 
    # We can use the API to get the data we need.
    query = "Ans(nodeId, yr, mn, sales, " \
            " vol, pm_sale, pm_vol, " \
            " p3m_sale, p3m_vol, " \
            " p12m_sale, p12m_vol, " \
            " pm_numreviews, pm_avgrating" \
            " p3m_numreviews, p3m_avgrating, " \
            " p12m_numreviews, p12m_avgrating)" \
            " :- mlfeatures( nodeId, yr, mn, " \
            " pm_sales, vol, " \
            " lm_sale, lm_vol, " \
            " l3m_sale, l3m_vol, " \
            " l12m_sale, l12m_vol, " \
            " lm_avg_rating, lm_avg_sent), nodeId in {}".format(tuple(nodeId_lst))

    #df = Mediator_query(query)

    #if df == Null : print "error"

    df_predictions = pd.DataFrame()

    supercat_array = findSuperCats(nodeId_lst)

    for sc in [1,2,3,4]:

        ## This should be changed to use the API and grab the data
        df = pd.read_csv('ML_feat_cat{}.csv'.format(sc))
        df = df.drop(df.columns[0], axis=1)
        # Sort by date and time
        df = df.sort_values(['yr', 'mon'])

        df = df[df['nodeid'].isin(nodeId_lst)]

        df_out = df.iloc[:, 0:4]

        features = df.iloc[:, 5:]
        features['month'] = df.iloc[:,1]

        predictions = df.iloc[:,3]

        y_log_tr = predictions.apply(lambda el: 0 if el == 0 else np.log10(el))

        features_np = features.as_matrix()
        predictions_np = y_log_tr.as_matrix()

        X_test = features
        y_test = y_log_tr

        df_out['predicted books sold'] = 10**clf[sc].predict(X_test)  # Make the predict from the model

        df_out = df_out[['nodeid', 'mon', 'yr', 'total_sales_volume', 'predicted books sold']]
        df_predictions = pd.concat([df_predictions, df_out])

    df_predictions = df_predictions.rename(columns={'mon':'month'}) 
    df_predictions['error'] = df_predictions['total_sales_volume'] - df_predictions['predicted books sold']
    df_predictions = df_predictions.sort_values(['nodeid', 'yr', 'month'])
    
    return df_predictions


