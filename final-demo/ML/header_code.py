# General Imports
import sys
sys.path.append('./python') # Add the facets overview python code to the python path
import numpy as np
import pandas as pd
import csv
import matplotlib.pyplot as plt
import seaborn

#from __future__ import print_function

# EDA Imports
from EDA_source_API import getNodeIds
from ipywidgets import interact, interactive, fixed, interact_manual
import ipywidgets as widgets
from IPython.display import display

# ML_pred Imports
from model_predictions import findSuperCats, predict_model

HAVE_FACETS=False
if HAVE_FACETS:
    # Facets Imports
    # getting obvious and expected import error for facets from me (kyle)
    from generic_feature_statistics_generator import GenericFeatureStatisticsGenerator
    import base64
    from IPython.core.display import display, HTML # facets visuals
    
# Dropdown function for EDA
# call: display(stakeholder_category) in cell.
user_catg_picker = widgets.Dropdown(
    options=['Education & Reference', 'Geography & Cultures', 'Programming', "Women's Health", 'Growing Up & Facts of Life'],
    description='Category:',
    disabled=False
)

# Pull supercategory mappings into a dict
def get_supercat_mappings():
    data = pd.read_csv("supercategories.csv", header=0)
    nodeid_csv = data['NodeID'].tolist()
    supercat_csv = data['Supercategory'].tolist()
    return dict(zip(nodeid_csv,supercat_csv)) # return a dic of 75 top nodeIDs and supercat num

# Plot predictions Error
def error_plotter(predictions):
    plt.figure(figsize=(12,8));
    predictions['error'].hist(bins=60);
    plt.xticks(fontsize=12);
    plt.yticks(fontsize=12);
    plt.xlabel('Actual books sold minus predicted number of books sold', fontsize=14);
    plt.ylabel('Count', fontsize=14);
    plt.title('Error in prediction', fontsize=32);
    plt.annotate('Median: {:.0f} books off on the prediction.'.format(predictions['error'].abs().round().median()), 
                 xy=(0.3, 0.75), xycoords='axes fraction', fontsize=18, color='red');
    plt.annotate(r'Predicted fewer books than needed', 
                 xy=(0.3, 0.5), xycoords='axes fraction', fontsize=18, color='blue');
    return plt.draw()