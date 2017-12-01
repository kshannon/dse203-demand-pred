# coding: utf-8

class Mediator(object):
    '''Mediator to allow access to data sources via mediated schema definition'''
    
    def __init__(self, mapping_dict):
        self.mapping_dict = mapping_dict
        
    def queryMediator(self, datalog):
        '''function to return dataframe corresponding to datalog query over mediated schema'''
        return None