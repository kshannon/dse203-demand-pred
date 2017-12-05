import json
import Datalog_Parsing as dp;
import wrapper2 as wp
import wrapperfactory as wf

import json
import random
import string

import Datalog_Parsing as dp

class SqlGenerator:
    
    def __init__(self, sqltype="postgres"):
        self.sqltype = sqltype
        
        with open('sourcedictionary.json', 'r') as f:
             sourcedesc = json.load(f)
        with open('metadictionary.json', 'r') as f:
             metainfo = json.load(f)
        self.sourcedesc = sourcedesc 
        self.metainfo = metainfo

    def randomAlias(self):
         return ''.join(random.choice(string.ascii_uppercase) for _ in range(3))

    def groupByClause(self, group_by, conditions):

         #print group_by
         by = ','.join(group_by['by'])
         sql = "SELECT %s(%s) as %s,%s FROM %s" %  (
              group_by['function'],
              group_by['on'],
              group_by['alias'],
              by,
              group_by['predicate']['predicate'])
         if conditions:
              sql = "%s WHERE %s" % (sql, self.conditionClause(conditions))
         sql = "%s GROUP BY %s" % (sql,by)
         #print sql
         return sql

    def selectClause(self, predicates, conditions ):
        objects = []
        # need to match common atoms to build join; work on next
        atoms = []
        mapper = {}
        #for p in predicates:
        #    m = {sourcedesc['S1']['mv_mlview']['datalogmap'][i]:p['atoms'][i]
        #                  for i in range(len(p['atoms'])) 
        #                  if '_' not in p['atoms'][i]}
        #    mapper = dict(m.items() + mapper.items())
        withclause = []
        for p in predicates:
            alias = self.randomAlias()
            #columnalias = {p['atoms'][i]:sourcedesc['S1']['mv_mlview']['datalogmap'][i] 
            #              for i in range(len(p['atoms'])) 
            #              if '_' not in p['atoms'][i]}
            o = p
            o['alias'] = alias
            print "checking object in selectClause"
            print o
            
            object_name = p['source'] + "." + p['predicate']
            #print "OBJECT NAME:" + object_name
            #print self.sourcedesc[object_name]
            if object_name in self.sourcedesc and 'view' in self.sourcedesc[object_name]:
                withview = self.sourcedesc[object_name]['table'] + 'view'
                o['predicate'] = withview
                withclause.append("%s AS (%s)" % 
                                  (withview,
                                   self.sourcedesc[object_name]['view']) )
            objects.append(o)
            atoms += p['atoms']
            #mapper = {part['body.parsed'][0]['atoms'][i]:sourcedesc['S1']['mv_mlview']['datalogmap'][i] 
            #          for i in range(len(part['body.parsed'][0]['atoms'])) 
            #          if '_' not in part['body.parsed'][0]['atoms'][i]}
            fromlist   = [ "%s AS %s" % (o['predicate'], o['alias']) for o in objects]
            columnlist = []
            for c in o['atoms']:
                if c == '_':
                    continue
                columnlist.append( "%s.%s" % (o['alias'],c))
        '''
        select_items = list(set(list1).union(set(list2)))
        join_items = list(set(list1).intersection(set(list2)))
        '''
        fromlist   = ','.join(fromlist)
        columnlist = ','.join(columnlist)
        #print conditions
        select = ''
        conditionlist = None
        if conditions:
            conditionlist = self.conditionClause(conditions)
        #print withclause
        if withclause:
            select = select + "WITH %s" % (','.join(withclause))
            #print select
        select = select + "SELECT %s FROM %s" % (columnlist, fromlist)
        if conditionlist:
            select = select + " WHERE %s" % (conditionlist)

        return select

    def conditionClause (self, conditions ):
         condition_strings = []
         if not conditions:
            return None
         for c in conditions:
              term = None
              c = c['condition']
              if c['qualifier'] == "in":
                   term = "%s in (%s)" % (c['lhs'],c['rhs'])
              elif c['qualifier'] == "in" and self.sqltype == "asterix":
                   term = "%s in [%s]" % (c['lhs'],c['rhs'])
              else:
                   term = "%s %s %s" % (c['lhs'], c['qualifier'], c['rhs'])
              condition_strings.append(term)

         condition_clause = ' AND '.join(condition_strings)
         return condition_clause

    def sqlGenerator(self, datalog):
        sql = None
        processed = dp.processDatalog(datalog)
        num_parts = len(processed['single_parts'])
        for idx, part in enumerate(processed['single_parts']):
            #print "PART:"
            #print part

            group = part['groupby.parsed']
            head  = part['head.parsed']
            body  = part['body.parsed']
            order = part['orderby.parsed']
            topn  = part['topn.parsed']
            conds = part['conditions.parsed']
            #print conds
            if body:
                sql = self.selectClause(body,conds)
            elif group:
                sql = self.groupByClause(group,conds)
            elif topn:
                None

        return sql