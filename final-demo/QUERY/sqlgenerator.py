import json
import random
import string
from itertools import permutations

import Datalog_Parsing as dp

class SqlGenerator:
    
    def __init__(self, sqltype="postgres",tablelogger=None):

        self.sqltype = sqltype
        ##print "init sqltype:" + self.sqltype
        
        with open('sourcedictionary.json', 'r') as f:
             sourcedesc = json.load(f)
        with open('metadictionary.json', 'r') as f:
             metainfo = json.load(f)
        self.sourcedesc = sourcedesc 
        self.metainfo = metainfo
        self.logger = None
        self.logger = tablelogger

    def logit(self,data):
        if self.logger:
            self.logger(data)
        else:
            print(data)

    def randomAlias(self):
         return ''.join(random.choice(string.ascii_uppercase) for _ in range(3))

    def groupByClause(self, group_by, conditions):

         ###print group_by
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
         ###print sql
         return sql

    def selectClause(self, predicates, conditions ):
        objects = []
        # need to match common atoms to build join; work on next
        atoms = []
        withclause = []
        for p in predicates:
            alias = self.randomAlias()
            o = p
            o['alias'] = alias
            o['alias.atoms'] = ["%s.%s" % (alias, a) if a != '_' else '_' for a in o['atoms']]
            
            object_name = p['source'] + "." + p['predicate']
            if object_name in self.sourcedesc and 'view' in self.sourcedesc[object_name]:
                withview = self.sourcedesc[object_name]['table'] + 'view'
                o['predicate'] = withview
                withclause.append("%s AS (%s)" % 
                                  (withview,
                                   self.sourcedesc[object_name]['view']) )
            objects.append(o)
            atoms += [a for a in o['atoms'] if a not in '_']
            fromlist   = [ "%s AS %s" % (o['predicate'], o['alias']) for o in objects]
            columnlist = [a for a in o['alias.atoms'] if a not in '_' ]

        # Buiding natural join

        commonattributes = set()
        for (i, j) in permutations(range(len(predicates)), 2):
            commonattributes = commonattributes.union(set.intersection(set(predicates[i]['atoms']), set(predicates[j]['atoms'])))

        naturaljoins = []
        join = []
        # seems like i should be able to do both at once
        pred_atom_list  = [p['atoms'] for p in predicates]
        pred_alias_list = [p['alias.atoms'] for p in predicates]

        for j in commonattributes:
            w = pred_atom_list
            for w_index in range(len(w)):
                wi = w[w_index]
                if j in wi:
                    i = wi.index(j)
                    join.append(pred_alias_list[w_index][i])
                    #print "index:" + str(i)
                    #print "got:" + str(wi[i])
                    #print len(join)
                if len(join) == 2:
                    #print "found pair:" + str(join)
                    naturaljoins.append(join)
                    #print "reseting to find next join pair"
                    join = []



        # should these be added as condition; probably not
        #print naturaljoins
        naturaljoinstructure = []
        for nj in naturaljoins:
            naturaljoinstructure.append({'condition': {'lhs':nj[0] ,'qualifier':'=','rhs':nj[1]}})
        if naturaljoinstructure:
            self.logit("NATURAL JOINS ADDED:" + self.conditionClause(naturaljoinstructure))
        # End building natural join

        fromlist   = ','.join(fromlist)
        columnlist = ','.join(columnlist)
        ###print conditions
        select = ''
        conditionlist = []

        if naturaljoinstructure:
            conditions = conditions + naturaljoinstructure
        if conditions:
            conditionlist = self.conditionClause(conditions)
        ###print withclause
        if withclause:
            select = select + "WITH %s" % (','.join(withclause))
            ###print select
        select = select + "SELECT %s FROM %s" % (columnlist, fromlist)
        if conditionlist:
            select = select + " WHERE %s" % (conditionlist)

        return select

    def conditionClause (self, conditions ):
        condition_strings = []
        ##print "TYPE:" + self.sqltype
        if not conditions:
            return None
        for c in conditions:
            ##print "CONDITION CLAUSE"
            ##print c
            term = None
            c = c['condition']
            if c['qualifier'] == "in" and self.sqltype != "asterix" :
               term = "%s in (%s)" % (c['lhs'],c['rhs'])
            elif c['qualifier'] == "in" and self.sqltype == "asterix":
                ##print "ASTERIX IN CLAUSE"
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
            ###print "PART:"
            ###print part

            group = part['groupby.parsed']
            head  = part['head.parsed']
            body  = part['body.parsed']
            order = part['orderby.parsed']
            topn  = part['topn.parsed']
            conds = part['conditions.parsed']
            ###print conds
            if body:
                sql = self.selectClause(body,conds)
            elif group:
                sql = self.groupByClause(group,conds)
            elif topn:
                None

        return sql