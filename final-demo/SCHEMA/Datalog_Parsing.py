# coding: utf-8

# # Datalog Parsing Notebook

import pandas as pd
import psycopg2 as pg
import types
import re

# Package versions:
# - python 2.7
# - pandas 20.2
# - psycopg2 2.7.3.1 (dt dec pq3 ext lo64)
# - re 2.2.1

# Notebook includes functions to perform following operations:
# - split datalog queries with multiple heads into list of individual datalog statements with single head each
# - extract datalog head, body, group_by, order_by, top_n, and conditions for datalog statement with single head
# - extract individual parts from single group_by, order_by, and top_n string
# - rebuild datalog strings from individual datalog parts
# 
# Notes:
# 1. The above categories are distinct, i.e. group by, order_by, and top_n are not considered to be part of the datalog body or conditions.
# 2. Datalog assumed to adhere to standard syntax defined in the following sources:
#     - http://logic.stanford.edu/reports/LG-2012-01.pdf
#     - https://pages.iai.uni-bonn.de/manthey_rainer/IIS_1718/manualDES4.1.pdf
# 3. Parser assumes multi-step datalog strings are separated by a period
# 4. Special treatment of conditions of form "attribute in (a,b,c,d)" had to be taken in cleanDatalog, getBody, and getConds functions - "attribute in (a,b,c,d)" was converted to "attribute%in%(a,b,c,d)"

# variables
groupby_regex = 'group_?by\(.*?\){2}' # assumes 2 ending close parens
orderby_regex = 'order_?by\(.*?\]\)'
topn_regex = 'top\(.*?\){2}'
#additional_heads_regex = '[\.]+(.*?):-'
additional_heads_regex = '\.([^\.]*?):-'
valid_qualifiers = '<|>|<=|>=|=|!=|is|is\snot'
##operator  = ',|;'

# Parsing Functions

def cleanDatalog(datalog):
    '''removes whitespace, leading and trailing commas, replaces multiple consecutive 
    commas with single comma, replaces single quotes with double quotes, and removes
    final period'''
    
    # replace "attribute in (a,b,c,d)" with "attribute%in%(a,b,c,d)"
    datalog = re.sub(' in ','%in%',datalog)
    
    datalog = ''.join(datalog.split())
    datalog = datalog.rstrip(',')
    datalog = datalog.lstrip(',')
    datalog = datalog.rstrip('.')
    datalog = re.sub(',+',',', datalog)
    datalog = re.sub('\'','"', datalog)
    datalog = datalog#.lower()
    
    return datalog

def getRegex(s):
    '''converts string with parens to regex compatible string'''
    s = re.sub('\(','\(', s)
    s = re.sub('\)','\)', s)
    return s

def getHeads(datalog):
    '''function to extract individual heads of datalog queries, used for splitting in getDatalogSteps function'''
    d = cleanDatalog(datalog)
    
    # get initial head
    heads = [d.split(':-')[0]]
    
    # get additional heads
    additional_heads = re.findall(additional_heads_regex, d)

    # extend heads list to include additional heads
    heads.extend(additional_heads)

    return heads

def getDatalogSteps(datalog):
    '''creates a list of individual datalog queries if multiple heads'''
    d = cleanDatalog(datalog)
    datalog_list = []
    heads = getHeads(d)
    
    for idx,h in enumerate(heads):
        # re-append ':-' so as not to split order_by and group_by
        h = h+':-'
        
        # split on head and extract corresponding body
        b = re.split(getRegex(h), d)[1]
        
        # split on next head to eliminate next datalog query from result
        if idx < len(heads)-1:  # there is a next head
            next_h = heads[idx+1]
            b = re.split(getRegex(next_h+':-'), b)[0]
            
        # clean up commas and append concatenated head and body to datalog list
        h_b = cleanDatalog(h+b)
        datalog_list.append(h_b)

    return datalog_list

def getHead(datalog):
    '''returns datalog head of single query'''
    d = cleanDatalog(datalog)
    return ''.join(re.split(':-', d)[0].split())

def getBody(datalog):
    '''function to get body of single datalog query; excludes groupbys, orderbys, topn
    (NOTE: does not work if multiple heads - need to call getDatalogSteps first)'''
    d = cleanDatalog(datalog)
    b_and_c = d.split(':-')[1]
    b_and_c = ''.join(b_and_c.split())
    
    #remove group_bys, order_bys, topn
    b_and_c = re.sub(groupby_regex, '', b_and_c)
    b_and_c = re.sub(orderby_regex, '', b_and_c)
    b_and_c = re.sub(topn_regex, '', b_and_c)
    
    b_and_c = re.split('([^\)]+\))', b_and_c) # splits on ending parens, which doesn't work for "nodeId in (1,2,3)"
    body = [b for b in b_and_c if b[-1:]==')'] # keeps atoms with ending paren as body atoms
    body = [b for b in body if not '%in%' in b] # remove "attribute%in%(a,b,c)" from body atoms
    body = [b[1:] if b[0]==',' else b for b in body]
    body = [''.join(b.split()) for b in body]
    
    return body

def getConds(datalog):
    '''function to get conditions for single datalog query'''
    d = cleanDatalog(datalog)
    
    # body and conditions are second element after splitting on ':-'
    b_and_c = d.split(':-')[1]
    
    #remove group_bys, order_bys, and topn
    b_and_c = re.sub(groupby_regex, '', b_and_c)
    b_and_c = re.sub(orderby_regex, '', b_and_c)
    b_and_c = re.sub(topn_regex, '', b_and_c)
    
    # split body and head on last closing paren
    b_and_c = re.split('([^\)]+\))', b_and_c)
    
    # add list elements without closing paren as last character to conditions
    conds = [c for c in b_and_c if c[-1:]!=')' and c!='']
    
    # split individual conditions into list
    if len(conds) > 0:
        conds = [c.split(',') for c in conds if c!=''][0]
        conds = [c for c in conds if c != '']

    # get list elements with '%in%' to add to conditions - i.e. "attribute%in%(a,b,c)
    addl_conds = [cleanDatalog(c) for c in b_and_c if '%in%' in c]
    
    # convert "attribute%in%(a,b,c)" to "attribute in (a,b,c)"
    addl_conds = [re.sub('%in%',' in ',c) for c in addl_conds]
    
    # add addl_conds to conds
    conds.extend(addl_conds)
    
    return conds

def getOrderBy(datalog):
    '''gets order_by conditions'''
    d = cleanDatalog(datalog)
    ob = re.findall(orderby_regex, d)
    return ob

def getOrderByParts(datalog):
    '''creates dictionary of order_by query, order_by variables, and order_by criteria for single order_by statement'''
    ob = cleanDatalog(datalog)
   
    query = re.findall('(?<=by\()[^\)]+\)', ob)
    ob_vars = re.findall(',(\[.*?\]),', ob)
    ob_criteria = re.findall('\],(\[.*?\])\)', ob)

    return {'query':query[0], 'vars':ob_vars[0], 'criteria':ob_criteria[0]}

def getGroupBy(datalog):
    '''extracts list of group_by statements from single datalog query'''
    d = cleanDatalog(datalog)
    gb = re.findall(groupby_regex, d)
    return gb

def parseGroupBy(gb):
    '''creates dictionary of group_by subgoal, aggregate expression, and aggregate vars for single group_by statement'''
    try:
        gb = cleanDatalog(gb)
        by = None
        query = None
        alias = None
        function = None
        on = None
        aggvars = re.findall('\[(.*?)\]', gb)
        if aggvars:
            by = aggvars[0].split(',')
        subgoal = re.findall('(?<=group_by\()[^\)]+\)', gb)
        if subgoal:
            query = parsePredicateAtoms(subgoal[0])
        aggexp =  re.findall('(?<=\],).+(?=\))', gb)
        if aggexp:
            maggexp = re.findall('(\w+)=(\w+)\((\w+)\)',aggexp[0])
            if maggexp:
                maggexp = maggexp[0]
                alias =  maggexp[0]
                function = maggexp[1]
                on = maggexp[2]
    except:
        raise Exception ("Error parsing group_by %s" % (gb))
    
    return {'predicate':query,
            'alias':alias,
            'function':function,
            'on':on,
            'by':by}

def getGroupByParts(gb):
    '''creates dictionary of group_by subgoal, aggregate expression, and aggregate vars for single group_by statement'''
    gb = cleanDatalog(gb)
    aggvars = re.findall('(\[(.*?)\])', gb)
    subgoal = re.findall('(?<=by\()[^\)]+\)', gb) + re.findall('(?<=group_by\()[^\)]+\)', gb)
    aggexp = re.findall('(?<=\],).+(?=\))', gb)

    return {'agg_vars': aggvars[0],
            'subgoal': subgoal[0],
            'agg_exp': aggexp[0]}

def rebuildGroupBy(gbParts):
    '''rebuilds group_by statement from groupby subgoal, aggregate variables, and aggregate expression'''
    subgoal = gbParts['subgoal']
    agg_vars = gbParts['agg_vars']
    agg_exp = gbParts['agg_exp']
    #print subgoal
    gb_str = 'group_by({0},{1},{2})'.format(subgoal, agg_vars, agg_exp)
    return gb_str

def getTopN(datalog):
    '''extracts list of topN statements from single datalog query'''
    d = cleanDatalog(datalog)
    topn = re.findall(topn_regex, d)
    return topn

def getTopNparts(topn):
    '''creates dictionary of topN n and query from single topN clause'''
    n = re.findall('(?<=top\()[0-9]+', topn)
    query = re.findall('(?<=[0-9],).+(?=\))', topn)
    return {'n':int(n[0]),'query':query[0]}

def parseTopNparts(topn):
    '''creates dictionary of topN n and query from single topN clause'''
    n = re.findall('(?<=top\()[0-9]+', topn)
    query = re.findall('(?<=[0-9],).+(?=\))', topn)
    results = {'n':int(n[0]),'query':parsePredicateAtoms(query[0])}

    return results

def rebuildTopN(topNparts):
    '''rebuilds topN statement from n and query'''
    n = topNparts['n']
    query = topNparts['query']
    topn_str = 'top({0},{1})'.format(n, query)
    return topn_str

def getDatalogParts(datalog):
    '''function to decompose datalog query into constituent parts - head, body, group_by, order_by, top_n, conditions'''
    body = getBody(datalog)
    orderby  = getOrderBy(datalog)
    groupby = getGroupBy(datalog)
    condition_clause = getConds(datalog)
    topn_clause = getTopN(datalog)
    head = getHead(datalog)

    parsed_predicates = None
    if body:
        parsed_predicates = [parsePredicateAtoms(pred) for pred in body]
    parsed_orderby = None
    parsed_conditions = None
    parsed_topn = None
    parsed_groupby = None
    if groupby:
        parsed_groupby = parseGroupBy(groupby[0])
    if orderby:
        parsed_orderby   = parseOrderBy(orderby[0])
    if condition_clause:
        parsed_conditions= parseConditionClause(condition_clause)
    if topn_clause:
        parsed_topn = parseTopNparts(topn_clause[0])
    if head:
        parsed_head = parseHead(head)

    return {'head': head,
            'head.parsed': parsed_head,
            'body': body,
            'body.parsed': parsed_predicates,
            'groupby': groupby,
            'groupby.parsed': parsed_groupby,
            'orderby': orderby,
            'orderby.parsed': parsed_orderby,
            'topn': topn_clause,
            'topn.parsed': parsed_topn,
            'conditions': condition_clause,
            'conditions.parsed': parsed_conditions
           }

def buildDatalogString(datalogParts):
    '''rebuilds datalog string from datalog parts'''
    dh = datalogParts['head']
    db = ','.join(b for b in datalogParts['body'])
    dg = ','.join(g for g in datalogParts['groupby'])
    dc = ','.join(c for c in datalogParts['conditions'])
    do = ','.join(g for g in datalogParts['orderby'])
    dt = ','.join(t for t in datalogParts['topn'])
    
    # rebuild string
    datalog_str = dh+':-'+cleanDatalog(','.join([db,dg,do,dt,dc]))
    
    # replace "attribute%in%(a,b,c)" with "attribute in (a,b,c)"
    datalog_str = re.sub('%in%', ' in ', datalog_str)
    
    return datalog_str

def parseOrderBy(datalog):
    '''creates dictionary of order_by query, order_by variables, and order_by criteria for single order_by statement'''

    results = None
    try:
        ob = cleanDatalog(datalog)

        query = re.findall('(?<=by\()[^\)]+\)', ob)
        ob_vars = re.findall(',\[(.*?)\],', ob)
        ob_criteria = re.findall('\],\[(.*?)\]\)', ob)
        by = []
        c = 0
        if ob_criteria:
            ordering = ob_criteria[0].split(',')
            c = len(ordering)

        for i,v in enumerate(ob_vars[0].split(',')):
            a = [v,'a']
            if i < c:
                a[1] = ordering[i]
            by.append(a)

        predicate = parsePredicateAtoms( query[0] )
        results = {'predicate':predicate['predicate'],
            'atoms':predicate['atoms'],
            'by':by}
    except:
        raise Exception( "Error parsing order by %s" % (datalog))
        
    return results

def parsePredicateAtoms(datalog):
    results = None
    source = None
    try:
        m = re.findall('(\w+)\.(\w+)\(', datalog)
        if m:
            source=m[0][0]                       
        m = re.findall('(\w+)\(([\w,]+)\)', datalog)
        if m:
            predicate = m[0][0]
            atoms = m[0][1].split(',')
        results = {'predicate':predicate,'atoms':list(atoms)}
        if source:
            results['source'] = source
    except:
        raise Exception( "Error parsing predication %s" % (datalog))
    return results

def parseCondition(condition):

    parsed = None
    try:
        expressions = ['(\w+)(%s)(\w+)' % (valid_qualifiers),'(\w+)[%|\s](in)[%|\s]\((.*)\)']
        for pattern in expressions:
            m = re.findall(pattern, condition)
            parsed = None
            if m:
                m = list(m[0])
                if m and len(m) == 3:
                    # created with "condition" to distinquish from AND, OR, IN, NOT IS, IS NULL... operators
                    parsed = {'condition': {'lhs':m[0],'qualifier':m[1],'rhs':m[2]}}
                    break
    except:
        raise Exception( "Error parsing condition %s" % (condition))
    return parsed

def parseHead(head):
    return parsePredicateAtoms(head)

def parseConditionClause(conditions):
    conditions = [parseCondition(cond) for cond in conditions]
    return conditions

def processDatalog(datalog):
    '''function that takes datalog string and returns string, individual steps, and parts'''
    datalog_info = {}
    steps = getDatalogSteps(datalog)
    
    parts = []
    for s in steps:
        parts.append(getDatalogParts(s))
        
    datalog_info['full_string'] = cleanDatalog(datalog)
    datalog_info['single_strings'] = steps
    datalog_info['single_parts'] = parts
    return datalog_info