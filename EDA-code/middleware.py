from __future__ import print_function
from rx import Observable, Observer
from sqlalchemy import create_engine, text
import json
import pprint
import re
from lxml import etree
import requests

pp = pprint.PrettyPrinter(depth=6)

asterixconfig={}

def VirtualIntegrationSchema():
    xml = '''
        <root>
            <PG processor="postgresql_exec" conf="./postgresql.conf">
                <SQLBook>
                    <public>
                        <customers>
                            <COL NAME="customerid" TYPE="String" />
                            <COL NAME="firstname" TYPE="String" />
                        </customers>
                        <reviews>
                            <COL NAME="customerid" TYPE="String" />
                            <COL NAME="firstname" TYPE="String" />
                        </reviews>
                    </public>
                </SQLBook>
                <DB>
                    <SCHEMA>
                        <VIEW1>
                            <COL NAME="Col1" TYPE="String" />
                            <COL NAME="Col2" TYPE="Integer" />
                        </VIEW1>
                        <VIEW2>
                            <COL NAME="Col1" TYPE="String" />
                            <COL NAME="Col2" TYPE="Integer" />
                        </VIEW2>
                    </SCHEMA>
                </DB>
                <orm2>
                <public>
                    <AddressTable>
                        <COL NAME="Col1" TYPE="String" />
                        <COL NAME="Col2" TYPE="Integer" />
                    </AddressTable>
                </public>
            </orm2>
            <dbTest>
                <public>
                    <table1>
                        <COL NAME="id" TYPE="Integer" />
                        <COL NAME="name" TYPE="String" />
                    </table1>
                </public>
            </dbTest>
        </PG>
        <AX processor="asterixdb_exec2" conf="./astrixdb.conf">
                <AstxDB>
                    <TableBT>
                        <COL NAME="id" TYPE="Integer" />
                        <COL NAME="childName" TYPE="String" />
                    </TableBT>
                </AstxDB>
            </AX>
        </root>
    '''
    return xml


def get_domain(val):
    rec = re.compile(r'^([A-Z][A-Z])/')
    res = rec.findall(val)
    return ''.join(res)


class SomeObserver(Observer):

    def __init__(self, root, domain):
        self.root = root
        self.domain = domain

    def on_next(self, value):
        processor = self.root.find(value["domain"]).attrib["processor"]

        if value["domain"] not in self.domain.keys():
            self.domain[value["domain"]] = {}

        self.domain[value["domain"]][value["key"]] = {
            "processor": processor,
            "db": value["db"],
            "where": value["where"],
            "return": value["return"]
        }

    def on_completed(self):
        print("Done!")

    def on_error(self, error):
        print("Error Occurred: {0}".format(error))


class SomeObserver3(Observer):

    def __init__(self, root, domain, resultset):
        self.root = root
        self.domain = domain
        self.resultset = resultset

    def postgresql_exec(self, alias=None, where=None, db=None, ret=None):
        # print("From PGsql")

        engine = create_engine('postgresql+psycopg2://postgres@45.79.91.219/dbTest')
        conn = engine.connect()

        def runquery():
            realdb = self.root.find(db + "/..").tag
            table = self.root.find("{0}".format(db)).tag

            t = (','.join(ret))
            # print(t)

            sql = """
            SELECT {0} FROM {1} {2}
            LIMIT 1000;
            """
            stmt = text(sql.format(t, table, alias))

            return Observable.from_(conn.execute(stmt))

        def myComplete():
            key1 = where.split('=')[0].strip()
            key2 = where.split('=')[1].strip()

            # print(alias, key1)
            # print(self.resultset)
            #
            # print(self.resultset[alias]['cols'].index(key1))

            colId1 = self.resultset[alias]['cols'].index(key1)

            # print(colId1)
            #
            # print([x for x in self.resultset[alias]['rows']])

            keyids = set([x[colId1] for x in self.resultset[alias]['rows']])
            w = (' OR '.join(['{0} = {1}'.format(key2, x) for x in keyids]))
            asterixconfig['keys'] = w

            print("finished")

        def myError(x):
            pass

        def save(x):
            if alias not in self.resultset.keys():
                self.resultset[alias] = {
                    'cols': ret,
                    'rows': []
                }

            self.resultset[alias]["rows"].append(x)

        runquery() \
            .subscribe(save, myError, myComplete)

        conn.close()

    def asterixdb_exec2(self, alias=None, where=None, db=None, ret=None):
        headers = {'Content-type': 'application/x-www-form-urlencoded'}

        realdb = self.root.find(db + "/..").tag
        table = self.root.find("{0}".format(db)).tag

        t = (','.join(ret))

        # print(db, t, alias)

        sql_template = """
        USE {0}
        select {2} from {1} {2}
        WHERE <0>;
        """
        asterixconfig['sql'] = sql_template.format(realdb, table, alias)
        asterixconfig['ret'] = ret
        asterixconfig['alias'] = alias
        asterixconfig['db'] = db
        asterixconfig['where'] = where
        # print(asterixconfig['sql'])

    def on_next(self, value):
        targets = ([x[1] for x in [(value[2], x.strip()) for x in value[5].split(",")] if x[0] in x[1]])

        argsdict = {
            'alias': value[2],
            'where': value[3],
            'db': value[4],
            'ret': targets
        }

        #locals() (value[0])
        try:
            method = getattr(self, value[0])
        except AttributeError:
            raise NotImplementedError(
                "Class `{}` does not implement `{}`".format(self.__class__.__name__, value[0]))

        method(**argsdict)

            # locals()[value[0]](**argsdict)

    def on_completed(self):
        pass

    def on_error(self, error):
        print("Error Occurred: {0}".format(error))


class SomeObserver2(Observer):

    def __init__(self, root, domain, resultset):
        self.root = root
        self.domain = domain
        self.resultset = resultset

    def on_next(self, value):
        o3 = SomeObserver3(self.root, self.domain, self.resultset)

        Observable.from_(self.domain[value]) \
            .map(lambda x: (self.domain[value][x]['processor'],
                            value,
                            x,
                            self.domain[value][x]['where'],
                            self.domain[value][x]['db'],
                            self.domain[value][x]['return'])) \
            .subscribe(o3)

    def on_completed(self):
        pass

    def on_error(self, error):
        print("Error Occurred: {0}".format(error))



class WebSession():

    def __init__(self, vis):
        self.root = etree.XML(vis)
        self.domain = {}
        # self.viv = {}
        self.resultset = {}

    # def getVirtualIntegrationSchema(self):
    #     return self.xml

    def asterixdb_exec3(self, alias=None, where=None, db=None, ret=None):
        headers = {'Content-type': 'application/x-www-form-urlencoded'}
        sqlT = asterixconfig['sql']
        sqlT = sqlT.replace("<", "{").replace(">", "}")
        sql = sqlT.format(asterixconfig['keys'])
        # print(sql)

        ret = asterixconfig['ret']
        alias = asterixconfig['alias']

        response = requests.post('http://45.79.91.219:19002/query/service', data=sql, headers=headers)

        jsonobj = json.loads(response.text.replace("{ ,", "{ "))

        def extract_fields(x):
            return tuple([x[field.replace(alias + ".", "")] for field in ret])

        def save(x):
            if alias not in self.resultset.keys():
                self.resultset[alias] = {
                    'cols': ret,
                    'rows': []
                }
            self.resultset[alias]["rows"].append(x)

        Observable.from_(jsonobj["results"]) \
            .map(lambda x: x[alias]) \
            .map(extract_fields) \
            .subscribe(save)


    def get_result_sets(self, q):
        self.jl = json.loads(q)

        o1 = SomeObserver(self.root, self.domain)

        Observable.from_(self.jl["from"].keys()) \
            .map(lambda x: {
            "domain": get_domain(self.jl["from"][x]),
            "key": x, "db": self.jl["from"][x], \
            "where": self.jl["where"], "return": self.jl["return"] \
            }) \
            .subscribe(o1)  # lambda x: print(x))

        o2 = SomeObserver2(self.root, self.domain, self.resultset)

        Observable.from_(self.domain) \
            .subscribe(o2)

        # print(self.resultset)

        # print(asterixconfig)
        # print(self.resultset.keys())

        self.asterixdb_exec3(alias=self.resultset.keys()[0])
        print(self.resultset)

        key1 = asterixconfig['where'].split('=')[0].strip()
        key2 = asterixconfig['where'].split('=')[1].strip()

        aliases = self.resultset.keys()

        colKeyIndex1 = self.resultset[aliases[0]]['cols'].index(key1)
        colKeyIndex2 = self.resultset[aliases[1]]['cols'].index(key2)

        colId1 = self.resultset[aliases[0]]['cols'].index(key1)

        j_result = {
            "rows" : []
        }

        for i, row1 in enumerate(self.resultset[aliases[0]]['rows']):
            row1Key = (row1[colId1])
            for j, row2 in enumerate(self.resultset[aliases[1]]['rows']):
                if row1[colKeyIndex1] == row2[colKeyIndex2]:
                    j_result['rows'].append(tuple([x for x in row1] + [x for x in row2]))

        #print(json.dumps(j_result))

        return j_result 
