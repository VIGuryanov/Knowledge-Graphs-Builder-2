from SPARQLWrapper import SPARQLWrapper, JSON, POST, DIGEST
from lib.triple import Triple, TripleItem, TripleItemType, TripleItemVariable
import copy

class VirtuosoSPARQL:
    def __init__(self, domain):
        if domain[-1] == '/':
            domain = domain[:-1]

        self.sparql = SPARQLWrapper(f'{domain}/sparql')        
        self.sparql.setReturnFormat(JSON)
    
    def select(self, select: list[str | TripleItemVariable], graph_iri = None, where: list[Triple] = [], optional: list[Triple] = [], minus: list[Triple] = [], filter = '', group_by: list[TripleItemVariable] = [], order_by: list[TripleItemVariable] = [], having = '', limit: int = -1, offset: int = 0):
        variables = set(self.__extract_variables(where)).union(self.__extract_variables(optional)).union(self.__extract_variables(minus))
        if not set([a.value for a in select if type(a) is TripleItemVariable]).issubset(variables):
            raise Exception("select contains unknown variables")
        if not set([a.value for a in group_by]).issubset(variables):
            raise Exception("Group by contains unknown variables")
        if not set([a.value for a in order_by]).issubset(variables):
            raise Exception("Order by contains unknown variables")
        
        limit_offseted = limit
        if limit_offseted == -1 and offset != 0:
            limit_offseted = 10000
        
        query = f'''
            SELECT {' '.join(map(lambda x: str(x), select))}
            {(f'FROM <{graph_iri}>', '')[graph_iri is None]}
            WHERE 
            {{
                {'.\n'.join(map(lambda x: f'{str(x.subject)} {str(x.predicate)} {str(x.object)}', where))}
                {(f'''OPTIONAL
                {{
                    {'.\n'.join(map(lambda x: f'{str(x.subject)} {str(x.predicate)} {str(x.object)}', optional))}
                }}
                ''', '')[len(optional) <= 0]}
                {(f'''MINUS
                {{
                    {'.\n'.join(map(lambda x: f'{str(x.subject)} {str(x.predicate)} {str(x.object)}', minus))}
                }}
                ''', '')[len(minus) <= 0]}
                {(f'FILTER {filter}', '')[len(filter) == 0]}
                
            }}
            {(f'GROUP BY {' '.join(map(lambda x: str(x), group_by))}', '')[len(group_by) == 0]}
            {(f'HAVING {having}', '')[len(group_by) == 0 or len(having) == 0]}
            {(f'ORDER BY {' '.join(map(lambda x: str(x), group_by))}', '')[len(order_by) == 0]}
            {(f'LIMIT {limit_offseted}', '')[limit_offseted < 0]}
            {(f'OFFSET {offset}', '')[offset <= 0]}
            '''
        res =  self.execute_query(query)["results"]["bindings"]
        if len(res) == 10000:
            if limit == -1:
                res.extend(self.select(select, graph_iri, where, optional, minus, filter, group_by, order_by, having, offset=offset+10000))
            elif limit > 10000:
                res.extend(self.select(select, graph_iri, where, optional, minus, filter, group_by, order_by, having, limit=limit-10000, offset=offset+10000))
        
        return res
    
    def get_triples(self, select: list[str | TripleItemVariable], graph_iri = None, where: list[Triple] = [], optional: list[Triple] = [], minus: list[Triple] = [], filter = '', group_by: list[TripleItemVariable] = [], order_by: list[TripleItemVariable] = [], having = '', limit: int = -1, offset: int = 0) -> list[Triple]:
        select_res = self.select(select, graph_iri, where, optional, minus, filter, group_by, order_by, having, limit, offset)
        triples = []

        reqs = where + optional
        for req in reqs:
            if not (type(req.object) is TripleItemVariable or type(req.predicate) is TripleItemVariable or type(req.subject) is TripleItemVariable):
                reqs.remove(req)

        for row in select_res:
            for req in reqs:
                triple = copy.deepcopy(req)

                if type(triple.object) is TripleItemVariable:
                    if triple.object.value in row:
                        triple.object = TripleItem.create_from_dict(row[triple.object.value])
                    else:
                        continue

                if type(triple.predicate) is TripleItemVariable:
                    if triple.predicate.value in row:
                        triple.predicate = TripleItem.create_from_dict(row[triple.predicate.value])
                    else:
                        continue

                if type(triple.subject) is TripleItemVariable:
                    if triple.subject.value in row:
                        triple.subject = TripleItem.create_from_dict(row[triple.subject.value])
                    else:
                        continue
                
                triples.append(triple)

        return triples
        
    def __extract_variables(self, list: list[Triple]):
        return [k.value for ks in map(lambda x: [x.subject, x.predicate, x.object], list) for k in ks if k.type == TripleItemType.var]

    def execute_query(self, query:str):
        try:
            self.sparql.setQuery(query)
            return self.sparql.query().convert()
        except Exception as e:
            print(e)
        
class VirtuosoSPARQLAuth(VirtuosoSPARQL):
    def __init__(self, domain, login, password, autocommit = False):
        super().__init__(domain)

        self.sparql = SPARQLWrapper(f'{self.sparql.endpoint}-auth')        
        self.sparql.setReturnFormat(JSON)
        self.sparql.setHTTPAuth(DIGEST)
        self.sparql.setCredentials(login, password)
        self.sparql.setMethod(POST)

        self.__tocommit = []
        self.__autocommit = autocommit

    def create_graph(self, graph_iri):
        self.execute_commit_query(f"CREATE GRAPH <{graph_iri}>")

    def drop_graph(self, graph_iri):
        self.execute_commit_query(f"DROP GRAPH <{graph_iri}>")

    def insert(self, graph_iri, values: list[Triple]):
        inserts = map(lambda x: f'INSERT IN GRAPH <{graph_iri}> {{ {str(x.subject)} {str(x.predicate)} {str(x.object)} }}', values)
        self.execute_commit_query("\n".join(inserts))

    def delete(self, graph_iri, values: list[Triple]):
        inserts = map(lambda x: f'DELETE DATA FROM <{graph_iri}> {{ {str(x.subject)} {str(x.predicate)} {str(x.object)} }}', values)
        self.execute_commit_query("\n".join(inserts))

    def execute_commit_query(self, query:str):
        self.__tocommit.append(query)

        if self.__autocommit:
            self.commit()

    def commit(self):
        if len(self.__tocommit) == 0:
            return
        
        for i in range(0, len(self.__tocommit), 100):        
            query = "\n".join(self.__tocommit[i:i+100])
            res = self.execute_query(query)

        self.__tocommit = []
        return res
