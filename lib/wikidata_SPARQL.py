from SPARQLWrapper import SPARQLWrapper, JSON

class WikidataSPARQL:

    __sparql = SPARQLWrapper('https://query.wikidata.org/sparql')
    __sparql.setReturnFormat(JSON)

    @staticmethod
    def find_by_label(label: str, fields: list[str] = [], optional: dict[str, str] = {}) -> list[dict]:
        """[summary]

        Args:
            label - item label in Wikidata\n
            fields - specify return field of item like item, itemLabel, itemDescription or itemAltLabel\n
            optional - additional relations of item in format {'Wikidata relation type' : 'name for result'}\n

        Returns:
            list of query results
        """

        return_data = WikidataSPARQL.__concat_request_fields(fields, optional)
        optional_data = WikidataSPARQL.__concat_optional_fields(optional)
            
        WikidataSPARQL.__sparql.setQuery(f"""
            SELECT {return_data}
            WHERE
            {{
                ?item ?label "{label}".
                OPTIONAL
                {{
                    {optional_data}
                }}
                SERVICE wikibase:label {{ bd:serviceParam wikibase:language "mul,en". }}
            }}
            """)

        return WikidataSPARQL.__sparql.queryAndConvert()["results"]["bindings"]
    
    @staticmethod
    def find_by_id(id: str, fields: list[str] = [], optional: dict[str, str] = {}) -> list[dict]:
        """[summary]

        Args:
            id - item id in Wikidata\n
            fields - specify return field of item like item, itemLabel, itemDescription or itemAltLabel\n
            optional - additional relations of item in format {'Wikidata relation type' : 'name for result'}\n

        Returns:
            list of query results
        """
        
        return_data = WikidataSPARQL.__concat_request_fields(fields, optional)
        optional_data = WikidataSPARQL.__concat_optional_fields(optional)

        WikidataSPARQL.__sparql.setQuery(f"""
            SELECT {return_data}
            WHERE
            {{
                VALUES ?item {{{id}}}.
                OPTIONAL
                {{
                    {optional_data}
                }}
                SERVICE wikibase:label {{ bd:serviceParam wikibase:language "mul,en". }}
            }}
            """)

        return WikidataSPARQL.__sparql.queryAndConvert()["results"]["bindings"]

    @staticmethod
    def find_by_relations(relations: dict[str, str], fields: list[str] = [], optional: dict[str, str] = {}) -> list[dict]:
        """[summary]

        Args:
            relations - find items match relations in format {'Wikidata relation type' : 'Wikidata id of related to item'}\n
            fields - specify return field of item like item, itemLabel, itemDescription or itemAltLabel\n
            optional - additional relations of item in format {'Wikidata relation type' : 'name for result'}\n

        Returns:
            list of query results
        """
                
        return_data = WikidataSPARQL.__concat_request_fields(fields, optional)
        optional_data = WikidataSPARQL.__concat_optional_fields(optional)
        relations = WikidataSPARQL.__concat_relations(relations)

        WikidataSPARQL.__sparql.setQuery(f"""
            SELECT {return_data}
            WHERE
            {{   
                {relations}
                OPTIONAL
                {{
                    {optional_data}
                }}
                SERVICE wikibase:label {{ bd:serviceParam wikibase:language "mul,en". }}
            }}
            """)

        return WikidataSPARQL.__sparql.queryAndConvert()["results"]["bindings"]
    
    @staticmethod
    def execute_any_query(query):
        WikidataSPARQL.__sparql.setQuery(query)

        return WikidataSPARQL.__sparql.queryAndConvert()["results"]["bindings"]
    
    @staticmethod
    def __concat_relations(relations):
        res = ""
        for wikidata_relation, related_to_id in relations.items():
            res = f'{res} ?item {wikidata_relation} {related_to_id}.'
        
        return res

    @staticmethod
    def __concat_request_fields(fields, optional):
        res = ""
        for field in fields:
            res = f'{res} ?{field}'

        for key, value in optional.items():
            res = f'{res} ?{value}'
        
        return res
    
    @staticmethod
    def __concat_optional_fields(fields):
        res = ""
        for key, value in fields.items():
            res = f'{res}?item {key} ?{value}. '
        
        return res