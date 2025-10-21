from lib.triple import TripleItemIri
from Environment import Environment

class StandardPredicates:

    _class = TripleItemIri('http://www.w3.org/2000/01/rdf-schema#Class')
    _label = TripleItemIri('http://www.w3.org/2000/01/rdf-schema#label')
    _comment = TripleItemIri('http://www.w3.org/2000/01/rdf-schema#comment')
    _predicate = TripleItemIri(f'{Environment._scheme}#predicate')
    _entity = TripleItemIri(f'{Environment._scheme}#entity')
    _wikidata_id = TripleItemIri(f'{Environment._scheme}#wikidata_id')
    _equal = TripleItemIri(f'{Environment._scheme}#equal')

    _register = {}

    @staticmethod
    def register_predicate(key:str, value:TripleItemIri):
        StandardPredicates._register[key] = value
    
    @staticmethod
    def search_register(key:str):
        if key in StandardPredicates._register:
            return StandardPredicates._register[key]
        return None

props = vars(StandardPredicates)
preds = [c for c in props if c[0] == '_' and c[1] != '_']
StandardPredicates._register = {key[1:]: props[key] for key in preds}