from Environment import Environment
from lib.standard_predicates import StandardPredicates
from abc import ABC, abstractmethod

class DBModelBase(ABC):
    _table_name = ''    

    @abstractmethod
    def get_insert_values() -> dict:
        pass

    @abstractmethod
    def get_update_values(self) -> dict:
        pass

class GraphModelBase(DBModelBase):
    _graph_entity_type = None

    def get_insert_values(self) -> dict:
        res = {}
        if self.id != 0:
            res['Id'] = self.id
        if self.iri is None:
            res['iri'] = ''
        else:
            res['iri'] = self.iri
        return res

    def get_update_values(self) -> dict:
        res = {}
        res['iri'] = self.iri
        return res
    
    def refresh_iri(self):
        pass

class Entities(GraphModelBase):
    _table_name = 'Entities'
    _graph_entity_type = StandardPredicates._entity

    def __init__(self, id = 0, iri = None):
        self.id = id
        self.iri = iri
        # if iri is None:
            
        # else:
        #     self.iri = iri
    
    def refresh_iri(self):
        if self.iri is None and self.id != 0:
            self.iri = f'{Environment._domain}/entities#{self.id}'

class Predicates(GraphModelBase):
    _table_name = 'Predicates'
    _graph_entity_type = StandardPredicates._predicate

    def __init__(self, id = 0, iri = None):
        self.id = id
        self.iri = iri
        # if iri is None:
        #     self.iri = f'{Environment.domain}/predicates#{id}'
        # else:
        #     self.iri = iri

    def refresh_iri(self):
        if self.iri is None and self.id != 0:
            self.iri = f'{Environment._domain}/predicates#{self.id}'