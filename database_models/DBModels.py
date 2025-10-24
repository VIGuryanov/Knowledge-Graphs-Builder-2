from Environment import Environment
from lib.standard_predicates import StandardPredicates
from abc import ABC, abstractmethod
from typing import override


class DBModelBase(ABC):
    _table_name = ''    

    @abstractmethod
    def get_insert_values(self) -> dict:
        pass

    @abstractmethod
    def get_update_values(self) -> dict:
        pass

class GraphModelBase(DBModelBase):
    def __init__(self, id = 0, iri = None):
        self.id = id
        self.iri = iri
    
    
    _graph_entity_type = None
    
    @override
    def get_insert_values(self) -> dict:
        res = {}
        if self.id != 0:
            res['Id'] = self.id
        if self.iri is None:
            res['iri'] = ''
        else:
            res['iri'] = self.iri
        return res
    
    @override
    def get_update_values(self) -> dict:
        res = {}
        res['iri'] = self.iri
        return res
    
    def refresh_iri(self):
        pass

class Entities(GraphModelBase):
    def __init__(self, id = 0, iri = None):
        super().__init__(id, iri)
        # if iri is None:
            
        # else:
        #     self.iri = iri


    _table_name = 'Entities'
    _graph_entity_type = StandardPredicates._entity
    
    @override
    def refresh_iri(self):
        if self.iri is None and self.id != 0:
            self.iri = f'{Environment._domain}/entities#{self.id}'

class Predicates(GraphModelBase):
    _table_name = 'Predicates'
    _graph_entity_type = StandardPredicates._predicate

    def __init__(self, id = 0, iri = None):
        super().__init__(id, iri)
        # if iri is None:
        #     self.iri = f'{Environment.domain}/predicates#{id}'
        # else:
        #     self.iri = iri
    
    @override
    def refresh_iri(self):
        if self.iri is None and self.id != 0:
            self.iri = f'{Environment._domain}/predicates#{self.id}'