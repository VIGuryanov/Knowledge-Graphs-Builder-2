from lib.virtuoso_SPARQL import VirtuosoSPARQLAuth, VirtuosoSPARQL
import lib.postgres_SQL as postgresSQL
from lib.triple import Triple, TripleItemType, TripleItemIri, TripleItemStr, TripleItemVariable
from database_models.DBModels import GraphModelBase, Entities, Predicates
from lib.standard_predicates import StandardPredicates

class GraphLogic:   
    def __init__(self, sparql_connection: VirtuosoSPARQLAuth, sql_connection: postgresSQL.PostgreSQL, graph_include_metadata = True):
        self.__sparql_connection = sparql_connection
        self.__sql_connection = sql_connection
        self.__graph_include_metadata = graph_include_metadata
        self.__node_cache = {}
        self.__triple_cache = {}

    def create_graph(self, graph):
        self.__sparql_connection.create_graph(graph)

    def drop_graph(self, graph):
        self.__sparql_connection.drop_graph(graph)

    def __graph_cache_node(self, graph, iri):
        if not graph in self.__node_cache:
            self.__node_cache[graph] = set()
        self.__node_cache[graph].add(iri)
        
    def __graph_cache_triple(self, graph, triple: Triple):
        if not graph in self.__node_cache:
            self.__triple_cache[graph] = {}

        if not triple.subject.value in self.__triple_cache[graph]:
            self.__triple_cache[graph][triple.subject.value] = {}
        
        if not triple.predicate.value in self.__triple_cache[graph][triple.subject.value]:
            self.__triple_cache[graph][triple.subject.value][triple.predicate.value] = set()
        
        self.__triple_cache[graph][triple.subject.value][triple.predicate.value].add(str(triple.object))
    
    def triple_in_graph(self, graph, triple:Triple):
        if (graph in self.__triple_cache and 
            triple.subject.value in self.__triple_cache[graph] and 
            triple.predicate.value in self.__triple_cache[graph][triple.subject.value] and 
            str(triple.object) in self.__triple_cache[graph][triple.subject.value][triple.predicate.value]):
            return True
        
        ret = self.__sparql_connection.select(['1'], graph, [triple])
        if len(ret) == 0:
            return False
        
        self.__graph_cache_triple(graph, triple)
        return True

    def node_in_graph(self, graph, entity:GraphModelBase) -> bool:
        if graph in self.__node_cache and entity.iri in self.__node_cache[graph]:
            return True
        
        ret = self.__sparql_connection.select(['1'], graph, [Triple(TripleItemIri(entity.iri), StandardPredicates._class, entity._graph_entity_type)])

        if len(ret) == 0:
            return False

        self.__graph_cache_node(graph, entity.iri)
        return True
    
    def graph_set_equality(self, graph, entity1:GraphModelBase, entity2:GraphModelBase, equality_type = None):
        if_entity1_known = self.reldb_get_node(entity1)
        if_entity2_known = self.reldb_get_node(entity2)

        if not if_entity1_known:
            raise Exception(f'{entity1.iri} not registered as {entity1._graph_entity_type}')
        if not if_entity2_known:
            raise Exception(f'{entity2.iri} not registered as {entity2._graph_entity_type}')

        if self.__graph_include_metadata and not self.node_in_graph(graph, if_entity1_known):
            self.__sparql_connection.insert(graph, [Triple(TripleItemIri(if_entity1_known.iri), StandardPredicates._class, if_entity1_known._graph_entity_type)])

        if self.__graph_include_metadata and not self.node_in_graph(graph, if_entity2_known):
            self.__sparql_connection.insert(graph, [Triple(TripleItemIri(if_entity2_known.iri), StandardPredicates._class, if_entity2_known._graph_entity_type)])
        
        self.__sparql_connection.insert(graph, [Triple(TripleItemIri(if_entity1_known.iri), StandardPredicates._equal, TripleItemIri(if_entity2_known.iri))])
        self.__sparql_connection.insert(graph, [Triple(TripleItemIri(if_entity2_known.iri), StandardPredicates._equal, TripleItemIri(if_entity1_known.iri))])

    def reldb_get_node(self, entity:GraphModelBase):
        if entity.id != 0:
            check_exists = self.__sql_connection.find_by_id(entity._table_name, entity.id)
            if len(check_exists) != 0:
                check_exists = check_exists[0]
                if entity.iri is None:
                    entity.iri = check_exists.iri
                    return entity
                
                elif entity.iri != check_exists.iri:
                    raise Exception(f'entity iri {entity.iri} not equal to database {check_exists.iri}')
                
        if not entity.iri is None:
            check_exists = self.__sql_connection.find_by_field(entity._table_name, 'iri', entity.iri)
            if len(check_exists) != 0:
                check_exists = check_exists[0]
                entity.id = check_exists.Id

                return entity
            
        return None

    def graph_get_or_create_node(self, graph, entity:GraphModelBase, label = '') -> TripleItemIri:
        #if entity is known, get and return it      
        if_exist = self.reldb_get_node(entity)
        if not if_exist is None:
            if self.__graph_include_metadata and not self.node_in_graph(graph, if_exist):
                self.__sparql_connection.insert(graph, [Triple(TripleItemIri(if_exist.iri), StandardPredicates._class, if_exist._graph_entity_type)])
            return TripleItemIri(if_exist.iri)

        #before creation check if iri already registered as another type
        if not entity.iri is None:
            opposite = Entities(iri=entity.iri) if entity._graph_entity_type == Predicates._graph_entity_type else Predicates(iri=entity.iri)
            if_opposite = self.reldb_get_node(opposite)
            if not if_opposite is None:
                raise Exception(f"iri {entity.iri} already registered as {opposite._graph_entity_type} not possible to register as {entity._graph_entity_type}")

        id = self.__sql_connection.insert(entity._table_name, entity.get_insert_values())
        entity.id = id

        if entity.iri is None:
            entity.refresh_iri()
            self.__sql_connection.update(entity._table_name, entity.id, entity.get_update_values())

        if self.__graph_include_metadata:
            self.__sparql_connection.insert(graph, [Triple(TripleItemIri(entity.iri), StandardPredicates._class, entity._graph_entity_type)])
            self.__graph_cache_node(graph, entity.iri)

        if label != '':
            self.__sparql_connection.insert(graph, [Triple(TripleItemIri(entity.iri), StandardPredicates._label, TripleItemStr(label))])
        return TripleItemIri(entity.iri)

    def graph_add_triple(self, graph, triple: Triple, autocreate = False):
        if triple.object.type == TripleItemType.var or triple.subject.type == TripleItemType.var or triple.predicate.type == TripleItemType.var:
            raise Exception(f"Triple item shouldn't be a variable")

        if not autocreate:
            is_predicate_exists = not self.reldb_get_node(Predicates(iri = triple.predicate.value)) is None
            if not is_predicate_exists:
                raise Exception(f"Predicate {triple.predicate.value} not found")
        else:
            triple.predicate = self.graph_get_or_create_node(graph, Predicates(iri=triple.predicate.value))
        
        if triple.object.type == TripleItemType.iri:
            subject_as_entity = not self.reldb_get_node(Entities(iri=triple.subject.value)) is None            
            subject_as_predicate = False #If subject already an entity no need to check if it a predicate
            if not subject_as_entity:
                subject_as_predicate = not self.reldb_get_node(Predicates(iri=triple.subject.value)) is None

            object_as_entity = not self.reldb_get_node(Entities(iri=triple.object.value)) is None            
            object_as_predicate = False #If object already an entity no need to check if it a predicate
            if not object_as_entity:
                object_as_predicate = not self.reldb_get_node(Predicates(iri=triple.object.value)) is None
            
            if subject_as_entity and object_as_entity or subject_as_predicate and object_as_predicate:
                pass
            elif subject_as_entity and object_as_predicate or subject_as_predicate and object_as_entity:
                raise Exception(f"Subject and Object type missmatch")
            elif subject_as_entity:
                if autocreate:
                    triple.object = self.graph_get_or_create_node(graph, Entities(iri=triple.object.value))
                else:
                    raise Exception(f"Object {triple.object.value} not found")
            elif subject_as_predicate:
                if autocreate:
                    triple.object = self.graph_get_or_create_node(graph, Predicates(iri=triple.object.value))
                else:
                    raise Exception(f"Object {triple.object.value} not found")
            else:
                if not autocreate:
                    raise Exception(f"Subject {triple.subject.value} not found")
                else:
                    if object_as_entity:
                        triple.subject = self.graph_get_or_create_node(graph, Entities(iri=triple.subject.value))
                    elif object_as_predicate:
                        triple.subject = self.graph_get_or_create_node(graph, Predicates(iri=triple.subject.value))
                    else:
                        triple.object = self.graph_get_or_create_node(graph, Entities(iri=triple.object.value))
                        triple.subject = self.graph_get_or_create_node(graph, Entities(iri=triple.subject.value))
        else:
            if not autocreate:
                is_subj_entity = not self.reldb_get_node(Entities(iri=triple.subject.value)) is None  
                if not is_subj_entity:
                    is_subj_predicate = not self.reldb_get_node(Predicates(iri=triple.subject.value)) is None  
                    if not is_subj_predicate:
                        raise Exception(f"Subject {triple.subject.value} not found")
            else:
                is_pred = self.reldb_get_node(Predicates(iri=triple.subject.value))
                if is_pred is None:
                    triple.subject = self.graph_get_or_create_node(graph, Entities(iri=triple.subject.value))

        if not self.triple_in_graph(graph, triple):
            self.__sparql_connection.insert(graph, [triple])

    def copy_graph(self, graph_from, graph_to, graph_from_sparql:VirtuosoSPARQL = None):
        self.create_graph(graph_to)

        if graph_from_sparql is None:
            graph_from_sparql = self.__sparql_connection

        all_data = graph_from_sparql.get_triples(['*'], graph_from, [Triple(TripleItemVariable('a'), TripleItemVariable('b'), TripleItemVariable('c'))])
        predicates_iri_set = set(map(lambda x: x.predicate.value, all_data))
        len_cache = 0
        while len(predicates_iri_set) != len_cache:
            len_cache = len(predicates_iri_set)
            for triple in all_data:
                if triple.object.value in predicates_iri_set:
                    predicates_iri_set.add(triple.subject.value)
        
        for predicate_iri in predicates_iri_set:
            self.graph_get_or_create_node(graph_to, Predicates(predicate_iri))
        
        for triple in all_data:
            self.graph_add_triple(graph_to, triple, autocreate=True)
    
    def commit(self):
        self.__sparql_connection.commit()
        self.__sql_connection.commit()
        self.__node_cache = {}
        self.__triple_cache = {}
