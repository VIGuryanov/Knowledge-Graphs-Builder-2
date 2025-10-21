import sqlalchemy as db
from lib.virtuoso_SPARQL import VirtuosoSPARQLAuth
from lib.virtuoso_SQL import VirtuosoSQL
from lib.triple import Triple, TripleItem, TripleItemType, TripleItemInt, TripleItemIri, TripleItemStr, TripleItemVariable
from database_models.DBModels import Entities, Predicates
from graph_logic import GraphLogic
from Environment import Environment
from lib.standard_predicates import StandardPredicates

# Create an in-memory SQLite database engine
class Migration:

    def __init__(self, sparql_connection: VirtuosoSPARQLAuth, sql_connection: VirtuosoSQL):
        self.sparql_connection = sparql_connection
        self.sql_connection = sql_connection

    def up(self):
        engine = self.sql_connection._engine
        connection = engine.connect()
        metadata = db.MetaData()

        entities = db.Table(Entities._table_name, metadata,
            db.Column('Id', db.Integer, primary_key=True, nullable=False, index=True),
            db.Column('iri', db.VARCHAR(), unique=True, nullable=False, index=True))
        
        predicates = db.Table(Predicates._table_name, metadata,
            db.Column('Id', db.Integer, primary_key=True, nullable=False, index=True),
            db.Column('iri', db.VARCHAR(), unique=True, nullable=False, index=True))
                
        entities.create(engine, checkfirst=True)
        predicates.create(engine, checkfirst=True)

        graph_iri = Environment._scheme
        self.sparql_connection.create_graph(graph_iri)

        self.sparql_connection.insert(graph_iri, [
            #register rdfs:Class
            Triple(StandardPredicates._class, StandardPredicates._class, StandardPredicates._predicate),
            Triple(StandardPredicates._class, StandardPredicates._label, TripleItemStr('Class')),
            #register predicate
            Triple(StandardPredicates._predicate, StandardPredicates._class, StandardPredicates._predicate),
            Triple(StandardPredicates._predicate, StandardPredicates._label, TripleItemStr('predicate')),
            Triple(StandardPredicates._predicate, StandardPredicates._comment, TripleItemStr('A base class for all predicates.')),
            #register entity
            Triple(StandardPredicates._entity, StandardPredicates._class, StandardPredicates._entity),
            Triple(StandardPredicates._entity, StandardPredicates._label, TripleItemStr('entity')),
            Triple(StandardPredicates._entity, StandardPredicates._comment, TripleItemStr('A base class for all entities.')),

            #register rdfs:label
            Triple(StandardPredicates._label, StandardPredicates._class, StandardPredicates._predicate),
            Triple(StandardPredicates._label, StandardPredicates._label, TripleItemStr('label')),
            Triple(StandardPredicates._label, StandardPredicates._comment, TripleItemStr('A human-readable name for the subject.')),

            #register rdfs:comment
            Triple(StandardPredicates._comment, StandardPredicates._class, StandardPredicates._predicate),
            Triple(StandardPredicates._comment, StandardPredicates._label, TripleItemStr('comment')),
            Triple(StandardPredicates._comment, StandardPredicates._comment, TripleItemStr('A description of the subject resource.')),
                        
            #register wikidata_id
            Triple(StandardPredicates._wikidata_id, StandardPredicates._class, StandardPredicates._predicate),
            Triple(StandardPredicates._wikidata_id, StandardPredicates._label, TripleItemStr('wikidata_id')),
            Triple(StandardPredicates._wikidata_id, StandardPredicates._comment, TripleItemStr('An id on Wikidata resource')),
            ])
        
        query = db.insert(predicates).values(Id = 1, iri = StandardPredicates._class.value)
        connection.execute(query)
        query = db.insert(predicates).values(Id = 2, iri = StandardPredicates._label.value)
        connection.execute(query)
        query = db.insert(predicates).values(Id = 3, iri = StandardPredicates._comment.value)
        connection.execute(query)
        query = db.insert(predicates).values(Id = 4, iri = StandardPredicates._predicate.value)
        connection.execute(query)
        query = db.insert(predicates).values(Id = 5, iri = StandardPredicates._entity.value)
        connection.execute(query)
        query = db.insert(predicates).values(Id = 6, iri = StandardPredicates._wikidata_id.value)
        connection.execute(query)
        
        connection.commit()
        self.sparql_connection.commit()

    def down(self):
        engine = self.sql_connection._engine
        metadata = db.MetaData()

        db.Table(Entities._table_name, metadata, autoload_with=engine).drop(engine, checkfirst=True)
        db.Table(Predicates._table_name, metadata, autoload_with=engine).drop(engine, checkfirst=True)

        self.sparql_connection.drop_graph(Environment._scheme)
        self.sparql_connection.commit()