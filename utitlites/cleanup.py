from lib.virtuoso_SPARQL import VirtuosoSPARQLAuth
from lib.postgres_SQL import PostgreSQL
from graph_logic import GraphLogic
import utitlites.connectors
import migration
from Environment import Environment


def cleanup(sparql: VirtuosoSPARQLAuth, sql: PostgreSQL, migration: migration.Migration):
    migration.down()
    sparql.drop_graph(f'{Environment._domain}/itis')
    sparql.drop_graph(f'{Environment._domain}/copy')
    sparql.drop_graph('math')
    _ = sparql.commit()


if __name__ == '__main__':
    sparql = utitlites.connectors.sparql()
    sql = utitlites.connectors.sql()
    c = migration.Migration(sparql, sql)
    cleanup(sparql, sql, c)


# def virtuoso_cleanup(graph_logic: GraphLogic, graph_iri: str):
#     graph_logic.drop_graph(graph_iri)


# def postgres_cleanup():
#     # connection = psycopg2.connect(
#     #     database="postgres", user='postgres', 
#     #     password='postgres', host='localhost', port='5432'
#     # )
#     # cursor = connection.cursor()
#     # cursor.execute('DROP TABLE IF EXISTS "Entities" CASCADE;')
#     # cursor.execute('DROP TABLE IF EXISTS "Predicates" CASCADE;')
#     # connection.commit()
    