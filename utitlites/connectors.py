import lib.virtuoso_SPARQL as v_SPARQL
import lib.postgres_SQL as p_SQL


def sparql():
    return v_SPARQL.VirtuosoSPARQLAuth('http://127.0.0.1:8890/', login='dba', password='dba')


def sql():
    return p_SQL.PostgreSQL('postgres', 'postgres')
    