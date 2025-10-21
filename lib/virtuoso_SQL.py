import sqlalchemy as db

class VirtuosoSQL:
    def __init__(self, login, password):#, autocommit = False):
        self._engine = db.create_engine(f'virtuoso+pyodbc://{login}:{password}@VOS')
        self.__metadata = db.MetaData()
        self.__connection = self._engine.connect()

    def insert(self, table:str, values:dict):
        query = db.insert(db.Table(table, self.__metadata, autoload_with=self._engine)).values(values)
        result = self.__connection.execute(query).inserted_primary_key

        if result is None:
            return None
        
        return result[0]
    
    def update(self, table:str, id, values:dict):
        table = db.Table(table, self.__metadata, autoload_with=self._engine)
        query = db.update(table).where(table.c.Id == id).values(values)

        result = self.__connection.execute(query)

    
    def find_by_id(self, table:str, id):
        db_table = db.Table(table, self.__metadata, autoload_with=self._engine)
        query = db.select(db_table).where(db_table.c.Id == id)
        result = self.__connection.execute(query).fetchall()
        
        return result
    
    def find_by_field(self, table:str, field_name:str, field_value):
        db_table = db.Table(table, self.__metadata, autoload_with=self._engine)
        query = db.select(db_table).where(getattr(db_table.c, field_name) == field_value)
        result = self.__connection.execute(query).fetchall()
        
        return result
    
    def commit(self):
        self.__connection.commit()
        