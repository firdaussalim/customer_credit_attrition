import psycopg2

class DataCrud:
    def __init__(self, data_connection):
        self.data_connection = data_connection
        
    def createDatabase(self, database_name:str):
        '''
        Create a database into postgresql server
        
        database_name (str) : your new database name
        '''
        try:
            sql = f"CREATE DATABASE {database_name}"
            self.data_connection.exec(sql)
            print(f"database {database_name} successfully created")
        except (Exception, psycopg2.DatabaseError) as e:
            print(f"Error : {e}")
    
    def dropDatabase(self, database_name:str):
        '''
        Delete a database from postgresql server
        
        database_name (str) : your new database name
        '''
        try:
            sql = f"DROP DATABASE {database_name}"
            self.data_connection.exec(sql)
            print(f"Database {database_name} successfully deleted")
        except (Exception, psycopg2.DatabaseError) as e:
            print(f"Error : {e}")
            
    def createTable(self, table_name:str, column:list):
        '''
        Create a new table into database
        table_name (str) : new table name that not exist in database
        column (list) : list of column name and datatype, must be a string
        '''
        try:
            col = ", ".join(column)
            sql = f"CREATE TABLE {table_name}({col})"
            self.data_connection.exec(sql)
        except (Exception, psycopg2.DatabaseError) as e:
            print(f"Error : {e}")

    def importFile(self, path:str, columns:list, table_name:str ):
        '''
        Importing csv file into postgresql database
        path (str) : csv file location
        columnns (list) : List of column name that want to be inserted by each values from csv files
        table_name (str) : table name
        '''
        try:
            col = ','.join(columns)
            sql =f'''copy {table_name}({col})
                    FROM {path}
                    DELIMITER ','
                    CSV HEADER;'''
            self.data_connection.exec(sql)
        except (Exception, psycopg2.DatabaseError) as e:
            print(f"Error : {e}")