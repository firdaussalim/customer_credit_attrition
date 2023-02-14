import psycopg2

class DataConnection:
    def __init__(self, database, user, password, host='localhost', port=5432):
        '''
        Initialize the connection to the database :
        host (str) : the host of the database (default : 'localhost')
        database (str) : the name of the database
        user (str): the username for the database
        password (str) : the password for the database
        port (int) : the database port (default : 5432)
        '''
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        
    def connect(self):
        ''' 
        Connect to the database using the provided paramaters
        '''
        try:
            self.conn = psycopg2.connect(
                host = self.host,
                port = self.port,
                database = self.database,
                user = self.user,
                password = self.password
            )
            # self.conn.autocommit = True
            self.cursor = self.conn.cursor()
            print('Connected to the database')
        except (Exception, psycopg2.DatabaseError) as e:
            print(f"Error: {e}")
    
    def close(self):
        '''
        Close the connection to the database
        '''
        try :
            if self.conn:
                self.cursor.close()
                self.conn.close()
                print('Connection closed.')
            else:
                print('There is no opened database')
        except (Exception, psycopg2.DatabaseError) as e:
            print(f"Error: {e}")

    
    def fetching(self, query):
        '''
        Fetch the data from database using SELECT query
        query (str) : the query to be executed

        return fetching data and database column names
        '''
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            column = [desc[0] for desc in cursor.description]
            return result, column
        except (Exception, psycopg2.DatabaseError) as e:
            print(f"Error: {e}")

    def exec(self, query, values=None):
        '''
        Execute custome query
        query (str) : sql query in string
        values : list of values if want to insert more than 1 values
        '''
        cursor = self.conn.cursor()
        try:
            if values:
                cursor.executemany(query, values)
            else:
                cursor.execute(query)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as e:
            self.conn.rollback()
            print(f"Error: {e}")

