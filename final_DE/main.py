from data_connection import DataConnection
# from data_explorer import DataExplorer
from data_crud import DataCrud

# establish postgresql server connection

conn = DataConnection(
    user='firdaus',
    password='postgres',
    database='customer_credit',
)

crud = DataCrud(conn)

conn.connect()

# create table category_db
## Column
category_col = [
    "id INTEGER PRIMARY KEY", 
    "card_category VARCHAR(128) UNIQUE"
]

marital_col = [
    "id INTEGER PRIMARY KEY",
    "marital_status VARCHAR(50) UNIQUE"
]

status_col = [
    "id INTEGER PRIMARY KEY",
    "status VARCHAR(50) UNIQUE"
]

education_col = [
    "id INTEGER PRIMARY KEY",
    "education_level VARCHAR(50) UNIQUE"
]

customer_col = [
    "clientnum INTEGER PRIMARY KEY",
    "idstatus INTEGER",
    "customer_age INTEGER",
    "gender VARCHAR(50)",
    "dependent_count INTEGER",
    "educationid INTEGER",
    "maritalid INTEGER",
    "income_category VARCHAR(50)",
    "card_categoryid INTEGER",
    "months_on_book INTEGER",
    "total_relationship_count INTEGER",
    "months_inactive_12_mon INTEGER",
    "contacs_count_12_mon INTEGER",
    "credit_limit NUMERIC",
    "total_revolving_bal INTEGER",
    "avg_open_to_buy NUMERIC",
    "total_trans_amt INTEGER",
    "total_trans_ct INTEGER",
    "avg_utilization_ratio NUMERIC",
    "FOREIGN KEY (idstatus) REFERENCES status_db(id)",
    "FOREIGN KEY (educationid) REFERENCES education_db(id)",
    "FOREIGN KEY (maritalid) REFERENCES marital_db(id)",
    "FOREIGN KEY (card_categoryid) REFERENCES category_db(id)"
]

## Executing function to create table in database
crud.createTable('category_db', category_col)
crud.createTable('marital_db', marital_col)
crud.createTable('status_db', status_col)
crud.createTable('education_db', education_col)
crud.createTable('customer_data_history', customer_col)


# Import csv files
## column
category_col_csv = ['id', 'card_category']
marital_col_csv = ["id","marital_status"]
status_col_csv = ["id", "status"]
education_col_csv = ["id", "education_level"]
customer_col_csv = [
    "clientnum","idstatus","customer_age","gender","dependent_count","educationid","maritalid","income_category",
    "card_categoryid","months_on_book","total_relationship_count","months_inactive_12_mon","contacs_count_12_mon",
    "credit_limit","total_revolving_bal","avg_open_to_buy","total_trans_amt","total_trans_ct","avg_utilization_ratio"]

# execute importFile method
crud.importFile("'/home/firdaus/project/customer_data/category_db.csv'",category_col_csv, 'category_db')
crud.importFile("'/home/firdaus/project/customer_data/marital_db.csv'",marital_col_csv, 'marital_db')
crud.importFile("'/home/firdaus/project/customer_data/status_db.csv'",status_col_csv, 'status_db')
crud.importFile("'/home/firdaus/project/customer_data/education_db.csv'",education_col_csv, 'education_db')
crud.importFile("'/home/firdaus/project/customer_data/customer_data_history.csv'",customer_col_csv, 'customer_data_history')

# query = ''' SELECT * FROM category_db;'''

# res, col = conn.fetching(query)
# print(res)


# close connection

conn.close()
