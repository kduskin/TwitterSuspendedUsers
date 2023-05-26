import pandas as pd
import psycopg2
import psycopg2.extras as extras


def connect_to_db(host,user,db):
    """
    Function to connect to the database, returns a connection instance
    host (str): host name
    user (str): user name
    db (str): databse name
    """
    connection = psycopg2.connect(host=host,
                                  user=user,
                                  database=db,
                                  cursor_factory=extras.DictCursor)
    return connection

def run_query(q,db):
    """
    Runs a query and returns the results as a dataframe
    q (str): psql query
    db (psycopg2 connection object): database 
    """
    results = []
    # conn = db.conn()
    with db.cursor() as cur:
        cur.execute(q)

        for row in cur:
            results.append(dict(row))

    data = pd.DataFrame(results)
    return data


def copy_from_db(q,db,file):
    """
    Runs a query and saves the results to a csv file. 
    q (str): psql query
    db (psycopg2 connection object): database 
    file (str): filename to save the results to 
    """
    # conn = db.conn()
    with db.cursor() as cur:
        outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(q)

        with open(file, 'w') as f:
            cur.copy_expert(outputquery, f)
            
            
def copy_to_db(db,table, file, columns):
    """
    Copies a table from a file to the database
    db (psycopg2 connection object): database 
    table (str): name of the table to create
    file (str): filename to upload data from  
    columns (tuple): names of columns for the table
    """
    with db.cursor() as cur:
        cur.copy_from(file, table, columns=columns)
        
def insert_dataframe(conn, df, table):
    """
    Using psycopg2.extras.execute_batch() to insert a dataframe
    conn (): connection
    df (pandas dataframe): dataframe to insert
    table (str): table to insert to
    """
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    
    # SQL query to execute
    # query  = "INSERT INTO %s(%s) VALUES(%%s,%%s)" % (table, cols)
    query  = "INSERT INTO %s(%s) VALUES(%s)" % (table, cols, ",".join(["%s"]*len(df.columns)))
    
    cursor = conn.cursor()
    extras.execute_batch(cursor, query, tuples)
    conn.commit()
    cursor.close()