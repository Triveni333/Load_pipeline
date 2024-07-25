import psycopg2 as pg
from psycopg2 import Error
import csv

def createConnection(dbname, username, password, host, port):
    connection = None
    try:
        connection = pg.connect(
            database = dbname,
            user = username,
            password = password,
            host = host,
            port = port
        )
        print("DB connection successful")
    except Error as e:
        print("Error {e} occured")
    return connection
        
  
if __name__ == "__main__" : 
    connection = createConnection("TestDatabase", "postgres", "root123", "localhost", "5432")
    cursor = connection.cursor()
    result = cursor.execute("""CREATE TABLE IF NOT EXISTS Series( id SERIAL, Series_reference varchar(40), Period float, Data_value float, STATUS char(10), UNITS varchar(20))""")
    print(result)
    
    rows = []
    with open('No missing data values.csv', 'r') as file:
        reader = csv.reader(file)
        next(reader)
        for i in reader:
            rows.append(i)
    
    for row in rows :
        cursor.execute("""INSERT INTO Series("series_reference", "period", "data_value", "status", "units") values(%s, %s, %s, %s, %s)""",(row[1], row[2], row[3], row[5], row[6])) 
      
    connection.commit()
