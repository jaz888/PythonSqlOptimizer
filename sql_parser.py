import sqlparse
import pymysql.cursors
import configparser
import json
import sys

configParser = configparser.RawConfigParser()
configFilePath = r'dbconfig.ini'
configParser.read(configFilePath)
db_username = configParser.get('dbconfig', 'username')
db_password = configParser.get('dbconfig', 'password')

# Connect to the database
connection = pymysql.connect(host='localhost',
                             user=db_username,
                             password=db_password,
                             db='dist',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

cursor = connection.cursor()
sql = "SELECT DISTINCT p1.developer FROM projects p1, projects p2 WHERE p1.developer = p2.developer AND p1.title <> p2.title"
cursor.execute(sql)
result = cursor.fetchall()
for res in result:
    pass
    #print(res)
connection.close()



# try to store data in namedtuple
def select_from_tables_where(sql):
    parsed = sqlparse.parse(sql)[0]
    for token in parsed.tokens:
        print(type(token) , ': ' , token)
        
select_from_tables_where('select subtable.name from (select * from person p1, person p2 where p1.name = p2.name and p1.country <> p2.country) sub_table')