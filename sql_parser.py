import sqlparse
import pymysql.cursors
import ConfigParser
import json
import sys

configParser = ConfigParser.RawConfigParser()
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
    print(res)
connection.close()