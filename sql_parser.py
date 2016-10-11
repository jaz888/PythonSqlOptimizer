import sqlparse
import pymysql.cursors
import configparser
import json
import sys
from collections import namedtuple

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


DB = dict()
class table:
    def __init__(self):
        self.data = set()
    
def parse(sql):
    parsed = sqlparse.parse(sql)[0]
    token = parsed.tokens[0]
    if type(token).__name__ == 'Token' and str(token).upper() == 'INSERT':
        return insert_into(parsed)
    elif type(token).__name__ == 'Token' and str(token).upper() == 'SELECT':
        return select_from(parsed)
        
# def resolve_identifier(sql):

def insert_into(parsed):
    setTable = False
    setValue = False
    table = None
    value = None
    attributes = []
    for token in parsed.tokens:
        if type(token).__name__ == 'Token' and str(token).upper() == 'INTO':
            setTable = True
        elif type(token).__name__ == 'Token' and str(token).upper() == 'VALUES':
            setValue = True
        elif type(token).__name__ == 'Identifier':
            if setTable:
                table = str(token)
        elif type(token).__name__ == 'Parenthesis':
            if setValue:
                value = eval(str(token))
                DB[table].add(value)
                
                
# def create_table(parsed):
    
# def select_from(parsed):
    
DB['testTable'] = set()
parse('insert into testTable values (1,2,3)')
parse('insert into testTable values (2,3,4)')
print(DB['testTable'])
# 'select subtable.name from (select * from person p1, person p2 where p1.name = p2.name and p1.country <> p2.country) sub_table'