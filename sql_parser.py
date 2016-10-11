import sqlparse
import pymysql.cursors
import configparser
import json
import sys
from collections import namedtuple
sys.path.append("/root/Dev/incoq")
from incoq.runtime import *

# configParser = configparser.RawConfigParser()
# configFilePath = r'dbconfig.ini'
# configParser.read(configFilePath)
# db_username = configParser.get('dbconfig', 'username')
# db_password = configParser.get('dbconfig', 'password')

# # Connect to the database
# connection = pymysql.connect(host='localhost',
#                              user=db_username,
#                              password=db_password,
#                              db='dist',
#                              charset='utf8mb4',
#                              cursorclass=pymysql.cursors.DictCursor)

# cursor = connection.cursor()
# sql = "SELECT DISTINCT p1.developer FROM projects p1, projects p2 WHERE p1.developer = p2.developer AND p1.title <> p2.title"
# cursor.execute(sql)
# result = cursor.fetchall()
# for res in result:
#     pass
#     #print(res)
# connection.close()


DB = dict()
class table:
    def __init__(self, attributes):
        self.data = Set()
        self.attributes = [a for a in attributes]
    def addItem(self, item):
        newItem = Obj()
        idx = 0
        for attribute in self.attributes:
            newItem.__setattr__(attribute, item[idx])
            idx += 1
        self.data.add(newItem)
    def pretty_format(self):
        res = []
        for item in self.data:
            res.append([item.__getattribute__(attribute) for attribute in self.attributes])
        return str(res)
        
def parse(sql):
    parsed = sqlparse.parse(sql)[0]
    token = parsed.tokens[0]
    if type(token).__name__ == 'Token' and str(token).upper() == 'INSERT':
        return insert_into(parsed)
    elif type(token).__name__ == 'Token' and str(token).upper() == 'SELECT':
        return select_from(parsed)
    elif type(token).__name__ == 'Token' and str(token).upper() == 'CREATE':
        return create_table(parsed)
        
# def resolve_identifier(sql):

def insert_into(parsed):
    setTable = False
    setValue = False
    tableName = None
    value = None
    for token in parsed.tokens:
        if type(token).__name__ == 'Token' and str(token).upper() == 'INTO':
            setTable = True
        elif type(token).__name__ == 'Token' and str(token).upper() == 'VALUES':
            setValue = True
        elif type(token).__name__ == 'Identifier':
            if setTable:
                tableName = str(token)
        elif type(token).__name__ == 'Parenthesis':
            if setValue:
                value = eval(str(token))
                DB[tableName].addItem(value)
                
                
def create_table(parsed):
    setTable = False
    tableName = None
    for token in parsed.tokens:
        if type(token).__name__ == 'Token' and str(token).upper() == 'TABLE':
            setTable = True
        elif type(token).__name__ == 'Identifier':
            if setTable:
                tableName = str(token)
        elif type(token).__name__ == 'Parenthesis':
            attributes = []
            for sub_token in token.tokens:
                if type(sub_token).__name__ == 'Identifier':
                    attributes.append(str(sub_token))
            DB[tableName] = table(attributes)
def select_from(parsed):
    pass
    
    
parse('CREATE TABLE student (id int, name varchar, country varchar)')
parse('INSERT INTO student VALUES (1,"Jieao","China")')
parse('INSERT INTO student VALUES (2,"Zhu","USA")')
print(DB)
print(DB['student'].pretty_format())
# 'select subtable.name from (select * from person p1, person p2 where p1.name = p2.name and p1.country <> p2.country) sub_table'
# 'insert into testTable values (1,2,3)'
# 'CREATE TABLE student (id int, name varchar(255), country varchar(255))'