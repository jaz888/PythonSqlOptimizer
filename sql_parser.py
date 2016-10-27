import sqlparse
import pymysql.cursors
import configparser
import json
import sys
import re
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
    def select(self, cond):
        selection_exp = "QUERY('Q', {entry for entry in self.data " + cond + "})"
        print(selection_exp)
        resEntrySet = eval(selection_exp)
        resTable = table(self.attributes)
        for entry in resEntrySet:
            newItem = Obj()
            for attribute in self.attributes:
                newItem.__setattr__(attribute, entry.__getattribute__(attribute))
            resTable.data.add(newItem)
        return resTable
        
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
            
# should return a table
def select_from(parsed):
    if len(parsed.tokens) == 1:
        return DB[str(parsed[0])]
    attributes = []
    stage = 0
    _table = None
    where = None
    for token in parsed.tokens:
        #print(token)
        #print(type(token).__name__)
        if type(token).__name__ == 'Token' and str(token).upper() == 'SELECT':
            stage += 1
        elif stage == 1 and type(token).__name__ == 'IdentifierList':
            for identifier in token:
                if type(identifier).__name__ == "Identifier":
                    attributes.append(str(identifier))
            stage += 1
        elif stage == 1 and type(token).__name__ == 'Identifier':
            attributes.append(str(token))
            stage += 1
        elif stage == 2 and str(token).upper() == 'FROM':
            stage += 1
        elif stage == 3:
            if type(token).__name__ == "Parenthesis":
                _table = select_from(parse(re.search( "\((.*)\)" ,str(token)).group(1)))
                stage += 1
            elif type(token).__name__ == "Identifier":
                
                _table = DB[str(token)]
                stage += 1
        elif stage == 4 and type(token).__name__ == 'Where':
            if_exp = "if "
            for cond in token:
                if type(cond).__name__ != "Token":
                    if "entry." in if_exp:
                        if_exp += " and "
                    if_exp += "entry."+str(cond)
            stage += 1
        else:
            pass
        
    res = _table.select(if_exp)
    
    return res
        
        
    
    
parse('CREATE TABLE student (id int, name varchar, country varchar)')
parse('INSERT INTO student VALUES (1,"Jieao","China")')
parse('INSERT INTO student VALUES (2,"Zhu","USA")')
selectedData = parse('SELECT name FROM student where id == 1')
print(selectedData)
print(selectedData.pretty_format())
# print(DB)
# print(DB['student'].pretty_format())
# 'select subtable.name from (select * from person p1, person p2 where p1.name = p2.name and p1.country <> p2.country) sub_table'
# 'insert into testTable values (1,2,3)'
# 'CREATE TABLE student (id int, name varchar(255), country varchar(255))'