import sqlparse
import pymysql.cursors
import configparser
import json
import sys
import re
from collections import namedtuple
import time
sys.path.append("/root/Dev/incoq")
from incoq.runtime import *

time_total = 0.0

def running_time(func):
    def wrapper(*args):
        global time_total
        start_time = time.time()
        res = func(*args)
        print(*args)
        consumed = time.time() - start_time
        print("--- %s seconds ---" % consumed)
        time_total += consumed
        print()
        return res
    return wrapper
    
    
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
        
        
        
def query(name_table_mapping, cond):
    # selection_exp = "QUERY('Q', {entry for entry in self.data " + cond + "})"
    # # print(selection_exp)
    # resEntrySet = eval(selection_exp)
    # resTable = table(self.attributes)
    # for entry in resEntrySet:
    #     newItem = Obj()
    #     for attribute in self.attributes:
    #         newItem.__setattr__(attribute, entry.__getattribute__(attribute))
    #     resTable.data.add(newItem)
    # return resTable
    
    # make a joined table
    pass
    
    
    

@running_time 
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
    conditions = None
    join_table_mapping = dict()
    for token in parsed.tokens:
        # print(token)
        # print("type",type(token).__name__)
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
        elif stage == 1 and type(token).__name__ == 'Token' and str(token) == "*":
            attributes = str(token)
            stage += 1
        elif stage == 2 and str(token).upper() == 'FROM':
            stage += 1
        elif stage == 3:
            if type(token).__name__ == "Parenthesis":
                partial_sql = re.search( "\((.*)\)" ,str(token)).group(1)
                _table = parse(partial_sql)
                join_table_mapping['anno'] = _table
                stage += 1
            elif type(token).__name__ == "Identifier":
                _table = DB[str(token)]
                join_table_mapping[str(token)] = _table
                stage += 1
            elif type(token).__name__ == "IdentifierList":
                for identifier in token:
                    if type(identifier).__name__ == "Identifier":
                        identifier = str(identifier)
                        if " " in identifier:
                            join_table_mapping[identifier.split(" ")[1]] = identifier.split(" ")[0]
                        else:
                            join_table_mapping[identifier] = identifier
                for table_name in join_table_mapping:
                    join_table_mapping[table_name] = DB[join_table_mapping[table_name]]
                stage += 1
        elif stage == 4 and type(token).__name__ == 'Where':
            conditions = []
            for cond in token:
                if type(cond).__name__ != "Token":
                    conditions.append(str(cond))
            stage += 1
        else:
            pass
    # res_with_all_col = None
    # if(not if_exp):
    #     res_with_all_col = _table
    # else:
    #     res_with_all_col = _table.select(if_exp)
    # res_table = table(res_with_all_col.attributes if isinstance(attributes, str) else attributes) 
    # for entry in res_with_all_col.data:
    #     newItem = Obj()
    #     for attribute in res_table.attributes:
    #         newItem.__setattr__(attribute, entry.__getattribute__(attribute))
    #     res_table.data.add(newItem)
    
    res_table = query(join_table_mapping, conditions)
    return res_table
        

    
    
parse('CREATE TABLE student (id int, name varchar, age varchar)')
parse('INSERT INTO student VALUES (1,"Jieao1","21")')
parse('INSERT INTO student VALUES (2,"Jieao2","22")')
parse('INSERT INTO student VALUES (3,"Jieao3","23")')
parse('INSERT INTO student VALUES (4,"Jieao4","24")')
parse('INSERT INTO student VALUES (5,"Jieao5","25")')
parse('INSERT INTO student VALUES (6,"Jieao6","26")')
parse('INSERT INTO student VALUES (7,"Jieao7","26")')
parse('INSERT INTO student VALUES (8,"Jieao8","26")')
# selectedData = parse('SELECT name FROM (SELECT * FROM student where id == 2)')
# selectedData = parse('SELECT name FROM (SELECT * FROM student where id == 2)')
# selectedData = parse('SELECT name FROM (SELECT * FROM student where id == 2)')
selectedData = parse('SELECT * FROM student s1, student s2 WHERE s1.age == s2.age AND s1.name != s2.name')

# selectedData = parse('SELECT name FROM (SELECT * FROM student where id == 2)')
# print("TOTAL")
# print("--- %s seconds ---" % time_total)
# print(selectedData.pretty_format())
# print(DB)
# print(DB['student'].pretty_format())
# 'select subtable.name from (select * from person p1, person p2 where p1.name = p2.name and p1.country <> p2.country) sub_table'
# 'insert into testTable values (1,2,3)'
# 'CREATE TABLE student (id int, name varchar(255), country varchar(255))'