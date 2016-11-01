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
        
        
        
def query(name_table_mapping, conditions):
    # print("condition:", conditions)
    # print(name_table_mapping)
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
    
    # check & modify format of conditions
    default_table_name = None
    for table_name in name_table_mapping.keys():
        default_table_name = table_name
    updated_conditions = []
    if conditions:
        for condition in conditions:
            parsed = sqlparse.parse(condition)[0]
            for token in parsed.tokens:
                tokenStr = str(token)
                if "<>" in tokenStr:
                    tokenStr = tokenStr.replace("<>", "!=", 1)
                    splitted = tokenStr.split("!=", 1)
                    for idx in range(len(splitted)):
                        splitted[idx] = splitted[idx].strip()
                        if "." not in splitted[idx] and splitted[idx] in name_table_mapping[default_table_name].attributes:
                            splitted[idx] = default_table_name + "." + splitted[idx]
                    tokenStr = "!=".join(splitted)
                elif ">=" in tokenStr:
                    splitted = tokenStr.split(">=", 1)
                    for idx in range(len(splitted)):
                        splitted[idx] = splitted[idx].strip()
                        if "." not in splitted[idx] and splitted[idx] in name_table_mapping[default_table_name].attributes:
                            splitted[idx] = default_table_name + "." + splitted[idx]
                    tokenStr = ">=".join(splitted)
                elif "<=" in tokenStr:
                    splitted = tokenStr.split("<=", 1)
                    for idx in range(len(splitted)):
                        splitted[idx] = splitted[idx].strip()
                        if "." not in splitted[idx] and splitted[idx] in name_table_mapping[default_table_name].attributes:
                            splitted[idx] = default_table_name + "." + splitted[idx]
                    tokenStr = "<=".join(splitted)
                elif "=" in tokenStr:
                    tokenStr = tokenStr.replace("=", "==", 1)
                    splitted = tokenStr.split("==", 1)
                    for idx in range(len(splitted)):
                        splitted[idx] = splitted[idx].strip()
                        if "." not in splitted[idx] and splitted[idx] in name_table_mapping[default_table_name].attributes:
                            splitted[idx] = default_table_name + "." + splitted[idx]
                    tokenStr = "==".join(splitted)
                updated_conditions.append(tokenStr)
                
    # does it need to pass conditions to join_table function ?
    # hence it can cut some branches while traversing
    # joined = join_tables(name_table_mapping)
    query_str = generate_query(name_table_mapping, updated_conditions)
    #print("query str : ", query_str)
    # result = eval(query_str)
    # result = eval("[(s1,s2) for s1 in name_table_mapping['s1'].data for s2 in name_table_mapping['s2'].data  if s1.age==s2.age and s1.name!=s2.name ]")
    # g = globals()
    # g['name_table_mapping'] = name_table_mapping
    # result = eval("[(s1,s2) for s1 in name_table_mapping['s1'].data for s2 in name_table_mapping['s2'].data  if s1.age==s2.age and s1.name!=s2.name ]", {"name_table_mapping": name_table_mapping})
    # print(query_str)
    result = eval(query_str, {"name_table_mapping":name_table_mapping, "QUERY":QUERY})
    
    res_attributes = []
    for table_alias in name_table_mapping.keys():
        for attribute in name_table_mapping[table_name].attributes:
            # if table_alias != default_table_name:
            #     res_attributes.append(table_alias+"."+attribute)
            # else:
            #     res_attributes.append(attribute)
            res_attributes.append(table_alias+"."+attribute)
    res_table = table(res_attributes)
    # print("res attri:", res_attributes)
    
    # fill up table content
    for item in result:
        it = iter(item)
        item_obj = Obj()
        for x in it:
            table_alias = x
            partial_data = next(it)
            for attribute in name_table_mapping[table_alias].attributes:
                # if table_alias != default_table_name:
                #     item_obj.__setattr__(table_alias+"."+attribute, partial_data.__getattribute__(attribute))
                # else:
                #     item_obj.__setattr__(attribute, partial_data.__getattribute__(attribute))
                item_obj.__setattr__(table_alias+"."+attribute, partial_data.__getattribute__(attribute))
        res_table.data.add(item_obj)
    return res_table
            
    
def generate_query(name_table_mapping, conditions):
    # for s2 in name_table_mapping["s2"] if s2.age == 26
    query_str = "QUERY('Q', {("
    #query_str = "[{"
    
    for table_name in name_table_mapping.keys():
        query_str += "'" + table_name + "'," + table_name + ","
    query_str = query_str[:-1]
    query_str += ") "
    for table_name in name_table_mapping.keys():
        query_str += "for " + table_name + " in name_table_mapping['" + table_name + "'].data "
    if conditions:
        query_str += " if " 
    for condition in conditions:
        query_str += condition
        query_str += " "
    query_str += "})"
    # query_str += "]"
    return query_str
    
def join_tables(name_table_mapping):
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
    default_table_name = "$$_default_table_name_$$"
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
                default_table_name = 'anno'
            elif type(token).__name__ == "Identifier":
                _table = DB[str(token)]
                join_table_mapping[str(token)] = _table
                stage += 1
                default_table_name = str(token)
            elif type(token).__name__ == "IdentifierList":
                for identifier in token:
                    if type(identifier).__name__ == "Identifier":
                        identifier = str(identifier)
                        if " " in identifier:
                            join_table_mapping[identifier.split(" ")[1]] = identifier.split(" ")[0]
                        else:
                            join_table_mapping[identifier] = identifier
                            default_table_name = identifier
                for table_name in join_table_mapping:
                    join_table_mapping[table_name] = DB[join_table_mapping[table_name]]
                stage += 1
        elif stage == 4 and type(token).__name__ == 'Where':
            conditions = []
            for cond in token:
                if type(cond).__name__ != "Token":
                    conditions.append(str(cond))
                else:
                    if str(cond).strip() != "" and str(cond).upper() != "WHERE":
                        conditions.append(str(cond).lower())
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
    
    res_with_all_col = query(join_table_mapping, conditions)
    # print(res_with_all_col.data)
    attributes_formatted = res_with_all_col.attributes[:]
    for i in range(len(attributes_formatted)):
        if default_table_name+"." in attributes_formatted[i]:
            attributes_formatted[i] = attributes_formatted[i].replace(default_table_name+".", "", 1)
    res_table = table(attributes_formatted if isinstance(attributes, str) else attributes)
    
    #print(res_with_all_col.data)
    for entry in res_with_all_col.data:
        newItem = Obj()
        for attribute in res_with_all_col.attributes:
            new_attribute_name = attribute if default_table_name+"." not in attribute else attribute.replace(default_table_name+".", "", 1)
            if isinstance(attributes, str) or new_attribute_name in attributes:
                newItem.__setattr__(new_attribute_name, entry.__getattribute__(attribute))
        res_table.data.add(newItem)
    
    # filter out attributes
    
    return res_table
        

    
    
parse('CREATE TABLE student (id int, name varchar, age varchar)')
parse('INSERT INTO student VALUES (1,"Jieao1",21)')
parse('INSERT INTO student VALUES (2,"Jieao2",22)')
parse('INSERT INTO student VALUES (3,"Jieao3",23)')
parse('INSERT INTO student VALUES (4,"Jieao4",24)')
parse('INSERT INTO student VALUES (5,"Jieao5",25)')
parse('INSERT INTO student VALUES (6,"Jieao6",26)')
parse('INSERT INTO student VALUES (7,"Jieao7",26)')
parse('INSERT INTO student VALUES (8,"Jieao8",26)')
# selectedData = parse('SELECT name FROM (SELECT * FROM student where id == 2)')
# selectedData = parse('SELECT name FROM (SELECT * FROM student where id == 2)')
# selectedData = parse('SELECT name FROM (SELECT * FROM student where id == 2)')
selectedData = parse('SELECT * FROM student WHERE age <> 26')
print(selectedData.pretty_format())
selectedData = parse('SELECT s1.name FROM student s1, student s2 WHERE s1.age = s2.age AND s1.name <> s2.name')
print(selectedData.pretty_format())
selectedData = parse('SELECT name FROM (SELECT * FROM student where id = 2)')
print(selectedData.pretty_format())
# print(DB)
# print(DB['student'].pretty_format())
# 'select subtable.name from (select * from person p1, person p2 where p1.name = p2.name and p1.country <> p2.country) sub_table'
# 'insert into testTable values (1,2,3)'
# 'CREATE TABLE student (id int, name varchar(255), country varchar(255))'


print("TOTAL")
print("--- %s seconds ---" % time_total)