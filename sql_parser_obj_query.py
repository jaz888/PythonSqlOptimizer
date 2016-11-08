import sqlparse
import pymysql.cursors
import configparser
import json
import sys
import re
import os
from collections import namedtuple
import time
sys.path.append("/root/Dev/incoq2")
from incoq.runtime import *

time_total = 0.0

sql_incoq_mapping = {}


# insert output file below

def func0(name_table_mapping, QUERY):
	newItem = Map()
	newItem['id'] = 1
	newItem['name'] = 'Jieao1'
	newItem['age'] = 21
	return newItem
sql_incoq_mapping['INSERT INTO student VALUES (1,"Jieao1",21)'] = func0
def func1(name_table_mapping, QUERY):
	newItem = Map()
	newItem['id'] = 2
	newItem['name'] = 'Jieao2'
	newItem['age'] = 22
	return newItem
sql_incoq_mapping['INSERT INTO student VALUES (2,"Jieao2",22)'] = func1
def func2(name_table_mapping, QUERY):
	newItem = Map()
	newItem['id'] = 3
	newItem['name'] = 'Jieao3'
	newItem['age'] = 23
	return newItem
sql_incoq_mapping['INSERT INTO student VALUES (3,"Jieao3",23)'] = func2
def func3(name_table_mapping, QUERY):
	newItem = Map()
	newItem['id'] = 4
	newItem['name'] = 'Jieao4'
	newItem['age'] = 24
	return newItem
sql_incoq_mapping['INSERT INTO student VALUES (4,"Jieao4",24)'] = func3
def func4(name_table_mapping, QUERY):
	newItem = Map()
	newItem['id'] = 5
	newItem['name'] = 'Jieao5'
	newItem['age'] = 25
	return newItem
sql_incoq_mapping['INSERT INTO student VALUES (5,"Jieao5",25)'] = func4
def func5(name_table_mapping, QUERY):
	newItem = Map()
	newItem['id'] = 6
	newItem['name'] = 'Jieao6'
	newItem['age'] = 26
	return newItem
sql_incoq_mapping['INSERT INTO student VALUES (6,"Jieao6",26)'] = func5
def func6(name_table_mapping, QUERY):
	newItem = Map()
	newItem['id'] = 7
	newItem['name'] = 'Jieao7'
	newItem['age'] = 26
	return newItem
sql_incoq_mapping['INSERT INTO student VALUES (7,"Jieao7",26)'] = func6
def func7(name_table_mapping, QUERY):
	newItem = Map()
	newItem['id'] = 8
	newItem['name'] = 'Jieao8'
	newItem['age'] = 26
	return newItem
sql_incoq_mapping['INSERT INTO student VALUES (8,"Jieao8",26)'] = func7
def func8(name_table_mapping, QUERY):
	print(name_table_mapping['student'].data)
	return QUERY('Q8', {('student',student) for student in name_table_mapping['student'].data  if student['age']!=26 })
sql_incoq_mapping['SELECT * FROM student WHERE age <> 26'] = func8
def func9(name_table_mapping, QUERY):
	return QUERY('Q9', {('s1',s1,'s2',s2) for s1 in name_table_mapping['s1'].data for s2 in name_table_mapping['s2'].data  if s1['age']==s2['age'] and s1['name']!=s2['name'] })
sql_incoq_mapping['SELECT s1.name FROM student s1, student s2 WHERE s1.age = s2.age AND s1.name <> s2.name'] = func9
def func10(name_table_mapping, QUERY):
	return QUERY('Q10', {('student',student) for student in name_table_mapping['student'].data  if student['id']==2 })
sql_incoq_mapping['SELECT * FROM student where id = 2'] = func10
def func11(name_table_mapping, QUERY):
	return QUERY('Q11', {('anno',anno) for anno in name_table_mapping['anno'].data })
sql_incoq_mapping['SELECT name FROM (SELECT * FROM student where id = 2)'] = func11


# insert output file above


def running_time(func):
    def wrapper(arg1, arg2):
        global time_total
        print()
        start_time = time.time()
        res = func(arg1, arg2)
        print(arg2)
        consumed = time.time() - start_time
        print("--- %s seconds ---" % consumed)
        time_total += consumed
        return res
    return wrapper
    
    
class table:
    def __init__(self, attributes):
        self.data = Set()
        self.attributes = [a for a in attributes]
    def addItem(self, item, sql, db):
        newItem = Map()
        genQuery = []
        genQuery.append("newItem = Map()")
        idx = 0
        for attribute in self.attributes:
            newItem[attribute] = item[idx]
            query_str_temp = "newItem['" + str(attribute) + "'] = "
            if type(item[idx]).__name__ == 'str':
                query_str_temp += "'"
            query_str_temp += str(item[idx])
            if type(item[idx]).__name__ == 'str':
                query_str_temp += "'"
            genQuery.append(query_str_temp)
            idx += 1
        self.data.add(newItem)
        
        with open("middle.py", "a") as myfile:
                myfile.write("def func" + str(db.sql_incoq_mapping_count) + "(name_table_mapping, QUERY):")
                for query_str in genQuery:
                    myfile.write("\n\t" + query_str)
                myfile.write("\n\treturn newItem\n")
                myfile.write("sql_incoq_mapping['" + sql + "'] = func" + str(db.sql_incoq_mapping_count) + "\n")
                db.sql_incoq_mapping_count += 1
    def pretty_format(self):
        res = []
        for item in self.data:
            res.append([item[attribute] for attribute in self.attributes])
        return str(res)
        
class DB:
    def __init__(self):
        self.DB = dict()
        self.sql_incoq_mapping_count = 0
        
    def query(self, name_table_mapping, conditions, sql):
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
        
        if sql not in sql_incoq_mapping:
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
                                    splitted[idx] = default_table_name + "['" + splitted[idx] + "']"
                                else:
                                    left, right = False, False
                                    splitted_by_point = splitted[idx].split(".")
                                    final_exp = []
                                    for part in splitted_by_point:
                                        if right:
                                            final_exp.append("']")
                                        if left:
                                            final_exp.append("['")
                                            right = True
                                        final_exp.append(part)
                                        left = True
                                    if right:
                                        final_exp.append("']")
                                    splitted[idx] = "".join(final_exp)
                            tokenStr = "!=".join(splitted)
                        elif ">=" in tokenStr:
                            splitted = tokenStr.split(">=", 1)
                            for idx in range(len(splitted)):
                                splitted[idx] = splitted[idx].strip()
                                if "." not in splitted[idx] and splitted[idx] in name_table_mapping[default_table_name].attributes:
                                    splitted[idx] = default_table_name + "['" + splitted[idx] + "']"
                                else:
                                    left, right = False, False
                                    splitted_by_point = splitted[idx].split(".")
                                    final_exp = []
                                    for part in splitted_by_point:
                                        if right:
                                            final_exp.append("']")
                                        if left:
                                            final_exp.append("['")
                                            right = True
                                        final_exp.append(part)
                                        left = True
                                    if right:
                                        final_exp.append("']")
                                    splitted[idx] = "".join(final_exp)
                            tokenStr = ">=".join(splitted)
                        elif "<=" in tokenStr:
                            splitted = tokenStr.split("<=", 1)
                            for idx in range(len(splitted)):
                                splitted[idx] = splitted[idx].strip()
                                if "." not in splitted[idx] and splitted[idx] in name_table_mapping[default_table_name].attributes:
                                    splitted[idx] = default_table_name + "['" + splitted[idx] + "']"
                                else:
                                    left, right = False, False
                                    splitted_by_point = splitted[idx].split(".")
                                    final_exp = []
                                    for part in splitted_by_point:
                                        if right:
                                            final_exp.append("']")
                                        if left:
                                            final_exp.append("['")
                                            right = True
                                        final_exp.append(part)
                                        left = True
                                    if right:
                                        final_exp.append("']")
                                    splitted[idx] = "".join(final_exp)
                            tokenStr = "<=".join(splitted)
                        elif "=" in tokenStr:
                            tokenStr = tokenStr.replace("=", "==", 1)
                            splitted = tokenStr.split("==", 1)
                            for idx in range(len(splitted)):
                                splitted[idx] = splitted[idx].strip()
                                if "." not in splitted[idx] and splitted[idx] in name_table_mapping[default_table_name].attributes:
                                    splitted[idx] = default_table_name + "['" + splitted[idx] + "']"
                                else:
                                    left, right = False, False
                                    splitted_by_point = splitted[idx].split(".")
                                    final_exp = []
                                    for part in splitted_by_point:
                                        if right:
                                            final_exp.append("']")
                                        if left:
                                            final_exp.append("['")
                                            right = True
                                        final_exp.append(part)
                                        left = True
                                    if right:
                                        final_exp.append("']")
                                    splitted[idx] = "".join(final_exp)
                            tokenStr = "==".join(splitted)
                        updated_conditions.append(tokenStr)
            print(updated_conditions)
                        
            # does it need to pass conditions to join_table function ?
            # hence it can cut some branches while traversing
            # joined = join_tables(name_table_mapping)
            query_str = self.generate_query(name_table_mapping, updated_conditions)
            #print("query str : ", query_str)
            # result = eval(query_str)
            # result = eval("[(s1,s2) for s1 in name_table_mapping['s1'].data for s2 in name_table_mapping['s2'].data  if s1.age==s2.age and s1.name!=s2.name ]")
            # g = globals()
            # g['name_table_mapping'] = name_table_mapping
            # result = eval("[(s1,s2) for s1 in name_table_mapping['s1'].data for s2 in name_table_mapping['s2'].data  if s1.age==s2.age and s1.name!=s2.name ]", {"name_table_mapping": name_table_mapping})
            # print(query_str)
            print("eval:" + query_str)
            result = eval(query_str, {"name_table_mapping":name_table_mapping, "QUERY":QUERY})
            
            with open("middle.py", "a") as myfile:
                myfile.write("def func" + str(self.sql_incoq_mapping_count) + "(name_table_mapping, QUERY):")
                
                myfile.write("\n\treturn " + query_str)
                myfile.write("\n")
                myfile.write("sql_incoq_mapping['" + sql + "'] = func" + str(self.sql_incoq_mapping_count) + "\n")
                self.sql_incoq_mapping_count += 1
        else:
            result = sql_incoq_mapping[sql](name_table_mapping, QUERY)
        
        res_attributes = []
        for table_alias in name_table_mapping.keys():
            for attribute in name_table_mapping[table_alias].attributes:
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
            item_map = Map()
            for x in it:
                table_alias = x
                partial_data = next(it)
                for attribute in name_table_mapping[table_alias].attributes:
                    # if table_alias != default_table_name:
                    #     item_obj.__setattr__(table_alias+"."+attribute, partial_data.__getattribute__(attribute))
                    # else:
                    #     item_obj.__setattr__(attribute, partial_data.__getattribute__(attribute))
                    item_map[table_alias+"."+attribute] = partial_data[attribute]
            res_table.data.add(item_map)
        return res_table
                
        
    def generate_query(self, name_table_mapping, conditions):
        # for s2 in name_table_mapping["s2"] if s2.age == 26
        query_str = "QUERY('Q" + str(self.sql_incoq_mapping_count) + "', {("
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
        # print(query_str)
        return query_str
        
    def join_tables(name_table_mapping):
        pass
        
        
    
    @running_time 
    def execute(self, sql):
        parsed = sqlparse.parse(sql)[0]
        token = parsed.tokens[0]
        if type(token).__name__ == 'Token' and str(token).upper() == 'INSERT':
            return self.insert_into(parsed, sql)
        elif type(token).__name__ == 'Token' and str(token).upper() == 'SELECT':
            return self.select_from(parsed, sql)
        elif type(token).__name__ == 'Token' and str(token).upper() == 'CREATE':
            return self.create_table(parsed)
            
    # def resolve_identifier(sql):
    
    def insert_into(self, parsed, sql):
        
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
                    if sql not in sql_incoq_mapping:
                        value = eval(str(token))
                        self.DB[tableName].addItem(value, sql, self)
                    else:
                        self.DB[tableName].data.add(sql_incoq_mapping[sql]({}, QUERY))
                    
                    
    def create_table(self, parsed):
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
                self.DB[tableName] = table(attributes)
          
          
    
        
    
    # should return a table
    def select_from(self, parsed, sql):
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
                    _table = self.execute(partial_sql)
                    join_table_mapping['anno'] = _table
                    stage += 1
                    default_table_name = 'anno'
                elif type(token).__name__ == "Identifier":
                    _table = self.DB[str(token)]
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
                        join_table_mapping[table_name] = self.DB[join_table_mapping[table_name]]
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
        
        res_with_all_col = self.query(join_table_mapping, conditions, sql)
        # print(res_with_all_col.data)
        attributes_formatted = res_with_all_col.attributes[:]
        for i in range(len(attributes_formatted)):
            if default_table_name+"." in attributes_formatted[i]:
                attributes_formatted[i] = attributes_formatted[i].replace(default_table_name+".", "", 1)
        res_table = table(attributes_formatted if isinstance(attributes, str) else attributes)
        
        #print(res_with_all_col.data)
        for entry in res_with_all_col.data:
            newItem = Map()
            for attribute in res_with_all_col.attributes:
                new_attribute_name = attribute if default_table_name+"." not in attribute else attribute.replace(default_table_name+".", "", 1)
                if isinstance(attributes, str) or new_attribute_name in attributes:
                    newItem[new_attribute_name] = entry[attribute]
            res_table.data.add(newItem)
        
        # filter out attributes
        
        return res_table
            