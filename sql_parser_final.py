import sqlparse
import pymysql.cursors
import configparser
import json
import sys
import re
from collections import namedtuple
import time
sys.path.append('/root/Dev/incoq2')
time_total = 0.0
open('middle.txt', 'w+')
sql_incoq_mapping = {}
def func0(name_table_mapping, QUERY):
    # Cost: O(?)
    #       O(?)
    return {('student', student) for student in name_table_mapping['student'].data if (student.age != 26)}

sql_incoq_mapping['SELECT * FROM student WHERE age <> 26'] = func0
def func1(name_table_mapping, QUERY):
    # Cost: O(?)
    #       O(?)
    return {('s1', s1, 's2', s2) for s1 in name_table_mapping['s1'].data for s2 in name_table_mapping['s2'].data if ((s1.age == s2.age) and (s1.name != s2.name))}

sql_incoq_mapping['SELECT s1.name FROM student s1, student s2 WHERE s1.age = s2.age AND s1.name <> s2.name'] = func1
def func2(name_table_mapping, QUERY):
    # Cost: O(?)
    #       O(?)
    return {('student', student) for student in name_table_mapping['student'].data if (student.id == 2)}

sql_incoq_mapping['SELECT * FROM student where id = 2'] = func2
def func3(name_table_mapping, QUERY):
    # Cost: O(?)
    #       O(?)
    return {('anno', anno) for anno in name_table_mapping['anno'].data}

sql_incoq_mapping['SELECT name FROM (SELECT * FROM student where id = 2)'] = func3
