import sys
sys.path.append("/root/Dev/incoq2")
from incoq.runtime import *
import sqlparse
import pymysql.cursors
import configparser
import json
import re
import os
from collections import namedtuple
import time
        
# CREATE TABLE student (id int, name varchar, age varchar)
student_table = Set()


def makeStudent(id, name, age):
    s = Obj()
    s.id = id
    s.name = name
    s.age = age
    return s
    
# INSERT INTO student VALUES (1,"Jieao1",21)
student_table.add( makeStudent(1, 'Jieao1', 21) )

# INSERT INTO student VALUES (2,"Jieao2",22)
student_table.add( makeStudent(2, 'Jieao2', 22) )

# INSERT INTO student VALUES (3,"Jieao3",23)
student_table.add( makeStudent(3, 'Jieao3', 23) )

# INSERT INTO student VALUES (4,"Jieao4",23)
student_table.add( makeStudent(4, 'Jieao4', 23) )

# INSERT INTO student VALUES (5,"Jieao5",25)
student_table.add( makeStudent(5, 'Jieao5', 25) )

# SELECT * FROM student WHERE age <> 26
def q1(table):
    res = QUERY('Q1', {s for s in table  if s.age !=23 })
    resTable = Set()
    for s in res:
        resTable.add(s)
    return resTable
print(q1(student_table))

# SELECT s1.name FROM student s1, student s2 WHERE s1.age = s2.age AND s1.name <> s2.name
def q2(table):
    res = QUERY('Q2', {s1 for s1 in table for s2 in table if s1.age == s2.age and s1.name != s2.name})
    resTable = Set()
    for s in res:
        resTable.add(s)
    return resTable
print(q2(student_table))

