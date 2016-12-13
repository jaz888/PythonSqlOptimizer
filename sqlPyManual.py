import sys
sys.path.append("/root/Dev/incoq2")
from incoq.runtime import *
CONFIG(
    obj_domain = 'true',
)
import sqlparse
import pymysql.cursors
import configparser
import json
import re
import os
from collections import namedtuple
import time



def main(): 
    # CREATE TABLE student (id int, name varchar, age varchar)
    student_table = Set()
    
    
    # def makeStudent(id, name, age):
    #     s = Obj()
    #     s.id = id
    #     s.name = name
    #     s.age = age
    #     return s
        
    # INSERT INTO student VALUES (1,"Jieao1",21)
    student01 = Obj()
    student01.id = 1
    student01.name = 'name1'
    student01.age = 21
    student_table.add( student01 )
    
    # INSERT INTO student VALUES (2,"Jieao2",22)
    student02 = Obj()
    student02.id = 2
    student02.name = 'name2'
    student02.age = 22
    student_table.add( student02 )
    
    # INSERT INTO student VALUES (3,"Jieao3",23)
    student03 = Obj()
    student03.id = 3
    student03.name = 'name3'
    student03.age = 23
    student_table.add( student03 )
    
    # INSERT INTO student VALUES (4,"Jieao4",23)
    student04 = Obj()
    student04.id = 4
    student04.name = 'name4'
    student04.age = 23
    student_table.add( student04 )
    
    # INSERT INTO student VALUES (5,"Jieao5",25)
    student05 = Obj()
    student05.id = 5
    student05.name = 'name5'
    student05.age = 25
    student_table.add( student05 )
    
    # SELECT * FROM student WHERE age <> 26
    res = QUERY('Q1', {s for s in student_table  if s.age !=23 })
    resTable = Set()
    for s in res:
        resTable.add(s)
    print(resTable)
    
    # SELECT s1.name FROM student s1, student s2 WHERE s1.age = s2.age AND s1.name <> s2.name
    res = QUERY('Q2', {s1 for s1 in student_table for s2 in student_table if s1.age == s2.age and s1.name != s2.name})
    resTable = Set()
    for s in res:
        resTable.add(s)
    print(resTable)

if (__name__ == '__main__'):
    main()