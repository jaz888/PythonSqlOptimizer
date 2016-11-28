import sys
sys.path.append('/root/Dev/incoq2')
from incoq.runtime import *
# student_table : {Top}
student_table = Set()

import sqlparse
import pymysql.cursors
import configparser
import json
import re
import os
from collections import namedtuple
import time
def makeStudent(id, name, age):
    # Cost: O(1)
    #       O(1)
    s = Obj()
    s.id = id
    s.name = name
    s.age = age
    return s

_v1 = makeStudent(1, 'Jieao1', 21)
student_table.add(_v1)
_v2 = makeStudent(2, 'Jieao2', 22)
student_table.add(_v2)
_v3 = makeStudent(3, 'Jieao3', 23)
student_table.add(_v3)
_v4 = makeStudent(4, 'Jieao4', 23)
student_table.add(_v4)
_v5 = makeStudent(5, 'Jieao5', 25)
student_table.add(_v5)
def q1(table):
    # Cost: O((? + res))
    #       O((? + res))
    res = {s for s in table if (s.age != 23)}
    resTable = Set()
    for s in res:
        resTable.add(s)
    return resTable

print(q1(student_table))
def q2(table):
    # Cost: O((? + res))
    #       O((? + res))
    res = {s1 for s1 in table for s2 in table if ((s1.age == s2.age) and (s1.name != s2.name))}
    resTable = Set()
    for s in res:
        resTable.add(s)
    return resTable

print(q2(student_table))
