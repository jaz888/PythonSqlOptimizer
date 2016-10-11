import sqlparse
import pymysql.cursors
import configparser
import json
import sys
from collections import namedtuple
sys.path.append("/root/Dev/incoq")
from incoq.runtime import *
x = Obj()
print(dir(x))
x.__setattr__('x', 123)