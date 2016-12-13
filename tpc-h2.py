from incoq.runtime import *
import sys
sys.path.append('/root/Dev/incoq2')
import sqlparse
import pymysql.cursors
import configparser
import json
import re
import os
from collections import namedtuple
import time
import datetime
def main():
    # Cost: O((? + (groups * resFull)))
    #       O((?^2 * resFull))
    part = Set()
    supplier = Set()
    partsupp = Set()
    customer = Set()
    orders = Set()
    lineitem = Set()
    nation = Set()
    region = Set()
    tmpobj = Obj()
    tmpobj.l_orderkey = 100
    tmpobj.l_partkey = 100
    tmpobj.l_suppkey = 100
    tmpobj.l_linenumber = 100
    tmpobj.l_quantity = 100.0
    tmpobj.l_extendedprice = 100.0
    tmpobj.l_discount = 20.0
    tmpobj.l_tax = 10.0
    tmpobj.l_returnflag = 'Y'
    tmpobj.l_linestatus = 'Y'
    tmpobj.l_shipdate = datetime.date(2003, 8, 6)
    tmpobj.l_commitdate = datetime.date(2003, 8, 7)
    tmpobj.l_receiptdate = datetime.date(2003, 8, 8)
    tmpobj.l_shipinstruct = 'SHIPINSTRUCT'
    tmpobj.l_shipmode = 'EXP'
    tmpobj.l_comment = 'NONE'
    lineitem.add(tmpobj)
    date_1 = datetime.date(2200, 12, 1)
    resFull = {item for item in lineitem if (item.l_shipdate <= date_1)}
    groups = {(item.l_returnflag, item.l_linestatus) for item in resFull}
    res = Set()
    for group in groups:
        sum_base_price = 0.0
        sum_qty = 0.0
        sum_charge = 0.0
        count = 0
        for item in resFull:
            if ((item.l_returnflag == group[0]) and (item.l_linestatus == group[1])):
                sum_base_price = (sum_base_price + item.l_extendedprice)
                sum_qty = (sum_qty + item.l_quantity)
                sum_disc_price = (sum_disc_price + (item.l_extendedprice * (1.0 - item.l_discount)))
                sum_charge = (sum_charge + ((item.l_extendedprice * (1.0 - item.l_discount)) * (1.0 + item.l_tax)))
                sum_disc = (sum_disc + item.l_discount)
                count = (count + 1)
        avg_qty = (sum_qty / count)
        avg_price = (sum_base_price / count)
        avg_disc = (sum_disc / count)
        tmpObj = Obj()
        tmpObj.l_returnflag = group[0]
        tmpObj.l_returnflag = group[1]
        tmpObj.sum_qty = sum_qty
        tmpObj.sum_base_price = sum_base_price
        tmpObj.sum_disc_price = sum_disc_price
        tmpObj.sum_charge = sum_charge
        tmpObj.avg_qty = avg_qty
        tmpObj.avg_price = avg_price
        tmpObj.avg_disc = avg_disc
        tmpObj.count_order = count
        res.add(tmpObj)
    print(res)

if (__name__ == '__main__'):
    main()
