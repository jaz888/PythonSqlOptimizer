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
import datetime


# create table


# put all implementations in main function
def main():
# create table part (

# 	p_partkey		serial primary key,
# 	p_name			varchar(55),
# 	p_mfgr			char(25),
# 	p_brand			char(10),
# 	p_type			varchar(25),
# 	p_size			integer,
# 	p_container		char(10),
# 	p_retailprice	decimal,
# 	p_comment		varchar(23)
# );
    part = Set()
    
# create table supplier (
# 	s_suppkey		serial primary key,
# 	s_name			char(25),
# 	s_address		varchar(40),
# 	s_nationkey		bigint not null, -- references n_nationkey
# 	s_phone			char(15),
# 	s_acctbal		decimal,
# 	s_comment		varchar(101)
# );
    supplier = Set()

# create table partsupp (
# 	ps_partkey		bigint not null, -- references p_partkey
# 	ps_suppkey		bigint not null, -- references s_suppkey
# 	ps_availqty		integer,
# 	ps_supplycost	decimal,
# 	ps_comment		varchar(199),
# 	primary key (ps_partkey, ps_suppkey)
# );
    partsupp = Set()
    
# create table customer (
# 	c_custkey		serial primary key,
# 	c_name			varchar(25),
# 	c_address		varchar(40),
# 	c_nationkey		bigint not null, -- references n_nationkey
# 	c_phone			char(15),
# 	c_acctbal		decimal,
# 	c_mktsegment	char(10),
# 	c_comment		varchar(117)
# );
    customer = Set()
    
# create table orders (
# 	o_orderkey		serial primary key,
# 	o_custkey		bigint not null, -- references c_custkey
# 	o_orderstatus	char(1),
# 	o_totalprice	decimal,
# 	o_orderdate		date,
# 	o_orderpriority	char(15),
# 	o_clerk			char(15),
# 	o_shippriority	integer,
# 	o_comment		varchar(79)
# );
    orders = Set()
    
# create table lineitem (
# 	l_orderkey		bigint not null, -- references o_orderkey
# 	l_partkey		bigint not null, -- references p_partkey (compound fk to partsupp)
# 	l_suppkey		bigint not null, -- references s_suppkey (compound fk to partsupp)
# 	l_linenumber	integer,
# 	l_quantity		decimal,
# 	l_extendedprice	decimal,
# 	l_discount		decimal,
# 	l_tax			decimal,
# 	l_returnflag	char(1),
# 	l_linestatus	char(1),
# 	l_shipdate		date,
# 	l_commitdate	date,
# 	l_receiptdate	date,
# 	l_shipinstruct	char(25),
# 	l_shipmode		char(10),
# 	l_comment		varchar(44),
# 	primary key (l_orderkey, l_linenumber)
# );
    lineitem  = Set()
    
# create table nation (
# 	n_nationkey		serial primary key,
# 	n_name			char(25),
# 	n_regionkey		bigint not null,  -- references r_regionkey
# 	n_comment		varchar(152)
# );
    nation = Set()
    
# create table region (
# 	r_regionkey	serial primary key,
# 	r_name		char(25),
# 	r_comment	varchar(152)
# );
    region = Set()
    
    # generate random data
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
    
    
    
# quert NO.1
# SELECT
#     l_returnflag,
#     l_linestatus,
#     sum(l_quantity) as sum_qty,
#     sum(l_extendedprice) as sum_base_price,
#     sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
#     sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
#     avg(l_quantity) as avg_qty,
#     avg(l_extendedprice) as avg_price,
#     avg(l_discount) as avg_disc,
#     count(*) as count_order
# FROM
#     lineitem
# WHERE
#     l_shipdate <= date '1998-12-01' - interval '90' day
# GROUP BY
#     l_returnflag,
#     l_linestatus
# ORDER BY
#     l_returnflag,

    date_1 = (datetime.date(2200, 12, 1) - datetime.timedelta(days=90))
    resFull = QUERY('Q1', {item for item in lineitem  if item.l_shipdate <= date_1})
    groups = {(item.l_returnflag, item.l_linestatus) for item in resFull}
    res = Set()
    for group in groups:
        sum_base_price = sum(item.l_extendedprice for item in resFull if item.l_returnflag == group[0] and item.l_linestatus == group[1])
        sum_qty = sum(item.l_quantity for item in resFull if item.l_returnflag == group[0] and item.l_linestatus == group[1])
        sum_disc_price = sum(item.l_extendedprice * (1.0 - item.l_discount) for item in resFull if item.l_returnflag == group[0] and item.l_linestatus == group[1])
        sum_charge = sum(item.l_extendedprice * (1.0 - item.l_discount) * (1.0 + item.l_tax) for item in resFull if item.l_returnflag == group[0] and item.l_linestatus == group[1])
        
        count = len( [ item for item in resFull if item.l_returnflag == group[0] and item.l_linestatus == group[1] ])
        
        avg_qty =sum_qty / count
        avg_price = sum_base_price / count
        avg_disc = sum(item.l_discount for item in resFull if item.l_returnflag == group[0] and item.l_linestatus == group[1]) / count
        count = len([item for item in resFull if item.l_returnflag == group[0] and item.l_linestatus == group[1]])
        
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
    
    # for item in resFull:
    #     tmpobj = Obj()
    #     tmpobj.l_returnflag = item.l_returnflag
    #     tmpobj.l_linestatus = item.l_linestatus
    #     tmpobj.sum_qty = sum_qty
    #     tmpobj.sum_base_price = sum_base_price
    #     tmpobj.sum_disc_price = item.l_extendedprice * (1 - item.l_discount)
    print(res)
if (__name__ == '__main__'):
    main()