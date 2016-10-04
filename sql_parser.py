import sqlparse
import pymysql.cursors
import ConfigParser
import json
import sys

configParser = ConfigParser.RawConfigParser()
configFilePath = r'dbconfig.ini'
configParser.read(configFilePath)
db_username = configParser.get('dbconfig', 'username')
db_password = configParser.get('dbconfig', 'password')

# Connect to the database
connection = pymysql.connect(host='localhost',
                             user=db_username,
                             password=db_password,
                             db='PythonSqlOptimizer',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

try:
    with connection.cursor() as cursor:
        # Create a new record
        sql = "INSERT INTO `users` (`email`, `password`) VALUES (%s, %s)"
        cursor.execute(sql, ('webmaster@python.org', 'very-secret'))

    # connection is not autocommit by default. So you must commit to save
    # your changes.
    connection.commit()

    with connection.cursor() as cursor:
        # Read a single record
        sql = "SELECT `id`, `password` FROM `users` WHERE `email`=%s"
        cursor.execute(sql, ('webmaster@python.org',))
        result = cursor.fetchone()
        print(result)
finally:
    connection.close()