import os
from sql_parser import DB, sql_incoq_mapping

db = DB()
db.execute('CREATE TABLE student (id int, name varchar, age varchar)')
db.execute('INSERT INTO student VALUES (1,"Jieao1",21)')
db.execute('INSERT INTO student VALUES (2,"Jieao2",22)')
db.execute('INSERT INTO student VALUES (3,"Jieao3",23)')
db.execute('INSERT INTO student VALUES (4,"Jieao4",24)')
db.execute('INSERT INTO student VALUES (5,"Jieao5",25)')
db.execute('INSERT INTO student VALUES (6,"Jieao6",26)')
db.execute('INSERT INTO student VALUES (7,"Jieao7",26)')
db.execute('INSERT INTO student VALUES (8,"Jieao8",26)')
selectedData = db.execute('SELECT * FROM student WHERE age <> 26')
selectedData = db.execute('SELECT s1.name FROM student s1, student s2 WHERE s1.age = s2.age AND s1.name <> s2.name')
selectedData = db.execute('SELECT name FROM (SELECT * FROM student where id = 2)')



# if len(sql_incoq_mapping) == 0:
#     with open('sql_parser.py', 'r') as source_code_file:
#         os.system('export PYENV_ROOT="/root/.pyenv" && export PATH="/root/.pyenv/bin:$PATH" && eval "$(pyenv init -)" &&cd .. && cd incoq-mars && pyenv local 3.4.3 && python -m incoq ../PythonSqlOptimizer/middle.py ../PythonSqlOptimizer/middle2.py')
#         with open('middle2.py', 'r') as middle_source_code_file:
#             middle_source_code = middle_source_code_file.read()
#             with open('sql_parser_incoq.py', 'w+') as final_code_file:
#                 final_code_file.write(source_code_file.read().replace('## REPLACE ME ##', "#"+middle_source_code, 1).replace('open("middle.py", "w+")', '', 1))

if len(sql_incoq_mapping) == 0:
    with open('sql_parser.py', 'r') as source:
        with open('middle.py', 'r') as middle:
            with open('sql_parser_obj_query.py', 'w+') as final:
                final.write(source.read().replace("## REPLACE ME ##", middle.read(), 1).replace('open("middle.py", "w+")', '', 1))