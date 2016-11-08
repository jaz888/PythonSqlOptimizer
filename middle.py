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
	return QUERY('Q8', {('student',student) for student in name_table_mapping['student'].data  if student['age']!=26 })
sql_incoq_mapping['SELECT * FROM student WHERE age <> 26'] = func8
def func9(name_table_mapping, QUERY):
	return QUERY('Q9', {('s2',s2,'s1',s1) for s2 in name_table_mapping['s2'].data for s1 in name_table_mapping['s1'].data  if s1['age']==s2['age'] and s1['name']!=s2['name'] })
sql_incoq_mapping['SELECT s1.name FROM student s1, student s2 WHERE s1.age = s2.age AND s1.name <> s2.name'] = func9
def func10(name_table_mapping, QUERY):
	return QUERY('Q10', {('student',student) for student in name_table_mapping['student'].data  if student['id']==2 })
sql_incoq_mapping['SELECT * FROM student where id = 2'] = func10
def func11(name_table_mapping, QUERY):
	return QUERY('Q11', {('anno',anno) for anno in name_table_mapping['anno'].data })
sql_incoq_mapping['SELECT name FROM (SELECT * FROM student where id = 2)'] = func11
