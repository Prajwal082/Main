# class Triangle:
    
# 	def __init__(self,b,h):
# 		self.base = b
# 		self.height = h

# 	# def __str__(self) -> str:
# 	# 	return '{},{}'.format(self.base,self.height)
	
# 	def trianglearea(self):
# 		return self.base * self.height *0.5
	

# 	def addsum(self):
# 		return  self.trianglearea(Triangle(6))

# ob = Triangle(2,2)
# # print(ob)
# # print(ob.trianglearea())
# # print(ob.addsum())
# ob.trianglearea()

import pyodbc

cnxn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};"
                      "Server=DESKTOP-0A2HT13;"
                      "Database=Databricks;"
                      "UID=prajwal;"
                      "PWD=Prajwal082;"
                      "Trusted_Connection=yes;")


cursor = cnxn.cursor()
cursor.execute('SELECT * FROM [dbo].[Customer]')

for row in cursor:
    print('row = %r' % (row,))

# import pyodbc
# conn_str = pyodbc.connect(
#     'Driver={PostgreSQL ODBC Driver(ANSI)};'
#     'Server=192.168.10.6;'
#     'Port=5432;'
#     'Database=Test;'
#     'UID=postgres;'
#     'PWD=1234;'
# )
# conn = pyodbc.connect(conn_str, autocommit=True) # Error occurs here
# cursor = cnxn.cursor()
# cursor.execute('select * from students')

# for row in cursor:
#     print('row = %r' % (row,))