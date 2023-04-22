import pyodbc

# cnxn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};"
#                       "Server=DESKTOP-0A2HT13;"
#                       "Database=Databricks;"
#                       "UID=prajwal;"
#                       "PWD=Prajwal082;"
#                       "Trusted_Connection=yes;")


# cursor = cnxn.cursor()
# cursor.execute('SELECT * FROM [dbo].[Customer]')

# for row in cursor:
#     print('row = %r' % (row,))

import pyodbc
conn_str = pyodbc.connect(
    'Driver={org.postgresql.Driver};'
    'Server=localhost;'
    'Port=5432;'
    'Database=Test;'
    'UID=postgres;'
    'PWD=1234;'
)
conn = pyodbc.connect(conn_str, autocommit=True) # Error occurs here
cursor = cnxn.cursor()
cursor.execute('select * from students')

for row in cursor:
    print('row = %r' % (row,))