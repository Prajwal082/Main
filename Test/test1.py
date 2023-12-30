# #
# #     if isinstance(json_data, dict):
# #         for key, value in json_data.items():
# #             leaf_nodes.extend(find_leaf_nodes(value))
# #     elif isinstance(json_data, list):
# #         for item in json_data:
# #             leaf_nodes.extend(find_leaf_nodes(item))
# #     else:
# #         # This is a leaf node, so we add it to the list
# #         leaf_nodes.append(json_data)

# #     return leaf_nodes
# # ''

# # # Sample JSON data
# json_data = {
#     "ssms" : {
#         "cred" : {
#             "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
#             "url" : "jdbc:sqlserver://DESKTOP-0A2HT13;databaseName={Database};",
#             "user" : "prajwal",
#             "password" : 789
#         },

#         "dbtable" : "dbo.file_list",
#         "pyodbc" : "mssql+pyodbc://{user}:{password}@DESKTOP-0A2HT13/Databricks?driver=ODBC Driver 17 for SQL Server"
#     },

#     "directory" : {
#         "src" : "D:\\Databricks\\SRC\\Delivery_data",
#         "syspath" : "d:\\Databricks\\Vsprojects\\"
#     }
# }
# # leaf_nodes = find_leaf_nodes(json_data,'ssms.cred')
# # print(leaf_nodes)

# # sting = ['ssms','cred'] 
# # sting = '.'.join(sting[1:])
# # print(sting)


