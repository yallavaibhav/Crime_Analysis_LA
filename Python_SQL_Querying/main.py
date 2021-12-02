# import mysql.connector
# from getpass import getpass
# from mysql.connector import connect, Error
# #
# try:
#     with connect(
#         host="localhost",
#         user=input("Enter username: "),
#         password=input("Enter password: "),
#     ) as connection:
#         create_db_query = "CREATE DATABASE Crime_Analysis"
#         with connection.cursor() as cursor:
#             cursor.execute(create_db_query)
# except Error as e:
#     print(e)

# import  mysql.connector
# from getpass import getpass
# from mysql.connector import connect, Error
# try:
#     with connect(
#         host="localhost",
#         user=input("Enter username: "),
#         password=input("Enter password: "),
#         database="Crime_Analysis",
#     ) as connection:
#         print(connection)
# except Error as e:
#     print(e)


import mysql.connector
mydb = mysql.connector.connect(
 host="localhost",
        user=input("Enter username: "),
        password=input("Enter password: "),
        database="Crime_Analysis",
)
#     ) as connection:
#         print(connection)
# except Error as e:
#     print(e)

mycursor = mydb.cursor()
create_crime_tables = """
CREATE OR REPLACE  TABLE crime_data_TF(
     DR_NO integer primary key,DATE_OCC DATE, area_id integer,area_name string,
     crm_id integer,crm_cd_desc string,vict_age integer,VICT_SEX string,VICT_DESCENT string,
     premise_id integer,premise_desc string,
  weapon_used_id integer,weapon_desc string, crime_status string, status_desc string,
  LOCATION string,CROSS_STREET string,latitude integer,longitude integer,id integer)
"""
with connection.cursor() as cursor:
    cursor.execute(create_crime_tables)
    connection.commit()
mydb.close()
#
# create_crime_tables =""""
# create or replace table VICTIM_descent(Vict_Descent string primary key,
# Vict_Descent_desc string)"""

# create_crime_tables =""""
# create or replace table VICTIM_SEX(Vict_SEX_code string primary key,Vict_SEX_desc string);"""




# import pandas as pd
# mycursor = mydb.cursor()
# crime_data_TF = pd.read_csv(r'C:\Users\yalla\Downloads\Crime_TF.csv')
# for i,row in crime_data_TF.iterrows():
#     sql = "INSERT INTO Crime_Analysis.crime_data_TF VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
#     mycursor.execute(sql,tuple(row))
#     print("Record Inserted")
#     mydb.commit()















import  mysql.connector
from getpass import getpass
from mysql.connector import connect, Error
try:
    with connect(
        host="localhost",
        user=input("Enter username: "),
        password=input("Enter password: "),
        database="Crime_database",
    ) as connection:
        print(connection)
except Error as e:
    print(e)



with connection.cursor() as cursor:
    cursor.execute(create_crime_tables)
    connection.commit()
create_crime_tables = """
CREATE OR REPLACE  TABLE crime_data_TF(
     DR_NO integer primary key,DATE_OCC DATE, area_id integer,area_name string,
     crm_id integer,crm_cd_desc string,vict_age integer,VICT_SEX string,VICT_DESCENT string,
     premise_id integer,premise_desc string,
  weapon_used_id integer,weapon_desc string, crime_status string, status_desc string,
  LOCATION string,CROSS_STREET string,latitude integer,longitude integer,id integer)
"""
mydb.close()




