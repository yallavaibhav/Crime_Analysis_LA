import mysql.connector
import pandas as pd
import pyodbc

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="root",
    database="Crime_Analysis"
)

mycursor = mydb.cursor()
mycursor.execute("create table VICTIM_descent(Vict_Descent varchar(5) primary key,Vict_Descent_desc varchar(50))");
# mycursor.execute("create table VICTIM_SEX(Vict_SEX_code varchar(5) primary key,Vict_SEX_desc varchar(10))");
mycursor.execute("CREATE TABLE crime_data_TF(DR_NO INT,DATE_OCC DATE, area_id INT,area_name varchar(50),crm_id INT,"
                 "crm_cd_desc varchar(250),vict_age INT,VICT_SEX varchar(5),VICT_DESCENT varchar(5),premise_id INT,"
                 "premise_desc varchar(250),weapon_used_id INT, crime_status varchar(50), "
                 "status_desc varchar(50),LOCATION varchar(50),latitude INT,"
                 "longitude integer,id INT,FOREIGN KEY (VICT_DESCENT) REFERENCES VICTIM_descent(Vict_Descent),"
                 "FOREIGN KEY (VICT_SEX) REFERENCES VICTIM_SEX(Vict_SEX_code))");

df = pd.read_csv(r'D:\Database\Project\VICT_DESC.csv')
for i, row in df.iterrows():
    # here %S means string values
    # print(tuple(row))
    sql = "INSERT INTO Crime_Analysis.VICTIM_descent VALUES (%s,%s)"
    mycursor.execute(sql, tuple(row))
    print("Record inserted")
    # the connection is not auto committed by default, so we must commit to save our changes
    mydb.commit()


df = pd.read_csv(r'D:\Database\Project\VICT_sex.csv')
for i, row in df.iterrows():
    # here %S means string values
    # print(tuple(row))
    sql = "INSERT INTO Crime_Analysis.VICTIM_SEX VALUES (%s,%s)"
    mycursor.execute(sql, tuple(row))
    print("Record inserted")
    # the connection is not auto committed by default, so we must commit to save our changes
    mydb.commit()

df = pd.read_csv(r'C:\Users\yalla\Downloads\Crime_TF.csv')
for i,row in df.iterrows():
    # here %S means string values
    # print(tuple(row))
    sql = "INSERT INTO Crime_Analysis.crime_data_TF VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
    mycursor.execute(sql, tuple(row))
    print("Record inserted")
    mydb.commit()

select_Crimee = """select V.Vict_SEX_desc,count(V.Vict_SEX_code) from crime_data_tf c
                    inner join victim_sex V
                    on C.VICT_SEX = V.Vict_SEX_code
                    where DATE_OCC between '2020-12-25' and '2021-01-15' and area_name= 'Southwest'
                    group by V.Vict_SEX_desc;"""
with mydb.cursor() as cursor:
    cursor.execute(select_Crimee)
    for row in cursor.fetchall():
        print(row)

select_Crimee = """select V.Vict_Descent_desc,count(V.Vict_Descent) Number_of_victims from crime_data_tf c
                    inner join victim_descent V
                    on C.VICT_DESCENT = V.Vict_Descent
                    where area_name = "Foothill"
                    and Date_occ between "2011-12-19" and "2012-01-20"
                    group by V.Vict_Descent
                    order by Number_of_victims desc ;"""
with mydb.cursor() as cursor:
    cursor.execute(select_Crimee)
    for row in cursor.fetchall():
        print(row)
