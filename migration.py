
import psycopg2

import csv

connection = psycopg2.connect("dbname=movie_lens user=movie_lens")
cursor = connection.cursor()


with open('u.item', encoding = "latin1") as g:
    reader2 = csv.reader(g, delimiter='|')
    item2 = []
    for row in reader2:
        item2.append(row)

cursor.execute("DROP TABLE IF EXISTS movie_data;")

create_table_command = """
CREATE TABLE movie_data (
    movie_id int Primary Key,
    movie_title VARCHAR(100),
    release_dte VARCHAR(11),
    videorelease_dte VARCHAR(10),
    IMDbURL VARCHAR(150),
    unknown boolean,
    Action boolean,
    Adventure boolean,
    Animation boolean,
    Children boolean,
    Comedy boolean,
    Crime boolean,
    Documentary boolean,
    Drama boolean,
    Fantasy boolean,
    Film_Noir boolean,
    Horror boolean,
    Musical boolean,
    Mystery boolean,
    Romance boolean,
    SciFi boolean,
    Thriller boolean,
    War boolean,
    Western boolean

);
"""

cursor.execute(create_table_command)
for row in item2:
    cursor.execute("INSERT INTO movie_data VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
    (row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],
    row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15],
    row[16],row[17],row[18],row[19],row[20],row[21],row[22],row[23]))



with open('u.user', encoding = "latin1") as h:
    reader3 = csv.reader(h, delimiter='|')

    item3 = []
    for row in reader3:
        item3.append(row)


cursor.execute("DROP TABLE IF EXISTS userinfo;")

create_table_command = """
CREATE TABLE userinfo (
    userid int Primary Key ,
    age int,
    gender Varchar(1),
    occupation Varchar(20),
    zipcode Varchar(10)

);
"""

cursor.execute(create_table_command)
for row in item3:
    cursor.execute("INSERT INTO userinfo VALUES (%s,%s,%s,%s,%s)",
    (row[0],row[1],row[2],row[3],row[4]))



with open('u.data', encoding = "latin1") as f:
    reader = csv.reader(f, delimiter='\t')
    item = []
    for row in reader:
        item.append(row)

cursor.execute("DROP TABLE IF EXISTS main_data;")

create_table_command = """
CREATE TABLE main_data (
    id int references userinfo(userid),
    itemid int references movie_data(movie_id),
    rating int,
    timestmp int

);
"""

cursor.execute(create_table_command)
for row in item:
    cursor.execute("INSERT INTO main_data VALUES (%s,%s,%s,%s)",
    (row[0],row[1],row[2],row[3]))




connection.commit()


cursor.close()
connection.close()
