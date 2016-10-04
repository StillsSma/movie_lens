
with open('u.genre', encoding = "latin1") as h:
    reader3 = csv.reader(h, delimiter='|')
    for row in reader3:
        print(row)

cursor.execute("DROP TABLE IF EXISTS genre;")

create_table_command = """
CREATE TABLE genre (
    id int,
    name varchar(20)

);
"""

cursor.execute(create_table_command)
for row in item:
    cursor.execute("INSERT INTO genre VALUES (%s,%s)",
    (row[1],row[0]))
