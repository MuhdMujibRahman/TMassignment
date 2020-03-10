
import csv
import os
import glob
import psycopg2


conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=Admin1234")
cur = conn.cursor()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dir_path =os.path.join(BASE_DIR,"TMassignment/csv_files/*.csv")


def readCSV():
    counter = 0
    for fname in glob.glob(dir_path):
        data = csv.reader(open(fname, 'r'))
        counter+= 1
        table_name="table_"+str(counter)

        if checkTableExists(conn,table_name)==False:
            create_table(counter)
            conn.commit
        next(data)



def inserting_data(data, counter):
    for row in data:
        if counter == 2:
            cur.execute("INSERT INTO " + "table_" + str(counter) + " VALUES (%s)", row)
            conn.commit
        else:
            cur.execute("INSERT INTO " + "table_" + str(counter) + " VALUES (%s,%s)", row)
            conn.commit
def create_table(x):
    if x==1 :
        cur.execute("""CREATE TABLE table_1(
        task_id text PRIMARY KEY,
        skill text)""")
    elif x==2:
        cur.execute("""CREATE TABLE table_2(
                team_id text PRIMARY KEY)""")
    elif x==3:
        cur.execute("""CREATE TABLE table_3(
                        team_id text ,
                        skill text)""")

def checkTableExists(dbcon, tablename):
    dbcur = dbcon.cursor()
    dbcur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if dbcur.fetchone()[0] == 1:
        dbcur.close()
        return True

    dbcur.close()
    return False

def join_table():
    cur.execute("""
    SELECT 
        table_1.task_id,
        table_2.team_id,
        table_3.skill
    FROM table_1
    INNER JOIN table_3
    ON table_3.skill = table_1.skill
    INNER JOIN table_2
    ON table_2.team_id = table_3.team_id""")
    for row in cur.fetchall():
        print("Task_id =", row[0])
        print("Team_id", row[1])
        print("Skill", row[2], "\n")

def display():
    cur.execute("""
    SELECT * 
    FROM table_1"""
    )
    for row in cur.fetchall():
        print("team_id = ", row[0])
        print("skill =",row[1])

readCSV()
join_table()