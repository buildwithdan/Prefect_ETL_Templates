import requests
import sqlite3

db_name = 'data.db'
sql_statement1 = """UPDATE users 
                    set name = 'Alice2', age = 2
                    WHERE name = 'Alice'"""

sql_statement2 = "SELECT * FROM users"

users = [
    ('Alice99', 444),
    ('Bob99', 44),
    ('Charlie99', 22)
]

sql_3 = """INSERT INTO users (name , age)
            VALUES (?, ?)"""

# start db
con = sqlite3.connect(f'{db_name}')

cur = con.cursor()

# res1 = cur.execute (f'{sql_statement1}')
try:
    res3 = cur.executemany(f'{sql_3}',users)

    res2 = cur.execute (f'{sql_statement2}')
    result = res2.fetchall()

    con.commit()

    print(result)

except sqlite3.Error as e:
    print(f"Error occcured: {e}")

finally:
    con.close()
