import sqlite3

# Connect to the SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('data.db')

# Create a cursor object to interact with the database
cursor = conn.cursor()

# Create a table
cursor.execute('''
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    age INTEGER NOT NULL
)
''')

# Insert some data into the table
users = [
    ('Alice', 30),
    ('Bob', 25),
    ('Charlie', 35)
]

cursor.executemany('''
INSERT INTO users (name, age)
VALUES (?, ?)
''', users)

# Commit the changes
conn.commit()

# Query the data to verify the insertion
cursor.execute('SELECT * FROM users')
rows = cursor.fetchall()

print(rows)

# # Print the results
# for row in rows:
#     print(row)

# Close the connection
conn.close()