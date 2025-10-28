import psycopg2

db_host = 'ky-database-1.cktmgwwi82dn.us-east-1.rds.amazonaws.com'
db_name = 'KYdatabase'
db_user = 'KYpostgres'
db_pass = 'jw2p5gaPgZl9ozQ6'

connection = psycopg2.connect(host = db_host, database = db_name, user = db_user, password = db_pass)

print("Connected to the database")

cursor = connection.cursor()
cursor.execute('SELECT version()')
db_version = cursor.fetchone()
print(db_version)

cursor.close()
