import psycopg2

# Connection parameters
db_host = "postgres"
db_port = "5432"
db_name = "airflow"
db_user = "airflow"
db_password = "airflow" # TODO: Okay for dev, use Secrets in prod

connection = psycopg2.connect(
    host=db_host,
    port=db_port,
    database=db_name,
    user=db_user,
    password=db_password
)

# Create a cursor object using the connection
cursor = connection.cursor()


print('')
print('')
print('')
# should pass
cursor.execute("SELECT * FROM prices;")
print('should have passed')
print('')
print('')
print('')

print('connection had to happen')

cursor.execute("SELECT * FROM public.priceeouas;")
print('should have failed')

# Fetch the result of the query
# db_version = cursor.fetchone()
# print(f"Connected to PostgreSQL database, version: {db_version}")
