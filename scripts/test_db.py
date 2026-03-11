import psycopg2

try:
    conn = psycopg2.connect(
        dbname="postgres",   # default database
        user="postgres",     # default PostgreSQL user
        password="Dharm@07", # your password
        host="localhost",
        port="5432"
    )

    print("✅ Connection successful!")

    conn.close()

except Exception as e:
    print("❌ Connection failed:", e)