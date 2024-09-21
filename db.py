import psycopg2
from psycopg2 import sql

def create_database_if_not_exists(db_name, user, password, host='localhost', port='5432'):
    # Connect to the postgres'
    conn = psycopg2.connect(dbname="postgres", user=user, password=password, host=host, port=port)
    conn.autocommit = True
    cursor = conn.cursor()

    # Check if the database already exists
    cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [db_name])
    exists = cursor.fetchone()

    # If it does not exist, create the database
    if not exists:
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
        print(f"Database '{db_name}' created successfully.")
    else:
        print(f"Database '{db_name}' already exists.")

    # Close the connection to the default 'postgres' database
    cursor.close()
    conn.close()

def create_table_if_not_exists(db_name, user, password, host='localhost', port='5432'):
    # Connect to the new database
    conn = psycopg2.connect(dbname=db_name, user=user, password=password, host=host, port=port)
    cursor = conn.cursor()

    # Check if the table already exists
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_name = 'enrolments'
        );
    """)
    table_exists = cursor.fetchone()[0]

    # If the table doesn't exist, create it
    if not table_exists:
        cursor.execute("""
            CREATE TABLE Enrolments (
                entity_code VARCHAR PRIMARY KEY,
                municipality_code VARCHAR,
                state_code VARCHAR,
                state_name VARCHAR,
                city_name VARCHAR,
                enrolment_date DATE,
                enrolment_count INT
            );
        """)
        print("Table 'Enrolments' created successfully.")
    else:
        print("Table 'Enrolments' already exists.")

    # Commit and close the connection
    conn.commit()
    cursor.close()
    conn.close()

# Usage
db_name = "enrolments_db"
user = "admin"
password = "password"

# Check and create the database and table
create_database_if_not_exists(db_name, user, password)
create_table_if_not_exists(db_name, user, password)