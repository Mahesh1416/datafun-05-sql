#1. Create a database using python
import sqlite3
import pandas as pd
import pathlib

# Define the database file in the current root project directory
db_file = pathlib.Path("project5.sqlite3")

def create_database():
    """Function to create a database. Connecting for the first time
    will create a new database file if it doesn't exist yet.
    Close the connection after creating the database
    to avoid locking the file."""
    try:
        conn = sqlite3.connect(db_file)
        conn.close()
        print("Database created successfully.")
    except sqlite3.Error as e:
        print("Error creating the database:", e)

def main():
    create_database()

if __name__ == "__main__":
    main()

#2A. Tables:Create

import sqlite3

# Connect to (or create) the database
conn = sqlite3.connect("books.db")
cursor = conn.cursor()

# Create authors table
cursor.execute("""
CREATE TABLE IF NOT EXISTS authors (
    author_id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    year_born INTEGER
);
""")

# Create books table
cursor.execute("""
CREATE TABLE IF NOT EXISTS books (
    book_id TEXT PRIMARY KEY,
    title TEXT,
    year_published INTEGER,
    author_id TEXT,
    FOREIGN KEY (author_id) REFERENCES authors(author_id)
);
""")

# Commit and close the connection
conn.commit()
conn.close()

##2B. Create tables
def create_tables():
    """Function to read and execute SQL statements to create tables"""
    try:
        with sqlite3.connect(db_file) as conn:
            sql_file = pathlib.Path("sql", "create_tables.sql")
            with open(sql_file, "r") as file:
                sql_script = file.read()
            conn.executescript(sql_script)
            print("Tables created successfully.")
    except sqlite3.Error as e:
        print("Error creating tables:", e)

#3B. Records: Create from Data Files and appending previous tables

import sqlite3
import pandas as pd
import pathlib

# Define your database file
db_file = "books.db"

def insert_data_from_csv():
    """
    Function to read data from CSV files in the 'data' folder and insert them
    into the existing 'authors' and 'books' tables without altering table structure.
    """
    try:
        # Define the paths to your CSV files
        author_data_path = pathlib.Path("data", "authors.csv")
        book_data_path = pathlib.Path("data", "books.csv")

        # Read the CSV files into pandas DataFrames
        authors_df = pd.read_csv(author_data_path)
        books_df = pd.read_csv(book_data_path)

        # Connect to the SQLite database
        with sqlite3.connect(db_file) as conn:
            # Insert data into existing tables WITHOUT dropping/replacing them
            authors_df.to_sql("authors", conn, if_exists="append", index=False)
            books_df.to_sql("books", conn, if_exists="append", index=False)

            print("Data inserted successfully.")

    except pd.errors.EmptyDataError:
        print("One of the CSV files is empty.")
    except FileNotFoundError as e:
        print("CSV file not found:", e)
    except sqlite3.IntegrityError as e:
        print("Integrity error (e.g., duplicate primary key):", e)
    except sqlite3.Error as e:
        print("SQLite error:", e)

#4. Records: Read with SQL Select
import sqlite3
import pandas as pd


