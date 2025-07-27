import sqlite3
import csv
import os

DB_FILE = "project5.sqlite3"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
AUTHORS_CSV = os.path.join(BASE_DIR, "authors.csv")
BOOKS_CSV = os.path.join(BASE_DIR, "books.csv")

def create_tables():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS authors (
                author_id TEXT PRIMARY KEY,
                first TEXT NOT NULL,
                last TEXT NOT NULL
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS books (
                book_id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                year_published INTEGER,
                author_id TEXT,
                FOREIGN KEY (author_id) REFERENCES authors(author_id)
            );
        """)
        conn.commit()
        print("Tables created.")

def insert_authors_from_csv():
    with open(AUTHORS_CSV, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        authors = [(row['author_id'], row['first'], row['last']) for row in reader]

    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.executemany(
            "INSERT OR IGNORE INTO authors (author_id, first, last) VALUES (?, ?, ?);", 
            authors
        )
        conn.commit()
        print(f"Inserted {len(authors)} authors.")

def insert_books_from_csv():
    with open(BOOKS_CSV, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        books = [
            (row['book_id'], row['title'], int(row['year_published']), row['author_id']) 
            for row in reader
        ]

    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.executemany(
            "INSERT OR IGNORE INTO books (book_id, title, year_published, author_id) VALUES (?, ?, ?, ?);",
            books
        )
        conn.commit()
        print(f"Inserted {len(books)} books.")

def run_select_all():
    print("\n🔍 SELECT ALL BOOKS")
    with sqlite3.connect(DB_FILE) as conn:
        results = conn.execute("SELECT * FROM books;").fetchall()
        for row in results:
            print(row)

def run_join_query():
    print("\n🔗 JOIN AUTHORS AND BOOKS")
    with sqlite3.connect(DB_FILE) as conn:
        query = """
        SELECT authors.first, authors.last, books.title, books.year_published
        FROM authors
        INNER JOIN books ON authors.author_id = books.author_id;
        """
        results = conn.execute(query).fetchall()
        for row in results:
            print(row)

def run_update_query():
    print("\n✏️ UPDATE BOOK TITLE")
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("""
        UPDATE books
        SET title = 'The Hobbit (new)'
        WHERE title = 'The Hobbit';
        """)
        conn.commit()
        print("Updated title where needed.")

def run_delete_query():
    print("\n🗑️ DELETE OLD BOOKS (before 1900)")
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("DELETE FROM books WHERE year_published < 1900;")
        conn.commit()
        print("Deleted books published before 1900.")

def run_group_by_query():
    print("\n📊 BOOK COUNT BY YEAR_PUBLISHED")
    with sqlite3.connect(DB_FILE) as conn:
        query = """
        SELECT year_published, COUNT(*) AS book_count
        FROM books
        GROUP BY year_published;
        """
        results = conn.execute(query).fetchall()
        for row in results:
            print(row)

def main():
    create_tables()
    insert_authors_from_csv()
    insert_books_from_csv()

    run_select_all()
    run_join_query()
    run_update_query()
    run_delete_query()
    run_group_by_query()

if __name__ == "__main__":
    main()
