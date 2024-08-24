"""
Represent a SQL Query and its result as a dictionary
"""

import sqlite3
from collections import abc
from contextlib import suppress

class SQL(abc.MutableMapping):
    def __init__(self, dbname: str = ':memory:', items=[], **kwargs):
        self.dbname = dbname
        self.conn = sqlite3.connect(self.dbname)
        cursor = self.conn.cursor()

        with suppress(sqlite3.OperationalError):
            cursor.execute("CREATE TABLE users (name TEXT, email TEXT)")
            cursor.execute("INSERT INTO users VALUES ('John Doe', 'email')")

        self.update(items, **kwargs)

    def __get__(self, key):
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM users WHERE name = '{key}'")
        return cursor.fetchall()

    def __delitem__(self, key):
        cursor = self.conn.cursor()
        cursor.execute(f"DELETE FROM users WHERE name = '{key}'")
        self.conn.commit()

    def __getitem__(self, item):
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM users WHERE name = '{item}'")
        return cursor.fetchall()

    def __iter__(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM users")
        return iter(cursor.fetchall())

    def __len__(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users")
        return cursor.fetchone()[0]

    def __setitem__(self, key, value):
        cursor = self.conn.cursor()
        cursor.execute(f"INSERT INTO users VALUES ('{key}', '{value}')")
        self.conn.commit()


def test_create_query():
    users = SQL()
    assert users['John Doe'] == [('John Doe', 'email')]

def test_assert_len():
    assert len(SQL()) == 1




