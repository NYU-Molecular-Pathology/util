#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
unit tests for the sqlite_tools module
"""
import unittest
import sqlite_tools as sqt
import sqlite3
from sqlite3 import IntegrityError

dump1 = "fixtures/dump.csv"

class TestSuperFilter(unittest.TestCase):
    def test_true(self):
        self.assertTrue(True, 'Demo assertion')

    def test_fail(self):
        self.assertFalse(False)

    def test_error(self):
        self.assertRaises(ValueError)

    def test_make_table1(self):
        """
        Make a new db; it should have no tables
        """
        conn = sqlite3.connect(":memory:")
        expected_names = []
        table_names = sqt.get_table_names(conn = conn)
        self.assertTrue(expected_names == table_names, 'Table names do not match expected names')

    def test_make_table2(self):
        """
        Make a new db with a single table
        """
        conn = sqlite3.connect(":memory:")
        expected_names = ["foooooooooooo"]
        sqt.create_table(conn = conn, table_name = "foooooooooooo", col_name = "baaar")
        table_names = sqt.get_table_names(conn = conn)
        self.assertTrue(expected_names == table_names, 'Table names do not match expected names')

    def test_make_table_colnames1(self):
        """
        Checks the table column names
        """
        conn = sqlite3.connect(":memory:")
        expected_names = ["baaar"]
        sqt.create_table(conn = conn, table_name = "foooooooooooo", col_name = "baaar")
        colnames = sqt.get_colnames(conn = conn, table_name = "foooooooooooo")
        self.assertTrue(expected_names == colnames, 'Column names do not match expected names')

    def test_add_column1(self):
        """
        Checks the tables columns names
        """
        conn = sqlite3.connect(":memory:")
        expected_names = ["baaar", "baaazz"]
        sqt.create_table(conn = conn, table_name = "foooooooooooo", col_name = "baaar")
        sqt.add_column(conn = conn, table_name = "foooooooooooo", col_name = "baaazz")
        colnames = sqt.get_colnames(conn = conn, table_name = "foooooooooooo")
        self.assertTrue(expected_names == colnames, 'Column names do not match expected names')

    def test_sqlite_insert1(self):
        """
        Inserts values into the database, checks the dump contents
        """
        conn = sqlite3.connect(":memory:")
        row = {"baaar": "4", "baaazz": "8"}
        sqt.create_table(conn = conn, table_name = "foooooooooooo", col_name = "baaar")
        sqt.add_column(conn = conn, table_name = "foooooooooooo", col_name = "baaazz")
        sqt.sqlite_insert(conn = conn, table_name = "foooooooooooo", row = row)
        expected_lines = ['BEGIN TRANSACTION;',
                        u"CREATE TABLE foooooooooooo (baaar TEXT, 'baaazz' TEXT);",
                        u'INSERT INTO "foooooooooooo" VALUES(\'4\',\'8\');',
                        'COMMIT;']
        lines = []
        for line in conn.iterdump():
            lines.append(line)
        self.assertTrue(expected_lines == lines, 'Expected db dump lines do not match db dump')

    def test_sqlite_insert2(self):
        """
        Inserts values into the database, checks the query results
        """
        conn = sqlite3.connect(":memory:")
        row = {"baaar": "4", "baaazz": "8"}
        sqt.create_table(conn = conn, table_name = "foooooooooooo", col_name = "baaar")
        sqt.add_column(conn = conn, table_name = "foooooooooooo", col_name = "baaazz")
        sqt.sqlite_insert(conn = conn, table_name = "foooooooooooo", row = row)
        # "SELECT {0} FROM {1} WHERE {2} = '{3}'".format(select_col, table_name, match_col, value)
        vals = sqt.get_vals(conn = conn, table_name = "foooooooooooo", select_col = "baaazz", match_col = "baaar", value = "4" )
        expected_vals = ['8']
        self.assertTrue(vals == expected_vals, 'Expected db values do not match db values')

    def test_sqlite_insert_fail1(self):
        """
        Tries to insert the same row twice when a primary key is set
        """
        conn = sqlite3.connect(":memory:")
        row = {"baaar": "4", "baaazz": "8"}
        sqt.create_table(conn = conn, table_name = "foooooooooooo", col_name = "baaar", is_primary_key = True)
        sqt.add_column(conn = conn, table_name = "foooooooooooo", col_name = "baaazz")
        sqt.sqlite_insert(conn = conn, table_name = "foooooooooooo", row = row)
        self.assertRaises(IntegrityError, sqt.sqlite_insert, conn, "foooooooooooo", row)

    def test_sanitize_str1(self):
        """
        Cleans a character string for use in the databases
        """
        bad_str = "Foo Bar Baz "
        good_str = "Foo_Bar_Baz"
        self.assertTrue(sqt.sanitize_str(string = bad_str) == good_str, 'Sanitized character string does not match expected value')

    def test_dump_csv1(self):
        """
        Checks .csv dump
        """
        conn = sqlite3.connect(":memory:")
        row = {"baaar": "4", "baaazz": "8"}
        sqt.create_table(conn = conn, table_name = "foooooooooooo", col_name = "baaar", is_primary_key = True)
        sqt.add_column(conn = conn, table_name = "foooooooooooo", col_name = "baaazz")
        sqt.sqlite_insert(conn = conn, table_name = "foooooooooooo", row = row)
        sqt.dump_csv(conn, table_name = "foooooooooooo", output_file = "fixtures/test_dump.csv")
        dump_contents1 = open("fixtures/test_dump.csv", "r").read()
        dump_contents2 = open(dump1, "r").read()
        self.assertTrue(dump_contents1 == dump_contents2, 'Db csv dump file contents do not match expected file output')






if __name__ == '__main__':
    unittest.main()
