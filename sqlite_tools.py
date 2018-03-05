#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Functions for working with SQLite databases
"""
import sqlite3
import hashlib
import csv

# ~~~~~ FUNCTIONS ~~~~~ #
def get_table_names(conn):
    """
    Gets all the names of tables in the database

    Parameters
    ----------
    conn: sqlite3.Connection object
        connection object to the database

    Returns
    -------
    list
        a list of table names (str)
    """
    with conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        names = cursor.fetchall()
    return([item for sublist in names for item in sublist])

def create_table(conn, table_name, col_name, col_type = "TEXT", is_primary_key = False):
    """
    Create a table if it doesnt exist with a starting column

    Parameters
    ----------
    conn: sqlite3.Connection object
        connection object to the database
    table_name: str
        the name of the table to create
    col_name: str
        the name of the first column to create in the table
    col_type: str
        the SQLite data type for the column
    is_primary_key: bool
        whether or not the column is the primary key for the table
    """
    with conn:
        cursor = conn.cursor()
        sql_cmd = 'CREATE TABLE IF NOT EXISTS {0}'.format(table_name)
        if is_primary_key:
            table_cmd = ' ({0} {1} PRIMARY KEY)'.format(col_name, col_type)
        else:
            table_cmd = ' ({0} {1})'.format(col_name, col_type)
        sql_cmd = sql_cmd + table_cmd
        cursor.execute(sql_cmd)

def get_colnames(conn, table_name):
    """
    Gets the column names from a table

    Parameters
    ----------
    conn: sqlite3.Connection object
        connection object to the database
    table_name: str
        the name of the table to get column names from

    Returns
    -------
    list
        a list of column names (str)
    """
    colnames = []
    sql_cmd = 'select * from {0}'.format(table_name)
    with conn:
        cursor = conn.execute(sql_cmd)
        for item in cursor.description:
            colnames.append(item[0])
    return(colnames)

def add_column(conn, table_name, col_name, col_type = "TEXT", default_val = None):
    """
    Adds a column to a table

    Parameters
    ----------
    conn: sqlite3.Connection object
        connection object to the database
    table_name: str
        the name of the table in which to create the column
    col_name: str
        the name of the column to create
    col_type: str
        the SQLite data type for the column
    default_val: str
        a default value to use for the column
    """
    sql_cmd = "ALTER TABLE {0} ADD COLUMN '{1}' {2}".format(table_name, col_name, col_type)
    if default_val:
        default_val_cmd = " DEFAULT '{0}'".format(default_val)
        sql_cmd = sql_cmd + default_val_cmd
    try:
        with conn:
            cursor = conn.cursor()
            cursor.execute(sql_cmd)
    except:
        # the column already exists...
        pass

def sqlite_insert(conn, table_name, row, ignore = False):
    """
    Inserts a row into a table

    Parameters
    ----------
    conn: sqlite3.Connection object
        connection object to the database
    table_name: str
        the name of the table in which to insert the row
    row: dict
        a dictionary of key: value pairs corresponding to the column names and values of the items in the row to be added
    ignore: bool
        whether the entry should be ignored if it already exists in the table

    Examples
    --------
    Example usage::

        row = {'key': key, 'value': val}
        sqlite_insert(conn = conn, table_name = vals_table_name, row = row, ignore = True)

    """
    cols = ', '.join('"{0}"'.format(col) for col in row.keys())
    vals = ', '.join(':{0}'.format(col) for col in row.keys())
    sql = 'INSERT '
    if ignore:
        sql = sql + 'OR IGNORE '
    sql = sql + 'INTO "{0}" ({1}) VALUES ({2})'.format(table_name, cols, vals)
    with conn:
        conn.cursor().execute(sql, row)

def md5_str(item):
    """
    Gets the md5sum on the string representation of an object

    Parameters
    ----------
    item:
        Python object to get the md5 sum from; should be coercible to 'str'

    Returns
    -------
    str
        the md5 hash for the item
    """
    try:
        # python 2.x
        md5 = hashlib.md5(str(item)).hexdigest()
    except:
        # python 3.x
        md5 = hashlib.md5(str(item).encode('utf-8')).hexdigest()
    return(md5)

def row_exists(conn, table_name, col_name, value):
    """
    Checks to see if a row exists in a table

    Parameters
    ----------
    conn: sqlite3.Connection object
        connection object to the database
    table_name: str
        the name of the table in which to search for the row
    col_name: str
        the name of the column to search for the value in
    value: str
        the value to use as a key to check if a row exists
    """
    sql_cmd = 'SELECT count(*) FROM {0} WHERE {1} = "{2}"'.format(table_name, col_name, value)
    with conn:
        cursor = conn.execute(sql_cmd)
        data = cursor.fetchone()[0]
    if data == 0:
        return(False)
    else:
        return(True)

def get_vals(conn, table_name, select_col, match_col, value):
    """
    Query a value from the database

    Parameters
    ----------
    conn: sqlite3.Connection object
        connection object to the database
    table_name: str
        the name of the table in which to search for the row
    select_col: str
        the name of the column with the values to return
    match_col: str
        the name of the column to match a key to find values
    value: str
        the key to use for matching

    Examples
    --------
    Example usage::

        get_val(conn = conn, table_name = "runs", select_col = "samplesheet", match_col = "run", value = "180213_NB501073_0034_AHWJLLAFXX")

        # SELECT samplesheet FROM runs WHERE run = "180213_NB501073_0034_AHWJLLAFXX";

    """
    sql_cmd = "SELECT {0} FROM {1} WHERE {2} = '{3}'".format(select_col, table_name, match_col, value)
    results = []
    with conn:
        cursor = conn.execute(sql_cmd)
        data = cursor.fetchall()
        for item in data:
            results.append(item[0])
    return(results)

def dump_sqlite(conn, output_file):
    """
    Dumps the contents of a database to a SQLite formatted output file

    Parameters
    ----------
    conn: sqlite3.Connection object
        connection object to the database
    output_file: str
        file to write SQLite output to
    """
    with open(output_file, "w") as f:
        for line in conn.iterdump():
            f.write("{0}\n".format(line))

def load_sqlite(conn, sqlite_file):
    """
    Loads a database from a SQLite formatted file

    Parameters
    ----------
    conn: sqlite3.Connection object
        connection object to the database
    """
    with open(sqlite_file) as f:
        sql_cmd = f.read()
        conn.executescript(sql_cmd)

def dump_csv(conn, table_name, output_file, delimiter = ',', quoting = csv.QUOTE_MINIMAL):
    """
    Dumps a table from a SQLite database to a file

    Parameters
    ----------
    conn: sqlite3.Connection object
        connection object to the database
    table_name: str
        the name of the table to be dumped
    output_file: str
        the name of the file to write the table contents to
    delimiter: str
        field delimter for output file
    quoting: csv.CONSTANT
        ``csv`` module constant to be used for quoting of the output file (https://docs.python.org/2/library/csv.html#csv.QUOTE_ALL)
    """
    colnames = get_colnames(conn = conn, table_name = table_name)
    cursor = conn.cursor()
    with open(output_file, "w") as f:
        writer = csv.DictWriter(f, delimiter = delimiter, fieldnames = colnames, quoting = quoting)
        writer.writeheader()
        for item in cursor.execute("SELECT * FROM {0}".format(table_name)):
            writer.writerow({key:value for key, value in zip(colnames, item)})

def sanitize_str(string):
    """
    Cleans a character string for use in the database as a header
    """
    string = string.strip().replace(' ', '_')
    return(string)

def sanitize_dict_keys(d):
    """
    Cleans a dictionary's keys for use in the database as a header by creating a new dict with 'cleaned' keys
    """
    new_dict = { sanitize_str(key): value for key, value in d.items() }
    return(new_dict)
