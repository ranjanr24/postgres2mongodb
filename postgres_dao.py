import psycopg2

__author__ = 'ranjan'


'''
* This script contains helper methods for postgres and acts as a database access layer for postgres.
'''


def get_all_table_names(pg_conn):
    """
    This method returns all the table names of the current database of pg_conn as a tuple
    @param pg_conn:
    @return:
    """

    query = "SELECT tablename FROM pg_catalog.pg_tables where schemaname != 'information_schema' and schemaname != 'pg_catalog'"

    try:
        pg_cursor = pg_conn.cursor()

        pg_cursor.execute(query)

        # (staff, actor, etc)
        pg_tables = pg_cursor.fetchall()

        pg_cursor.close()

        return pg_tables

    except Exception as e:

        print 'Unable to fetch table names for the connection.', e

        return ()


def get_postgres_connection(config):
    """

    @param config:
    @return:
    """
    # Establish postgres-ql connection
    pg_dbname = config['postgres']['databaseName']

    pg_user = config['postgres']['user']

    pg_host = config['postgres']['host']

    pg_password = config['postgres']['password']

    pg_port = config['postgres']['port']

    pg_connect_params = "dbname='%s' user='%s' host='%s' password='%s' port='%s'" %(pg_dbname, pg_user, pg_host, pg_password, pg_port)

    try:
        # Based on the config_json establish a postgres connection
        pg_conn = psycopg2.connect(pg_connect_params)

        pg_conn.set_session(readonly=True, autocommit=True)

        return pg_conn

    except Exception as e:

        print 'Unable to establish postgres connection.', e

        return None;


def get_column_and_type(pg_conn, table_name):
    """
    This method fetches the columns and data-types of each column of a table and
    stores in a list (list of dict) in an order. The order is same as that of the columns when
    the columns are listed in postgres.
    @param pg_conn:
    @param table_name:
    @return:
    """

    query = "SELECT column_name,data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = "+"'"+table_name+"'"

    try:

        pg_table_cursor = pg_conn.cursor()

        pg_table_cursor.execute(query)

        columns = pg_table_cursor.fetchall();

        pg_table_cursor.close()

        column_with_type_list = []

        for column in columns:

            column_with_datatype = {'column': column[0], 'datatype': column[1]}

            column_with_type_list.append(column_with_datatype);

        return column_with_type_list

    except Exception as e:

        print 'Unable to get column-data-type object for table_name', table_name, e

        return []


def get_paginated_500_rows(pg_conn, table_name, offset):
    """
    This method fetches the rows of a table using the table name and an offset.
    Currently only 500 rows will be returned.
    @param pg_conn:
    @param table_name:
    @param offset:
    @return:
    """

    try:

        pg_row_cursor = pg_conn.cursor()

        pg_row_cursor.execute("SELECT * FROM "+"\""+table_name+"\""+" OFFSET "+str(offset)+" ROWS FETCH FIRST 500 ROW ONLY" )

        pg_row_results = pg_row_cursor.fetchall();

        pg_row_cursor.close()

        return pg_row_results

    except Exception as e:

        print 'Unable to fetch rows from table: ', table_name, e

        return ()

    return


def get_table_rows_count(pg_conn, table_name):

    try:

        cursor = pg_conn.cursor()

        query = "SELECT COUNT(*) FROM "+"\""+table_name+"\""

        cursor.execute(query)

        result = cursor.fetchone()

        cursor.close()

        return result[0] if result else 0

    except Exception as e:

        print 'Unable to fetch count of rows of table: ', table_name, e

        return 0




