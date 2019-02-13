import datetime
import postgres_dao
from bson.decimal128 import Decimal128
import sys

__author__ = 'ranjan'


'''
* This script moves the data from postgres to mongodb as it is without adding any DBRefs.
'''


def migrate(config_json, pg_conn, mongo_db):
    """

    @param config_json:
    @param pg_conn:
    @param mongo_client:
    @return:
    """

    # Get the list of all the tables present in the database
    # select tablename from pg_catalog.pg_tables where schemaname != 'information_schema' and
    # schemaname != 'pg_catalog';
    pg_tables = postgres_dao.get_all_table_names(pg_conn)

    for pg_table in pg_tables:

        print ''

        # Get all the columns and its datatype in a structured format of the current table (pg_table[0])
        # Op: [{column: 'xyz', datatype: 'character varying'}]
        # The above list is an ordered list . That is the items are added in such a way that
        # when you run \dt table_name , the list of columns shown is same as that of
        # columns added to the list below.
        table_columns_datatypes = postgres_dao.get_column_and_type(pg_conn, pg_table[0])

        # print 'table_columns_datatypes: ', table_columns_datatypes

        table_rows_cursor = pg_conn.cursor()

        table_rows_cursor.execute("SELECT * FROM "+"\""+pg_table[0]+"\"")

        table_rows_count = postgres_dao.get_table_rows_count(pg_conn, pg_table[0])

        progress_count = 0

        # offset = 0

        while True:

            pg_row_result = table_rows_cursor.fetchone()

            progress_count += 1

            if not pg_row_result:

                progress_count = 0

                break

            store_pg_row_to_mongodb(pg_row_result, table_columns_datatypes, pg_table[0], mongo_db)

            progress(progress_count, table_rows_count, suffix='Table: '+pg_table[0])

        table_rows_cursor.close()

        # break   # Break table  (Remove in productin)

    return 0;


def progress(count, total, suffix=''):
    """
    Inline progress bar implementation.
    @param count:
    @param total:
    @param suffix:
    @return:
    """

    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))
    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)
    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', suffix))
    sys.stdout.flush()


def store_pg_row_to_mongodb(row_data, table_columns_datatypes, table_name, mongo_db):
    """
    This method stores individual row data of postgres to mongodb.
    :param row_data:
    :param table_columns_datatypes:
    :param table_name:
    :param mongo_db:
    :return:
    """

    mongo_store_bson = {}

    for index, row_item in enumerate(row_data):

        mongo_store_bson[table_columns_datatypes[index]['column']] = get_bson_value(row_item, table_columns_datatypes[index]['datatype'])

    # print 'mongo_store_bson: ', mongo_store_bson

    mongo_db[table_name].insert(mongo_store_bson)

    return


def get_bson_value(row_value, pg_datatype):
    """
    This method returns a datastructure that can be stored in mongodb. The row_value can
    be of any data-type and the pg_datatype can be either integer or varchar or etc.
    Based on the pg_datatype, the json or date or string will be constructed such that it
    can be stored in the mongodb.
    """

    if pg_datatype == 'timestamp without time zone':

        # print 'Date: ', row_value

        return row_value

    elif pg_datatype == 'bytea':

        return ''

    elif pg_datatype == 'integer' or pg_datatype == 'smallint' or pg_datatype == 'bigint':

        return row_value

    elif pg_datatype == 'character varying' or pg_datatype == 'character' or pg_datatype == 'text':

        return row_value

    elif pg_datatype == 'boolean':

        return True if row_value else False

    elif pg_datatype == 'numeric':

        # print 'Type: ', type(row_value), 'Row value: ', row_value

        # return float(row_value)
        return Decimal128(row_value) if row_value else 0

    elif pg_datatype == 'tsvector':

        # json_str = '{'+row_value.replace(',', '-')+'}'

        # json_str = re.sub("[ ]", ",", json_str)

        # try:

        # return  ast.literal_eval(json_str)

        # except:

        # print 'Exception occured for string: ', row_value, ' Json string: ', json_str

        # pass

        # print json_str

        # return json.loads(json_str)
        return row_value

    elif pg_datatype == 'date':

        return datetime.datetime(row_value.year, row_value.month, row_value.day) if row_value else ""

    else:

        print 'Implementation not yet done for '+pg_datatype, ' with data ', row_value

        return row_value
