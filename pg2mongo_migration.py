#!/usr/bin/env python

import sys
import json
import file_utils
import pg2mongo_initial_migration
import populate_constraints
import traceback
import postgres_dao
import mongodb_helper
import mongo_metadata_dao

__author__ = 'ranjan'


'''
* This script first migrates or moves all the tables and rows of posgtres as is to 
mongodb.
* Next, the relationships between each tables are mapped in mongodb using the DBRefs.

'''


def main(argv):
    """

    @param argv:
    @return:
    """

    config = None

    try:

        # Check whether the configuration json file is a valid json file.
        config = json.loads(file_utils.get_file_contents_as_string('config.json'))

    except ValueError as e:
        print '''
                --------
                ERROR :
                --------
                Please provide a proper json file.
                ''', e

        return

    # Establish mongodb connection
    mongo_client = mongodb_helper.get_mongodb_client(config)

    if not mongo_client:

        print 'Mongodb client object is null. Check the exception thrown.'

        return

    # Based on the configuration file, get the postgres connection object
    pg_conn = postgres_dao.get_postgres_connection(config)

    if not pg_conn:

        print 'Postgres connection object is null. Check the exception thrown.'

        return

    try:
        print 'Dropping mongo database: ',config['mongo']['databaseName']

        # Drop the database before creating(if exists)
        mongo_client.drop_database(config['mongo']['databaseName'])

        print 'Creating mongodb database: ',config['mongo']['databaseName']

        # Create a database in mongodb using the config
        db = mongo_client[config['mongo']['databaseName']]

        print 'Adding constraints information of postgres into mongodb metadata collection.'

        # Insert the relationships or constraints of all the table in metadata collection
        mongo_metadata_dao.add_pg_table_constraints(db, pg_conn)

        # Initial migration (Migrate data from postgres to mongodb as is.)
        pg2mongo_initial_migration.migrate(config, pg_conn, db)

        print 'Finished initial pg2mongodb migration. Now finalizing constraints links.'

        # Now that all the tables and rows are added into mongodb, link the
        # individual collections in mongodb using DBRefs. i.e : Link the foreign keys as well
        # as the unique key constraints in postgres into mongodb using metadata collection
        populate_constraints.populate(mongo_client, config)

    except Exception as e:

        print 'Unexpected exception.', e

        exp = traceback.format_exc()

        print exp

    finally:

        print 'Closing all DB connections.'

        # Close connections
        pg_conn.close()
        mongo_client.close()

    return 0

if __name__ == "__main__":
    main(sys.argv)
