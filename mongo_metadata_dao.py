__author__ = 'ranjan'


'''
* This script contains helper methods that does database operations on metadata collection.
'''


def add_pg_table_constraints(mongo_db, pg_conn):
    """
    This method adds all the postgres table's constraints (Primary key, Foreign key, Unique key etc )
    into metadata collection.
    @param mongo_db:
    @param pg_conn:
    @return:
    """

    query = """SELECT conrelid::regclass AS table_from
     , conname
     , pg_get_constraintdef(c.oid)
     , contype
    FROM   pg_constraint c
    JOIN   pg_namespace n ON n.oid = c.connamespace
    WHERE  n.nspname = 'public'
    ORDER  BY conrelid::regclass::text, contype DESC
    """

    try:

        cursor = pg_conn.cursor()

        cursor.execute(query)

        contraints_results = cursor.fetchall();

        cursor.close()

    except Exception as e:

        print 'Unable to add constraints to metadata collection.', e

        return -1

    # Iterate over the constrains and fill the meatadata collection with the contraints_results
    for constraints in contraints_results:

        mongo_store_bson = {
            'table_name': constraints[0],
            'constraint_name': constraints[1],
            'constraint_def': constraints[2],
            'constraint_type': constraints[3]
        }

        mongo_db['metadata'].insert_one(mongo_store_bson)

    return 0
