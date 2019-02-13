import pymongo
from collections import OrderedDict
import sys

__author__ = 'ranjan'


'''
* This script adds appropriate constraints stored in metadata database to other remaining collections.
* The metadata collection contains foreign key, primary key and other constrains information of all the tables of the
  database.
* Before calling the populate function below, make sure that all the rows of postgres are already stored in mongodb.
* populate function below adds DBRefs to all the collections baed on data stored in the metadata colletion.
'''


def populate(mongo_client, config_json):
    """
    This method adds constraints to all the collections based on the data in metadata.
    @param mongo_client:
    @return:
    """

    db = mongo_client[config_json['mongo']['databaseName']]

    # Get list of all the collections in the current database
    collection_names = db.metadata.distinct("table_name")

    for collection in collection_names:

        if collection == '-':
            continue

        print 'Collection: ', collection

        metadata_documents_cursor = db.metadata.find({'table_name': collection})

        # Get the constraint information of current collection using metadata collection
        metadata_documents = list(metadata_documents_cursor)

        metadata_documents_cursor.close()

        # Find the primary key column name of the current table and add appropriate index (unique) in mongodb
        primary_key_column_name = get_primary_key_column(metadata_documents)

        # print 'primary_key_column_name: ', primary_key_column_name

        add_pg_primary_key_constraint_to_collection(primary_key_column_name, db, collection)

        unique_key_columns = get_unique_key_column(metadata_documents)

        add_pg_unique_key_constraint_to_collection(unique_key_columns, db, collection)

        fill_foreign_key_constraints(metadata_documents, db)

        # break # Remove in production

        # metadata_documents.close()

    return


def get_parsed_fk_data_dict(constraint_def, database_name):

    if not constraint_def:

        return {}

    post_foreign_key_text = constraint_def.split("FOREIGN KEY",1)[1]

    curr_column_name = post_foreign_key_text[post_foreign_key_text.find("(")+1:post_foreign_key_text.find(")")]

    # This will print film(film_id) ON UPDATE CASCADE ON DELETE RESTRICT
    post_references_text = post_foreign_key_text.split("REFERENCES",1)[1]

    # Split post_references_text using space as delimiter
    post_references_text_split = post_references_text.split(' ')[1]

    # Get the contents inside the bracket of post_references_text_split
    ref_column_name = post_references_text_split[post_references_text_split.find("(")+1:post_references_text_split.find(")")]

    # Now remove ref_column_name from post_references_text_split
    ref_table_name = post_references_text_split.replace('('+ref_column_name+')', '')

    return {
        "$ref": ref_table_name,
        "$db": database_name,
        "curr_column_name": curr_column_name,
        "ref_column_name": ref_column_name
    }


def fill_foreign_key_constraints(metadata_documents, db):
    """
    This method fills the DBRefs of the current collection.
    @param metadata_documents: Current processing table's constraints information
    @param db:
    @return:
    """

    if len(metadata_documents) == 0:

        return

    current_collection_name = metadata_documents[0]['table_name']

    # Iterate over the metadata_documents and process docuement that has f as constraint_type
    for metadata_document in metadata_documents:

        if metadata_document['constraint_type'] == 'f':

            constraint_def = metadata_document['constraint_def']

            # constraint_def contains: FOREIGN KEY (person_id) REFERENCES person(id)
            # Where first data inside bracket is the current collection's or table's column name
            # The second bracket (id) is the column name of the parent table or collection
            # Text before the second bracket is the parent table name

            # parse the constraint_def and get a dictionary having the current column name, reference column name etc
            # Ex: {'ref_column_name': u'id', '$db': u'rosetta', 'curr_column_name': u'person_id', '$ref': u'person'}
            column_foreign_key_constraint = get_parsed_fk_data_dict(constraint_def, db.name)

            # First make sure that the current collection to be updated is indexed based on
            # column_foreign_key_constraint['curr_column_name']
            print 'Creating index of collection ', current_collection_name, ' with field: ',\
                column_foreign_key_constraint['curr_column_name']
            db[current_collection_name].create_index(column_foreign_key_constraint['curr_column_name'])

            print 'Filling foreign key of ', current_collection_name, ' from ', column_foreign_key_constraint["$ref"]

            # Make sure that the db[current_collection_name] is not empty
            current_collection_count = db[current_collection_name].count()

            if current_collection_count == 0:

                continue

            # Iterate over the '$ref' collection
            parent_collection_cursor = db[column_foreign_key_constraint["$ref"]].find()

            for parent_collection_document in parent_collection_cursor:

                parent_collection_document_id = parent_collection_document['_id']

                # Now add the parent_collection_document_id to column_foreign_key_constraint to complete DBRef
                column_foreign_key_constraint["$id"] = parent_collection_document_id

                # Now arrange column_foreign_key_constraint such that it compatible with the DBRef structure
                column_foreign_key_constraint_ordered = get_ordered_dbref(column_foreign_key_constraint)

                # print 'column_foreign_key_constraint_ordered: ', column_foreign_key_constraint_ordered

                update_data = {column_foreign_key_constraint_ordered['$ref']: column_foreign_key_constraint_ordered}

                # Now update the current_collection_name where data of curr_column_name is same as
                db[current_collection_name].find_and_modify(query={
                    column_foreign_key_constraint_ordered['curr_column_name']: parent_collection_document[column_foreign_key_constraint_ordered['ref_column_name']]
                }, update={"$set": update_data}, upsert=False, full_response=True)

            parent_collection_cursor.close()

        # End if metadata_document['constraint_type'] == 'f'

    return 0


def get_ordered_dbref(column_foreign_key_constraint):
    """
    This method converts unordered DBRef dictionary to ordered DBRef dictionary
    @param column_foreign_key_constraint:
    @return:
    """

    if not column_foreign_key_constraint:

        return

    ordered_dict = OrderedDict()

    # print 'Debug: ', input_dictionary[input_dictionary_key]

    ordered_dict["$ref"] = column_foreign_key_constraint["$ref"]

    ordered_dict["$id"] = column_foreign_key_constraint["$id"]

    ordered_dict["$db"] = column_foreign_key_constraint["$db"]

    ordered_dict["ref_column_name"] = column_foreign_key_constraint["ref_column_name"]

    ordered_dict["curr_column_name"] = column_foreign_key_constraint["curr_column_name"]

    return ordered_dict


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


def get_primary_key_column(metadata_documents):
    """
    This method fetches the primary key column name of the current table.
    @param metadata_documents: Constraints information about a table
    @return:
    """

    # Iterate over metadata_documents and find the primary key constraint object
    for metadata_document in metadata_documents:

        if metadata_document['constraint_type'] == 'p':

            constraint_def = metadata_document['constraint_def']

            return constraint_def[constraint_def.find("(")+1:constraint_def.find(")")]

    return ''


def get_unique_key_column(metadata_documents):
    """
    This method fetches the unique key column name of the current table.
    @param metadata_documents: Constraints information about a table
    @return:
    """

    # Iterate over metadata_documents and find the primary key constraint object
    for metadata_document in metadata_documents:

        if metadata_document['constraint_type'] == 'u':

            constraint_def = metadata_document['constraint_def']

            return constraint_def[constraint_def.find("(")+1:constraint_def.find(")")]

    return ''


def add_pg_primary_key_constraint_to_collection(primary_key_column_name, db, collection_name):
    """
    This method adds the primary key column of current table (collection_name) and creates an
    index on the mongodb collection so created.
    :param primary_key_column_name:
    :param db:
    :param collection_name:
    :return:
    """

    if primary_key_column_name:

        # Check whether 2 indexes are present
        column_indexes = primary_key_column_name.split(',')

        if len(column_indexes) == 1:
            db[collection_name].create_index([(column_indexes[0].strip(), pymongo.ASCENDING)], unique=True)

        elif len(column_indexes) == 2:
            db[collection_name].create_index([(column_indexes[0].strip(), pymongo.ASCENDING),
                                             (column_indexes[1].strip(), pymongo.ASCENDING)], unique=True)
    return


def add_pg_unique_key_constraint_to_collection(unique_key_columns, db, collection_name):
    """

    :param unique_key_columns:
    :param db:
    :param collection_name:
    :return:
    """

    if unique_key_columns:
            # Check whether 2 indexes are present
            unique_column_indexes = unique_key_columns.split(',')

            if len(unique_column_indexes) == 1:
                db[collection_name].create_index([(unique_column_indexes[0].strip(), pymongo.ASCENDING)], unique=True)

            elif len(unique_column_indexes) == 2:

                db[collection_name].create_index([(unique_column_indexes[0].strip(), pymongo.ASCENDING),
                                             (unique_column_indexes[1].strip(), pymongo.ASCENDING)], unique=True)
    return
