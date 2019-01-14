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






        # Remove in production
        # if collection != 'order_person':
        #    continue





        print 'Collection: ', collection

        metadata_documents_cursor = db.metadata.find({'table_name': collection})

        # Get the constraint information of current collection using metadata collection
        metadata_documents = list(metadata_documents_cursor)

        metadata_documents_cursor.close()

        # Find the primary key column name of the current table and add appropriate index (unique) in mongodb
        primary_key_column_name = get_primary_key_column(metadata_documents)

        print 'primary_key_column_name: ', primary_key_column_name

        if primary_key_column_name:

            # Check whether 2 indexes are present
            column_indexes = primary_key_column_name.split(',')

            if len(column_indexes) == 1:
                db[collection].create_index([(column_indexes[0].strip(), pymongo.ASCENDING)], unique=True)

            elif len(column_indexes) == 2:

                db[collection].create_index([(column_indexes[0].strip(), pymongo.ASCENDING),
                                             (column_indexes[1].strip(), pymongo.ASCENDING)], unique=True)

        unique_key_columns = get_unique_key_column(metadata_documents)

        if unique_key_columns:
            # Check whether 2 indexes are present
            unique_column_indexes = unique_key_columns.split(',')

            if len(unique_column_indexes) == 1:
                db[collection].create_index([(unique_column_indexes[0].strip(), pymongo.ASCENDING)], unique=True)

            elif len(unique_column_indexes) == 2:

                db[collection].create_index([(unique_column_indexes[0].strip(), pymongo.ASCENDING),
                                             (unique_column_indexes[1].strip(), pymongo.ASCENDING)], unique=True)

        fill_foreign_key_constraints(metadata_documents, db)

        '''

        # Get the foreign key constraints of current table in a structured format
        foreign_key_info = get_foreign_key_info(metadata_documents, config_json['mongo']['databaseName'])

        print 'foreign_key_info: ', foreign_key_info

        # Get the count of the collection
        collection_count = db[collection].count()

        progress_count = 0

        # Iterate over the current collection rows and fill in the appropriate foreign keys using DBRef
        collection_documents = db[collection].find()

        for document in collection_documents:

            # Get the first level keys of foreign_key_info
            foreign_key_info_keys = foreign_key_info.keys()

            for foreign_key_info_key in foreign_key_info_keys:

                foreign_key_info_temp = foreign_key_info.copy()

                # Except foreign_key_info_key remove all keys
                for key in foreign_key_info_temp.keys():

                    if key != foreign_key_info_key:

                        del foreign_key_info_temp[key]

                # Get the curr_column_name key value. This is the name of the column in document which
                # is linked to the other table
                curr_column_name = foreign_key_info_temp[foreign_key_info_key]['curr_column_name']

                # Get the value of 'document' with key curr_column_name, this is nothing but
                # the id/value of the document (usually a number) which is the primary key of other table
                document_id_value = document[curr_column_name]

                # Get the reference table and column name
                ref_column_name = foreign_key_info_temp[foreign_key_info_key]['ref_column_name']

                ref_table_name = foreign_key_info_temp[foreign_key_info_key]["$ref"]

                # print 'ref_table_name: ', ref_table_name

                # print 'Command: ', "db."+ref_table_name+".findOne({'"+ref_column_name+"':"+str(document_id_value)+"})"

                # Now query the ref_table_name with column name ref_column_name and with value document_id_value
                ref_document = db[ref_table_name].find_one({ref_column_name: document_id_value}, {'_id': 1})

                # print 'ref_document["_id"]: ', ref_document['_id']

                if len(ref_document.keys()) == 0:

                    continue

                # Store the id of ref_document into foreign_key_info to make it complete (DBRef)
                foreign_key_info_temp[foreign_key_info_key]["$id"] = ref_document['_id']

                # print 'foreign_key_info: ', foreign_key_info

                foreign_key_info_temp = get_first_level_ordered_dict(foreign_key_info_temp)

                # print 'foreign_key_info: ', foreign_key_info_temp

                if len(foreign_key_info.keys()):

                    # Update the document
                    db[collection].find_and_modify(query={'_id': document['_id']}, update={"$set": foreign_key_info_temp},
                        upsert=False, full_response=True)

                progress_count += 1

                progress(progress_count, collection_count, suffix='')

        collection_documents.close()

        '''

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
            # Data before the second bracket is the parent table name

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
    This method fetches the primary key column name of the current table.
    @param metadata_documents: Constraints information about a table
    @return:
    """

    # Iterate over metadata_documents and find the primary key constraint object
    for metadata_document in metadata_documents:

        if metadata_document['constraint_type'] == 'u':

            constraint_def = metadata_document['constraint_def']

            return constraint_def[constraint_def.find("(")+1:constraint_def.find(")")]

    return ''


def get_foreign_key_info(metadata_documents, database_name):
    """
    @deprecated
    @param metadata_documents:
    @param database_name:
    @return:
    """

    foreign_key_dict = {}

    # Iterate over metadata_documents and find constraint type f
    for metadata_document in metadata_documents:

        print 'metadata_document: ', metadata_document

        if metadata_document['constraint_type'] == 'f':

            constraint_def = metadata_document['constraint_def']

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

            foreign_key_dict[ref_table_name] = {
                                        "$ref": ref_table_name,
                                        "$db": database_name,
                                        "curr_column_name": curr_column_name,
                                        "ref_column_name": ref_column_name}

    return foreign_key_dict


def get_first_level_ordered_dict(input_dictionary):
    """
    @deprecated
    @param input_dictionary:
    @return:
    """
    input_dictionary_keys = input_dictionary.keys()

    parent_dict = {}

    for input_dictionary_key in input_dictionary_keys:

        ordered_dict = OrderedDict()

        '''
        {u'city': {'ref_column_name': u'city_id', '$db': u'dvdrental', 'curr_column_name': u'city_id', '$id': ObjectId('5c331a1e768b7c6efe0e7727'), '$ref': u'city'}}
        '''

        # print 'Debug: ', input_dictionary[input_dictionary_key]

        ordered_dict["$ref"] = input_dictionary[input_dictionary_key]["$ref"]

        ordered_dict["$id"] = input_dictionary[input_dictionary_key]["$id"]

        ordered_dict["$db"] = input_dictionary[input_dictionary_key]["$db"]

        ordered_dict["ref_column_name"] = input_dictionary[input_dictionary_key]["ref_column_name"]

        ordered_dict["curr_column_name"] = input_dictionary[input_dictionary_key]["curr_column_name"]

        parent_dict[input_dictionary_key] = ordered_dict

    return parent_dict
