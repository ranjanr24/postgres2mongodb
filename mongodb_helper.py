from pymongo import MongoClient

__author__ = 'ranjan'


'''
* This script contains generic helper methods to deal with mongodb.
'''


def get_mongodb_client(config):
    """
    This method returns the mongodb client object using the configuration file.
    @param config:
    @return:
    """
    try:

        mongo_client = MongoClient(config['mongo']['host'])

        return mongo_client

    except Exception as e:

        print 'Unable to establish mongodb connection.', e

        return None
