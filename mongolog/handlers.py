
import logging

from pymongo.connection import Connection


class MongoFormatter(logging.Formatter):
    def format(self, record):
        """Format exception object as a string"""
        data = record._raw.copy()
        if 'exc_info' in data and data['exc_info']:
            data['exc_info'] = self.formatException(data['exc_info'])
        return data
    

class MongoHandler(logging.Handler):
    """ Custom log handler

    Logs all messages to a mongo collection. This  handler is 
    designed to be used with the standard python logging mechanism.
    """

    def __init__(self,  db, collection, host='127.0.0.1', port=27017,level=logging.NOTSET):
        """ Init log handler and store the collection handle """
        logging.Handler.__init__(self, level)
        self.connection = Connection.__init__((host, port))
        self.formatter = MongoFormatter()
        self.host = host
        self.port = port
        self.db = self.connection[db]
        self.collection = db[collection]

    def emit(self,record):
        """ Store the record to the collection. Async insert """
        self.collection.save(self.format(record))

