
import logging

from pymongo.connection import Connection


class MongoFormatter(logging.Formatter):
    def format(self, record):
        """Format exception object as a string"""
        data = record._raw.copy()
        if 'exc_info' in data and data['exc_info']:
            data['exc_info'] = self.formatException(data['exc_info'])
        return data
    

class MongoHandler(logging.Handler,Connection):
    """ Custom log handler

    Logs all messages to a mongo collection. This  handler is 
    designed to be used with the standard python logging mechanism.
    """

    @classmethod
    def to(cls, db, collection, host='localhost', port=None, level=logging.NOTSET):
        """ Create a handler for a given  """
        return cls(Connection(host, port)[db][collection])
        
    def __init__(self, host='127.0.0.1', port=27017, db, collection, level=logging.NOTSET):
        """ Init log handler and store the collection handle """
        logging.Handler.__init__(self, level)
        Connection.__init__((host, port)[db][collection])
        self.collection = collection
        self.formatter = MongoFormatter()
        self.host = host
        self.port = port
        self.db = db

    def emit(self,record):
        """ Store the record to the collection. Async insert """
        self.collection.save(self.format(record))

