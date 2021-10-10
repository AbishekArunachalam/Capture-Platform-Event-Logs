import unittest
import configparser
from sqlalchemy import exc
from src.record_events import EventCapture


class TestEventCapture(unittest.TestCase):

    def setUp(self):
        config = configparser.ConfigParser()
        config.read('../config.ini')
        self.hostname = "test_host"
        self.user_name = "test_user"
        self.password = config['mysqldb']['password']
        self.db_name = config['mysqldb']['db_name']
        self.table_name = config['mysqldb']['table_name']
        self.port = int(config['mysqldb']['port'])

    def test_initialise_db_connection(self):
        self.assertRaises(exc.SQLAlchemyError,
                          EventCapture.initialise_db_connection(self))

    def test_read_json(self):
        path = "../test_data/dummy_events.json"
        self.assertRaises(Exception,
                          EventCapture.read_json(self, path))
