import sys
from os.path import dirname, join, abspath
sys.path.insert(0, abspath(join(dirname(__file__), '../..')))
from tests.test import LoadtoTest
import unittest
import json
from data_models.data_model import DataModel
import logging

class LoadDBIceLandDrivers(DataModel):

    # init create statement because they cannot be none (as per this implementation)
    # lets not change it now
    @property
    def create_statement(self):
        return """
        """

    # init drop statement because they cannot be none (as per this implementation)
    # lets not change it now
    @property
    def drop_statement(self):
        return """
        """

    @property
    def select_statement(self):
        return """
            select * from iceland.drivers;
        """

class LoadSourceIceLandDrivers(LoadtoTest):
    @property
    def path_to_local_source_file(self):
        return 'data/api_iceland_samferda_drivers.json'

    def extract(self):
        with open(self.path_to_local_source_file) as f:
            return json.load(f)

class TestIceLandDriversETL(unittest.TestCase):

    @property
    def source_json(self):
        obj = LoadSourceIceLandDrivers()
        return obj.extract()

    @property
    def db_data(self):
        obj = LoadDBIceLandDrivers()
        return obj.select_table()

    def test_data_completeness(self):
        # if number of records in source data and DB is same
        self.assertEqual(len(self.source_json['results']), len(self.db_data))

    # def test_data_correctness(self):
    #     # asserts based on business logic
    #     # chat with data analysts to discuss when the data is correct
    #     pass

    # def test_data_constraints(self):
    #     # asserts based on number of null values to allow
    #     # chat with data analysts to discuss when the data values needs to be null and when not
    #     pass

    # def test_data_dates(self):
    #     # asserts based on validating dates
    #     # chat with data analysts to discuss what data range is allowed
    #     pass

    # def test_data_transformation(self):
    #     # asserts based on transformations in this table
    #     pass

if __name__ == '__main__':
    print("Testing drivers done")
    logger.info({ 'message': 'Testing drivers done' })
    unittest.main()