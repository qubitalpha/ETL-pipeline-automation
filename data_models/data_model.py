""" Base class for data models from which all inherit.
"""
import os
import logging

from database.database import Database

DATABASE_HOST = os.getenv('DB_HOST')
DATABASE_PORT = os.getenv('DB_PORT')
DATABASE_USERNAME = os.getenv('DB_USERNAME')
DATABASE_PASSWORD = os.getenv('DB_PASSWORD')
DATABASE_DBNAME = os.getenv('DB_DBNAME')

class DataModel(object):
    """Master class with shared functionality for data models
    """

    @property
    def create_statement(self):
        """Statement to create table

        Returns:
            str: Create statement

        Raises:
            AttributeError: If property is not explicitly set in inheriting class
        """
        raise AttributeError('No CREATE statement defined')

    @property
    def drop_statement(self):
        """Statement to drop table

        Returns:
            str: Drop statement

        Raises:
            AttributeError: If property is not explicitly set in inheriting class
        """
        raise AttributeError('No DROP statement defined')

    @property
    def select_statement(self):
        """Statement to select table

        Returns:
            str: Select statement

        Raises:
            AttributeError: If property is not explicitly set in inheriting class
        """
        raise AttributeError('No SELECT statement defined')

    def create_table(self):
        self.database.execute(self.create_statement)

    def drop_table(self):
        self.database.execute(self.drop_statement)

    def select_table(self):
        return self.database.execute(self.select_statement)

    def __init__(self, *_, **kwargs):
        """Initialize Datamodel
        """
        try:
            if not self.create_statement or not isinstance(self.create_statement, str):
                raise AttributeError('Invalid CREATE statement defined')
            if not self.drop_statement or not isinstance(self.drop_statement, str):
                raise AttributeError('Invalid DROP statement defined')
        except:
            raise

        # Initialise database
        self.database = Database()

        # Initialise logging
        self.logger = logging.getLogger('assignment-etl')
