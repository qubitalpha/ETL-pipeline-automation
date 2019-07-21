""" Contains base ETL classes from which others inherit
"""

import os # to get environment variables
import logging
from database.database import Database

DATABASE_HOST = os.getenv('DB_HOST')
DATABASE_PORT = os.getenv('DB_PORT')
DATABASE_USERNAME = os.getenv('DB_USERNAME')
DATABASE_PASSWORD = os.getenv('DB_PASSWORD')
DATABASE_DBNAME = os.getenv('DB_DBNAME')

class ETLJob(object):
    """Master class with shared functionality for ETL jobs.
    """

    @property
    def table(self):
        """Name of the table

        Returns:
            str: Table name

        Raises:
            AttributeError: If property is not explicitly set in inheriting class
        """
        raise AttributeError('No table name defined')

    @property
    def schema(self):
        """Schema where the table is stored

        Returns:
            str: Schema name

        Raises:
            AttributeError: If property is not explicitly set in inheriting class
        """
        raise AttributeError('No schema name defined')

    def __init__(self, *_, **kwargs):
        """Initialize ETL
        """
        try:
            if not self.schema or not isinstance(self.schema, str):
                raise AttributeError('Invalid schema name defined')
            if not self.table or not isinstance(self.table, str):
                raise AttributeError('Invalid table name defined')
        except:
            raise

        # Initialise database
        self.database = Database()

        # Initialise logging
        self.logger = logging.getLogger('assignment-etl')

    def run(self):
        """Extract data, transform it and copy to database

        Returns:
            boolean: True if successful, inheriting classes should raise an error otherwise

        Raises:
            ExtractionError: If an error occurs during the extraction step
            TransformationError: If an error occurs during the transformation step
            LoadError: If loading data into the database failed
        """
        self.logger.info("Start extraction")
        extracted_data = self.extract()
        self.logger.info("End extraction")

        self.logger.info('Start processing')
        is_successful = self.process(extracted_data)
        self.logger.info('End processing')

        return is_successful

    def extract(self):
        """Extract raw unformatted source data

        Returns:
            object: Any object representing the result from the extraction process.

        Raises:
            ExtractionError: If an error occurs during the extraction step.
            NotImplementedError: If this method has not been overriden by the inheriting class.

        """
        raise NotImplementedError()
    
    def process(self, extracted_data):
        self.logger.info("Start transformation")
        transformed_data = self.transform(extracted_data)
        self.logger.info("End transformation")

        self.logger.info("Start load")
        self.load(transformed_data)
        self.logger.info("End load")

        return True

    def transform(self, extracted_data):
        """Take data from the `extract()` step and transform it into one string.

        Args:
            extracted_data: any object returned by the `extract()` step.

        Returns:
            list: A list of lists containing the transformed rows and columns.

        Raises:
            TransformationError: If an error occurs during the transformation step.
            NotImplementedError: If this method has not been overriden by the inheriting class.

        """
        raise NotImplementedError()
    
    def load(self, transformed_data):
        target_table = self.schema + '.' + self.table
        self.database.copy(target_table, transformed_data)

import json
class LocalJSONFileJob(ETLJob):
    """Base class for ETL jobs that need to ingest locally stored JSON files.
    """

    @property
    def path_to_local_source_file(self):
        """The path to a local file to ingest

        Returns:
            str: Path to file

        Raises:
            AttributeError: If property is not explicitly set in inheriting class
        """
        raise AttributeError('No file path defined')

    @property
    def properties_to_extract(self):
        """Defines the properties (and order) to be extracted from each object

        Returns:
            [str]: property name
            Example:
                [
                    "weight",
                    "height"
                ]

        Raises:
            AttributeError: If property is not explicitly set in inheriting class
        """
        raise AttributeError('No property to column map defined')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def extract(self):
        with open(self.path_to_local_source_file) as f:
            return json.load(f)

    def transform(self, extracted_data):
        items = extracted_data["results"]   # The API always wraps results

        parsed_data = []
        for item in items:
            row = []
            for prop in self.properties_to_extract:
                row.append(item[prop])
            parsed_data.append(row)

        return parsed_data
