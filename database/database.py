""" Contains the class to connect to the database and perform operations 
"""
import io
import os
import logging
import psycopg2

from psycopg2 import extras

DATABASE_HOST = os.getenv('DB_HOST')
DATABASE_PORT = os.getenv('DB_PORT')
DATABASE_USERNAME = os.getenv('DB_USERNAME')
DATABASE_PASSWORD = os.getenv('DB_PASSWORD')
DATABASE_DBNAME = os.getenv('DB_DBNAME')

class Database:
    DEFAULT_COLUMN_DELIMITER = "\t"
    DEFAULT_LINE_DELIMITER = "\n"

    def __init__(
        self,
        column_delimiter=DEFAULT_COLUMN_DELIMITER,
        line_delimiter=DEFAULT_LINE_DELIMITER,
    ):
        """
        Args:
            column_delimiter (:obj:`str`, optional): Character to be used as column delimiter in data string (default `DEFAULT_COLUMN_DELIMITER`)
            line_delimiter (:obj:`str`, optional): Character to be used as line delimiter in TSV data (default `DEFAULT_LINE_DELIMITER`)
            
        """
        self.column_delimiter = column_delimiter
        self.line_delimiter = line_delimiter
        self.logger = logging.getLogger('assignment-etl')
        self._establish_connection()

    def __del__(self):
        if self.connection.closed == 0:
            self._close_connection()
        
    def execute(self, statement):
        self.logger.debug({ 'statement': statement })
        result = None
        if self.connection.closed != 0:
            self._establish_connection()
        cursor = self.connection.cursor(cursor_factory=extras.RealDictCursor)
        result = cursor.execute(statement)
        self.connection.commit()
        row_count = cursor.rowcount
        try:
            result = cursor.fetchall()
        except psycopg2.ProgrammingError as err:
            if str(err) == 'no results to fetch':
                result = row_count
            else:
                raise
        cursor.close()
        return result

    def copy(self, target_table, data):
        if data:
            self._copy(target_table, data)
        else:
            self.logger.warning('No data received. Skipping.')

    def _copy(self, target_table, data):
        if self.connection.closed != 0:
            self._establish_connection()
        cursor = self.connection.cursor()
        data_string = self._stringify_data(data, self.column_delimiter, self.line_delimiter)
        io_data = io.StringIO(data_string)
        cursor.copy_from(io_data, target_table, sep=self.column_delimiter, null='')
        self.connection.commit()
        io_data.close()
        cursor.close()

    def _stringify_data(self,data, column_delimiter, line_delimiter):
        return line_delimiter.join(
            [column_delimiter.join(line) for line in data]
        ) + line_delimiter

    def _establish_connection(self):
        self.logger.info({
            'message': 'Establish database connection'
        })
        try:
            print("Connecting to {user}:xxxx@{host}:{port}/{dbname}".format(
                dbname=DATABASE_DBNAME,
                user=DATABASE_USERNAME,
                host=DATABASE_HOST,
                port=DATABASE_PORT
            ))
            self.connection = psycopg2.connect(
                dbname=DATABASE_DBNAME,
                user=DATABASE_USERNAME,
                host=DATABASE_HOST,
                port=DATABASE_PORT,
                password=DATABASE_PASSWORD
            )
        except Exception as err:
            self.logger.error({
                'message': str(err)
            })
            raise err

    def _close_connection(self):
        self.connection.close()

    def _get_connection(self):
        return self.connection
