""" Contains base Test classes from which others inherit
"""

import os # to get environment variables
from os.path import dirname, join, abspath # to set sys path
import sys
sys.path.insert(0, abspath(join(dirname(__file__), '..'))) # set base class

class LoadtoTest(object):

    @property
    def path_to_local_source_file(self):
        """The path to a local file to test

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

    def extract(self):
        """Extract raw unformatted source data

        Returns:
            object: Any object representing the result from the extraction process.

        Raises:
            ExtractionError: If an error occurs during the extraction step.
            NotImplementedError: If this method has not been overriden by the inheriting class.

        """
        raise NotImplementedError()
