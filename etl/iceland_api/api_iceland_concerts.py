from etl.etl import LocalJSONFileJob

class ApiIcelandConcerts(LocalJSONFileJob):
    @property
    def path_to_local_source_file(self):
        return 'data/api_iceland_concerts.json'
    
    @property
    def schema(self):
        return 'iceland'

    @property
    def table(self):
        return 'concerts'

    @property
    def properties_to_extract(self):
        return [
            'eventDateName',
            'userGroupName',
            'eventHallName',
            'dateOfShow',
        ]
