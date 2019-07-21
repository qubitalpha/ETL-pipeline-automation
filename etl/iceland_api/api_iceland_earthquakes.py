from etl.etl import LocalJSONFileJob

class ApiIcelandEarthquakes(LocalJSONFileJob):
    @property
    def path_to_local_source_file(self):
        return 'data/api_iceland_earthquakes.json'
    
    @property
    def schema(self):
        return 'iceland'

    @property
    def table(self):
        return 'earthquakes'

    @property
    def properties_to_extract(self):
        return [
            'latitude',
            'longitude',
            'size',
            'timestamp',
        ]

    def transform(self, extracted_data):
        items = extracted_data["results"]   # The API always wraps results

        parsed_data = []
        for item in items:
            row = []

            # In the next iteration
            # 1. validate date
            # 2. validate time
            # 3. timestamp = combine time and date

            # tentatively remove time
            for prop in self.properties_to_extract:
                row.append(str(item[prop]))
            parsed_data.append(row)

        return parsed_data