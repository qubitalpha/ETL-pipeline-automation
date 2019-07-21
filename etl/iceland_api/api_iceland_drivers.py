from etl.etl import LocalJSONFileJob
import time

class ApiIcelandDrivers(LocalJSONFileJob):
    @property
    def path_to_local_source_file(self):
        return 'data/api_iceland_samferda_drivers.json'
    
    @property
    def schema(self):
        return 'iceland'

    @property
    def table(self):
        return 'drivers'

    @property
    def properties_to_extract(self):
        return [
            'from',
            'to',
            'time',
            'date',
        ]

    def validate_date(self, input_date):
        try:
            # check if time is in the format %H:%M
            time.strptime(input_date, '%Y-%m-%d')

            # to check if year greater than present year
            # split and get the year (Assuming datetime to be in format %Y-%m-%d)
            input_year = int(input_date.split('-')[0])
            if input_year > time.localtime(time.time()).tm_year:
                # raise valueerror and give todays date
                raise ValueError('Future')

            # all fine
            return input_date
        except ValueError:

            # as of now, return todays date
            # get todays date
            todays_date = time.localtime(time.time())
            year = str(todays_date.tm_year)
            month = str(todays_date.tm_mon)
            day = str(todays_date.tm_mday)

            # return in format %Y-%m-%d
            return year + '-' + month + '-' + day

    def validate_time(self, input_time):
        try:
            # check if time is in the format %H:%M
            time.strptime(input_time, '%H:%M')
            return input_time
        except ValueError:
            # as of now, return 00:00
            return "00:00"

    def transform(self, extracted_data):
        items = extracted_data["results"]   # The API always wraps results

        parsed_data = []
        for item in items:
            row = []

            # validate and combine time and date
            # assumptions
            # - if date is invalid, returning current date
            # - if time is invalid, returning 00:00
            item['date'] = self.validate_date(item['date']) \
                                + 'T' \
                                + self.validate_time(item['time'])

            for prop in ['from', 'to', 'date']:
                row.append(item[prop])
            parsed_data.append(row)

        return parsed_data
