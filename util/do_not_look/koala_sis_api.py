"""
This file defines the API that the candidate may use in this problem.

For the sake of the problem, please consult test_api_interface.py to understand how the API works.
Please do not scroll down to look at this implementation so that we may mimic some real life situations.
This file is NOT to be modified.




























































































Note: PLEASE TRY NOT TO READ THIS FILE. ONLY LOOK AT THIS IMPLEMENTATION IF YOU NEED A HINT.
"""
import csv
import json
import random
import time


class KoalaSisDataClient:
    # Mimic unreliable third party dependency in a pipeline
    CRASH_PROB = 0.3
    SLEEP_PER_RECORD = 0.1

    def __init__(self, credentials):
        self.credentials = credentials

    def _get_data(self, data, column_names, sort_order=None, batch_size=None, records_limit=None):
        if batch_size is None:
            batch_size = len(data)
        if records_limit is None:
            records_limit = len(data)

        if sort_order is None:
            random.shuffle(data)
        elif sort_order == 'asc':
            data.sort()
        elif sort_order == 'desc':
            data.sort(reverse=True)
        else:
            raise ValueError("Unrecognized parameter: {}".format(sort_order))

        if random.random() < self.CRASH_PROB:
            raise RuntimeError("Unknown Koala SIS API error")

        # convert data records to dict:
        dict_data = list()
        for row in data:
            dict_data.append(dict(zip(column_names, row)))
        data = dict_data

        n = batch_size
        data = data[:records_limit]
        for records in [data[i * n:(i + 1) * n] for i in range((len(data) + n - 1) // n)]:
            num_records = len(records)
            time.sleep(num_records * self.SLEEP_PER_RECORD)
            yield json.dumps(records)

    def get_student_data(self, sort_order=None, batch_size=None, records_limit=None):
        data, column_names = self._load_data_from_csv('./do_not_look/test_data_students.csv')
        return self._get_data(data, column_names, sort_order, batch_size, records_limit)

    def get_schools_data(self, sort_order=None, batch_size=None, records_limit=None):
        data, column_names = self._load_data_from_csv('./do_not_look/test_data_schools.csv')
        return self._get_data(data, column_names, sort_order, batch_size, records_limit)

    def get_enrollment_data(self, sort_order=None, batch_size=None, records_limit=None):
        data, column_names = self._load_data_from_csv('./do_not_look/test_data_enrollments.csv')
        return self._get_data(data, column_names, sort_order, batch_size, records_limit)

    def _load_data_from_csv(self, file_path):
        data = list()
        with open(file_path) as fh:
            csv_reader = csv.reader(fh, delimiter=',')
            # skip header
            cols = next(csv_reader)
            for row in csv_reader:
                # replace empty strings with Nones
                for col in range(0, len(row)):
                    if row[col] == '':
                        row[col] = None
                data.append(row)

        return data, cols
