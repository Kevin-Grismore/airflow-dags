"""
In this problem, you will write a hypothetical data pipeline to extract data from a source
Student Information System and conform it with a data schema for one of our Data Mart tables.

At Landing Zone, we use Apache Airflow for workflow orchestration, which breaks pipelines like
this into discrete tasks so that the whole pipeline doesn't have to be re-run if a portion
of it fails. Similarly, in this task you will write two functions which represent an 'extract'
task and a 'transform' task respectively.

For the 'extract' task, you can use the API defined in `koala_sis_api_interface.py` (meant
to represent a third party API) to get the data that you will need and store it in your local
file system (the /data/koala_sis folder). This is the `download_data_to_csv()` function.

For the 'transform' task, you should read in the local copies of your data made by the extract task
(the files you stored in the /data/koala_sis folder) and then merge and conform the data into a new
file, `student_demographics_and_enrollment.csv`, that will be stored in /data/data_mart. This is
the `conform_data(day)` function. The format (columns_ for this file will be: `SchoolId,NameOfInstitution,
StudentUniqueId,LastSurname,FirstName,DisplayName,Gender,EntryDate,ExitWithdrawDate`. Please
refer to the README for more explicit specifications for each of these columns.

Your goal is to implement these two functions and ensure that the file output at the end of your
pipeline (`student_demographics_and_enrollment.csv`) accurately represents the data in the source
system in the correct format.
"""

import logging
import typing as t
import csv
import json

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from pandas.tseries.offsets import BusinessDay

from util.do_not_look.koala_sis_api import KoalaSisDataClient
from util.simple_retry import simple_retry

CREDS = 'landing_zone_credentials'
DOWNLOAD_PATH = 'data/koala_sis'
DATA_MART_PATH = 'data/data_mart'
TARGET_SCHEMA = [
    'SchoolId',
    'NameOfInstitution',
    'StudentUniqueId',
    'LastSurname',
    'FirstName',
    'DisplayName',
    'Gender',
    'EntryDate',
    'ExitWithdrawDate',
]

@dag(
    dag_id="test-pipeline",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def test_pipeline():

    @simple_retry(max_retries=5)
    def get_sample_record(download_func: t.Callable) -> dict:
        """
        Gets the first record from a given Koala SIS 'get_*_data' function and returns
        it as a Python dictionary. Used to instantiate a csv.DictWriter, which needs
        access to the record's keys to use as column names.

        Retries up to simple_retry's configured max_retries limit.

        :param download_func: (t.Callable) a Koala SIS client function for fetching data

        :return: (dict) the sample record
        """
        return json.loads(list(download_func(sort_order='asc', records_limit=1))[0])[0]


    @simple_retry(max_retries=5)
    def write_records(download_func: t.Callable, writer: csv.DictWriter) -> None:
        """
        Gets batches of records from a given Koala SIS 'get_*_data' function and writes
        them to a csv.

        Retries up to simple_retry's configured max_retries limit.

        :param download_func: (t.Callable) a Koala SIS client function for fetching data
        :param writer: (csv.DictWriter) a writer instantiated with a sample record's keys
            as column names
        """
        for batch in download_func(sort_order='asc', batch_size=50):
            loaded_batch = json.loads(batch)
            writer.writerows(loaded_batch)
            logging.info(f'Wrote {len(loaded_batch)} records')

    @task(task_id='download_data_to_csv')
    def download_data_to_csv() -> None:
        """
        Uses the KoalaSisDataClient to download the students, schools, and enrollments data
        and stores them as CSV files in the data/koala_sis directory. The file names should
        be students.csv, schools.csv, and enrollments.csv.
        :return: None
        """
        client = KoalaSisDataClient(credentials=CREDS)

        download_map = {
            client.get_student_data: f'{DOWNLOAD_PATH}/students.csv',
            client.get_schools_data: f'{DOWNLOAD_PATH}/schools.csv',
            client.get_enrollment_data: f'{DOWNLOAD_PATH}/enrollments.csv',
        }

        for download_func, filename in download_map.items():
            sample_record = get_sample_record(download_func)

            logging.info(f'Writing {filename}...')
            with open(filename, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=sample_record.keys())
                writer.writeheader()
                write_records(download_func, writer)

    @task.virtualenv(
        task_id='conform_data', requirements=['pandas==2.0.1'], system_site_packages=False
    )
    def conform_data() -> None:
        """
        Reads in the CSV files from the data/koala_sis directory and merges them together
        into the student_demographics_and_enrollment.csv file, stored in the data/data_mart
        directory.
        :return: None
        """
        # Read in the tables
        students_df = pd.read_csv(f'{DOWNLOAD_PATH}/students.csv')
        schools_df = pd.read_csv(
            f'{DOWNLOAD_PATH}/schools.csv', parse_dates=['start_date', 'end_date']
        )
        enrollments_df = pd.read_csv(
            f'{DOWNLOAD_PATH}/enrollments.csv',
            parse_dates=['enrollment_start_date', 'enrollment_end_date'],
        )

        # Merge the tables on common ids
        out_df = schools_df.merge(
            right=enrollments_df,
            left_on='id',
            right_on='school_id',
            suffixes=('_sch', '_enrl'),
        ).merge(
            right=students_df,
            left_on='student_id',
            right_on='id',
        )

        # Name formatting
        out_df['first_name'] = out_df['first_name'].str.title()
        out_df['last_name'] = out_df['last_name'].str.title()
        out_df['DisplayName'] = out_df['last_name'] + ', ' + out_df['first_name']

        # Gender formatting
        out_df['gender'] = out_df['gender'].replace(
            {'male': 'M', 'female': 'F', 'other': 'X'}
        )

        # Find the next weekday for all non-null enrollment end dates that are earlier
        # than the end date for that school
        next_weekday = out_df[out_df['enrollment_end_date'] < out_df['end_date']][
            'enrollment_end_date'
        ] + BusinessDay(n=1)

        # For null enrollment end dates or enrollment end dates that match the school's end date,
        # use the end date of the school, then combine with non-null next weekdays
        out_df['ExitWithdrawDate'] = out_df[
            (out_df['enrollment_end_date'].isna())
            | (out_df['enrollment_end_date'] == out_df['end_date'])
        ]['end_date']
        out_df['ExitWithdrawDate'] = out_df['ExitWithdrawDate'].combine_first(next_weekday)

        # Rename columns
        out_df.rename(
            mapper={
                'school_id': 'SchoolId',
                'school_name': 'NameOfInstitution',
                'local_student_id': 'StudentUniqueId',
                'last_name': 'LastSurname',
                'first_name': 'FirstName',
                'gender': 'Gender',
                'enrollment_start_date': 'EntryDate',
            },
            axis='columns',
            inplace=True,
        )

        # Remove columns not required in output
        out_df.drop(
            labels=[
                col_name
                for col_name in list(out_df.columns)
                if col_name not in TARGET_SCHEMA
            ],
            axis='columns',
            inplace=True,
        )

        # Set desired column order and sort values for readability
        out_df = out_df.reindex(columns=TARGET_SCHEMA).sort_values(
            by=['SchoolId', 'StudentUniqueId']
        )

        # Write the output file
        logging.info(f'Writing {DATA_MART_PATH}/student_demographics_and_enrollment.csv...')
        out_df.to_csv(
            f'{DATA_MART_PATH}/student_demographics_and_enrollment.csv', index=False
        )

    download_data_to_csv() >> conform_data()
        
dag = test_pipeline()