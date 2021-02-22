import requests
import os
# import json
# package for flattening json in pandas df
from pandas.io.json import json_normalize

from utils import get_todays_date, get_property_url, wait_seconds
from configs import DEBUG
from loader import store_to_file_as_csv, upload_to_aws


class ETL:
    def __init__(self):
        self.data_raw = None
        self.data_transformed = None
        self.file_name_source = f"{self.get_file_name()}.json"
        self.file_name_target = f"{self.get_file_name()}.csv"
        self.load_path_source = self.get_data_path(self.file_name_source)
        self.load_path_target = self.get_data_path(self.file_name_target)
        self.bucket_name = 'liveai'

    def execute(self, property_type=None):
        if property_type is None:
            property_type = "APARTMENTBUY"  # TODO what is it for houses?

        print(property_type)
        self.data_raw = self.extract(property_type)
        self.data_transformed = self.transform(self.data_raw)
        self.load(self.data_transformed,
                  self.load_path_target, self.bucket_name, self.file_name_target)

    def get_file_name(self):
        today = get_todays_date()
        return "immo"
        # file_name = "thinkimmo.json" if DEBUG else f"{today}_thinkimmo.json"
        # return file_name

    def get_data_path(self, file_name):
        # '../data/thinkimmo.json'
        current_path = os.path.dirname(__file__)
        relative_path_to_file = f"../data/{file_name}"
        absolute_file_path = os.path.join(current_path, relative_path_to_file)
        return absolute_file_path

    def extract(self, property_type):
        total = 600  # initial 600 until we get total from API
        size = 300
        offset = 0

        data_properties_raw = []

        if DEBUG:
            print("**********************")
            print("Start extracting data")

        retries_count = 0
        retries_max = 3

        # while offset < total and retries_count < retries_max:
        for _ in range(2):

            url = get_property_url(property_type, size, offset)

            # Get Session for requests
            session = requests.Session()

            try:
                if DEBUG:
                    print("Get API data")

                response = session.get(url=url)

                if response.status_code == requests.codes.ok:
                    if DEBUG:
                        print("Request OK")

                    data_properties_raw_response = response.json()
                    data_properties_raw = data_properties_raw + \
                        data_properties_raw_response['results']

                    total = data_properties_raw_response["total"]
                    offset = offset + size
                    # reset retries count
                    retries_count = 0

                    if DEBUG:
                        print("Response data")
                        print(f"{total} found")

                    # wait_seconds()
                else:
                    print(
                        f"Error while getting properties for {retries_count+1} time.")
                    print(f"Status code: {response.status_code}")
                    print(f"Url: {url}")
                    # increase retries count
                    retries_count = retries_count + 1

            except:
                print("Error while getting properties.")
                print(f"Url: {url}")
                # stop looping
                total = 0

        if DEBUG:
            print("Finish extracting data")
            print("**********************")

        return data_properties_raw

    def transform(self, json_raw):
        if DEBUG:
            print("**********************")
            print("Start transforming data")

        # normalize (tranform nested jsons to multiple columns) to dataframe
        df_raw = json_normalize(json_raw)
        # flatten nested json arrays for 'platforms' and 'buyingPriceHistory'
        df_flat = df_raw.assign(
            platforms=df_raw['platforms']).explode('platforms')
        df_flat = df_flat.assign(
            buyingPriceHistory=df_raw['buyingPriceHistory']).explode('buyingPriceHistory')
        # again transform nested json columns to clean dataframe
        df_normalized = json_normalize(df_flat.to_dict('records'))

        if DEBUG:
            print("Finish transforming data")
            print("**********************")

        return df_normalized

    def load(self, df, file_path, bucket_name, file_name_target):
        if DEBUG:
            print("**********************")
            print("Start loading data")
            print(f"Store data into {file_path}")
        store_to_file_as_csv(df, file_path)
        if DEBUG:
            print(f"Upload file into bucket '{bucket_name}'")
        uploaded = upload_to_aws(
            file_path, bucket_name, file_name_target)

        if DEBUG:
            print("Finish loading data")
            print("**********************")
