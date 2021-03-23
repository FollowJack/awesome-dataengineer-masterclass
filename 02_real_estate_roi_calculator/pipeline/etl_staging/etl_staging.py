import os
from datetime import datetime
# import json
# package for flattening json in pandas df
from pandas.io.json import json_normalize

from shared.utils import get_todays_date, wait_seconds
from shared.configs_global import DEBUG
from etl_staging.loader import store_to_file_as_csv, store_to_file_as_json, upload_to_aws
from etl_staging.extractor import extract


class ETL:
    def __init__(self):
        self.data_raw = None
        self.data_transformed = None
        self.file_name_source = None
        self.file_name_target = None
        self.load_path_source = None
        self.load_path_target = None
        self.bucket_name = 'liveai'

    # property_category (APARTMENTBUY, HOUSEBUY)
    def execute(self, property_category=None):
        before = datetime.now()

        if property_category is None:
            property_category = "APARTMENTBUY"  # or HOUSEBUY
        if DEBUG:
            print(f"INFO: Execute for category: {property_category}")
        self.file_name_source = f"{self.get_file_name(property_category)}.json"
        self.file_name_target = f"{self.get_file_name(property_category)}.csv"
        self.load_path_source = self.get_data_path(self.file_name_source)
        self.load_path_target = self.get_data_path(self.file_name_target)
        self.data_raw = self.extract(property_category, self.load_path_source)
        self.data_transformed = self.transform(self.data_raw)
        self.load(self.data_transformed, self.load_path_target,
                  self.bucket_name, self.file_name_target)
        duration = (datetime.now() - before).total_seconds()
        print(
            f"SUCCESS: Pipeline successful - loaded {self.data_transformed.shape} entries in {duration} seconds.")

    def get_file_name(self, category):
        today = get_todays_date()
        property_category = "HOUSE" if category == 'HOUSEBUY' else 'FLAT'
        file_name = f"{today}_{property_category}_thinkimmo"
        # file_name = "thinkimmo.json" if DEBUG else f"{today}_thinkimmo.json"
        return file_name

    def get_data_path(self, file_name):
        # '../data/thinkimmo.json'
        current_path = os.path.dirname(__file__)
        relative_path_to_file = f"../../data/{file_name}"
        absolute_file_path = os.path.join(current_path, relative_path_to_file)
        return absolute_file_path

    def extract(self, property_type, path):
        if DEBUG:
            print("_"*50)
            print("INFO: Start extracting data")

        data_properties_raw = extract(property_type)
        store_to_file_as_json(data_properties_raw, path)

        if DEBUG:
            print(
                f"SUCCESS: Finish extracting data: {len(data_properties_raw)} entries.")
            print("_"*50)

        return data_properties_raw

    def transform(self, json_raw):
        if DEBUG:
            print("_"*50)
            print("INFO: Start transforming data")

        # normalize (tranform nested jsons to multiple columns) to dataframe
        df_raw = json_normalize(json_raw)
        # flatten nested json arrays for 'platforms' and 'buyingPriceHistory'
        df_flat = df_raw.assign(
            platforms=df_raw['platforms']).explode('platforms')
        df_flat = df_flat.assign(
            buyingPriceHistory=df_raw['buyingPriceHistory']).explode('buyingPriceHistory')
        # again transform nested json columns to clean dataframe
        df_normalized = json_normalize(df_flat.to_dict('records'))
        df_normalized.set_index('id', inplace=True)

        if DEBUG:
            print("SUCCESS: Finish transforming data")
            print("_"*50)

        return df_normalized

    def load(self, df, file_path, bucket_name, file_name_target):
        if DEBUG:
            print("_"*50)
            print(f"INFO: Start loading data into {file_path}")
        store_to_file_as_csv(df, file_path)
        if DEBUG:
            print(f"INFO: Upload file into bucket '{bucket_name}'")
        # uploaded = upload_to_aws(
        #     file_path, bucket_name, file_name_target)

        if DEBUG:
            print("SUCCESS: Finish loading data")
            print("_"*50)
