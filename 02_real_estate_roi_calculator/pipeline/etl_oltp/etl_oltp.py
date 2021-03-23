import os
import pandas as pd
import numpy as np
from datetime import datetime
from shared.configs_global import DEBUG, TABLES, columns_expose
from etl_oltp.database import Database
from etl_oltp.queries import query_select, query_select_where, query_insert_locations, query_insert_sources, query_insert_price_history, query_insert_exposes, query_create_table_locations, query_create_table_sources, query_create_table_price_history, query_create_table_exposes, query_update_sources, query_update_exposes, query_create_table_calculation_parameters, query_insert_calculation_parameters, query_select_latest
from etl_oltp.transformer import get_new_locations, get_new_sources, get_new_price_history, get_new_exposes, get_new_calculation_parameters
from shared.utils import get_todays_date, get_property_url, wait_seconds


class ETL:
    def __init__(self):
        self.data_raw = None
        self.data_clean = None
        self.data_locations_transformed = None
        self.data_exposes_transformed = None
        self.data_sources_transformed = None
        self.data_price_history_transformed = None
        self.data_locations_transformed_new = None
        self.data_exposes_transformed_new = None
        self.data_sources_transformed_new = None
        self.data_price_history_transformed_new = None
        self.data_locations_transformed_update = None
        self.data_exposes_transformed_update = None
        self.data_sources_transformed_update = None
        self.data_price_history_transformed_update = None
        self.file_name_source = None
        self.load_path_source = None
        self.bucket_name = 'liveai'  # TODO get from ENV file
        self.database = Database()

    def execute_calculation_parameters(self):
        self.database.drop_table(f"live_ai_v2.calculation_parameters")
        self.database.execute_query(query_create_table_calculation_parameters.format(
            table_name=f"live_ai_v2.calculation_parameters"))
        self.data_calculation_parameters_transformed_new = self.transform_new_batch(
            None, TABLES.CALCULATION_PARAMETERS, None)
        self.load_insert(self.database, self.data_calculation_parameters_transformed_new,
                         TABLES.CALCULATION_PARAMETERS)

    # property_category = "FLAT" or "HOUSE"
    def execute(self, property_category="FLAT", shoud_create_tables=True, load_path_source=None):
        before = datetime.now()

        self.file_name_source = f"{self.get_file_name(property_category)}.csv"
        if load_path_source is not None:
            self.load_path_source = load_path_source
        else:
            self.load_path_source = self.get_data_path(self.file_name_source)

        if DEBUG:
            print("*"*50)
            print(f"INFO: EXTRACT data from file {self.load_path_source}")
            print("*"*50)

        # EXTRACT
        self.data_raw = self.extract(
            self.bucket_name, self.load_path_source, self.file_name_source)
        if DEBUG:
            print("*"*50)
            print(f"INFO: CLEAN data")
            print("*"*50)
        self.data_clean = self.clean(self.data_raw, property_category)
        # Create tables
        if shoud_create_tables:
            if DEBUG:
                print("*"*50)
                print(f"INFO: DROP and CREATE tables")
                print("*"*50)
            self.execute_calculation_parameters()
            self.database.drop_table(f"live_ai_v2.locations")
            self.database.drop_table(f"live_ai_v2.exposes")
            self.database.drop_table(f"live_ai_v2.sources")
            self.database.drop_table(f"live_ai_v2.price_history")
            self.database.execute_query(query_create_table_locations.format(
                table_name=f"live_ai_v2.locations", table_name_foreign=f"live_ai_v2.exposes"))
            self.database.execute_query(query_create_table_exposes.format(
                table_name=f"live_ai_v2.exposes", table_name_foreign=f"live_ai_v2.locations"))
            self.database.execute_query(query_create_table_sources.format(
                table_name=f"live_ai_v2.sources", table_name_foreign=f"live_ai_v2.exposes"))
            self.database.execute_query(query_create_table_price_history.format(
                table_name=f"live_ai_v2.price_history", table_name_foreign=f"live_ai_v2.exposes"))
            # TRANSFORM
        if DEBUG:
            print("*"*50)
            print(f"INFO: TRANSFORM data")
            print("*"*50)
        # Tranform new batch of data
        self.data_locations_transformed = self.transform_new_batch(
            self.data_clean, TABLES.LOCATIONS, property_category)
        self.data_exposes_transformed = self.transform_new_batch(
            self.data_clean, TABLES.EXPOSES, property_category)
        self.data_sources_transformed = self.transform_new_batch(
            self.data_clean, TABLES.SOURCES, property_category)
        self.data_price_history_transformed = self.transform_new_batch(
            self.data_clean, TABLES.PRICE_HISTORY, property_category)
        # Transform new data to insert
        if DEBUG:
            print("*"*50)
            print(f"INFO: TRANSFORM data to insert")
            print("*"*50)
        self.data_locations_transformed_new = self.transform_insert(
            self.data_locations_transformed, TABLES.LOCATIONS)
        self.data_exposes_transformed_new = self.transform_insert(
            self.data_exposes_transformed, TABLES.EXPOSES)
        self.data_sources_transformed_new = self.transform_insert(
            self.data_sources_transformed, TABLES.SOURCES)
        self.data_price_history_transformed_new = self.transform_insert(
            self.data_price_history_transformed, TABLES.PRICE_HISTORY)
        # Transform data to be updated
        if DEBUG:
            print("*"*50)
            print(f"INFO: TRANSFORM data to update")
            print("*"*50)
        self.data_exposes_transformed_update = self.transform_existing(
            self.data_exposes_transformed, TABLES.EXPOSES, property_category)
        self.data_sources_transformed_update = self.transform_existing(
            self.data_sources_transformed, TABLES.SOURCES, property_category)
        # LOAD
        # Load new data into database
        if DEBUG:
            print("*"*50)
            print(f"INFO: LOAD data to insert")
            print("*"*50)
        self.load_insert(self.database, self.data_locations_transformed_new,
                         TABLES.LOCATIONS)
        self.load_insert(self.database, self.data_exposes_transformed_new,
                         TABLES.EXPOSES)
        self.load_insert(self.database, self.data_sources_transformed_new,
                         TABLES.SOURCES)
        self.load_insert(self.database, self.data_price_history_transformed_new,
                         TABLES.PRICE_HISTORY)
        # Load updated data into database
        if DEBUG:
            print("*"*50)
            print(f"INFO: LOAD data to update")
            print("*"*50)
        self.load_update(
            self.database, self.data_exposes_transformed_update, TABLES.EXPOSES)
        self.load_update(
            self.database, self.data_sources_transformed_update, TABLES.SOURCES)
        # TODO add check if pipeline was successful
        duration = (datetime.now() - before).total_seconds()
        print(
            f"SUCCESS: Pipeline finished successfully in {duration} seconds.")

    def get_file_name(self, property_category):
        today = get_todays_date()
        file_name = "thinkimmo.json" if DEBUG else f"{today}_{property_category}_thinkimmo.json"
        return file_name

    def get_data_path(self, file_name):
        # '../data/thinkimmo.json'
        current_path = os.path.dirname(__file__)
        relative_path_to_file = f"../../data/{file_name}"
        absolute_file_path = os.path.join(current_path, relative_path_to_file)
        return absolute_file_path

    def clean(self, df, property_category):
        before = datetime.now()

        # CLEAN data
        rowsCount = df['id'].nunique()

        # DROP
        # DROP all invalid exposes (Those which have no title, zip, buyingPrice or squaremeter)
        df = df.dropna(subset=['title', 'zip', 'buyingPrice', 'squareMeter'])
        # by: set non numeric to NaN and filter them out
        df['propertyCategory'] = property_category
        df = df[pd.to_numeric(df['squareMeter'], errors='coerce').notnull()]
        df = df[pd.to_numeric(df['buyingPrice'], errors='coerce').notnull()]
        df = df[pd.to_numeric(df['zip'], errors='coerce').notnull()]
        df = df[df['zip'].str.len() < 6]
        df = df[pd.to_numeric(df['address.lon'], errors='coerce').notnull()]
        df = df[pd.to_numeric(df['address.lat'], errors='coerce').notnull()]
        df = df[pd.to_numeric(
            df['buyingPriceHistory.buyingPrice'], errors='coerce').notnull()]
        # CAST
        # To numeric fields
        df['squareMeter'] = df['squareMeter'].astype(float)
        df['buyingPrice'] = df['buyingPrice'].astype(float)
        df['comission'] = pd.to_numeric(df['comission'], errors='coerce')
        df['houseMoney'] = pd.to_numeric(df['houseMoney'], errors='coerce')
        # To bool fields
        # TODO transform bool in to categorical value with unknown
        # df['rented'] = pd.to_numeric(df['rented'], errors='coerce') == 1
        # df['privateOffer'] = pd.to_numeric(
        #     df['privateOffer'], errors='coerce') == 1
        # df['foreClosure'] = pd.to_numeric(
        #     df['foreClosure'], errors='coerce') == 1
        # df['leasehold'] = pd.to_numeric(df['leasehold'], errors='coerce') == 1
        # df['privateOffer'] = pd.to_numeric(
        #     df['privateOffer'], errors='coerce') == 1
        # df['foreClosure'] = pd.to_numeric(
        #     df['foreClosure'], errors='coerce') == 1
        # df['leasehold'] = pd.to_numeric(df['leasehold'], errors='coerce') == 1
        # df['lift'] = pd.to_numeric(df['lift'], errors='coerce') == 1
        # df['balcony'] = pd.to_numeric(df['balcony'], errors='coerce') == 1
        # df['garden'] = pd.to_numeric(df['garden'], errors='coerce') == 1
        df['platforms.active'] = pd.to_numeric(
            df['platforms.active'], errors='coerce') == 1

        invalidVExposesCount = rowsCount - df['id'].nunique()
        if DEBUG:
            print(
                f"WARNING: There are {invalidVExposesCount} invalid exposes.")

        duration = (datetime.now() - before).total_seconds()
        print(f"SUCCESS: Clean data successfully in {duration} seconds.")

        return df

    def extract(self, bucket_name, load_path_source, file_name_source):
        if DEBUG:
            print("_"*50)
            print("INFO: Start extracting data")

        # TODO download file from S3 - file_name_source

        df = pd.read_csv(load_path_source, dtype={'zip': 'str'})

        if DEBUG:
            print(
                f"SUCCESS: Finish extracting data {df.shape} rows and entries")
            print("_"*50)

        return df

    def transform_new_batch(self, df_raw, table_name, property_category):
        # map the inputs to the function blocks
        options_transformer = {
            TABLES.LOCATIONS: get_new_locations,
            TABLES.SOURCES: get_new_sources,
            TABLES.PRICE_HISTORY: get_new_price_history,
            TABLES.EXPOSES: get_new_exposes,
            TABLES.CALCULATION_PARAMETERS: get_new_calculation_parameters
        }
        if DEBUG:
            print("_"*50)
            print(
                f"INFO: Start transforming all entries for table: '{table_name}'")

        property_category = 'FLAT' if property_category == 'FLAT' else 'HOUSE'

        if table_name == TABLES.EXPOSES:
            df_calculation_parameters = pd.read_sql(query_select_latest.format(
                table_name=TABLES.CALCULATION_PARAMETERS.value), con=self.database.connection)
            df_locations = pd.read_sql(query_select.format(
                table_name=TABLES.LOCATIONS.value), con=self.database.connection)
            # df_raw_with_parameters = pd.concat(
            #     [df_raw, df_calculation_parameters], axis=1)
            df_raw = self.database.get_left_join(
                df_raw, df_locations, 'zip', 'postal_code')

        df_new_batch = options_transformer[table_name](
            df_raw, property_category)

        if DEBUG:
            print(
                f"SUCCESS: Finish transforming all entries for table:'{table_name}' {df_new_batch.shape}")
            print("_"*50)

        return df_new_batch

    def transform_insert(self, df_transformed, table_name):
        options_key = {
            TABLES.LOCATIONS: "postal_code",
            TABLES.SOURCES: "id",
            TABLES.PRICE_HISTORY: "id",
            TABLES.EXPOSES: "id",
        }

        if DEBUG:
            print("_"*50)
            print(
                f"INFO: Start transforming new entries for table: '{table_name}'")

        df_existing = self.database.execute_query_in_pandas(query_select.format(
            table_name=table_name.value))
        # remove/update existing entries
        df_new_difference = self.database.get_left_anti_join(
            df_transformed, df_existing, options_key[table_name])

        if DEBUG:
            print(
                f"SUCCESS: Finish transforming new entries for table:'{table_name}' {df_new_difference.shape}")
            print("_"*50)

        return df_new_difference

    def transform_existing(self, df_transformed, table_name, property_category):
        if DEBUG:
            print("_"*50)
            print(
                f"INFO: Start transforming updated entries for table: '{table_name}'")

        # get existing entries
        df_existing = pd.read_sql(query_select_where.format(
            table_name=table_name.value, where_column='property_category', where_value=property_category), con=self.database.connection)

        # get only existing entries
        df_intersect = self.database.get_inner_join(
            df_existing, df_transformed, "id")

        # set other exposes/sources which are not included in batch inactive
        df_inactives = self.database.get_left_anti_join(
            df_existing, df_transformed, 'id')
        df_inactives = df_inactives[df_inactives['is_active'] == True]
        df_inactives['is_active'] = False
        df_inactives['valid_until'] = datetime.now()

        print(f"INFO: Set {df_inactives.shape} entries inactive")

        if table_name == TABLES.SOURCES:
            # drop entries that did not change
            df_intersect = df_intersect.drop(df_intersect[
                (df_intersect['is_active'] == df_intersect['is_active_new'])
            ].index)

            # update entries is_active attribute
            df_intersect['is_active'] = np.where(
                (df_intersect['is_active'] != df_intersect['is_active_new']), df_intersect['is_active_new'], df_intersect['is_active'])

            # update entries valid_until attribute
            df_intersect['valid_until'] = np.where(((df_intersect['is_active'] == True) & (
                df_intersect['is_active_new'] == False)), datetime.now(), '9999-12-31')

            # keep only desired colummns
            columns_keep = [
                'id', 'expose_id', 'is_active', 'source_key', 'source_id', 'url', 'creation_timestamp', 'valid_until']
            df_intersect = df_intersect[columns_keep]
            df_inactives = df_inactives[columns_keep]
        elif table_name == TABLES.EXPOSES:

            # DROP
            # drop entries that did not change
            # price is the same, source same
            df_intersect = df_intersect.drop(df_intersect[
                (df_intersect['source_key'] == df_intersect['source_key_new']) &
                (df_intersect['purchase_price'] == df_intersect['purchase_price_new']) &
                (df_intersect['is_active'] == df_intersect['is_active_new'])
            ].index)

            # UPDATE
            #   UPDATE PRICE + roi calculation
            #   same source
            #       lower  price --> update price
            df_intersect['purchase_price'] = np.where(
                ((df_intersect['purchase_price_new']
                  < df_intersect['purchase_price'])),
                df_intersect['purchase_price_new'], df_intersect['purchase_price'])
            #   UPDATE SOURCE
            #   different source + lower price --> update price
            df_intersect['source_key'] = np.where(
                ((df_intersect['purchase_price'] > df_intersect['purchase_price_new']) &
                 (df_intersect['source_key'] != df_intersect['source_key_new'])),
                df_intersect['source_key_new'], df_intersect['source_key'])
            df_intersect['source_url'] = np.where(
                ((df_intersect['purchase_price'] > df_intersect['purchase_price_new']) &
                 (df_intersect['source_key'] != df_intersect['source_key_new'])),
                df_intersect['source_url_new'], df_intersect['source_url'])
            #   UPDATE is_active
            df_intersect['is_active'] = np.where(
                (df_intersect['is_active'] != df_intersect['is_active_new']), df_intersect['is_active_new'], df_intersect['is_active'])
            # TODO check first if still has other active source
            #   before active now inactive + has no other active source --> set inactive
            #   before inactive now active + has no other active source --> set inactive
            # update entries valid_until attribute
            df_intersect['valid_until'] = np.where(((df_intersect['is_active'] == True) & (
                df_intersect['is_active_new'] == False)), datetime.now(), '9999-12-31')
            # keep only expose wich are active and lowest purchase price
            df_intersect = df_intersect.sort_values(
                ['id', 'is_active', 'purchase_price'], ascending=[True, False, True])
            # keep only desired colummns
            columns_keep = [
                'id', 'is_active', 'valid_until', 'purchase_price', 'source_key', 'source_url']
            df_intersect = df_intersect[columns_keep]
            df_inactives = df_inactives[columns_keep]
        df = df_intersect.append(df_inactives, ignore_index=True)
        if DEBUG:
            print(
                f"SUCCESS: Finish transforming updated entries for table: '{table_name}' {df.shape}")
            print("_"*50)
        return df

    def load_insert(self, database, df, table_name):
        if DEBUG:
            print("_"*50)
            print(f"INFO: Start loading insert data for table '{table_name}'")

        if df.shape[0] == 0:
            print(f"WARNING: No new data for table:'{table_name}'")
            print("_"*50)
            return

        # map the inputs to the function blocks
        options = {
            TABLES.LOCATIONS: query_insert_locations,
            TABLES.SOURCES: query_insert_sources,
            TABLES.PRICE_HISTORY: query_insert_price_history,
            TABLES.EXPOSES: query_insert_exposes,
            TABLES.CALCULATION_PARAMETERS: query_insert_calculation_parameters
        }

        before = datetime.now()
        database.execute_query_many_insert(
            options[table_name], df.values.tolist())
        duration = (datetime.now() - before).total_seconds()
        if DEBUG:
            print(
                f"SUCCESS: In table: '{table_name}' load {df.shape[0]} {table_name} in {duration} seconds.")
            print("_"*50)

    def load_update(self, database, df, table_name):
        if DEBUG:
            print("_"*50)
            print(f"Start loading updated data for table '{table_name}'")

        if df.shape[0] == 0:
            print(f"WARNING: No data update for table:'{table_name}'")
            print("_"*50)
            return

        # map the inputs to the function blocks
        options = {
            TABLES.SOURCES: query_update_sources,
            TABLES.EXPOSES: query_update_exposes,
        }

        before = datetime.now()
        database.execute_query_many_update(
            options[table_name], df.values.tolist())
        duration = (datetime.now() - before).total_seconds()
        if DEBUG:
            print(
                f"SUCCESS: In table '{table_name}' load {df.shape[0]} {table_name} in {duration} seconds.")
            print("_"*50)
