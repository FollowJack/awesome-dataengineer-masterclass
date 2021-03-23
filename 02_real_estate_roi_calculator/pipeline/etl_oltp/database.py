import psycopg2
import pandas as pd
import os
import sys
from dotenv import load_dotenv
from etl_oltp.queries import query_drop_table_if_exists, query_create_table_locations


class Database():
    """ Connect to the PostgreSQL database server with credentials given from .env"""

    def __init__(self):
        self.connection_parameter = self.get_connection_parameter()
        self.connection = self.connect(self.connection_parameter)

    def get_connection_parameter(self):
        load_dotenv()
        DB_HOST = os.getenv('DB_HOST')
        DB_NAME = os.getenv('DB_NAME')
        DB_USER = os.getenv('DB_USER')
        DB_PASSWORD = os.getenv('DB_PASSWORD')
        DB_SCHEMA = os.getenv('DB_SCHEMA')

        connection_parameter = {
            "host": DB_HOST,
            "database": DB_NAME,
            "user": DB_USER,
            "password": DB_PASSWORD,
            "connect_timeout": 3,
            "keepalives": 0,
        }
        # "keepalives_idle": 250,
        # "keepalives_interval": 5,
        # "keepalives_count": 5
        return connection_parameter

    def connect(self, connection_parameter):
        """ Connect to the PostgreSQL database server """
        connection = None
        try:
            # connect to the PostgreSQL server
            print('INFO: Connect to PostgreSQL db')
            connection = psycopg2.connect(
                **connection_parameter)  # ?tcpKeepAlive=true
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            sys.exit(1)
        print("SUCCESS: DB Connection successful")
        return connection

    def execute_query_in_pandas(self, query):
        try:
            df = pd.read_sql_query(query, self.connection)
        except psycopg2.OperationalError:
            print("ERROR: Operational error")
            self.connection.close()
            self.connection = self.connect(self.connection_parameter)
            df = pd.read_sql_query(query, self.connection)
        except (Exception) as error:
            print(f"ERROR: General error: {error}")
            self.connection.close()
            self.connection = self.connect(self.connection_parameter)
            df = pd.read_sql_query(query, self.connection)
        return df

    # TODO rename
    def execute_query(self, query):
        is_success = False
        try:
            cursor = self.connection.cursor()

            try:
                cursor.execute(query)
            except psycopg2.IntegrityError:
                self.connection.rollback()
            else:
                self.connection.commit()

            cursor.close()
            is_success = True
        except (Exception, psycopg2.DatabaseError) as error:
            print('ERROR: while query -', error)
        print("SUCCESS: Query DONE" if is_success else "ERROR: Query invalid")

    def execute_query_many_update(self, query, values):
        is_success = False
        try:
            cursor = self.connection.cursor()

            try:
                tuples = [tuple(x) for x in values]
                placeholders = "(" + "%s,"*len(values[0])
                placeholders = placeholders[:-1] + ")"
                values_for_db = [cursor.mogrify(placeholders, tup).decode(
                    'utf8') for tup in tuples]
                # append values to query
                query_complete = query.format(values=",".join(values_for_db))

                # for debug puposes
                # print("DEBUG: Query")
                # print("*"*2000)
                # print(f"Query: {query_complete}")
                # print("*"*2000)

                cursor.execute(query_complete, values_for_db)
            except (psycopg2.DataError) as db_error:
                self.connection.rollback()
                print('ERROR: Data - While many query update -', db_error)
            except (psycopg2.IntegrityError) as db_error:
                self.connection.rollback()
                print('ERROR: Integrity - While many query update -', db_error)
            else:
                self.connection.commit()

            cursor.close()
            is_success = True
        except (Exception, psycopg2.DatabaseError) as error:
            print('ERROR: while many query update -', error)
            self.connection = self.connect(self.connection_parameter)
            self.execute_query_many_update(query, values)
        print("SUCCESS: Many query update DONE" if is_success else "ERROR: Many query update invalid")

    def execute_query_many_insert(self, query, values):
        is_success = False
        try:
            cursor = self.connection.cursor()

            try:
                # Create a list of tupples from the dataframe values
                tuples = [tuple(x) for x in values]
                # SQL quert to execute
                # get length of columns from first row
                placeholders = "(" + "%s,"*len(values[0])
                placeholders = placeholders[:-1] + ")"
                values_for_db = [cursor.mogrify(placeholders, tup).decode(
                    'utf8') for tup in tuples]
                # append values to query
                query_complete = query.format(values=",".join(values_for_db))

                # for debug puposes
                # print("DEBUG: Query")
                # print("*"*3000)
                # print(f"Query: {query_complete}")
                # print("*"*3000)

                cursor.execute(query_complete, values_for_db)
            except (psycopg2.DataError) as db_error:
                self.connection.rollback()
                print('ERROR: Data - While many query update -', db_error)
                self.execute_query_many_insert(query, values)
            except (psycopg2.IntegrityError) as db_error:
                self.connection.rollback()
                print('ERROR: Integrity - While many query update -', db_error)
                self.execute_query_many_insert(query, values)
            except (psycopg2.DatabaseError) as db_error:
                self.connection.rollback()
                print('ERROR: Database - While many query update -', db_error)
                self.connection = self.connect(self.connection_parameter)
                self.execute_query_many_insert(query, values)
            else:
                print("SUCCESS: Many query update commit")
                self.connection.commit()

            cursor.close()
            is_success = True
        except (Exception) as error:
            print('ERROR: while many query insert -', error)
            self.connection = self.connect(self.connection_parameter)
            self.execute_query_many_insert(query, values)

        print("SUCCESS: Many query update DONE" if is_success else "ERROR: Many query update invalid")

    def drop_table(self, table_name):
        print(f"INFO: Delete table: {table_name}")
        self.execute_query(query_drop_table_if_exists.format(
            table_name=table_name))

    def get_left_anti_join(self, df_left, df_right, column_key):
        key_difference = set(df_left[column_key]).difference(
            df_right[column_key])
        where_difference = df_left[column_key].isin(key_difference)
        return df_left[where_difference]

    def get_left_join(self, df_left, df_right, column_key_left, column_key_right):
        df_intersect = pd.merge(
            df_left, df_right, how='left', left_on=column_key_left, right_on=column_key_right,  suffixes=('', '_add'))
        return df_intersect

    def get_inner_join(self, df_left, df_right, column_key):
        df_intersect = pd.merge(
            df_left, df_right, on=column_key,  suffixes=('', '_new'))
        return df_intersect
