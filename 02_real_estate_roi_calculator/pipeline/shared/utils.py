from datetime import datetime
import time

from etl_staging.configs_staging import url_properties
from shared.configs_global import DEBUG


def get_todays_date():
    return datetime.today().strftime('%Y-%m-%d')


def get_property_url(property_type, size, offset):
    url = url_properties.format(
        property_type=property_type, size=size, offset=offset)
    if DEBUG:
        print(f"INFO: Type: {property_type}, size: {size}, offset: {offset}")
        print(f"INFO: Property url: {url}")
    return url


def wait_seconds():
    wait_in_seconds = 10
    print(f"Wait for {wait_in_seconds}")
    time.sleep(wait_in_seconds)
