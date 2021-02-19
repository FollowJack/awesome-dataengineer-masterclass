from datetime import datetime
import time

from configs import DEBUG, url_properties


def get_todays_date():
    return datetime.today().strftime('%Y-%m-%d')


def get_property_url(property_type, size, offset):
    url = url_properties.format(
        property_type=property_type, size=size, offset=offset)
    if DEBUG:
        print(f"Type: {property_type}, size: {size}, offset: {offset}")
        print(f"Property url: {url}")
    return url


def wait_seconds():
    wait_in_seconds = 10
    print(f"Wait for {wait_in_seconds}")
    time.sleep(wait_in_seconds)
