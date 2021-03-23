from datetime import datetime
from shared.utils import get_property_url
from shared.configs_global import DEBUG
import requests


def extract(property_type):
    total = 600  # initial 600 until we get total from API
    size = 300
    offset = 0

    data_properties_raw = []

    retries_count = 0
    retries_max = 3

    before = datetime.now()

    # TODO get all exposes
    # for _ in range(2):
    while offset < total and retries_count < retries_max:

        url = get_property_url(property_type, size, offset)

        # Get Session for requests
        session = requests.Session()

        try:
            if DEBUG:
                print(
                    f"INFO: Get API data batch {offset}-{offset+size} of total: {total}")

            response = session.get(url=url)
            print(response)

            if response.status_code == requests.codes.ok:
                if DEBUG:
                    print("SUCCESS: Request OK")

                data_properties_raw_response = response.json()
                data_properties_raw = data_properties_raw + \
                    data_properties_raw_response['results']

                total = data_properties_raw_response["total"]
                offset = offset + size
                # reset retries count
                retries_count = 0

                if DEBUG:
                    print(f"INFO: Response data {total} found")

                # wait_seconds()
            else:
                print(
                    f"ERROR: while getting properties for {retries_count+1} time.")
                print(f"ERROR: Status code: {response.status_code}")
                print(f"ERROR: Url - {url}")
                # increase retries count
                retries_count = retries_count + 1

        except requests.exceptions.RequestException as error:
            print("ERORR:  while getting properties.")
            print(f"ERROR: Url - {url}")
            print(f"ERROR:{error}")
            # stop looping
            total = 0

    duration = (datetime.now() - before).total_seconds()
    print(f"SUCCESS: Data extracted in {duration} seconds.")

    return data_properties_raw
