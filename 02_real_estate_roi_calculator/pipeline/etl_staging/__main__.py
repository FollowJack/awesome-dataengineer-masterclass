from etl_staging import ETL
import argparse
import sys

sys.path.insert(1, '../shared')
sys.path.insert(2, '../data')
sys.path.insert(3, '../etl_staging')
sys.path.insert(4, '../pipeline')


def main(property_category):
    etl = ETL()
    etl.execute(property_category)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Execute the exposes source pipeline')
    parser.add_argument('--category', metavar='str', required=True,
                        help='choose either APARTMENTBUY or HOUSEBUY')
    args = parser.parse_args()
    main(property_category=args.category)
    main()
