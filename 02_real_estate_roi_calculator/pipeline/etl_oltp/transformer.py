from datetime import datetime
import hashlib
import json
import pandas as pd
import numpy as np
from shared.configs_global import columns_expose, columns_locations, columns_sources, columns_price_history
from shared.configs_global import DEBUG


def is_json(my_json):
    try:
        json_object = json.loads(my_json)
    except (ValueError, TypeError) as e:
        return False
    return True


def get_expose_id_hash(row):
    try:
        return str(hashlib.md5((row['propertyCategory'] + row['zip'] + row['title'] + str(row['squareMeter'])).encode("utf8")).hexdigest())
        # return str(hash(row['zip'] + row['title'] + str(row['squareMeter'])))
    except Exception as e:
        print(f"ERROR:  while getting expose id hash for {row['id']}")
        print(f"ERROR: {e}")
        return None


def get_expose_id(df):
    return df.apply(lambda row: str(get_expose_id_hash(row)), axis=1)


def get_source_id(df):
    return df.apply(lambda row: str(hash(get_expose_id_hash(row) + str(row['platforms.url']))), axis=1)


def get_price_history_id(df):
    return df.apply(lambda row: str(hash(get_expose_id_hash(row) + str(row['buyingPriceHistory.platformName']) + str(row['buyingPriceHistory.buyingPrice']))), axis=1)


def get_roi_gross(cold_rent_year, purchase_price):
    # DE: Brutto Mietrendite
    # cold_rent_year / purchase_price
    return cold_rent_year / purchase_price * 100


def get_cashflow_after_operating_expenses(cold_rent, vacancy, house_allowance, interest, mortgage):
    # (cold_rent - vacancy_rate) - house_allowance - interest - mortgage
    return cold_rent - vacancy - house_allowance - interest - mortgage


def get_cashflow_after_taxes(cold_rent, vacancy, house_allowance, interest, mortgage, tax):
    return cold_rent - vacancy - house_allowance - interest - mortgage - tax


def get_cashflow_after_reserves(cold_rent, vacancy, house_allowance, interest, mortgage, tax, maintenance_reserve):
    return cold_rent - vacancy - house_allowance - interest - mortgage - tax - maintenance_reserve


def get_tax_year(cold_rent, vacancy, house_allowance, interest, tax_write_off, private_maintenance_reserve):
    # cold rent - vacancy - house allowance - interest - tax write off - private maintenance reserve
    return cold_rent - vacancy - house_allowance - interest - tax_write_off - private_maintenance_reserve


def get_new_calculation_parameters(_, __):
    if DEBUG:
        print("INFO: Transform calculation parameter entry")
    before = datetime.now()

    # create locations datframe

    df = pd.DataFrame(index=[0])
    df["notary_rate"] = 1.0
    df["rent_price_index_unil_40sqm"] = 8.0
    df["rent_price_index_unil_80sqm"] = 7.0
    df["rent_price_index_above_80sqm"] = 8.0
    df["downpayment_in_percent"] = 5.0
    df["interest_rate"] = 1.5
    df["mortgage_payment_rate"] = 3.0
    df["additional_costs_rate_per_m2"] = 1.5
    df["personal_tax_rate"] = 37.0
    df["land_register_rate"] = 0.5
    df["real_estate_transfer_tax"] = 6.5
    df["management_cost_in_percent"] = 35.0

    duration = (datetime.now() - before).total_seconds()
    print(
        f"SUCCESS: {df.shape} calculation_parameters entries formatted in {duration} seconds.")
    return df


def get_new_locations(df, _):
    if DEBUG:
        print("INFO: Transform locations entries")
    before = datetime.now()

    # create locations datframe

    df_locations_table = pd.DataFrame(columns=columns_locations)
    df_locations_table['postal_code'] = df['zip']
    df_locations_table['state'] = df['region']
    # replace bremen for google maps
    df_locations_table['state'] = df_locations_table['state'].replace(
        "Freie Hansestadt Bremen", "Bremen")
    df_locations_table['city'] = df['address.city']
    df_locations_table['city'] = df_locations_table['city'].fillna(df['address.town']).fillna(df['aggregations.location.name']).fillna(
        df['oAddress.is24.location']).fillna(df['originalAddress.location']).fillna(df['oAddress.ebk.location'])
    df_locations_table['longitude'] = None
    df_locations_table['latitude'] = None
    df_locations_table['rent_price_per_sqm_until_40sqm'] = None
    df_locations_table['rent_price_per_sqm_until_80sqm'] = None
    df_locations_table['rent_price_per_sqm_equal_above_80sqm'] = None
    df_locations_table['notary_rate'] = None
    df_locations_table['real_estate_transfer_tax'] = None
    df_locations_table['land_register_tax'] = None

    df_locations_table = df_locations_table.drop_duplicates(
        'postal_code', keep="first")

    duration = (datetime.now() - before).total_seconds()
    print(
        f"SUCCESS: {df_locations_table.shape} locations entries formatted in {duration} seconds.")
    return df_locations_table


def get_new_sources(df, property_category):
    if DEBUG:
        print("INFO: Transform source entries")
    before = datetime.now()

    # drop invalid sources
    df = df.dropna(subset=['platforms.url'])

    # create locations datframe
    df_source_table = pd.DataFrame(columns=columns_sources)
    df_source_table['id'] = get_source_id(df)
    df_source_table['expose_id'] = get_expose_id(df)
    # is always True or False
    df_source_table['is_active'] = df['platforms.active']
    df_source_table['source_key'] = df['platforms.name']
    df_source_table['source_id'] = df['platforms.id']
    df_source_table['property_category'] = property_category
    df_source_table['url'] = df['platforms.url']
    df_source_table['creation_timestamp'] = df['platforms.creationDate']
    df_source_table['valid_until'] = np.where(df['platforms.deactivationDate'].notnull(
    ), df['platforms.deactivationDate'], '9999-12-31') if 'platforms.deactivationDate' in df else '9999-12-31'

    # drop duplicates
    df_source_table = df_source_table.drop_duplicates('id')

    duration = (datetime.now() - before).total_seconds()
    print(
        f"SUCCESS: {df_source_table.shape} source entries formatted in {duration} seconds.")
    return df_source_table


def get_new_price_history(df, _):
    if DEBUG:
        print("INFO: Transform price history entries.")
    before = datetime.now()

    # drop empty price histories
    df = df.dropna(subset=['buyingPriceHistory.buyingPrice',
                           'buyingPriceHistory.platformName'])

    # create locations datframe
    # buyingPriceHistory.buyingPrice	buyingPriceHistory.platformName	buyingPriceHistory.creationDate	buyingPriceHistory	platforms.deactivationDate
    df_price_history_table = pd.DataFrame(columns=columns_price_history)
    df_price_history_table['id'] = get_price_history_id(df)
    df_price_history_table['expose_id'] = get_expose_id(df)
    df_price_history_table['purchase_price'] = df['buyingPriceHistory.buyingPrice']
    df_price_history_table['source_key'] = df['buyingPriceHistory.platformName']
    df_price_history_table['creation_timestamp'] = df['buyingPriceHistory.creationDate']

    df_price_history_table = df_price_history_table.drop_duplicates(
        'id', keep="first")

    duration = (datetime.now() - before).total_seconds()
    print(
        f"SUCCESS: {df_price_history_table.shape} price history entries formatted in {duration} seconds.")
    return df_price_history_table


def get_new_exposes(df, property_category):
    if DEBUG:
        print("INFO: Transform exposes entries.")
    before = datetime.now()

    # create locations datframe
    df_exposes_table = pd.DataFrame(columns=columns_expose)
    # hash as part of pandas
    df_exposes_table['id'] = get_expose_id(df)
    df_exposes_table['postal_code'] = df['zip']
    df_exposes_table['title'] = df['title'].str.replace("%", "%%")
    df_exposes_table['source_url'] = df['platforms.url']
    df_exposes_table['source_key'] = df['platforms.name']
    df_exposes_table['purchase_price'] = df['buyingPrice']
    df_exposes_table['living_space'] = df['squareMeter']
    df_exposes_table['is_active'] = df['active']
    df_exposes_table['city'] = df['address.city']
    df_exposes_table['city'] = df_exposes_table['city'].fillna(df['address.town']).fillna(df['aggregations.location.name']).fillna(
        df['oAddress.is24.location']).fillna(df['originalAddress.location']).fillna(df['oAddress.ebk.location']).replace(np.nan, None)
    df_exposes_table['state'] = df['region']
    df_exposes_table['state'] = df_exposes_table['state'].replace(
        "Freie Hansestadt Bremen", "Bremen")
    df_exposes_table['district'] = df['originalAddress.district'].fillna(df['address.city_district']).fillna(
        df['oAddress.is24.district']).fillna(df['address.borough']).fillna(df['address.suburb']).fillna("None")
    df_exposes_table['street'] = df['address.road'].fillna(
        df['originalAddress.street']).fillna(df['oAddress.is24.street'])
    df_exposes_table['street_number'] = df['address.house_number']
    df_exposes_table['address_display_name'] = df['address.displayName']
    df_exposes_table['property_category'] = property_category
    df_exposes_table['property_type'] = df['apartmentType'] if property_category == 'FLAT' else df['buildingType']
    df_exposes_table['construction_phase'] = df['constructionPhase'] if property_category == 'HOUSE' else None
    df_exposes_table['living_units'] = df['livingUnits'] if property_category == 'HOUSE' else None
    df_exposes_table['commercial_units'] = df['commercialUnits'] if property_category == 'HOUSE' else None

    df_exposes_table["longitude"] = df['address.lon']
    df_exposes_table["latitude"] = df['address.lat']
    df_exposes_table['condition'] = df['condition']
    df_exposes_table['creation_timestamp'] = np.where(
        df['publishDate'].notnull(), df['publishDate'], datetime.now())
    df_exposes_table['valid_until'] = np.where(
        (df['active'] == False), datetime.now(), '9999-12-31')
    df_exposes_table['is_private_offer'] = df['privateOffer']
    df_exposes_table['is_fore_closure'] = df['foreClosure']
    df_exposes_table['is_leasehold'] = df['leasehold']
    df_exposes_table['has_vaccancy'] = df['rented']
    # TODO split json array into list of image urls
    df_exposes_table['images'] = df['images'].str.replace(
        "'", '"').str.replace("False", "false").str.replace("True", "true").str.replace("%", "%%").apply(
        lambda value: value if is_json(value) else None)
    df_exposes_table['construction_year'] = df['constructionYear']
    df_exposes_table['floors'] = df['numberOfFloors']
    df_exposes_table['rooms'] = df['rooms']
    df_exposes_table['at_floor'] = df['floor']
    df_exposes_table['has_lift'] = df['lift']
    df_exposes_table['has_garden'] = df['garden']
    df_exposes_table['has_balcony'] = df['balcony']
    df_exposes_table['purchase_price_per_sqm'] = df['buyingPrice'] / \
        df['squareMeter']
    df_exposes_table['commission_rate'] = df['comission']
    df_exposes_table['commission_total'] = np.where(
        pd.to_numeric(df['comission'], errors='coerce').notnull(), df['comission'] * df['buyingPrice'] / 100, 0)
    df_exposes_table['rent_price_per_sqm_given'] = np.where(
        df['rentPriceCurrent'].notnull(), df['rentPriceCurrent'] / df['squareMeter'], np.nan)
    # TODO calculate by postal code or state or default value

    # 100 exposes from 50 different postal codes, 10 exposes have only state (no city)
    # for every expose i need 3 attributes from locations table
    # calculation: price index (depends on square meter <=40,<=80,>80)
    # some of price index per postal code will be null --> take our average from calculation_parameters table
    # information about this is in locations table

    # 1 should we do it while loading the exposes or should we do it afterwards as second run over all exposes

    # TODO get value from join of location table or default value from database
    df_exposes_table['rent_price_per_sqm_estimated'] = 7
    df_exposes_table['rent_price_total_given'] = df['rentPriceCurrent']
    df_exposes_table['rent_price_total_estimated'] = df_exposes_table['rent_price_per_sqm_estimated'] * \
        df_exposes_table['living_space']
    df_exposes_table['rent_price_total'] = np.where(df_exposes_table['rent_price_total_given'].notnull(
    ), df_exposes_table['rent_price_total_given'], df_exposes_table['rent_price_total_estimated'])

    # TODO default value has to come from calculation parameter table
    # 100% - 5% as equity and 3% as default value for mortgage
    df_exposes_table['mortgage_monthly'] = (
        (df['buyingPrice'] * (100-5)) * 3.0) / (100 * 100 * 12)
    # TODO default value has to come from calculation parameter table
    # 100% - 5% as equity and 1.5 % as default value for interest
    df_exposes_table['interest_monthly'] = df['buyingPrice'] * \
        (100-5) * (1.5) / (100 * 100 * 12)
    # TODO has to come from locations table (postal or state)
    df_exposes_table['notary_rate_in_percent'] = 1.5
    df_exposes_table['notary_rate_total'] = df['buyingPrice'] * \
        df_exposes_table['notary_rate_in_percent'] / 100
    # TODO has to come from locations table (postal or state)
    df_exposes_table['land_register_tax_in_percent'] = 0.5
    df_exposes_table['land_register_tax_total'] = df['buyingPrice'] * \
        df_exposes_table['land_register_tax_in_percent'] / 100
    # TODO has to come from locations table (postal or state)
    df_exposes_table['real_estate_transfer_tax_in_percent'] = 5.0
    df_exposes_table['real_estate_transfer_tax_total'] = df['buyingPrice'] * \
        df_exposes_table['real_estate_transfer_tax_in_percent'] / 100
    df_exposes_table['side_costs'] = df_exposes_table['notary_rate_total'] + \
        df_exposes_table['land_register_tax_total'] + \
        df_exposes_table['real_estate_transfer_tax_total'] + \
        df_exposes_table['commission_total']
    # TODO get SIDE COSTS + 5% of purchase price from calculation parameters
    df_exposes_table['investment_sum'] = df_exposes_table['side_costs'] + \
        (df_exposes_table['purchase_price'] * 5 / 100)

    # CALCULATIONS
    # TODO get 35 from mangement cost
    df_exposes_table['management_cost_given'] = df['houseMoney']
    df_exposes_table['management_cost_estimated'] = df_exposes_table['rent_price_total'] * 35 / 100
    df_exposes_table['management_cost'] = np.where(
        df_exposes_table['management_cost_given'].notnull(), df_exposes_table['management_cost_given'], df_exposes_table['management_cost_estimated'])

    df_exposes_table['cash_flow_gross_year'] = (df_exposes_table['rent_price_total'] * 12) - \
        df_exposes_table['management_cost'] - \
        df_exposes_table['interest_monthly'] - \
        df_exposes_table['mortgage_monthly']
    # TODO get 5 from investment sum in percent
    # side costs + equity = investment sum
    df_exposes_table['roi_equity'] = df_exposes_table['cash_flow_gross_year'] / \
        (df_exposes_table['side_costs'] + (df['buyingPrice'] * 5 / 100)) * 100
    df_exposes_table['roi_rent_gross'] = (
        df_exposes_table['rent_price_total'] * 12) / (df['buyingPrice']) * 100
    # TODO calculate (Cold rent per year - management costs - private maintenance reserve) / total purchase price
    # TODO get 10 euro per sqm for private maintenace reserve from table
    df_exposes_table['roi_pot_gross'] = ((df_exposes_table['rent_price_total'] * 12) - df_exposes_table['management_cost'] - (
        df['squareMeter']*10)) * 100 / (df['buyingPrice'] + df_exposes_table['side_costs'])

    # for insert we want only the first expose wich is active and has lowest price
    df_exposes_table = df_exposes_table.sort_values(
        ['id', 'is_active', 'purchase_price'], ascending=[True, False, True])
    df_exposes_table = df_exposes_table.drop_duplicates('id', keep="first")

    duration = (datetime.now() - before).total_seconds()
    print(
        f"SUCCESS: {df_exposes_table.shape} expose entries created in {duration} seconds.")
    return df_exposes_table
