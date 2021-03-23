from enum import Enum

# GLOBAL CONFIGS
DEBUG = True


class TABLES(Enum):
    LOCATIONS = "live_ai_v2.locations"
    SOURCES = "live_ai_v2.sources"
    PRICE_HISTORY = "live_ai_v2.price_history"
    EXPOSES = "live_ai_v2.exposes"
    CALCULATION_PARAMETERS = "live_ai_v2.calculation_parameters"


columns_calculation_parameters = [
    "notary_rate",
    "rent_price_index_unil_40sqm",
    "rent_price_index_unil_80sqm",
    "rent_price_index_above_80sqm",
    "downpayment_in_percent",
    "interest_rate",
    "mortgage_payment_rate",
    "additional_costs_rate_per_m2",
    "personal_tax_rate",
    "land_register_rate",
    "real_estate_transfer_tax",
    "management_cost_in_percent",
]

columns_locations = [
    "postal_code",
    "state",
    "city",
    "longitude",
    "latitude",
    "rent_price_per_sqm_until_40sqm",
    "rent_price_per_sqm_until_80sqm",
    "rent_price_per_sqm_equal_above_80sqm",
    "notary_rate",
    "real_estate_transfer_tax",
    "land_register_tax"
]

columns_sources = [
    "id",
    "expose_id",
    "is_active",
    "source_key",
    "source_id",
    "property_category",
    "url",
    "creation_timestamp",
    'valid_until'
]

columns_price_history = [
    "id",
    "expose_id",
    "purchase_price",
    "source_key",
    "creation_timestamp"
]

columns_expose = [
    "id",
    "postal_code",
    "title",
    "source_key",
    "source_url",
    "purchase_price",
    "living_space",
    "cash_flow_gross_year",
    "roi_equity",
    "roi_rent_gross",
    "roi_pot_gross",
    "is_active",
    "city",
    "state",
    "district",
    "street",
    "street_number",
    "address_display_name",
    "property_category",
    "property_type",
    "longitude",
    "latitude",
    "condition",
    "creation_timestamp",
    "valid_until",
    "is_private_offer",
    "is_fore_closure",
    "is_leasehold",
    "has_vaccancy",
    "images",
    "construction_year",
    "floors",
    "rooms",
    "at_floor",
    "has_garden",
    "has_balcony",
    "has_lift",
    "purchase_price_per_sqm",
    "commission_rate",
    "commission_total",
    "management_cost_given",
    "management_cost_estimated",
    "management_cost",
    "rent_price_per_sqm_given",
    "rent_price_per_sqm_estimated",
    "rent_price_total_given",
    "rent_price_total_estimated",
    "rent_price_total",
    "mortgage_monthly",
    "interest_monthly",
    "notary_rate_in_percent",
    "notary_rate_total",
    "land_register_tax_in_percent",
    "land_register_tax_total",
    "real_estate_transfer_tax_in_percent",
    "real_estate_transfer_tax_total",
    "investment_sum",
    "side_costs",
    "construction_phase",
    "living_units",
    "commercial_units",
]
