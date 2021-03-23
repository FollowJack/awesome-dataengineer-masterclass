query_select = "SELECT * FROM {table_name}"
query_select_where = "SELECT * FROM {table_name} WHERE {where_column} = '{where_value}'"
query_select_latest = "SELECT * FROM {table_name} ORDER BY valid_until LIMIT 1"
query_drop_table_if_exists = "DROP TABLE IF EXISTS {table_name} cascade"

query_create_table_calculation_parameters = """
    CREATE TABLE {table_name}(
        id SERIAL PRIMARY KEY,
        notary_rate DECIMAL NOT NULL,
        rent_price_index_unil_40sqm DECIMAL NOT NULL,
        rent_price_index_unil_80sqm DECIMAL NOT NULL,
        rent_price_index_above_80sqm DECIMAL NOT NULL,
        downpayment_in_percent DECIMAL NOT NULL,
        interest_rate DECIMAL NOT NULL,
        mortgage_payment_rate DECIMAL NOT NULL,
        additional_costs_rate_per_m2 DECIMAL NOT NULL,
        personal_tax_rate DECIMAL NOT NULL,
        land_register_rate DECIMAL NOT NULL,
        real_estate_transfer_tax DECIMAL NOT NULL,
        management_cost_in_percent DECIMAL NOT NULL,
        creation_timestmap TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        valid_until TIMESTAMP DEFAULT '9999-12-31'
    );"""

query_create_table_locations = """
    CREATE TABLE {table_name}(
        postal_code VARCHAR(5) PRIMARY KEY,
        state VARCHAR(256) NOT NULL,
        city VARCHAR(256) NULL,
        longitude DECIMAL NULL,
        latitude DECIMAL NULL,
        rent_price_per_sqm_until_40sqm DECIMAL NULL,
        rent_price_per_sqm_until_80sqm DECIMAL NULL,
        rent_price_per_sqm_equal_above_80sqm DECIMAL NULL,
        notary_rate DECIMAL NULL,
        real_estate_transfer_tax DECIMAL NULL,
        land_register_tax DECIMAL NULL,
        creation_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);"""

query_create_table_sources = """
    CREATE TABLE {table_name}(
        id VARCHAR(32) PRIMARY KEY,
        expose_id VARCHAR(32) NOT NULL,
        is_active BOOLEAN NOT NULL,
        source_key VARCHAR(256) NOT NULL,
        source_id VARCHAR(256) NULL,
        property_category VARCHAR(32) NOT NULL,
        url VARCHAR(512) NOT NULL,
        creation_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        valid_until TIMESTAMP NOT NULL DEFAULT '9999-12-31',
        CONSTRAINT expose_id FOREIGN KEY(expose_id)
            REFERENCES {table_name_foreign}(id)
            ON DELETE NO ACTION
);
"""
# CONSTRAINT fk_exposes FOREIGN KEY(expose_id) REFERENCES {table_name_foreign}(id) ON DELETE NO ACTION
# CONSTRAINT fk_exposes FOREIGN KEY(expose_id) REFERENCES {table_name_foreign}(id) ON DELETE NO ACTION
query_create_table_price_history = """
    CREATE TABLE {table_name}(
        id VARCHAR(32) PRIMARY KEY,
        expose_id VARCHAR(32) NOT NULL,
        purchase_price DECIMAL NOT NULL,
        source_key VARCHAR(256) NOT NULL,
        creation_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT expose_id FOREIGN KEY(expose_id)
            REFERENCES {table_name_foreign}(id)
            ON DELETE NO ACTION
);
"""
# change not null values (price_history_id,source_id, postal_code, title, ....)
query_create_table_exposes = """
    CREATE TABLE {table_name}(
        id VARCHAR(32) PRIMARY KEY,
        postal_code VARCHAR(5) NOT NULL,
        title VARCHAR(256) NOT NULL,
        source_key VARCHAR(256) NOT NULL,
        source_url VARCHAR(512) NOT NULL,
        purchase_price DECIMAL NOT NULL,
        living_space DECIMAL NOT NULL,
        cash_flow_gross_year DECIMAL NULL,
        roi_equity DECIMAL NULL,
        roi_rent_gross DECIMAL NULL,
        roi_pot_gross DECIMAL NULL,
        is_active BOOLEAN DEFAULT TRUE NOT NULL,
        city VARCHAR(256) NULL,
        state VARCHAR(256) NULL,
        district VARCHAR(256) NULL,
        street VARCHAR(256) NULL,
        street_number VARCHAR(32) NULL,
        address_display_name VARCHAR(512) NULL,
        property_category VARCHAR(32) NOT NULL,
        property_type VARCHAR(32) NOT NULL,
        longitude DECIMAL NOT NULL,
        latitude DECIMAL NOT NULL,
        condition VARCHAR(64) DEFAULT 'NO_INFORMATION',
        creation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        valid_until TIMESTAMP DEFAULT '9999-12-31',
        is_private_offer VARCHAR(16) DEFAULT 'UNKNOWN',
        is_fore_closure VARCHAR(16) DEFAULT 'UNKNOWN',
        is_leasehold VARCHAR(16) DEFAULT 'UNKNOWN',
        has_vaccancy VARCHAR(16) DEFAULT 'UNKNOWN',
        images JSON NULL,
        construction_year VARCHAR(8) NULL,
        floors VARCHAR(32) NULL,
        rooms VARCHAR(16) NULL,
        at_floor VARCHAR(32) NULL,
        has_garden VARCHAR(16) DEFAULT 'UNKNOWN',
        has_balcony VARCHAR(16) DEFAULT 'UNKNOWN',
        has_lift VARCHAR(16) DEFAULT 'UNKNOWN',
        purchase_price_per_sqm DECIMAL NULL,
        commission_rate DECIMAL NULL,
        commission_total DECIMAL NULL,
        management_cost_given DECIMAL NULL,
        management_cost_estimated DECIMAL NULL,
        management_cost DECIMAL NULL,
        rent_price_per_sqm_given DECIMAL NULL,
        rent_price_per_sqm_estimated DECIMAL NULL,
        rent_price_total_given DECIMAL NULL,
        rent_price_total_estimated DECIMAL NULL,
        rent_price_total DECIMAL NOT NULL,
        mortgage_monthly DECIMAL NULL,
        interest_monthly DECIMAL NULL,
        notary_rate_in_percent DECIMAL NULL,
        notary_rate_total DECIMAL NULL,
        land_register_tax_in_percent DECIMAL NULL,
        land_register_tax_total DECIMAL NULL,
        real_estate_transfer_tax_in_percent DECIMAL NULL,
        real_estate_transfer_tax_total DECIMAL NULL,
        investment_sum DECIMAL NULL,
        side_costs DECIMAL NOT NULL,
        construction_phase VARCHAR(32) NULL,
        living_units VARCHAR(32) NULL,
        commercial_units VARCHAR(32) NULL,
        CONSTRAINT fk_locations FOREIGN KEY(postal_code)
            REFERENCES {table_name_foreign}(postal_code)
            ON DELETE NO ACTION
);
"""

query_insert_calculation_parameters = """
    INSERT INTO live_ai_v2.calculation_parameters(
        notary_rate,
        rent_price_index_unil_40sqm,
        rent_price_index_unil_80sqm,
        rent_price_index_above_80sqm,
        downpayment_in_percent,
        interest_rate,
        mortgage_payment_rate,
        additional_costs_rate_per_m2,
        personal_tax_rate,
        land_register_rate,
        real_estate_transfer_tax,
        management_cost_in_percent
    )
    VALUES {values}"""

query_insert_locations = """
    INSERT INTO live_ai_v2.locations (
        postal_code,
        state,
        city,
        longitude,
        latitude,
        rent_price_per_sqm_until_40sqm,
        rent_price_per_sqm_until_80sqm,
        rent_price_per_sqm_equal_above_80sqm,
        notary_rate,
        real_estate_transfer_tax,
        land_register_tax
    )
    VALUES {values}"""

query_insert_sources = """
    INSERT INTO live_ai_v2.sources (
        id,
        expose_id,
        is_active,
        source_key,
        source_id,
        property_category,
        url,
        creation_timestamp,
        valid_until
    )
    VALUES {values}"""

query_insert_price_history = """
    INSERT INTO live_ai_v2.price_history (
        id,
        expose_id,
        purchase_price,
        source_key,
        creation_timestamp)
    VALUES {values}"""

query_insert_exposes = """
    INSERT INTO live_ai_v2.exposes (
        id,
        postal_code,
        title,
        source_key,
        source_url,
        purchase_price,
        living_space,
        cash_flow_gross_year,
        roi_equity,
        roi_rent_gross,
        roi_pot_gross,
        is_active,
        city,
        state,
        district,
        street,
        street_number,
        address_display_name,
        property_category,
        property_type,
        longitude,
        latitude,
        condition,
        creation_timestamp,
        valid_until,
        is_private_offer,
        is_fore_closure,
        is_leasehold,
        has_vaccancy,
        images,
        construction_year,
        floors,
        rooms,
        at_floor,
        has_garden,
        has_balcony,
        has_lift,
        purchase_price_per_sqm,
        commission_rate,
        commission_total,
        management_cost_given,
        management_cost_estimated,
        management_cost,
        rent_price_per_sqm_given,
        rent_price_per_sqm_estimated,
        rent_price_total_given,
        rent_price_total_estimated,
        rent_price_total,
        mortgage_monthly,
        interest_monthly,
        notary_rate_in_percent,
        notary_rate_total,
        land_register_tax_in_percent,
        land_register_tax_total,
        real_estate_transfer_tax_in_percent,
        real_estate_transfer_tax_total,
        investment_sum,
        side_costs,
        construction_phase,
        living_units,
        commercial_units
    )
    VALUES {values}"""

query_update_sources = """
    UPDATE live_ai_v2.sources as sources
    SET is_active    = sources_updated.is_active,
        valid_until  = sources_updated.valid_until::date
    FROM (VALUES {values}) AS sources_updated (id, expose_id,is_active,source_key,source_id,url,creation_timestamp,valid_until)
    WHERE sources.id = sources_updated.id"""

query_update_exposes = """
    UPDATE live_ai_v2.exposes as exposes
    SET is_active = exposes_updated.is_active,
        valid_until = exposes_updated.valid_until::timestamp,
        purchase_price = exposes_updated.purchase_price,
        source_key = exposes_updated.source_key,
        source_url = exposes_updated.source_url
    FROM (VALUES {values}) AS exposes_updated (
        id,
        is_active,
        valid_until,
        purchase_price,
        source_key,
        source_url
    )
    WHERE exposes.id = exposes_updated.id """
