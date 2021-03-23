/* DROP DATABASE */
DROP DATABASE IF EXISTS DenielExposesDb;

CREATE DATABASE DenielExposesDb;

/* DROP Table */
DROP TABLE IF EXISTS locations;
/* CREATE TABLE locations */
CREATE TABLE locations(
  id SERIAL PRIMARY KEY,
  postal_code VARCHAR(5) NOT NULL,
  state VARCHAR(200) NULL,
  city VARCHAR(200) NOT NULL,
  longitude INT NOT NULL,
  latitude INT NOT NULL,
  slug VARCHAR(50) NOT NULL,
  rent_price_index_lower_30sqm DECIMAL NULL,
  rent_price_index_lower_60sqm DECIMAL NULL,
  rent_price_index_lower_100sqm DECIMAL NULL,
  notary_rate DECIMAL NULL,
  real_estate_transfer_tax DECIMAL NULL,
  land_register_tax DECIMAL NULL,
  creation_date DATE NOT NULL DEFAULT CURRENT_DATE,
  updated_date DATE NOT NULL DEFAULT CURRENT_DATE	
);

/* DROP Table */
DROP TABLE IF EXISTS source;
/* CREATE TABLE source */
CREATE TABLE source(
  id SERIAL PRIMARY KEY,
  exposes_id INT NOT NULL,
  is_active BOOLEAN  NULL,
  name VARCHAR(200) NOT NULL,
  source_id VARCHAR(100) NULL,
  url VARCHAR(300) NULL,
  creation_date DATE NOT NULL DEFAULT CURRENT_DATE,
  de_activation_date DATE NOT NULL DEFAULT CURRENT_DATE	
);

/* DROP Table */
DROP TABLE IF EXISTS prices;
/* CREATE TABLE prices */
CREATE TABLE prices(
  id SERIAL PRIMARY KEY,
  purchase_price DECIMAL NULL,
  source_name VARCHAR(50) NULL,
  total_purchase_price DECIMAL NULL,
  total_side_costs DECIMAL NULL,
  commission_percentage DECIMAL NULL,
  commission_absolute DECIMAL NULL,
  notary_percentage DECIMAL NULL,
  notary_absolute DECIMAL NULL,
  entry_land_registry_tax_percentage DECIMAL NULL,
  entry_land_registry_tax_absolute DECIMAL NULL,
  created_at DATE NOT NULL DEFAULT CURRENT_DATE,
  valid_until DATE NOT NULL DEFAULT '9999-12-31'	
);

/* DROP Table */
DROP TABLE IF EXISTS exposes;
/* CREATE TABLE exposes */
CREATE TABLE exposes(
  id SERIAL PRIMARY KEY,
  location_id INT NOT NULL,
  price_id INT NOT NULL,
  source_id INT NOT NULL,
  title VARCHAR(200) NOT NULL,
  purchase_price DECIMAL NULL,
  district VARCHAR(200) NULL,
  house_number VARCHAR(5) NULL,
  street VARCHAR(5) NULL,
  address_display_name VARCHAR(400),
  zip_code VARCHAR(10) NULL,
  is_active BOOLEAN DEFAULT TRUE,
  property_type VARCHAR(5) NULL, -- THIS WOULD BE THE BEST , THERE ALWAYS BE ONE DATA SO NO MEANS OF ENUM WHICH WOULD BE COSTLY
  longitude DECIMAL NULL,
  latitude DECIMAL NULL,
  has_vaccancy BOOLEAN NULL,
  is_force_clouser BOOLEAN NULL,
  purchase_price_per_sqm DECIMAL NULL,
  latest_commission DECIMAL NULL,
  management_cost_given DECIMAL NULL,
  management_cost_estimated DECIMAL NULL,
  rent_price_per_sqm_given DECIMAL NULL,
  rent_price_per_sqm_estimated DECIMAL NULL,
  rent_price_total_estimated DECIMAL NULL,
  rent_price_total_given DECIMAL NULL,
  city_slug VARCHAR(200) NOT NULL,
  condition VARCHAR(20) NULL,
  construction_year INT NULL,
  floors DECIMAL NULL,
  at_floor DECIMAL NULL,
  living_space DECIMAL NULL,
  rooms VARCHAR(10) NULL,
  last_refurbishment_maintenance_reserve DATE NULL,
  lift BOOLEAN NULL,
  has_balcony BOOLEAN NULL,
  is_private_offer BOOLEAN NULL,
  has_garden BOOLEAN NULL,
  address VARCHAR(400) NOT NULL,
  image_captions VARCHAR(300) NULL,
  image_urls VARCHAR(300) NULL,
  cash_flow_gross DECIMAL NULL,
  cash_flow_net DECIMAL NULL,
  roi_equity DECIMAL NULL,
  mortgage_monthly DECIMAL NULL,
  mortgage_yearly DECIMAL NULL,
  mortgage_total DECIMAL NULL, 
  interest_monthly DECIMAL NULL,
  interest_yearly DECIMAL NULL,
  interest_total DECIMAL NULL,
  personal_tax DECIMAL NULL, 
  loan DECIMAL NULL,
  private_maintenance_reserve DECIMAL NULL,
  rent_price_index_until_30sqm DECIMAL NULL,
  rent_price_index_until_60sqm DECIMAL NULL,
  rent_price_index_above_60sqm DECIMAL NULL,
  notary_rate DECIMAL NULL,
  real_estate_transfer_tax DECIMAL NULL,
  land_register_tax DECIMAL NULL,
  created_at DATE NULL DEFAULT CURRENT_DATE,
  updated_at DATE NULL DEFAULT CURRENT_DATE
);

