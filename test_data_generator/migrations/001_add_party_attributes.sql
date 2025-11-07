-- Migration: Add missing columns to party.party table
-- Phase B: L2 Schema Enhancement
-- Date: 2025-11-06

USE party;

-- Add tax identification number (TIN) for all parties
-- Currently only in party.individual, but enterprises need it too
ALTER TABLE party.party
ADD COLUMN tax_identification_number VARCHAR(50) NULL
COMMENT 'Tax Identification Number (TIN) for this party.
Unique identifier for tax purposes.';

-- Add industry classification
ALTER TABLE party.party
ADD COLUMN industry_code VARCHAR(20) NULL
COMMENT 'ISIC industry classification code.
References reference.ref_industry.industry_code';

-- Add legal form for enterprises
ALTER TABLE party.party
ADD COLUMN legal_form_code VARCHAR(20) NULL
COMMENT 'Legal structure of entity (LLC, Corporation, Partnership, etc.).
References reference.ref_legal_form.legal_form_code.
Applicable to enterprises only.';

-- Add registration date for enterprises
ALTER TABLE party.party
ADD COLUMN registration_date DATE NULL
COMMENT 'Date of business registration with authorities.
Applicable to enterprises only.';

-- Add location hierarchy codes
ALTER TABLE party.party
ADD COLUMN country_code CHAR(3) NULL
COMMENT 'ISO 3166-1 alpha-3 country code.
References reference.ref_country.country_code';

ALTER TABLE party.party
ADD COLUMN district_code VARCHAR(20) NULL
COMMENT 'District/province code.
References reference.ref_district.district_code';

ALTER TABLE party.party
ADD COLUMN locality_code VARCHAR(20) NULL
COMMENT 'City/locality code.
References reference.ref_locality.locality_code';

-- Add foreign key constraints (optional - can be added later if needed)
-- Commented out for now to allow flexibility during testing

-- ALTER TABLE party.party
-- ADD CONSTRAINT fk_party_industry
-- FOREIGN KEY (industry_code) REFERENCES reference.ref_industry(industry_code);

-- ALTER TABLE party.party
-- ADD CONSTRAINT fk_party_legal_form
-- FOREIGN KEY (legal_form_code) REFERENCES reference.ref_legal_form(legal_form_code);

-- ALTER TABLE party.party
-- ADD CONSTRAINT fk_party_country
-- FOREIGN KEY (country_code) REFERENCES reference.ref_country.country_code);

-- ALTER TABLE party.party
-- ADD CONSTRAINT fk_party_district
-- FOREIGN KEY (district_code) REFERENCES reference.ref_district(district_code);

-- ALTER TABLE party.party
-- ADD CONSTRAINT fk_party_locality
-- FOREIGN KEY (locality_code) REFERENCES reference.ref_locality(locality_code);

-- Verification query
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    COLUMN_COMMENT
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'party'
  AND TABLE_NAME = 'party'
  AND COLUMN_NAME IN (
      'tax_identification_number',
      'industry_code',
      'legal_form_code',
      'registration_date',
      'country_code',
      'district_code',
      'locality_code'
  )
ORDER BY ORDINAL_POSITION;
