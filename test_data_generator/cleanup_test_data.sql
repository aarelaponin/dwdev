-- ============================================================================
-- Cleanup Script for TA-RDM Test Data Generator
-- Run this to delete all generated test data before re-running the generator
-- ============================================================================
-- CAUTION: This will delete ALL data from these tables!
-- Only run this on test/development databases, NEVER on production!
-- ============================================================================

USE reference;

-- Disable foreign key checks temporarily for faster deletion
SET FOREIGN_KEY_CHECKS = 0;

-- ============================================================================
-- Party Schema - Delete in reverse dependency order
-- ============================================================================

-- Compliance control data (depends on party)
DELETE FROM compliance_control.taxpayer_risk_profile;

-- Party subtypes (depend on party.party)
DELETE FROM party.individual;
-- Note: party.enterprise table doesn't exist in this schema

-- Main party table
DELETE FROM party.party;

-- ============================================================================
-- Reference Schema - Delete in reverse dependency order
-- ============================================================================

-- Payment and refund reference data
DELETE FROM reference.ref_payment_method;

-- Business reference data
DELETE FROM reference.ref_legal_form;
DELETE FROM reference.ref_industry;

-- Individual reference data
DELETE FROM reference.ref_marital_status;
DELETE FROM reference.ref_gender;

-- Currency
DELETE FROM reference.ref_currency;

-- Geographic hierarchy (delete children first)
DELETE FROM reference.ref_locality;
DELETE FROM reference.ref_district;
DELETE FROM reference.ref_region;
DELETE FROM reference.ref_country;

-- Re-enable foreign key checks
SET FOREIGN_KEY_CHECKS = 1;

-- ============================================================================
-- Verification - Check counts after cleanup
-- ============================================================================

SELECT 'Reference Tables' AS category,
       'ref_country' AS table_name,
       COUNT(*) AS record_count
FROM reference.ref_country

UNION ALL

SELECT 'Reference Tables', 'ref_region', COUNT(*)
FROM reference.ref_region

UNION ALL

SELECT 'Reference Tables', 'ref_district', COUNT(*)
FROM reference.ref_district

UNION ALL

SELECT 'Reference Tables', 'ref_locality', COUNT(*)
FROM reference.ref_locality

UNION ALL

SELECT 'Reference Tables', 'ref_currency', COUNT(*)
FROM reference.ref_currency

UNION ALL

SELECT 'Reference Tables', 'ref_gender', COUNT(*)
FROM reference.ref_gender

UNION ALL

SELECT 'Reference Tables', 'ref_marital_status', COUNT(*)
FROM reference.ref_marital_status

UNION ALL

SELECT 'Reference Tables', 'ref_industry', COUNT(*)
FROM reference.ref_industry

UNION ALL

SELECT 'Reference Tables', 'ref_legal_form', COUNT(*)
FROM reference.ref_legal_form

UNION ALL

SELECT 'Reference Tables', 'ref_payment_method', COUNT(*)
FROM reference.ref_payment_method

UNION ALL

SELECT 'Party Tables', 'party', COUNT(*)
FROM party.party

UNION ALL

SELECT 'Party Tables', 'individual', COUNT(*)
FROM party.individual

UNION ALL

SELECT 'Compliance Tables', 'taxpayer_risk_profile', COUNT(*)
FROM compliance_control.taxpayer_risk_profile;

-- ============================================================================
-- All tables should now show 0 records
-- ============================================================================
