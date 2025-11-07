-- ============================================================================
-- Database User Setup for TA-RDM Test Data Generator
-- Run this as MySQL root user to grant permissions to 'ta_testdata'
-- ============================================================================

-- Create user if not exists (skip if user already exists)
-- CREATE USER 'ta_testdata'@'localhost' IDENTIFIED BY 'your_password_here';

-- Grant ALL PRIVILEGES on all 12 TA-RDM schemas
GRANT ALL PRIVILEGES ON reference.* TO 'ta_testdata'@'localhost';
GRANT ALL PRIVILEGES ON party.* TO 'ta_testdata'@'localhost';
GRANT ALL PRIVILEGES ON tax_framework.* TO 'ta_testdata'@'localhost';
GRANT ALL PRIVILEGES ON registration.* TO 'ta_testdata'@'localhost';
GRANT ALL PRIVILEGES ON filing_assessment.* TO 'ta_testdata'@'localhost';
GRANT ALL PRIVILEGES ON accounting.* TO 'ta_testdata'@'localhost';
GRANT ALL PRIVILEGES ON payment_refund.* TO 'ta_testdata'@'localhost';
GRANT ALL PRIVILEGES ON compliance_control.* TO 'ta_testdata'@'localhost';
GRANT ALL PRIVILEGES ON document_management.* TO 'ta_testdata'@'localhost';
GRANT ALL PRIVILEGES ON withholding_tax.* TO 'ta_testdata'@'localhost';
GRANT ALL PRIVILEGES ON vat.* TO 'ta_testdata'@'localhost';
GRANT ALL PRIVILEGES ON income_tax.* TO 'ta_testdata'@'localhost';

-- Apply changes
FLUSH PRIVILEGES;

-- Verify permissions
SHOW GRANTS FOR 'ta_testdata'@'localhost';
