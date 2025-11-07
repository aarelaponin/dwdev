--
-- L2 Schema Fix Script v2 - Phase 1.2 (Correct Order)
-- Fixes all identified DDL issues from audit
--
-- Order of operations:
-- 1. Drop all FK constraints with issues
-- 2. Create missing tables
-- 3. Populate missing tables with minimal data
-- 4. Recreate FK constraints with correct paths
--

-- ====================================================================================
-- PART 1: DROP ALL FK CONSTRAINTS WITH ISSUES
-- ====================================================================================

-- Drop FK constraints from tax_return table
ALTER TABLE filing_assessment.tax_return DROP FOREIGN KEY fk_tax_return_tax_account_id;
ALTER TABLE filing_assessment.tax_return DROP FOREIGN KEY fk_tax_return_tax_period_id;
ALTER TABLE filing_assessment.tax_return DROP FOREIGN KEY fk_tax_return_form_id;
ALTER TABLE filing_assessment.tax_return DROP FOREIGN KEY fk_tax_return_form_version_id;
ALTER TABLE filing_assessment.tax_return DROP FOREIGN KEY fk_tax_return_prepared_by_party_id;
ALTER TABLE filing_assessment.tax_return DROP FOREIGN KEY fk_tax_return_previous_return_id;

-- Drop FK constraints from tax_return_line table
ALTER TABLE filing_assessment.tax_return_line DROP FOREIGN KEY fk_tax_return_line_tax_return_id;
ALTER TABLE filing_assessment.tax_return_line DROP FOREIGN KEY fk_tax_return_line_form_line_id;

-- Drop FK constraints from assessment table
ALTER TABLE filing_assessment.assessment DROP FOREIGN KEY fk_assessment_tax_account_id;
ALTER TABLE filing_assessment.assessment DROP FOREIGN KEY fk_assessment_tax_period_id;
ALTER TABLE filing_assessment.assessment DROP FOREIGN KEY fk_assessment_tax_return_id;
ALTER TABLE filing_assessment.assessment DROP FOREIGN KEY fk_assessment_previous_assessment_id;
ALTER TABLE filing_assessment.assessment DROP FOREIGN KEY fk_assessment_audit_case_id;

-- Drop FK constraints from assessment_line table
ALTER TABLE filing_assessment.assessment_line DROP FOREIGN KEY fk_assessment_line_assessment_id;
ALTER TABLE filing_assessment.assessment_line DROP FOREIGN KEY fk_assessment_line_form_line_id;

-- Drop FK constraints from penalty_assessment table
ALTER TABLE filing_assessment.penalty_assessment DROP FOREIGN KEY fk_penalty_assessment_assessment_id;

-- Drop FK constraints from interest_assessment table
ALTER TABLE filing_assessment.interest_assessment DROP FOREIGN KEY fk_interest_assessment_assessment_id;

-- ====================================================================================
-- PART 2: CREATE MISSING TABLES
-- ====================================================================================

-- Create form_version table (referenced by tax_return)
CREATE TABLE IF NOT EXISTS tax_framework.form_version (
    form_version_id BIGINT NOT NULL AUTO_INCREMENT,
    tax_form_id BIGINT NOT NULL,
    version_number INT NOT NULL,
    version_name VARCHAR(50) NOT NULL,
    effective_from_date DATE NOT NULL,
    effective_to_date DATE,
    is_current_version TINYINT NOT NULL DEFAULT 1,
    version_notes TEXT,
    created_by VARCHAR(100) NOT NULL,
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_by VARCHAR(100),
    modified_date TIMESTAMP,
    PRIMARY KEY (form_version_id),
    KEY idx_form_version_form_id (tax_form_id),
    KEY idx_form_version_effective (effective_from_date, effective_to_date),
    UNIQUE KEY uk_form_version (tax_form_id, version_number),
    CONSTRAINT fk_form_version_tax_form_id
        FOREIGN KEY (tax_form_id)
        REFERENCES tax_framework.tax_form(tax_form_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Create form_line table (referenced by tax_return_line and assessment_line)
CREATE TABLE IF NOT EXISTS tax_framework.form_line (
    form_line_id BIGINT NOT NULL AUTO_INCREMENT,
    tax_form_id BIGINT NOT NULL,
    form_version_id BIGINT,
    line_number VARCHAR(20) NOT NULL,
    line_sequence INT NOT NULL,
    line_label VARCHAR(200) NOT NULL,
    line_description TEXT,
    line_type ENUM('INPUT', 'CALCULATED', 'REFERENCE', 'HEADER') NOT NULL DEFAULT 'INPUT',
    data_type ENUM('NUMERIC', 'TEXT', 'DATE', 'BOOLEAN') NOT NULL DEFAULT 'NUMERIC',
    is_mandatory TINYINT NOT NULL DEFAULT 0,
    min_value DECIMAL(19,2),
    max_value DECIMAL(19,2),
    calculation_formula VARCHAR(500),
    validation_rules TEXT,
    parent_line_id BIGINT,
    sort_order INT,
    is_active TINYINT NOT NULL DEFAULT 1,
    created_by VARCHAR(100) NOT NULL,
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_by VARCHAR(100),
    modified_date TIMESTAMP,
    PRIMARY KEY (form_line_id),
    KEY idx_form_line_form_id (tax_form_id),
    KEY idx_form_line_version_id (form_version_id),
    KEY idx_form_line_parent (parent_line_id),
    KEY idx_form_line_number (line_number),
    UNIQUE KEY uk_form_line (tax_form_id, form_version_id, line_number),
    CONSTRAINT fk_form_line_tax_form_id
        FOREIGN KEY (tax_form_id)
        REFERENCES tax_framework.tax_form(tax_form_id),
    CONSTRAINT fk_form_line_form_version_id
        FOREIGN KEY (form_version_id)
        REFERENCES tax_framework.form_version(form_version_id),
    CONSTRAINT fk_form_line_parent_line_id
        FOREIGN KEY (parent_line_id)
        REFERENCES tax_framework.form_line(form_line_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ====================================================================================
-- PART 3: POPULATE MISSING TABLES WITH MINIMAL DATA
-- ====================================================================================

-- For each tax_form, create a version 1 record
INSERT IGNORE INTO tax_framework.form_version
    (tax_form_id, version_number, version_name, effective_from_date,
     is_current_version, created_by, created_date)
SELECT
    tax_form_id,
    1 as version_number,
    'Version 1.0' as version_name,
    '2023-01-01' as effective_from_date,
    1 as is_current_version,
    'SYSTEM_DDL_FIX' as created_by,
    NOW() as created_date
FROM tax_framework.tax_form;

-- Create minimal form lines for each form
INSERT IGNORE INTO tax_framework.form_line
    (tax_form_id, form_version_id, line_number, line_sequence,
     line_label, line_type, data_type, is_mandatory,
     created_by, created_date)
SELECT
    tf.tax_form_id,
    fv.form_version_id,
    CONCAT('L00', ROW_NUMBER() OVER (PARTITION BY tf.tax_form_id ORDER BY tf.tax_form_id)) as line_number,
    ROW_NUMBER() OVER (PARTITION BY tf.tax_form_id ORDER BY tf.tax_form_id) as line_sequence,
    'Main Line Item' as line_label,
    'INPUT' as line_type,
    'NUMERIC' as data_type,
    1 as is_mandatory,
    'SYSTEM_DDL_FIX' as created_by,
    NOW() as created_date
FROM tax_framework.tax_form tf
INNER JOIN tax_framework.form_version fv ON tf.tax_form_id = fv.tax_form_id
WHERE fv.version_number = 1;

-- ====================================================================================
-- PART 4: RECREATE FK CONSTRAINTS WITH CORRECT PATHS
-- ====================================================================================

-- Recreate FK constraints for tax_return table
ALTER TABLE filing_assessment.tax_return
    ADD CONSTRAINT fk_tax_return_tax_account_id
        FOREIGN KEY (tax_account_id)
        REFERENCES registration.tax_account(tax_account_id);

ALTER TABLE filing_assessment.tax_return
    ADD CONSTRAINT fk_tax_return_tax_period_id
        FOREIGN KEY (tax_period_id)
        REFERENCES tax_framework.tax_period(tax_period_id);

ALTER TABLE filing_assessment.tax_return
    ADD CONSTRAINT fk_tax_return_form_id
        FOREIGN KEY (form_id)
        REFERENCES tax_framework.tax_form(tax_form_id);

ALTER TABLE filing_assessment.tax_return
    ADD CONSTRAINT fk_tax_return_form_version_id
        FOREIGN KEY (form_version_id)
        REFERENCES tax_framework.form_version(form_version_id);

ALTER TABLE filing_assessment.tax_return
    ADD CONSTRAINT fk_tax_return_prepared_by_party_id
        FOREIGN KEY (prepared_by_party_id)
        REFERENCES party.party(party_id);

ALTER TABLE filing_assessment.tax_return
    ADD CONSTRAINT fk_tax_return_previous_return_id
        FOREIGN KEY (previous_return_id)
        REFERENCES filing_assessment.tax_return(tax_return_id);

-- Recreate FK constraints for tax_return_line table
ALTER TABLE filing_assessment.tax_return_line
    ADD CONSTRAINT fk_tax_return_line_tax_return_id
        FOREIGN KEY (tax_return_id)
        REFERENCES filing_assessment.tax_return(tax_return_id);

ALTER TABLE filing_assessment.tax_return_line
    ADD CONSTRAINT fk_tax_return_line_form_line_id
        FOREIGN KEY (form_line_id)
        REFERENCES tax_framework.form_line(form_line_id);

-- Recreate FK constraints for assessment table
ALTER TABLE filing_assessment.assessment
    ADD CONSTRAINT fk_assessment_tax_account_id
        FOREIGN KEY (tax_account_id)
        REFERENCES registration.tax_account(tax_account_id);

ALTER TABLE filing_assessment.assessment
    ADD CONSTRAINT fk_assessment_tax_period_id
        FOREIGN KEY (tax_period_id)
        REFERENCES tax_framework.tax_period(tax_period_id);

ALTER TABLE filing_assessment.assessment
    ADD CONSTRAINT fk_assessment_tax_return_id
        FOREIGN KEY (tax_return_id)
        REFERENCES filing_assessment.tax_return(tax_return_id);

ALTER TABLE filing_assessment.assessment
    ADD CONSTRAINT fk_assessment_previous_assessment_id
        FOREIGN KEY (previous_assessment_id)
        REFERENCES filing_assessment.assessment(assessment_id);

-- Recreate FK constraints for assessment_line table
ALTER TABLE filing_assessment.assessment_line
    ADD CONSTRAINT fk_assessment_line_assessment_id
        FOREIGN KEY (assessment_id)
        REFERENCES filing_assessment.assessment(assessment_id);

ALTER TABLE filing_assessment.assessment_line
    ADD CONSTRAINT fk_assessment_line_form_line_id
        FOREIGN KEY (form_line_id)
        REFERENCES tax_framework.form_line(form_line_id);

-- Recreate FK constraints for penalty_assessment table
ALTER TABLE filing_assessment.penalty_assessment
    ADD CONSTRAINT fk_penalty_assessment_assessment_id
        FOREIGN KEY (assessment_id)
        REFERENCES filing_assessment.assessment(assessment_id);

-- Recreate FK constraints for interest_assessment table
ALTER TABLE filing_assessment.interest_assessment
    ADD CONSTRAINT fk_interest_assessment_assessment_id
        FOREIGN KEY (assessment_id)
        REFERENCES filing_assessment.assessment(assessment_id);
