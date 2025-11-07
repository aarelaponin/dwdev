-- =====================================================================================
-- Tax Administration Data Warehouse - ClickHouse DDL Script
-- Layer 3: Analytical Foundation
-- =====================================================================================
-- 
-- Database: Sri Lanka Inland Revenue Department
-- Purpose: Comprehensive Data Warehouse for Compliance Risk Management
-- Source: TA-RDM Canonical Model (Layer 2)
-- Target: Dimensional Model (Layer 3)
-- 
-- Version: 1.0
-- Date: November 2025
-- 
-- Architecture:
-- - Conformed Dimensions (12+): Shared across all fact tables
-- - Fact Tables (12+): Transaction, Snapshot, and Accumulating facts
-- - Star Schema Design: Optimized for analytical queries
-- - ClickHouse Optimizations: Partitioning, columnar storage, compression
-- 
-- Key Features:
-- - TTT Principle: TIN-TaxType-Period as core concept
-- - SCD Type 2: Historical tracking for key dimensions
-- - Risk Management Focus: Optimized for compliance analytics
-- - Sub-second Query Performance: Through proper indexing and partitioning
-- =====================================================================================

-- =====================================================================================
-- DATABASE SETUP
-- =====================================================================================

CREATE DATABASE IF NOT EXISTS ta_dw
COMMENT 'Tax Administration Data Warehouse - Analytical Foundation Layer';

USE ta_dw;

-- =====================================================================================
-- SECTION 1: CONFORMED DIMENSIONS
-- =====================================================================================
-- These dimensions are shared across multiple fact tables to ensure consistency
-- and enable drill-across analysis. All dimensions use SCD Type 2 where appropriate
-- to track historical changes.
-- =====================================================================================

-- -------------------------------------------------------------------------------------
-- DIM_PARTY: Master dimension for all taxpayers (individuals and enterprises)
-- -------------------------------------------------------------------------------------
-- Purpose: Central dimension for taxpayer information
-- Source: party, individual, enterprise, party_address, party_contact
-- Type: SCD Type 2 (tracks historical changes in segment, risk, address)
-- Grain: One row per taxpayer per version
-- Volume: 500K-5M rows
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_party
(
    -- Surrogate Key
    party_key Int64 COMMENT 'Data warehouse surrogate key (auto-generated)',
    
    -- Natural Key from Source
    party_id Int64 COMMENT 'Natural key from TA-RDM party table',
    
    -- Core Attributes
    tin String COMMENT 'Tax Identification Number',
    party_type LowCardinality(String) COMMENT 'INDIVIDUAL or ENTERPRISE',
    party_name String COMMENT 'Full legal name',
    party_status LowCardinality(String) COMMENT 'ACTIVE, INACTIVE, SUSPENDED, DECEASED, DISSOLVED',
    
    -- Segmentation & Classification
    party_segment LowCardinality(String) COMMENT 'Large, Medium, Small, Micro taxpayer segment',
    industry_code LowCardinality(String) COMMENT 'ISIC industry classification code',
    industry_name String COMMENT 'Industry sector description',
    
    -- Risk Management
    risk_rating LowCardinality(String) COMMENT 'VERY_LOW, LOW, MEDIUM, HIGH, VERY_HIGH, CRITICAL',
    risk_score Decimal(5,2) COMMENT 'Numeric risk score 0-100',
    
    -- Individual-specific attributes (NULL for enterprises)
    birth_date Date COMMENT 'Date of birth (for individuals)',
    gender LowCardinality(Nullable(String)) COMMENT 'MALE, FEMALE, OTHER (for individuals)',
    
    -- Enterprise-specific attributes (NULL for individuals)
    legal_form LowCardinality(Nullable(String)) COMMENT 'LLC, CORPORATION, PARTNERSHIP, etc. (for enterprises)',
    registration_date Nullable(Date) COMMENT 'Business registration date (for enterprises)',
    employee_count Nullable(Int32) COMMENT 'Number of employees (for enterprises)',
    annual_turnover Nullable(Decimal(19,2)) COMMENT 'Annual turnover amount (for enterprises)',
    
    -- Geographic Information (denormalized)
    address_line1 String COMMENT 'Primary street address',
    address_line2 Nullable(String) COMMENT 'Secondary address line',
    city String COMMENT 'City/Town',
    district String COMMENT 'District/Province',
    postal_code Nullable(String) COMMENT 'Postal/ZIP code',
    country String DEFAULT 'LK' COMMENT 'Country code (ISO 3166-1)',
    
    -- Contact Information
    email Nullable(String) COMMENT 'Primary email address',
    phone Nullable(String) COMMENT 'Primary phone number',
    mobile Nullable(String) COMMENT 'Mobile phone number',
    
    -- SCD Type 2 Columns
    valid_from DateTime COMMENT 'Effective start date of this version',
    valid_to Nullable(DateTime) COMMENT 'Effective end date (NULL = current version)',
    is_current UInt8 DEFAULT 1 COMMENT '1 = current version, 0 = historical',
    
    -- Audit Columns
    created_by String COMMENT 'User/process that created this record',
    created_date DateTime DEFAULT now() COMMENT 'Record creation timestamp',
    updated_by Nullable(String) COMMENT 'User/process that last updated',
    updated_date Nullable(DateTime) COMMENT 'Last update timestamp',
    etl_batch_id Int64 COMMENT 'ETL batch identifier for traceability'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(valid_from)
ORDER BY (party_key, party_id, valid_from)
SETTINGS index_granularity = 8192
COMMENT 'Master dimension for taxpayer information with SCD Type 2 history';

-- Indexes for query optimization
CREATE INDEX idx_party_tin ON dim_party(tin) TYPE minmax GRANULARITY 4;
CREATE INDEX idx_party_current ON dim_party(is_current) TYPE set(2) GRANULARITY 1;
CREATE INDEX idx_party_status ON dim_party(party_status) TYPE set(10) GRANULARITY 1;
CREATE INDEX idx_party_segment ON dim_party(party_segment) TYPE set(10) GRANULARITY 1;
CREATE INDEX idx_party_risk ON dim_party(risk_rating) TYPE set(10) GRANULARITY 1;

-- -------------------------------------------------------------------------------------
-- DIM_TAX_TYPE: Dimension for tax types (CIT, PIT, VAT, etc.)
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_tax_type
(
    -- Surrogate Key
    tax_type_key Int32 COMMENT 'Data warehouse surrogate key',
    
    -- Natural Key
    tax_type_code LowCardinality(String) COMMENT 'Tax type code (CIT, PIT, VAT, EXCISE, etc.)',
    
    -- Attributes
    tax_type_name String COMMENT 'Full tax type name',
    tax_category LowCardinality(String) COMMENT 'INCOME, CONSUMPTION, PROPERTY, EXCISE, WITHHOLDING',
    is_direct_tax UInt8 COMMENT '1 = direct tax, 0 = indirect tax',
    filing_frequency LowCardinality(String) COMMENT 'MONTHLY, QUARTERLY, ANNUAL, TRANSACTION',
    
    -- Tax Rates (denormalized for analysis)
    standard_rate Nullable(Decimal(5,2)) COMMENT 'Standard tax rate percentage',
    rate_type LowCardinality(String) COMMENT 'FLAT, PROGRESSIVE, AD_VALOREM',
    
    -- Classification
    revenue_category LowCardinality(String) COMMENT 'Budget classification code',
    priority_level LowCardinality(String) COMMENT 'HIGH, MEDIUM, LOW priority for enforcement',
    
    -- SCD Type 2 Columns
    valid_from DateTime COMMENT 'Effective start date',
    valid_to Nullable(DateTime) COMMENT 'Effective end date (NULL = current)',
    is_current UInt8 DEFAULT 1 COMMENT 'Current version flag',
    
    -- Audit Columns
    created_by String,
    created_date DateTime DEFAULT now(),
    updated_by Nullable(String),
    updated_date Nullable(DateTime),
    etl_batch_id Int64
)
ENGINE = MergeTree()
ORDER BY (tax_type_key, tax_type_code)
SETTINGS index_granularity = 8192
COMMENT 'Tax type dimension with historical tracking';

-- -------------------------------------------------------------------------------------
-- DIM_TAX_PERIOD: Dimension for tax periods
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_tax_period
(
    -- Surrogate Key
    period_key Int32 COMMENT 'Surrogate key',
    
    -- Natural Key
    tax_period_id Int64 COMMENT 'Natural key from TA-RDM',
    period_code String COMMENT 'Period code (e.g., 2025-Q1, 2025-01)',
    
    -- Period Attributes
    tax_type_code LowCardinality(String) COMMENT 'Associated tax type',
    period_year Int16 COMMENT 'Calendar/fiscal year',
    period_quarter Nullable(UInt8) COMMENT 'Quarter (1-4)',
    period_month Nullable(UInt8) COMMENT 'Month (1-12)',
    
    -- Date Boundaries
    period_start_date Date COMMENT 'Period start date',
    period_end_date Date COMMENT 'Period end date',
    filing_due_date Date COMMENT 'Return filing due date',
    payment_due_date Date COMMENT 'Payment due date',
    
    -- Flags
    is_current_period UInt8 COMMENT 'Is this the current tax period',
    is_closed UInt8 COMMENT 'Period closed for filing',
    
    -- Audit Columns
    created_by String,
    created_date DateTime DEFAULT now(),
    etl_batch_id Int64
)
ENGINE = MergeTree()
ORDER BY (period_key, period_start_date)
SETTINGS index_granularity = 8192
COMMENT 'Tax period dimension';

-- -------------------------------------------------------------------------------------
-- DIM_TIME: Date dimension (conformed across all facts)
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_time
(
    -- Surrogate Key
    date_key Int32 COMMENT 'Date in YYYYMMDD format (e.g., 20250101)',
    
    -- Date Attributes
    full_date Date COMMENT 'Actual date',
    year Int16 COMMENT 'Year',
    quarter UInt8 COMMENT 'Quarter (1-4)',
    quarter_name String COMMENT 'Q1, Q2, Q3, Q4',
    month UInt8 COMMENT 'Month (1-12)',
    month_name String COMMENT 'January, February, ...',
    month_abbr String COMMENT 'Jan, Feb, ...',
    week_of_year UInt8 COMMENT 'ISO week number (1-53)',
    day_of_month UInt8 COMMENT 'Day of month (1-31)',
    day_of_week UInt8 COMMENT 'Day of week (1=Monday, 7=Sunday)',
    day_name String COMMENT 'Monday, Tuesday, ...',
    day_abbr String COMMENT 'Mon, Tue, ...',
    
    -- Fiscal Calendar
    fiscal_year Int16 COMMENT 'Fiscal year',
    fiscal_quarter UInt8 COMMENT 'Fiscal quarter',
    fiscal_month UInt8 COMMENT 'Fiscal month',
    
    -- Flags
    is_weekend UInt8 COMMENT 'Is weekend (Saturday/Sunday)',
    is_holiday UInt8 COMMENT 'Is public holiday',
    holiday_name Nullable(String) COMMENT 'Holiday name if applicable',
    is_business_day UInt8 COMMENT 'Is business day (not weekend or holiday)',
    
    -- Period Flags
    is_month_start UInt8,
    is_month_end UInt8,
    is_quarter_start UInt8,
    is_quarter_end UInt8,
    is_year_start UInt8,
    is_year_end UInt8
)
ENGINE = MergeTree()
ORDER BY date_key
SETTINGS index_granularity = 8192
COMMENT 'Date dimension for time-based analysis';

-- -------------------------------------------------------------------------------------
-- DIM_TAX_ACCOUNT: Dimension for tax accounts (TTT: TIN-TaxType link)
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_tax_account
(
    -- Surrogate Key
    tax_account_key Int64 COMMENT 'Surrogate key',
    
    -- Natural Key
    tax_account_id Int64 COMMENT 'Natural key from TA-RDM',
    tax_account_number String COMMENT 'Human-readable account number',
    
    -- Foreign Keys (denormalized)
    party_id Int64 COMMENT 'Taxpayer party ID',
    tin String COMMENT 'Taxpayer Identification Number',
    tax_type_code LowCardinality(String) COMMENT 'Tax type code',
    
    -- Account Attributes
    account_status LowCardinality(String) COMMENT 'ACTIVE, INACTIVE, DEREGISTERED, SUSPENDED',
    registration_date Date COMMENT 'Account registration date',
    deregistration_date Nullable(Date) COMMENT 'Deregistration date if applicable',
    registration_reason LowCardinality(String) COMMENT 'VOLUNTARY, MANDATORY, THIRD_PARTY_INITIATED',
    
    -- SCD Type 2 Columns
    valid_from DateTime,
    valid_to Nullable(DateTime),
    is_current UInt8 DEFAULT 1,
    
    -- Audit Columns
    created_by String,
    created_date DateTime DEFAULT now(),
    etl_batch_id Int64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(registration_date)
ORDER BY (tax_account_key, tax_account_id)
SETTINGS index_granularity = 8192
COMMENT 'Tax account dimension (TIN-TaxType linkage)';

-- -------------------------------------------------------------------------------------
-- DIM_LOCATION: Geographic dimension
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_location
(
    -- Surrogate Key
    location_key Int32 COMMENT 'Surrogate key',
    
    -- Natural Key
    location_id Int32 COMMENT 'Location ID from reference data',
    location_code String COMMENT 'Location code',
    
    -- Hierarchy (denormalized)
    location_name String COMMENT 'Location name',
    location_type LowCardinality(String) COMMENT 'COUNTRY, PROVINCE, DISTRICT, CITY, GRAMA_NILADHARI',
    
    -- Geographic Hierarchy
    country_code String DEFAULT 'LK',
    country_name String DEFAULT 'Sri Lanka',
    province_code Nullable(String),
    province_name Nullable(String),
    district_code Nullable(String),
    district_name Nullable(String),
    city_code Nullable(String),
    city_name Nullable(String),
    
    -- Coordinates
    latitude Nullable(Decimal(10,8)),
    longitude Nullable(Decimal(11,8)),
    
    -- Audit Columns
    created_by String,
    created_date DateTime DEFAULT now(),
    etl_batch_id Int64
)
ENGINE = MergeTree()
ORDER BY (location_key, location_code)
SETTINGS index_granularity = 8192
COMMENT 'Geographic location dimension';

-- -------------------------------------------------------------------------------------
-- DIM_ORGANIZATION_UNIT: Tax office organizational structure
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_organization_unit
(
    -- Surrogate Key
    org_unit_key Int32 COMMENT 'Surrogate key',
    
    -- Natural Key
    org_unit_id Int32 COMMENT 'Organization unit ID',
    org_unit_code String COMMENT 'Unit code',
    
    -- Attributes
    org_unit_name String COMMENT 'Organization unit name',
    org_unit_type LowCardinality(String) COMMENT 'HEADQUARTERS, REGIONAL_OFFICE, DISTRICT_OFFICE, SERVICE_CENTER',
    
    -- Hierarchy
    parent_org_unit_id Nullable(Int32),
    parent_org_unit_name Nullable(String),
    region_code Nullable(String),
    region_name Nullable(String),
    
    -- Contact
    address String,
    phone Nullable(String),
    email Nullable(String),
    
    -- Status
    is_active UInt8,
    
    -- Audit Columns
    created_by String,
    created_date DateTime DEFAULT now(),
    etl_batch_id Int64
)
ENGINE = MergeTree()
ORDER BY (org_unit_key, org_unit_code)
SETTINGS index_granularity = 8192
COMMENT 'Tax office organizational structure dimension';

-- -------------------------------------------------------------------------------------
-- DIM_OFFICER: Tax officer dimension
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_officer
(
    -- Surrogate Key
    officer_key Int32 COMMENT 'Surrogate key',
    
    -- Natural Key
    officer_id Int32 COMMENT 'Employee/user ID',
    employee_number String COMMENT 'Employee number',
    
    -- Attributes
    officer_name String COMMENT 'Full name',
    officer_role LowCardinality(String) COMMENT 'AUDITOR, COLLECTOR, ASSESSOR, SUPERVISOR, MANAGER',
    
    -- Organization
    org_unit_id Int32,
    org_unit_name String,
    region_name Nullable(String),
    
    -- Specialization
    specialization LowCardinality(Nullable(String)) COMMENT 'TAX_TYPE or function specialization',
    grade_level LowCardinality(String) COMMENT 'Officer grade/level',
    
    -- Status
    is_active UInt8,
    
    -- Audit Columns
    created_by String,
    created_date DateTime DEFAULT now(),
    etl_batch_id Int64
)
ENGINE = MergeTree()
ORDER BY (officer_key, officer_id)
SETTINGS index_granularity = 8192
COMMENT 'Tax officer dimension';

-- -------------------------------------------------------------------------------------
-- DIM_PAYMENT_METHOD: Payment channel dimension
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_payment_method
(
    -- Surrogate Key
    payment_method_key Int16 COMMENT 'Surrogate key',
    
    -- Natural Key
    payment_method_code LowCardinality(String) COMMENT 'Method code',
    
    -- Attributes
    payment_method_name String COMMENT 'Method name',
    payment_channel LowCardinality(String) COMMENT 'BANK, ONLINE, MOBILE, CASH, CHECK',
    is_electronic UInt8 COMMENT 'Is electronic payment',
    processing_time_hours Nullable(UInt16) COMMENT 'Typical processing time',
    
    -- Fees
    has_fee UInt8,
    fee_percentage Nullable(Decimal(5,2)),
    
    -- Status
    is_active UInt8,
    
    -- Audit Columns
    created_by String,
    created_date DateTime DEFAULT now(),
    etl_batch_id Int64
)
ENGINE = MergeTree()
ORDER BY payment_method_key
SETTINGS index_granularity = 8192
COMMENT 'Payment method/channel dimension';

-- -------------------------------------------------------------------------------------
-- DIM_RISK_CATEGORY: Risk category dimension
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_risk_category
(
    -- Surrogate Key
    risk_category_key Int16 COMMENT 'Surrogate key',
    
    -- Natural Key
    risk_category_code LowCardinality(String) COMMENT 'Category code',
    
    -- Attributes
    risk_category_name String COMMENT 'Category name',
    risk_type LowCardinality(String) COMMENT 'FILING, PAYMENT, REPORTING, AUDIT_HISTORY, THIRD_PARTY',
    severity_level LowCardinality(String) COMMENT 'LOW, MEDIUM, HIGH, CRITICAL',
    weight Decimal(5,2) COMMENT 'Weight in overall risk calculation (0-1)',
    
    -- Description
    risk_description String,
    
    -- Status
    is_active UInt8,
    
    -- Audit Columns
    created_by String,
    created_date DateTime DEFAULT now(),
    etl_batch_id Int64
)
ENGINE = MergeTree()
ORDER BY risk_category_key
SETTINGS index_granularity = 8192
COMMENT 'Risk category dimension for compliance risk management';

-- -------------------------------------------------------------------------------------
-- DIM_AUDIT_TYPE: Audit type dimension
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_audit_type
(
    -- Surrogate Key
    audit_type_key Int16 COMMENT 'Surrogate key',
    
    -- Natural Key
    audit_type_code LowCardinality(String) COMMENT 'Audit type code',
    
    -- Attributes
    audit_type_name String COMMENT 'Audit type name',
    audit_scope LowCardinality(String) COMMENT 'COMPREHENSIVE, FOCUSED, DESK, FIELD',
    typical_duration_days Nullable(UInt16) COMMENT 'Typical duration in days',
    
    -- Risk
    risk_weight Decimal(5,2) COMMENT 'Weight in risk assessment',
    
    -- Status
    is_active UInt8,
    
    -- Audit Columns
    created_by String,
    created_date DateTime DEFAULT now(),
    etl_batch_id Int64
)
ENGINE = MergeTree()
ORDER BY audit_type_key
SETTINGS index_granularity = 8192
COMMENT 'Audit type dimension';

-- -------------------------------------------------------------------------------------
-- DIM_COMPLIANCE_STATUS: Compliance status dimension
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_compliance_status
(
    -- Surrogate Key
    compliance_status_key Int16 COMMENT 'Surrogate key',
    
    -- Natural Key
    status_code LowCardinality(String) COMMENT 'Status code',
    
    -- Attributes
    status_name String COMMENT 'Status name',
    status_category LowCardinality(String) COMMENT 'FILING, PAYMENT, REGISTRATION',
    severity_level LowCardinality(String) COMMENT 'COMPLIANT, MINOR, MODERATE, SEVERE, CRITICAL',
    
    -- Description
    status_description String,
    
    -- Audit Columns
    created_by String,
    created_date DateTime DEFAULT now(),
    etl_batch_id Int64
)
ENGINE = MergeTree()
ORDER BY compliance_status_key
SETTINGS index_granularity = 8192
COMMENT 'Compliance status dimension';

-- =====================================================================================
-- SECTION 2: FACT TABLES
-- =====================================================================================
-- Fact tables capture measurable business events and metrics at specific granularity.
-- Each fact connects to conformed dimensions and stores numeric measures.
-- =====================================================================================

-- -------------------------------------------------------------------------------------
-- FACT_REGISTRATION: Taxpayer registration events
-- -------------------------------------------------------------------------------------
-- Purpose: Track tax account registration and deregistration events
-- Grain: One row per taxpayer per tax type registration event
-- Type: Transaction fact
-- Volume: 2M initial + 200K annual growth
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_registration
(
    -- Dimension Foreign Keys
    dim_party_key Int64 COMMENT 'FK to dim_party',
    dim_tax_type_key Int32 COMMENT 'FK to dim_tax_type',
    dim_tax_account_key Int64 COMMENT 'FK to dim_tax_account',
    dim_location_key Int32 COMMENT 'FK to dim_location (taxpayer location)',
    dim_org_unit_key Int32 COMMENT 'FK to dim_organization_unit (processing office)',
    dim_officer_key Int32 COMMENT 'FK to dim_officer (registration officer)',
    dim_date_key Int32 COMMENT 'FK to dim_time (registration date)',
    
    -- Degenerate Dimensions (IDs kept in fact for drill-through)
    tax_account_id Int64 COMMENT 'Natural key for drill-through',
    registration_number String COMMENT 'Registration certificate number',
    application_reference Nullable(String) COMMENT 'Application tracking reference',
    
    -- Measures
    registration_count Int16 DEFAULT 1 COMMENT 'Always 1 (for counting)',
    processing_days Int16 COMMENT 'Days from application to approval',
    
    -- Flags (as numeric for aggregation)
    is_voluntary UInt8 COMMENT '1=voluntary, 0=enforced',
    is_deregistration UInt8 COMMENT '1=deregistration, 0=registration',
    is_approved UInt8 COMMENT '1=approved, 0=rejected/pending',
    
    -- Dates
    application_date Date COMMENT 'Application submission date',
    approval_date Nullable(Date) COMMENT 'Approval/rejection date',
    effective_date Date COMMENT 'Effective date of registration',
    
    -- ETL Metadata
    etl_batch_id Int64,
    etl_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(effective_date)
ORDER BY (dim_party_key, dim_tax_type_key, effective_date, dim_date_key)
SETTINGS index_granularity = 8192
COMMENT 'Registration transaction fact - tracks registration/deregistration events';

-- Indexes
CREATE INDEX idx_fact_reg_party ON fact_registration(dim_party_key) TYPE minmax GRANULARITY 4;
CREATE INDEX idx_fact_reg_tax_type ON fact_registration(dim_tax_type_key) TYPE minmax GRANULARITY 4;

-- -------------------------------------------------------------------------------------
-- FACT_FILING: Tax return filing events
-- -------------------------------------------------------------------------------------
-- Purpose: Track tax return filing activity and compliance
-- Grain: One row per tax return filed
-- Type: Transaction fact
-- Volume: 5M initial + 2M annual growth = 15M over 5 years
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_filing
(
    -- Dimension Foreign Keys
    dim_party_key Int64 COMMENT 'FK to dim_party',
    dim_tax_type_key Int32 COMMENT 'FK to dim_tax_type',
    dim_tax_period_key Int32 COMMENT 'FK to dim_tax_period',
    dim_tax_account_key Int64 COMMENT 'FK to dim_tax_account',
    dim_location_key Int32 COMMENT 'FK to dim_location',
    dim_org_unit_key Int32 COMMENT 'FK to dim_organization_unit (filing office)',
    dim_date_key Int32 COMMENT 'FK to dim_time (filing date)',
    
    -- Degenerate Dimensions
    return_id Int64 COMMENT 'Natural key for drill-through',
    return_number String COMMENT 'Return reference number',
    
    -- Measures - Counts
    filing_count Int16 DEFAULT 1 COMMENT 'Always 1 (for counting)',
    
    -- Measures - Timing
    days_late Int16 COMMENT 'Days late (negative = early, 0 = on time, positive = late)',
    processing_days Nullable(Int16) COMMENT 'Days to process return',
    
    -- Measures - Financial
    declared_amount Decimal(19,2) COMMENT 'Tax amount declared by taxpayer',
    tax_base_amount Decimal(19,2) COMMENT 'Tax base (e.g., income, turnover)',
    
    -- Flags
    is_amended UInt8 COMMENT '1=amended return, 0=original',
    is_late UInt8 COMMENT '1=filed after due date',
    is_nil_return UInt8 COMMENT '1=nil return (no liability)',
    is_electronic UInt8 COMMENT '1=e-filed, 0=paper',
    is_auto_assessed UInt8 COMMENT '1=auto-assessed, 0=reviewed',
    
    -- Dates
    filing_date Date COMMENT 'Actual filing date',
    due_date Date COMMENT 'Return due date',
    period_start_date Date COMMENT 'Tax period start',
    period_end_date Date COMMENT 'Tax period end',
    assessment_date Nullable(Date) COMMENT 'Assessment completion date',
    
    -- Status
    filing_status LowCardinality(String) COMMENT 'DRAFT, SUBMITTED, ACCEPTED, REJECTED, AMENDED',
    
    -- ETL Metadata
    etl_batch_id Int64,
    etl_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(filing_date)
ORDER BY (dim_party_key, dim_tax_type_key, dim_tax_period_key, filing_date)
SETTINGS index_granularity = 8192
COMMENT 'Filing transaction fact - tracks tax return filing activity';

-- Indexes
CREATE INDEX idx_fact_filing_party ON fact_filing(dim_party_key) TYPE minmax GRANULARITY 4;
CREATE INDEX idx_fact_filing_late ON fact_filing(is_late) TYPE set(2) GRANULARITY 1;
CREATE INDEX idx_fact_filing_date ON fact_filing(filing_date) TYPE minmax GRANULARITY 4;

-- -------------------------------------------------------------------------------------
-- FACT_ASSESSMENT: Tax assessment events
-- -------------------------------------------------------------------------------------
-- Purpose: Track tax assessments (self, administrative, audit, BJA)
-- Grain: One row per assessment
-- Type: Transaction fact
-- Volume: 10M-100M assessments
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_assessment
(
    -- Dimension Foreign Keys
    dim_party_key Int64,
    dim_tax_type_key Int32,
    dim_tax_period_key Int32,
    dim_tax_account_key Int64,
    dim_location_key Int32,
    dim_org_unit_key Int32,
    dim_officer_key Int32 COMMENT 'Assessing officer',
    dim_date_key Int32 COMMENT 'Assessment date',
    
    -- Degenerate Dimensions
    assessment_id Int64 COMMENT 'Natural key',
    return_id Nullable(Int64) COMMENT 'Related return (if self-assessment)',
    assessment_number String COMMENT 'Assessment reference',
    
    -- Measures - Financial (all in LKR)
    assessed_amount Decimal(19,2) COMMENT 'Total assessed tax amount',
    tax_base_amount Decimal(19,2) COMMENT 'Tax base',
    declared_amount Nullable(Decimal(19,2)) COMMENT 'Amount declared by taxpayer (if from return)',
    
    -- Assessment Components
    principal_tax Decimal(19,2) COMMENT 'Principal tax amount',
    penalties Decimal(19,2) DEFAULT 0 COMMENT 'Total penalties',
    interest Decimal(19,2) DEFAULT 0 COMMENT 'Total interest',
    surcharges Decimal(19,2) DEFAULT 0 COMMENT 'Surcharges/fees',
    
    -- Adjustments
    adjustments Decimal(19,2) DEFAULT 0 COMMENT 'Assessment adjustments',
    credits_applied Decimal(19,2) DEFAULT 0 COMMENT 'Credits/prepayments applied',
    net_amount Decimal(19,2) COMMENT 'Net amount due (or refundable if negative)',
    
    -- Measures - Non-financial
    assessment_count Int16 DEFAULT 1,
    processing_days Nullable(Int16) COMMENT 'Days to complete assessment',
    
    -- Flags
    is_self_assessment UInt8 COMMENT '1=self-assessed (from return)',
    is_administrative UInt8 COMMENT '1=assessed by tax authority',
    is_audit_assessment UInt8 COMMENT '1=from audit findings',
    is_best_judgment UInt8 COMMENT '1=best judgment assessment (non-filer)',
    is_amended UInt8 COMMENT '1=amended assessment',
    is_final UInt8 COMMENT '1=final, 0=provisional',
    is_refund UInt8 COMMENT '1=results in refund',
    
    -- Dates
    assessment_date Date,
    due_date Date COMMENT 'Payment due date',
    period_start_date Date,
    period_end_date Date,
    finalization_date Nullable(Date),
    
    -- Status
    assessment_status LowCardinality(String) COMMENT 'DRAFT, ISSUED, OBJECTED, FINAL, REVERSED',
    
    -- ETL Metadata
    etl_batch_id Int64,
    etl_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(assessment_date)
ORDER BY (dim_party_key, dim_tax_type_key, dim_tax_period_key, assessment_date)
SETTINGS index_granularity = 8192
COMMENT 'Assessment transaction fact - tracks all tax assessments';

-- Indexes
CREATE INDEX idx_fact_assess_amount ON fact_assessment(assessed_amount) TYPE minmax GRANULARITY 4;
CREATE INDEX idx_fact_assess_type ON fact_assessment(is_self_assessment, is_audit_assessment) TYPE set(4) GRANULARITY 1;

-- -------------------------------------------------------------------------------------
-- FACT_PAYMENT: Payment transactions
-- -------------------------------------------------------------------------------------
-- Purpose: Track all payment transactions
-- Grain: One row per payment transaction
-- Type: Transaction fact
-- Volume: 10M initial + 4M annual = 30M over 5 years
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_payment
(
    -- Dimension Foreign Keys
    dim_party_key Int64,
    dim_tax_type_key Int32,
    dim_tax_account_key Int64,
    dim_payment_method_key Int16,
    dim_location_key Int32 COMMENT 'Payment location',
    dim_org_unit_key Int32 COMMENT 'Receiving office',
    dim_date_key Int32 COMMENT 'Payment date',
    
    -- Degenerate Dimensions
    payment_id Int64 COMMENT 'Natural key',
    payment_reference String COMMENT 'Payment reference number',
    transaction_reference Nullable(String) COMMENT 'Bank/external transaction reference',
    
    -- Measures - Financial
    payment_amount Decimal(19,2) COMMENT 'Total payment amount',
    allocated_amount Decimal(19,2) COMMENT 'Amount allocated to liabilities',
    unallocated_amount Decimal(19,2) DEFAULT 0 COMMENT 'Unallocated amount (overpayment)',
    bank_charges Decimal(19,2) DEFAULT 0 COMMENT 'Bank/processing charges',
    
    -- Measures - Counts
    payment_count Int16 DEFAULT 1,
    allocation_count Int16 COMMENT 'Number of assessments this payment covers',
    
    -- Flags
    is_allocated UInt8 COMMENT '1=fully allocated',
    is_partial_payment UInt8 COMMENT '1=partial payment of assessment',
    is_overpayment UInt8 COMMENT '1=payment exceeds liability',
    is_electronic UInt8 COMMENT '1=electronic payment',
    is_verified UInt8 COMMENT '1=verified with bank',
    is_reversed UInt8 DEFAULT 0 COMMENT '1=payment reversed',
    
    -- Dates
    payment_date Date COMMENT 'Payment transaction date',
    received_date Date COMMENT 'Date received by tax authority',
    allocation_date Nullable(Date) COMMENT 'Date allocated to assessments',
    verification_date Nullable(Date) COMMENT 'Bank verification date',
    
    -- Status
    payment_status LowCardinality(String) COMMENT 'RECEIVED, VERIFIED, ALLOCATED, SUSPENDED, REVERSED',
    
    -- ETL Metadata
    etl_batch_id Int64,
    etl_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(payment_date)
ORDER BY (dim_party_key, dim_tax_account_key, payment_date, dim_date_key)
SETTINGS index_granularity = 8192
COMMENT 'Payment transaction fact - tracks all payment receipts';

-- Indexes
CREATE INDEX idx_fact_payment_amount ON fact_payment(payment_amount) TYPE minmax GRANULARITY 4;
CREATE INDEX idx_fact_payment_status ON fact_payment(payment_status) TYPE set(10) GRANULARITY 1;

-- -------------------------------------------------------------------------------------
-- FACT_REFUND: Refund transactions
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_refund
(
    -- Dimension Foreign Keys
    dim_party_key Int64,
    dim_tax_type_key Int32,
    dim_tax_account_key Int64,
    dim_location_key Int32,
    dim_org_unit_key Int32 COMMENT 'Processing office',
    dim_officer_key Int32 COMMENT 'Approving officer',
    dim_date_key Int32 COMMENT 'Refund approval date',
    
    -- Degenerate Dimensions
    refund_id Int64 COMMENT 'Natural key',
    refund_number String COMMENT 'Refund voucher number',
    
    -- Measures - Financial
    refund_amount Decimal(19,2) COMMENT 'Total refund amount',
    offset_amount Decimal(19,2) DEFAULT 0 COMMENT 'Amount offset against other liabilities',
    net_refund_amount Decimal(19,2) COMMENT 'Net amount to be paid out',
    interest_paid Decimal(19,2) DEFAULT 0 COMMENT 'Interest paid on refund',
    
    -- Measures - Timing
    refund_count Int16 DEFAULT 1,
    processing_days Int16 COMMENT 'Days from request to disbursement',
    approval_days Nullable(Int16) COMMENT 'Days for approval',
    
    -- Flags
    is_verified UInt8 COMMENT '1=verified/audited',
    is_offset UInt8 COMMENT '1=offset against liabilities',
    is_electronic UInt8 COMMENT '1=electronic transfer',
    is_approved UInt8 COMMENT '1=approved for payment',
    is_disbursed UInt8 COMMENT '1=payment made',
    
    -- Dates
    request_date Date,
    approval_date Nullable(Date),
    disbursement_date Nullable(Date),
    
    -- Status
    refund_status LowCardinality(String) COMMENT 'REQUESTED, VERIFIED, APPROVED, DISBURSED, REJECTED',
    refund_type LowCardinality(String) COMMENT 'OVERPAYMENT, EXCESS_WITHHOLDING, ADVANCE_PAYMENT',
    
    -- ETL Metadata
    etl_batch_id Int64,
    etl_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(request_date)
ORDER BY (dim_party_key, dim_tax_account_key, request_date)
SETTINGS index_granularity = 8192
COMMENT 'Refund transaction fact';

-- -------------------------------------------------------------------------------------
-- FACT_ACCOUNT_BALANCE: Monthly account balance snapshots
-- -------------------------------------------------------------------------------------
-- Purpose: Periodic snapshot of tax account balances
-- Grain: One row per tax account per month
-- Type: Periodic snapshot fact
-- Volume: 1M initial + 150K/month = 10M over 5 years
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_account_balance
(
    -- Dimension Foreign Keys
    dim_party_key Int64,
    dim_tax_type_key Int32,
    dim_tax_account_key Int64,
    dim_location_key Int32,
    dim_date_key Int32 COMMENT 'Snapshot date (end of month)',
    
    -- Snapshot Date
    snapshot_date Date COMMENT 'Balance as of this date (month-end)',
    snapshot_year Int16,
    snapshot_month UInt8,
    
    -- Degenerate Dimensions
    tax_account_id Int64,
    
    -- Measures - Balances (Semi-additive: sum across accounts, not time)
    opening_balance Decimal(19,2) COMMENT 'Balance at start of period',
    closing_balance Decimal(19,2) COMMENT 'Balance at end of period',
    
    -- Period Activity
    assessed_amount Decimal(19,2) COMMENT 'Total assessments in period',
    payment_received Decimal(19,2) COMMENT 'Payments received in period',
    refund_paid Decimal(19,2) COMMENT 'Refunds paid in period',
    penalties_levied Decimal(19,2) COMMENT 'Penalties in period',
    interest_accrued Decimal(19,2) COMMENT 'Interest accrued in period',
    adjustments Decimal(19,2) COMMENT 'Adjustments (write-offs, etc.)',
    
    -- Derived Measures
    net_activity Decimal(19,2) COMMENT 'Net change in period (closing - opening)',
    arrears_amount Decimal(19,2) COMMENT 'Overdue amount',
    current_amount Decimal(19,2) COMMENT 'Current not-yet-due amount',
    
    -- Counts
    assessment_count Int16 COMMENT 'Number of assessments in period',
    payment_count Int16 COMMENT 'Number of payments in period',
    
    -- Flags
    has_arrears UInt8 COMMENT '1=has overdue amounts',
    is_credit_balance UInt8 COMMENT '1=overpayment (negative balance)',
    is_zero_balance UInt8 COMMENT '1=zero balance',
    
    -- Age of Debt
    days_in_arrears Nullable(Int16) COMMENT 'Age of oldest arrear',
    
    -- Account Status
    account_status LowCardinality(String),
    
    -- ETL Metadata
    etl_batch_id Int64,
    etl_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (dim_party_key, dim_tax_account_key, snapshot_date)
SETTINGS index_granularity = 8192
COMMENT 'Account balance periodic snapshot fact - monthly balances';

-- Indexes
CREATE INDEX idx_fact_balance_arrears ON fact_account_balance(has_arrears) TYPE set(2) GRANULARITY 1;
CREATE INDEX idx_fact_balance_amount ON fact_account_balance(closing_balance) TYPE minmax GRANULARITY 4;

-- -------------------------------------------------------------------------------------
-- FACT_AUDIT: Audit case fact
-- -------------------------------------------------------------------------------------
-- Purpose: Track audit cases and outcomes
-- Grain: One row per audit case
-- Type: Accumulating snapshot fact (tracks case lifecycle)
-- Volume: 10K-100K audit cases
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_audit
(
    -- Dimension Foreign Keys
    dim_party_key Int64,
    dim_tax_type_key Int32 COMMENT 'Primary tax type audited',
    dim_audit_type_key Int16,
    dim_org_unit_key Int32 COMMENT 'Audit office',
    dim_officer_key Int32 COMMENT 'Lead auditor',
    dim_date_key Int32 COMMENT 'Audit start date',
    
    -- Multiple Date Keys (Accumulating Snapshot Pattern)
    dim_start_date_key Int32,
    dim_planned_completion_date_key Nullable(Int32),
    dim_actual_completion_date_key Nullable(Int32),
    dim_report_issue_date_key Nullable(Int32),
    
    -- Degenerate Dimensions
    audit_case_id Int64 COMMENT 'Natural key',
    audit_case_number String,
    audit_plan_id Nullable(Int64) COMMENT 'From audit plan',
    
    -- Measures - Financial
    assessed_amount Decimal(19,2) COMMENT 'Additional tax assessed from audit',
    penalties_assessed Decimal(19,2) DEFAULT 0,
    interest_assessed Decimal(19,2) DEFAULT 0,
    total_amount Decimal(19,2) COMMENT 'Total additional liability',
    collected_amount Decimal(19,2) DEFAULT 0 COMMENT 'Amount collected so far',
    
    -- Measures - Counts and Duration
    audit_count Int16 DEFAULT 1,
    findings_count Int16 COMMENT 'Number of audit findings',
    tax_periods_covered Int16 COMMENT 'Number of periods audited',
    duration_days Nullable(Int16) COMMENT 'Actual duration',
    planned_duration_days Nullable(Int16),
    
    -- Flags
    is_risk_based UInt8 COMMENT '1=selected by risk model',
    is_completed UInt8 COMMENT '1=audit completed',
    has_findings UInt8 COMMENT '1=findings identified',
    is_appealed UInt8 COMMENT '1=taxpayer appealed',
    
    -- Dates
    selection_date Date,
    notification_date Nullable(Date),
    field_start_date Nullable(Date),
    field_end_date Nullable(Date),
    report_issue_date Nullable(Date),
    case_closure_date Nullable(Date),
    
    -- Status
    audit_status LowCardinality(String) COMMENT 'PLANNED, IN_PROGRESS, COMPLETED, CLOSED, APPEALED',
    
    -- ETL Metadata
    etl_batch_id Int64,
    etl_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(selection_date)
ORDER BY (dim_party_key, audit_case_id, selection_date)
SETTINGS index_granularity = 8192
COMMENT 'Audit accumulating snapshot fact - tracks audit lifecycle';

-- -------------------------------------------------------------------------------------
-- FACT_COLLECTION: Collection/enforcement actions
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_collection
(
    -- Dimension Foreign Keys
    dim_party_key Int64,
    dim_tax_type_key Int32,
    dim_tax_account_key Int64,
    dim_org_unit_key Int32,
    dim_officer_key Int32 COMMENT 'Collection officer',
    dim_date_key Int32 COMMENT 'Action date',
    
    -- Degenerate Dimensions
    collection_case_id Int64,
    enforcement_action_id Int64,
    case_number String,
    
    -- Measures - Financial
    debt_amount Decimal(19,2) COMMENT 'Total debt under collection',
    collected_amount Decimal(19,2) COMMENT 'Amount collected from this action',
    action_cost Decimal(19,2) DEFAULT 0 COMMENT 'Cost of enforcement action',
    effectiveness_ratio Decimal(10,4) COMMENT 'Collected / Debt',
    
    -- Measures - Counts
    action_count Int16 DEFAULT 1,
    
    -- Flags
    is_successful UInt8 COMMENT '1=action resulted in payment',
    is_escalated UInt8 COMMENT '1=escalated to higher enforcement',
    
    -- Dates
    action_date Date,
    case_open_date Date,
    debt_due_date Date,
    
    -- Action Details
    action_type LowCardinality(String) COMMENT 'NOTICE, DEMAND, GARNISHMENT, SEIZURE, etc.',
    enforcement_level LowCardinality(String) COMMENT 'REMINDER, FORMAL_DEMAND, LEGAL',
    case_status LowCardinality(String),
    
    -- Age
    debt_age_days Int16 COMMENT 'Age of debt at action',
    
    -- ETL Metadata
    etl_batch_id Int64,
    etl_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(action_date)
ORDER BY (dim_party_key, collection_case_id, action_date)
SETTINGS index_granularity = 8192
COMMENT 'Collection/enforcement transaction fact';

-- -------------------------------------------------------------------------------------
-- FACT_OBJECTION: Objection/appeal cases
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_objection
(
    -- Dimension Foreign Keys
    dim_party_key Int64,
    dim_tax_type_key Int32,
    dim_org_unit_key Int32 COMMENT 'Processing office',
    dim_officer_key Int32 COMMENT 'Case officer',
    dim_date_key Int32 COMMENT 'Filing date',
    
    -- Degenerate Dimensions
    objection_id Int64,
    objection_number String,
    assessment_id Nullable(Int64) COMMENT 'Disputed assessment',
    
    -- Measures - Financial
    disputed_amount Decimal(19,2) COMMENT 'Amount under dispute',
    resolved_amount Decimal(19,2) COMMENT 'Amount upheld/reduced after decision',
    taxpayer_favor_amount Decimal(19,2) COMMENT 'Amount reduced in taxpayer favor',
    
    -- Measures - Timing
    objection_count Int16 DEFAULT 1,
    resolution_days Nullable(Int16) COMMENT 'Days to resolve',
    
    -- Flags
    is_resolved UInt8,
    is_upheld UInt8 COMMENT '1=assessment upheld',
    is_reduced UInt8 COMMENT '1=assessment reduced',
    is_cancelled UInt8 COMMENT '1=assessment cancelled',
    is_appealed_further UInt8 COMMENT '1=appealed to higher level',
    
    -- Dates
    filing_date Date,
    hearing_date Nullable(Date),
    decision_date Nullable(Date),
    closure_date Nullable(Date),
    
    -- Status
    objection_status LowCardinality(String) COMMENT 'FILED, UNDER_REVIEW, HEARING, DECIDED, CLOSED',
    objection_level LowCardinality(String) COMMENT 'FIRST_LEVEL, APPEAL, COURT',
    decision_outcome LowCardinality(Nullable(String)) COMMENT 'UPHELD, PARTIALLY_UPHELD, CANCELLED',
    
    -- ETL Metadata
    etl_batch_id Int64,
    etl_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(filing_date)
ORDER BY (dim_party_key, objection_id, filing_date)
SETTINGS index_granularity = 8192
COMMENT 'Objection/appeal transaction fact';

-- -------------------------------------------------------------------------------------
-- FACT_RISK_ASSESSMENT: Taxpayer risk assessments
-- -------------------------------------------------------------------------------------
-- Purpose: Periodic snapshot of taxpayer risk scores
-- Grain: One row per taxpayer per risk assessment (typically quarterly/annually)
-- Type: Periodic snapshot
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_risk_assessment
(
    -- Dimension Foreign Keys
    dim_party_key Int64,
    dim_tax_type_key Nullable(Int32) COMMENT 'NULL for overall risk, specific for tax-type risk',
    dim_location_key Int32,
    dim_date_key Int32 COMMENT 'Assessment date',
    
    -- Snapshot Date
    assessment_date Date,
    assessment_year Int16,
    assessment_quarter UInt8,
    
    -- Degenerate Dimensions
    risk_profile_id Int64,
    party_id Int64,
    
    -- Measures - Risk Scores (0-100 scale)
    overall_risk_score Decimal(5,2) COMMENT 'Composite risk score',
    filing_risk_score Decimal(5,2) COMMENT 'Filing compliance risk',
    payment_risk_score Decimal(5,2) COMMENT 'Payment compliance risk',
    reporting_risk_score Decimal(5,2) COMMENT 'Reporting accuracy risk',
    audit_history_risk_score Decimal(5,2) COMMENT 'Historical audit findings risk',
    third_party_risk_score Decimal(5,2) COMMENT 'Third-party data mismatch risk',
    
    -- Risk Indicators
    non_filing_count Int16 COMMENT 'Number of unfiled returns',
    late_filing_count Int16 COMMENT 'Number of late filings',
    late_payment_count Int16 COMMENT 'Number of late payments',
    audit_assessment_count Int16 COMMENT 'Number of audit assessments',
    objection_count Int16 COMMENT 'Number of objections filed',
    
    -- Flags
    is_high_risk UInt8 COMMENT '1=high or critical risk',
    is_audit_candidate UInt8 COMMENT '1=recommended for audit',
    
    -- Risk Category
    risk_rating LowCardinality(String) COMMENT 'VERY_LOW, LOW, MEDIUM, HIGH, VERY_HIGH, CRITICAL',
    
    -- Previous Assessment Comparison
    previous_risk_score Nullable(Decimal(5,2)),
    risk_trend LowCardinality(Nullable(String)) COMMENT 'IMPROVING, STABLE, DETERIORATING',
    
    -- ETL Metadata
    etl_batch_id Int64,
    etl_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(assessment_date)
ORDER BY (dim_party_key, assessment_date)
SETTINGS index_granularity = 8192
COMMENT 'Risk assessment periodic snapshot fact';

-- Indexes
CREATE INDEX idx_fact_risk_score ON fact_risk_assessment(overall_risk_score) TYPE minmax GRANULARITY 4;
CREATE INDEX idx_fact_risk_high ON fact_risk_assessment(is_high_risk) TYPE set(2) GRANULARITY 1;

-- -------------------------------------------------------------------------------------
-- FACT_TAXPAYER_ACTIVITY: Monthly taxpayer activity snapshot
-- -------------------------------------------------------------------------------------
-- Purpose: Consolidated monthly snapshot of taxpayer activity across all areas
-- Grain: One row per taxpayer per month
-- Type: Periodic snapshot (360-degree view)
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_taxpayer_activity
(
    -- Dimension Foreign Keys
    dim_party_key Int64,
    dim_location_key Int32,
    dim_date_key Int32 COMMENT 'Month-end date',
    
    -- Snapshot Date
    snapshot_date Date COMMENT 'End of month',
    snapshot_year Int16,
    snapshot_month UInt8,
    
    -- Degenerate Dimensions
    party_id Int64,
    
    -- Measures - Filing Activity
    total_filings Int16 COMMENT 'Returns filed in month',
    on_time_filings Int16,
    late_filings Int16,
    non_filings Int16 COMMENT 'Expected but not filed',
    
    -- Measures - Payment Activity
    total_payments Int16 COMMENT 'Payments made in month',
    payment_amount Decimal(19,2) COMMENT 'Total paid',
    on_time_payment_amount Decimal(19,2),
    late_payment_amount Decimal(19,2),
    
    -- Measures - Assessment Activity
    total_assessments Int16,
    assessed_amount Decimal(19,2),
    self_assessed_amount Decimal(19,2),
    admin_assessed_amount Decimal(19,2),
    
    -- Measures - Account Status
    outstanding_balance Decimal(19,2),
    arrears_amount Decimal(19,2),
    credit_balance Decimal(19,2),
    
    -- Measures - Compliance Metrics
    filing_compliance_rate Decimal(5,2) COMMENT 'Percentage (0-100)',
    payment_compliance_rate Decimal(5,2),
    overall_compliance_score Decimal(5,2),
    
    -- Measures - Enforcement
    collection_actions_count Int16,
    audit_cases_count Int16,
    
    -- Flags
    has_active_audit UInt8,
    has_active_collection UInt8,
    has_active_objection UInt8,
    is_compliant UInt8 COMMENT '1=fully compliant this month',
    
    -- Risk
    current_risk_rating LowCardinality(String),
    
    -- ETL Metadata
    etl_batch_id Int64,
    etl_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (dim_party_key, snapshot_date)
SETTINGS index_granularity = 8192
COMMENT 'Taxpayer monthly activity snapshot fact - 360 degree view';

-- =====================================================================================
-- SECTION 3: AGGREGATE TABLES FOR PERFORMANCE
-- =====================================================================================
-- Pre-aggregated tables for common queries and reports
-- =====================================================================================

-- -------------------------------------------------------------------------------------
-- AGG_REVENUE_MONTHLY: Monthly revenue by tax type
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS agg_revenue_monthly
(
    -- Dimensions
    dim_tax_type_key Int32,
    dim_date_key Int32,
    snapshot_date Date,
    snapshot_year Int16,
    snapshot_month UInt8,
    
    -- Aggregated Measures
    revenue_collected Decimal(19,2) COMMENT 'Total revenue collected',
    taxpayer_count Int32 COMMENT 'Number of active taxpayers',
    filing_count Int32 COMMENT 'Total filings',
    payment_count Int32 COMMENT 'Total payments',
    assessment_count Int32 COMMENT 'Total assessments',
    
    -- Averages
    avg_per_taxpayer Decimal(19,2),
    avg_per_payment Decimal(19,2),
    
    -- ETL Metadata
    etl_batch_id Int64,
    etl_timestamp DateTime DEFAULT now()
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (dim_tax_type_key, snapshot_date)
SETTINGS index_granularity = 8192
COMMENT 'Monthly revenue aggregate by tax type';

-- -------------------------------------------------------------------------------------
-- AGG_COMPLIANCE_MONTHLY: Monthly compliance metrics
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS agg_compliance_monthly
(
    -- Dimensions
    dim_tax_type_key Int32,
    dim_location_key Int32,
    dim_date_key Int32,
    snapshot_date Date,
    snapshot_year Int16,
    snapshot_month UInt8,
    
    -- Compliance Counts
    expected_filings Int32,
    actual_filings Int32,
    on_time_filings Int32,
    late_filings Int32,
    non_filings Int32,
    
    -- Compliance Rates
    filing_compliance_rate Decimal(5,2),
    payment_compliance_rate Decimal(5,2),
    
    -- Risk Distribution
    low_risk_count Int32,
    medium_risk_count Int32,
    high_risk_count Int32,
    critical_risk_count Int32,
    
    -- ETL Metadata
    etl_batch_id Int64,
    etl_timestamp DateTime DEFAULT now()
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (dim_tax_type_key, dim_location_key, snapshot_date)
SETTINGS index_granularity = 8192
COMMENT 'Monthly compliance metrics aggregate';

-- =====================================================================================
-- SECTION 4: MATERIALIZED VIEWS FOR REAL-TIME ANALYTICS
-- =====================================================================================
-- Materialized views for frequently accessed queries
-- =====================================================================================

-- -------------------------------------------------------------------------------------
-- MV_CURRENT_DIMENSIONS: Materialized view of current dimension versions
-- -------------------------------------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dim_party_current
ENGINE = MergeTree()
ORDER BY party_key
AS
SELECT *
FROM dim_party
WHERE is_current = 1;

-- -------------------------------------------------------------------------------------
-- MV_TAXPAYER_SUMMARY: Real-time taxpayer summary
-- -------------------------------------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_taxpayer_summary
ENGINE = AggregatingMergeTree()
ORDER BY dim_party_key
AS
SELECT
    dim_party_key,
    countState() as total_filings,
    sumState(declared_amount) as total_declared,
    avgState(days_late) as avg_days_late,
    maxState(filing_date) as last_filing_date
FROM fact_filing
GROUP BY dim_party_key;

-- =====================================================================================
-- SECTION 5: REFERENCE/LOOKUP TABLES
-- =====================================================================================
-- Small reference tables for codes and classifications
-- =====================================================================================

-- -------------------------------------------------------------------------------------
-- REF_PARTY_STATUS: Party status codes
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ref_party_status
(
    status_code LowCardinality(String) COMMENT 'Status code',
    status_name String COMMENT 'Status name',
    status_description String COMMENT 'Description',
    is_active UInt8 COMMENT 'Is considered active status',
    sort_order Int16 COMMENT 'Display order'
)
ENGINE = MergeTree()
ORDER BY status_code
COMMENT 'Party status reference data';

-- Insert reference data
INSERT INTO ref_party_status VALUES
    ('ACTIVE', 'Active', 'Active and operational taxpayer', 1, 1),
    ('INACTIVE', 'Inactive', 'Inactive but can be reactivated', 0, 2),
    ('SUSPENDED', 'Suspended', 'Temporarily suspended by authority', 0, 3),
    ('DECEASED', 'Deceased', 'Individual deceased', 0, 4),
    ('DISSOLVED', 'Dissolved', 'Enterprise dissolved', 0, 5);

-- -------------------------------------------------------------------------------------
-- REF_PARTY_TYPE: Party type codes
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ref_party_type
(
    type_code LowCardinality(String),
    type_name String,
    type_description String,
    sort_order Int16
)
ENGINE = MergeTree()
ORDER BY type_code
COMMENT 'Party type reference data';

INSERT INTO ref_party_type VALUES
    ('INDIVIDUAL', 'Individual', 'Natural person', 1),
    ('ENTERPRISE', 'Enterprise', 'Business entity / Legal person', 2),
    ('GOVERNMENT', 'Government', 'Government entity', 3);

-- -------------------------------------------------------------------------------------
-- REF_RISK_RATING: Risk rating categories
-- -------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ref_risk_rating
(
    rating_code LowCardinality(String),
    rating_name String,
    score_min Decimal(5,2),
    score_max Decimal(5,2),
    color_code String COMMENT 'For visualization',
    sort_order Int16
)
ENGINE = MergeTree()
ORDER BY rating_code
COMMENT 'Risk rating reference data';

INSERT INTO ref_risk_rating VALUES
    ('VERY_LOW', 'Very Low Risk', 0.00, 20.00, '#00FF00', 1),
    ('LOW', 'Low Risk', 20.00, 40.00, '#90EE90', 2),
    ('MEDIUM', 'Medium Risk', 40.00, 60.00, '#FFFF00', 3),
    ('HIGH', 'High Risk', 60.00, 80.00, '#FFA500', 4),
    ('VERY_HIGH', 'Very High Risk', 80.00, 95.00, '#FF4500', 5),
    ('CRITICAL', 'Critical Risk', 95.00, 100.00, '#FF0000', 6);

-- =====================================================================================
-- SECTION 6: UTILITY VIEWS AND HELPER FUNCTIONS
-- =====================================================================================

-- -------------------------------------------------------------------------------------
-- View: Current taxpayer list with latest risk rating
-- -------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW v_taxpayer_current AS
SELECT
    p.party_key,
    p.party_id,
    p.tin,
    p.party_name,
    p.party_type,
    p.party_segment,
    p.risk_rating,
    p.risk_score,
    p.city,
    p.district,
    p.email,
    p.phone,
    p.party_status
FROM dim_party p
WHERE p.is_current = 1;

-- -------------------------------------------------------------------------------------
-- View: Filing compliance summary by tax type
-- -------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW v_filing_compliance_summary AS
SELECT
    dt.tax_type_code,
    dt.tax_type_name,
    d.year,
    d.quarter,
    COUNT(*) as total_filings,
    SUM(CASE WHEN f.is_late = 0 THEN 1 ELSE 0 END) as on_time_filings,
    SUM(CASE WHEN f.is_late = 1 THEN 1 ELSE 0 END) as late_filings,
    ROUND(SUM(CASE WHEN f.is_late = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as compliance_rate
FROM fact_filing f
JOIN dim_tax_type dt ON f.dim_tax_type_key = dt.tax_type_key
JOIN dim_time d ON f.dim_date_key = d.date_key
GROUP BY dt.tax_type_code, dt.tax_type_name, d.year, d.quarter;

-- -------------------------------------------------------------------------------------
-- View: Revenue collection by tax type and month
-- -------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW v_revenue_by_tax_type AS
SELECT
    dt.tax_type_code,
    dt.tax_type_name,
    dt.tax_category,
    d.year,
    d.month,
    d.month_name,
    SUM(p.payment_amount) as total_collected,
    COUNT(DISTINCT p.dim_party_key) as taxpayer_count,
    COUNT(*) as payment_count,
    AVG(p.payment_amount) as avg_payment
FROM fact_payment p
JOIN dim_tax_type dt ON p.dim_tax_type_key = dt.tax_type_key
JOIN dim_time d ON p.dim_date_key = d.date_key
GROUP BY 
    dt.tax_type_code,
    dt.tax_type_name,
    dt.tax_category,
    d.year,
    d.month,
    d.month_name;

-- -------------------------------------------------------------------------------------
-- View: High-risk taxpayers requiring attention
-- -------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW v_high_risk_taxpayers AS
SELECT
    p.party_key,
    p.tin,
    p.party_name,
    p.party_segment,
    p.risk_rating,
    p.risk_score,
    fa.outstanding_balance,
    fa.arrears_amount,
    fa.has_arrears,
    fra.overall_risk_score,
    fra.filing_risk_score,
    fra.payment_risk_score
FROM dim_party p
LEFT JOIN (
    SELECT 
        dim_party_key,
        SUM(closing_balance) as outstanding_balance,
        SUM(arrears_amount) as arrears_amount,
        MAX(has_arrears) as has_arrears
    FROM fact_account_balance
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM fact_account_balance)
    GROUP BY dim_party_key
) fa ON p.party_key = fa.dim_party_key
LEFT JOIN (
    SELECT 
        dim_party_key,
        overall_risk_score,
        filing_risk_score,
        payment_risk_score
    FROM fact_risk_assessment
    WHERE assessment_date = (SELECT MAX(assessment_date) FROM fact_risk_assessment)
) fra ON p.party_key = fra.dim_party_key
WHERE p.is_current = 1
  AND (p.risk_rating IN ('HIGH', 'VERY_HIGH', 'CRITICAL')
       OR fa.has_arrears = 1
       OR fra.overall_risk_score > 60);

-- =====================================================================================
-- SECTION 7: SAMPLE ANALYTICAL QUERIES
-- =====================================================================================

/*
-- Query 1: Filing compliance rate by tax type and quarter
SELECT
    tt.tax_type_name,
    t.year,
    t.quarter_name,
    COUNT(*) as total_filings,
    SUM(CASE WHEN f.is_late = 0 THEN 1 ELSE 0 END) as on_time,
    SUM(CASE WHEN f.is_late = 1 THEN 1 ELSE 0 END) as late,
    ROUND(SUM(CASE WHEN f.is_late = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as compliance_rate
FROM fact_filing f
JOIN dim_tax_type tt ON f.dim_tax_type_key = tt.tax_type_key
JOIN dim_time t ON f.dim_date_key = t.date_key
WHERE t.year = 2025
GROUP BY tt.tax_type_name, t.year, t.quarter_name
ORDER BY t.year, t.quarter, tt.tax_type_name;

-- Query 2: Revenue collection by month with year-over-year comparison
SELECT
    t.month_name,
    SUM(CASE WHEN t.year = 2025 THEN p.payment_amount ELSE 0 END) as revenue_2025,
    SUM(CASE WHEN t.year = 2024 THEN p.payment_amount ELSE 0 END) as revenue_2024,
    ROUND((SUM(CASE WHEN t.year = 2025 THEN p.payment_amount ELSE 0 END) - 
           SUM(CASE WHEN t.year = 2024 THEN p.payment_amount ELSE 0 END)) * 100.0 /
          NULLIF(SUM(CASE WHEN t.year = 2024 THEN p.payment_amount ELSE 0 END), 0), 2) as growth_rate
FROM fact_payment p
JOIN dim_time t ON p.dim_date_key = t.date_key
WHERE t.year IN (2024, 2025)
GROUP BY t.month, t.month_name
ORDER BY t.month;

-- Query 3: Top 10 taxpayers by risk score with outstanding balances
SELECT
    p.tin,
    p.party_name,
    p.party_segment,
    p.risk_rating,
    ra.overall_risk_score,
    ab.closing_balance as outstanding_balance,
    ab.arrears_amount
FROM dim_party p
JOIN fact_risk_assessment ra ON p.party_key = ra.dim_party_key
JOIN fact_account_balance ab ON p.party_key = ab.dim_party_key
WHERE p.is_current = 1
  AND ra.assessment_date = (SELECT MAX(assessment_date) FROM fact_risk_assessment)
  AND ab.snapshot_date = (SELECT MAX(snapshot_date) FROM fact_account_balance)
  AND ab.closing_balance > 0
ORDER BY ra.overall_risk_score DESC
LIMIT 10;

-- Query 4: Audit effectiveness - assessments vs collections
SELECT
    dt.tax_type_name,
    t.year,
    COUNT(*) as total_audits,
    SUM(a.assessed_amount) as total_assessed,
    SUM(a.collected_amount) as total_collected,
    ROUND(SUM(a.collected_amount) * 100.0 / NULLIF(SUM(a.assessed_amount), 0), 2) as collection_rate,
    AVG(a.duration_days) as avg_audit_days
FROM fact_audit a
JOIN dim_tax_type dt ON a.dim_tax_type_key = dt.tax_type_key
JOIN dim_time t ON a.dim_start_date_key = t.date_key
WHERE a.is_completed = 1
GROUP BY dt.tax_type_name, t.year
ORDER BY t.year DESC, dt.tax_type_name;

-- Query 5: Taxpayer segmentation analysis
SELECT
    p.party_segment,
    COUNT(DISTINCT p.party_key) as taxpayer_count,
    SUM(ab.closing_balance) as total_balance,
    AVG(ab.closing_balance) as avg_balance,
    SUM(CASE WHEN ab.has_arrears = 1 THEN 1 ELSE 0 END) as taxpayers_with_arrears,
    ROUND(SUM(CASE WHEN ab.has_arrears = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT p.party_key), 2) as arrears_rate
FROM dim_party p
LEFT JOIN fact_account_balance ab ON p.party_key = ab.dim_party_key
WHERE p.is_current = 1
  AND ab.snapshot_date = (SELECT MAX(snapshot_date) FROM fact_account_balance)
GROUP BY p.party_segment
ORDER BY taxpayer_count DESC;
*/

-- =====================================================================================
-- END OF SCRIPT
-- =====================================================================================

-- Summary Statistics
-- -------------------
-- Conformed Dimensions: 12 tables
-- Fact Tables: 10 tables (6 transaction, 3 snapshot, 1 accumulating)
-- Aggregate Tables: 2 tables
-- Reference Tables: 3 tables
-- Materialized Views: 2 views
-- Utility Views: 4 views
--
-- Total Tables: 27+
-- Estimated Initial Volume: ~20M rows
-- Estimated 5-Year Volume: ~100M rows
--
-- Optimizations:
-- - Partitioning by date for time-series data
-- - Appropriate ORDER BY for query patterns
-- - LowCardinality for enum-like columns
-- - Indexes on frequently filtered columns
-- - Materialized views for common queries
-- - Pre-aggregated tables for performance
--
-- Next Steps:
-- 1. Populate DIM_TIME with date range (2020-2030)
-- 2. Load reference data for all lookup tables
-- 3. Implement ETL processes from TA-RDM Layer 2
-- 4. Create monitoring and data quality checks
-- 5. Build Layer 4 data marts for specific departments
-- 6. Develop BI dashboards and reports
--
-- =====================================================================================
