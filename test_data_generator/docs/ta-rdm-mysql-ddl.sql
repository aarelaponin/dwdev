-- TA-RDM MySQL Database Schema (L2)
-- Generated: 2025-11-06
-- MySQL Version: 9.0.1

-- Character Set: utf8mb4
-- Collation: utf8mb4_unicode_ci


-- ==============================================================================
-- Schema: party
-- ==============================================================================

CREATE DATABASE IF NOT EXISTS `party`;
USE `party`;

-- ------------------------------------------------------------------------------
-- Table: individual
-- ------------------------------------------------------------------------------

CREATE TABLE `individual` (
  `individual_id` bigint NOT NULL COMMENT 'Primary key, references party.party_id',
  `party_id` bigint NOT NULL COMMENT 'Foreign key to parent party record',
  `tax_identification_number` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Tax Identification Number (TIN) for this individual',
  `first_name` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'First/given name',
  `last_name` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Last/family name',
  `middle_name` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Middle name(s) or patronymic',
  `birth_date` date NOT NULL COMMENT 'Date of birth',
  `gender_code` varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Gender classification',
  `deceased_date` date DEFAULT NULL COMMENT 'Date of death, if applicable',
  `citizenship_country_code` char(3) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'ISO 3166-1 alpha-3 country code of citizenship',
  `resident_flag` tinyint(1) NOT NULL COMMENT 'Tax residence status flag',
  `marital_status_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Marital status (if relevant for tax purposes)',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_date` timestamp NOT NULL,
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `modified_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`individual_id`),
  UNIQUE KEY `tax_identification_number` (`tax_identification_number`),
  KEY `fk_individual_party_id` (`party_id`),
  CONSTRAINT `fk_individual_party_id` FOREIGN KEY (`party_id`) REFERENCES `party` (`party_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Natural person entity with specific attributes for individuals.\nExtends the party entity with person-specific information.\n';

-- ------------------------------------------------------------------------------
-- Table: party
-- ------------------------------------------------------------------------------

CREATE TABLE `party` (
  `party_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for the party',
  `party_uuid` char(36) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Globally unique identifier for external integration.\nUsed for API references and cross-system correlation.\n',
  `party_type_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Discriminator for party subtype',
  `party_status_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Current operational status of the party',
  `party_name` varchar(500) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Full name or business name of the party',
  `party_short_name` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Abbreviated name for display purposes',
  `valid_from` date NOT NULL COMMENT 'Date from which this record version is valid',
  `valid_to` date DEFAULT NULL COMMENT 'Date until which this record version is valid.\nNULL indicates current/active record.\n',
  `is_current` tinyint(1) NOT NULL COMMENT 'Flag indicating if this is the current version',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Username of person who created this record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when record was created',
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Username of person who last modified this record',
  `modified_date` timestamp NULL DEFAULT NULL COMMENT 'Timestamp when record was last modified',
  `district_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'District/province code.\nReferences reference.ref_district.district_code',
  `locality_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'City/locality code.\nReferences reference.ref_locality.locality_code',
  `tax_identification_number` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Tax Identification Number (TIN) for this party',
  `industry_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'ISIC industry classification code',
  `legal_form_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Legal structure of entity (LLC, Corporation, etc.)',
  `registration_date` date DEFAULT NULL COMMENT 'Date of business registration with authorities',
  `country_code` char(3) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'ISO 3166-1 alpha-3 country code',
  PRIMARY KEY (`party_id`),
  UNIQUE KEY `party_uuid` (`party_uuid`),
  KEY `idx_party_type_status` (`party_type_code`,`party_status_code`),
  KEY `idx_party_valid_period` (`valid_from`,`valid_to`),
  KEY `idx_party_created_date` (`created_date`)
) ENGINE=InnoDB AUTO_INCREMENT=26 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Supertype entity representing any party that can interact with\nthe tax administration. Uses single-table inheritance with\nparty_type_code as discriminator.\n\nKey design decisions:\n- Uses surrogate key (party_id) for performance\n- Includes UUID for external';


-- ==============================================================================
-- Schema: registration
-- ==============================================================================

CREATE DATABASE IF NOT EXISTS `registration`;
USE `registration`;

-- ------------------------------------------------------------------------------
-- Table: business_license
-- ------------------------------------------------------------------------------

CREATE TABLE `business_license` (
  `license_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for business license',
  `party_id` bigint NOT NULL COMMENT 'Foreign key to party (license holder)',
  `license_number` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Official license number issued to taxpayer',
  `license_type_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Type of license:\n- GENERAL: General business license\n- ALCOHOL: Alcohol sales/production\n- TOBACCO: Tobacco sales/production\n- GAMING: Gaming/gambling operations\n- IMPORT_EXPORT: Import/export authorization\n- PROFESSIONAL: Professional practice license\n- ',
  `license_status_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Current status: PENDING, ACTIVE, SUSPENDED, EXPIRED, REVOKED, CANCELLED\n',
  `issue_date` date NOT NULL COMMENT 'Date license was issued',
  `effective_from_date` date NOT NULL COMMENT 'Date from which license is valid',
  `effective_to_date` date NOT NULL COMMENT 'Date until which license is valid',
  `renewal_date` date DEFAULT NULL COMMENT 'Date when license must be renewed',
  `establishment_id` bigint DEFAULT NULL COMMENT 'Foreign key to establishment if license is for specific\nbusiness location (rather than the legal entity overall).\n',
  `licensed_activity_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Specific activity authorized by this license.\nLinks to industry classification codes.\n',
  `license_fee_amount` decimal(19,2) DEFAULT NULL COMMENT 'Fee charged for license issuance or renewal',
  `fee_currency_code` char(3) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'ISO 4217 currency code for license fee',
  `fee_paid_date` date DEFAULT NULL COMMENT 'Date when license fee was paid',
  `triggers_tax_type_codes` varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Comma-separated list of tax type codes that should be\nautomatically registered when this license is issued.\nExample: "EXCISE_ALCOHOL,VAT"\n',
  `suspension_date` date DEFAULT NULL COMMENT 'Date license was suspended',
  `suspension_reason` text COLLATE utf8mb4_unicode_ci COMMENT 'Reason for suspending license',
  `revocation_date` date DEFAULT NULL COMMENT 'Date license was revoked',
  `revocation_reason` text COLLATE utf8mb4_unicode_ci COMMENT 'Reason for revoking license',
  `application_date` date DEFAULT NULL COMMENT 'Date when license was applied for',
  `approved_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Officer who approved the license',
  `approval_date` date DEFAULT NULL COMMENT 'Date license was approved',
  `notes` text COLLATE utf8mb4_unicode_ci COMMENT 'Additional notes about the license',
  `valid_from` date NOT NULL COMMENT 'Date from which this record version is valid',
  `valid_to` date DEFAULT NULL COMMENT 'Date until which this record version is valid',
  `is_current` tinyint(1) NOT NULL COMMENT 'Flag indicating if this is the current version',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Username of person who created this record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when record was created',
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Username of person who last modified',
  `modified_date` timestamp NULL DEFAULT NULL COMMENT 'Timestamp when record was last modified',
  PRIMARY KEY (`license_id`),
  UNIQUE KEY `license_number` (`license_number`),
  KEY `idx_business_lic_expiry` (`effective_to_date`,`license_status_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Tracks business licenses, permits, and authorizations issued to\ntaxpayers. Licenses may be required for certain business activities\nand often trigger automatic tax registration.\n\nExamples:\n- General business license\n- Industry-specific licenses (alcohol, ';

-- ------------------------------------------------------------------------------
-- Table: registration_compliance_case
-- ------------------------------------------------------------------------------

CREATE TABLE `registration_compliance_case` (
  `compliance_case_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for compliance case',
  `case_number` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Human-readable case reference number',
  `party_id` bigint NOT NULL COMMENT 'Taxpayer under review',
  `case_type_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Type of compliance issue:\nNOT_REGISTERED, MISSING_TAX_TYPE, INCORRECT_DATA,\nFRAUDULENT_REGISTRATION, THRESHOLD_BREACH\n',
  `tax_type_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Tax type involved in the case',
  `case_status_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Current status: DETECTED, UNDER_REVIEW, CONTACTED,\nRESOLVED, ENFORCED, DISMISSED\n',
  `detection_date` date NOT NULL COMMENT 'Date when non-compliance was detected',
  `detection_method_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'How was non-compliance detected:\nTHIRD_PARTY_DATA, AUDIT_FINDING, TAXPAYER_REPORT,\nDATA_MATCHING, OFFICER_OBSERVATION\n',
  `detection_source` text COLLATE utf8mb4_unicode_ci COMMENT 'Details of how case was detected',
  `assigned_officer_id` bigint DEFAULT NULL COMMENT 'Officer responsible for investigating case',
  `assignment_date` date DEFAULT NULL COMMENT 'Date case was assigned to officer',
  `priority_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Case priority: LOW, MEDIUM, HIGH, URGENT',
  `estimated_tax_exposure` decimal(19,2) DEFAULT NULL COMMENT 'Estimated amount of tax at risk due to non-registration.\nUsed for prioritization.\n',
  `contact_date` date DEFAULT NULL COMMENT 'Date taxpayer was first contacted about case',
  `taxpayer_response_date` date DEFAULT NULL COMMENT 'Date taxpayer responded',
  `taxpayer_response` text COLLATE utf8mb4_unicode_ci COMMENT 'Summary of taxpayer''s response',
  `resolution_date` date DEFAULT NULL COMMENT 'Date case was resolved',
  `resolution_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'How case was resolved:\nVOLUNTARY_REGISTRATION, ENFORCED_REGISTRATION,\nNOT_REQUIRED, DISMISSED, ONGOING\n',
  `resolution_notes` text COLLATE utf8mb4_unicode_ci COMMENT 'Detailed notes on case resolution',
  `tax_account_created_id` bigint DEFAULT NULL COMMENT 'If case resulted in tax registration, reference to the\ntax account created.\n',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Username of person who created this record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when record was created',
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Username of person who last modified',
  `modified_date` timestamp NULL DEFAULT NULL COMMENT 'Timestamp when record was last modified',
  PRIMARY KEY (`compliance_case_id`),
  UNIQUE KEY `case_number` (`case_number`),
  KEY `idx_reg_comp_open_cases` (`case_status_code`,`priority_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Tracks cases where tax authority monitors or enforces taxpayer\nregistration compliance. Created when:\n- Non-registered taxpayer detected performing taxable activity\n- Registered taxpayer should register for additional tax type\n- Registration data appears ';

-- ------------------------------------------------------------------------------
-- Table: tax_account
-- ------------------------------------------------------------------------------

CREATE TABLE `tax_account` (
  `tax_account_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for the tax account',
  `party_id` bigint NOT NULL COMMENT 'Foreign key to party table. Identifies which taxpayer this\naccount belongs to (the TIN part of TTT).\n',
  `tax_type_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Foreign key to tax_type table. Identifies which tax type this\naccount is for (the Tax Type part of TTT).\nExamples: CIT, PIT, VAT, EXCISE, PROPERTY_TAX\n',
  `tax_account_number` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Human-readable tax account number. Often formatted as\nTIN-TAX_TYPE (e.g., 1234567890-VAT). Used for external\nreferences and taxpayer communication.\n',
  `account_status_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Current operational status of the tax account.\n\nStatus definitions:\n- PENDING: Registration application received, not yet approved\n- ACTIVE: Fully operational, filing and payment obligations active\n- INACTIVE: Temporarily suspended (seasonal business, etc',
  `registration_date` date NOT NULL COMMENT 'Date when taxpayer was registered for this tax type.\nThis is the start date for all tax obligations.\n',
  `registration_method_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'How this tax account was created:\n- AUTOMATIC: Auto-registered based on tax rules (e.g., new business)\n- VOLUNTARY: Taxpayer application (e.g., VAT below threshold)\n- ENFORCED: Tax authority enforced registration\n- MIGRATION: Migrated from legacy system\n',
  `registration_reason` text COLLATE utf8mb4_unicode_ci COMMENT 'Free-text explanation for why this registration was created.\nEspecially important for enforced registrations.\n',
  `effective_from_date` date NOT NULL COMMENT 'Date from which tax obligations begin. Usually same as\nregistration_date but can be backdated or future-dated.\n',
  `effective_to_date` date DEFAULT NULL COMMENT 'Date when tax obligations end. NULL for active accounts.\nPopulated on deregistration.\n',
  `deregistration_date` date DEFAULT NULL COMMENT 'Date when taxpayer was deregistered from this tax type.\nPopulated when status changes to DEREGISTERED.\n',
  `deregistration_reason_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Reason for deregistration:\n- VOLUNTARY: Taxpayer request\n- OFFICIAL_DUTY: Tax authority initiated\n- DEATH: Individual deceased\n- DISSOLUTION: Business dissolved\n- BELOW_THRESHOLD: Turnover fell below threshold\n- CEASED_ACTIVITY: Business ceased operations',
  `last_filing_period_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Code of the last tax period for which filing is required.\nUsed during deregistration to determine final obligations.\n',
  `segmentation_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Taxpayer segmentation for this tax type. Determines service\nlevel, compliance approach, and dedicated officer assignment.\n\nCommon segments:\n- LARGE: Large taxpayers (dedicated service)\n- MEDIUM: Medium taxpayers (standard service)\n- SMALL: Small taxpayers',
  `assigned_office_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Tax office responsible for this account. Used for workload\ndistribution and taxpayer service.\n',
  `assigned_officer_id` bigint DEFAULT NULL COMMENT 'Specific tax officer assigned to manage this account.\nNULL for small taxpayers (self-service).\n',
  `annual_turnover` decimal(19,2) DEFAULT NULL COMMENT 'Declared or estimated annual turnover for this taxpayer.\nUsed for threshold validation (VAT, etc.) and segmentation.\n',
  `turnover_currency_code` char(3) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'ISO 4217 currency code for annual turnover',
  `is_group_filing` tinyint(1) NOT NULL COMMENT 'Whether this account participates in group filing (VAT groups,\nconsolidated tax groups, etc.). Group head files for group.\n',
  `group_head_account_id` bigint DEFAULT NULL COMMENT 'If is_group_filing=true and this is not the head, references\nthe tax account of the group head who files on behalf.\n',
  `filing_frequency_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'How often this taxpayer must file returns for this tax type.\nCan differ from tax type default based on turnover.\n\nExamples: MONTHLY, QUARTERLY, ANNUAL\n',
  `payment_due_day` int DEFAULT NULL COMMENT 'Day of month when payment is due. NULL means use tax type\ndefault. Can be customized based on agreement.\n',
  `is_exporter` tinyint(1) NOT NULL COMMENT 'Flag indicating if taxpayer is significant exporter.\nUsed for VAT refund processing (shorter refund period).\n',
  `export_percentage` decimal(5,2) DEFAULT NULL COMMENT 'Percentage of turnover from exports. Used to determine\nexporter status (typically >50%).\n',
  `special_regime_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Special tax regime applicable to this account:\n- DIPLOMATIC: Diplomatic missions (VAT refund, no filing)\n- NGO: Non-profit organizations\n- SMALL_BUSINESS: Simplified regime for small businesses\n- AGRICULTURAL: Special regime for agriculture\n- FREE_ZONE: F',
  `risk_score` int DEFAULT NULL COMMENT 'Current compliance risk score for this account (0-100).\nUpdated by risk assessment engine based on filing compliance,\npayment history, audit findings, etc.\n',
  `risk_category_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Risk category derived from risk score:\n- LOW: Low risk, minimal monitoring\n- MEDIUM: Standard monitoring\n- HIGH: Enhanced monitoring, audit selection\n- CRITICAL: Immediate attention required\n',
  `last_risk_assessment_date` date DEFAULT NULL COMMENT 'Date when risk score was last calculated',
  `notes` text COLLATE utf8mb4_unicode_ci COMMENT 'Free-text notes about this tax account. Used by officers\nto document special circumstances, agreements, etc.\n',
  `valid_from` date NOT NULL COMMENT 'Date from which this record version is valid',
  `valid_to` date DEFAULT NULL COMMENT 'Date until which this record version is valid.\nNULL indicates current version.\n',
  `is_current` tinyint(1) NOT NULL COMMENT 'Flag indicating if this is the current version',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Username of person who created this record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when record was created',
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Username of person who last modified this record',
  `modified_date` timestamp NULL DEFAULT NULL COMMENT 'Timestamp when record was last modified',
  PRIMARY KEY (`tax_account_id`),
  UNIQUE KEY `tax_account_number` (`tax_account_number`),
  KEY `idx_tax_account_party_tax_type` (`party_id`,`tax_type_code`),
  KEY `idx_tax_account_status_office` (`account_status_code`,`assigned_office_code`),
  KEY `idx_tax_account_segment` (`segmentation_code`,`tax_type_code`),
  KEY `idx_tax_account_risk` (`risk_category_code`,`account_status_code`),
  KEY `idx_tax_account_group` (`group_head_account_id`)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Central table implementing the TTT (TIN-Tax Type-Tax Period) principle.\nEach record represents one taxpayer''s registration for one specific\ntax type. This is the foundation for all tax administration operations.\n\nA tax account is created when:\n1. Automat';

-- ------------------------------------------------------------------------------
-- Table: tax_account_object
-- ------------------------------------------------------------------------------

CREATE TABLE `tax_account_object` (
  `tax_account_object_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for association',
  `tax_account_id` bigint NOT NULL COMMENT 'Foreign key to tax account',
  `taxable_object_id` bigint NOT NULL COMMENT 'Foreign key to taxable object',
  `effective_from_date` date NOT NULL COMMENT 'Date from which object is taxed under this account',
  `effective_to_date` date DEFAULT NULL COMMENT 'Date until which object is taxed under this account',
  `is_primary` tinyint(1) NOT NULL COMMENT 'Flag indicating if this is the primary tax account for the\nobject (useful when object subject to multiple taxes).\n',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Username of person who created this record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when record was created',
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Username of person who last modified',
  `modified_date` timestamp NULL DEFAULT NULL COMMENT 'Timestamp when record was last modified',
  PRIMARY KEY (`tax_account_object_id`),
  KEY `uk_tax_acct_obj` (`tax_account_id`,`taxable_object_id`,`effective_from_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Junction table linking tax accounts to taxable objects.\nSome taxes (property tax, vehicle tax) are assessed on specific\nobjects rather than on general business activity.\n\nThis table creates the association: which tax account is responsible\nfor which objec';

-- ------------------------------------------------------------------------------
-- Table: tax_account_period
-- ------------------------------------------------------------------------------

CREATE TABLE `tax_account_period` (
  `tax_account_period_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for tax account period',
  `tax_account_id` bigint NOT NULL COMMENT 'Foreign key to tax account',
  `tax_period_id` bigint NOT NULL COMMENT 'Foreign key to tax period',
  `filing_status_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Current filing status for this period:\n- NOT_FILED: No return received\n- FILED: Return filed on time\n- FILED_LATE: Return filed after deadline\n- AMENDED: Amended return filed\n- NIL_RETURN: Nil/zero return filed\n- EXEMPTED: Exempted from filing\n- WAIVED: F',
  `filing_due_date` date NOT NULL COMMENT 'Date by which return must be filed. Copied from tax_period\nbut can be extended on case-by-case basis.\n',
  `filing_date` date DEFAULT NULL COMMENT 'Actual date when return was filed',
  `is_filing_late` tinyint(1) NOT NULL COMMENT 'Flag indicating if filing was after due date',
  `payment_status_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Current payment status for this period:\n- NOT_PAID: No payment received\n- PAID: Fully paid\n- PARTIALLY_PAID: Partial payment\n- OVERPAID: Payment exceeds liability\n- REFUND_DUE: Refund owed to taxpayer\n',
  `payment_due_date` date NOT NULL COMMENT 'Date by which payment must be made',
  `last_reminder_date` date DEFAULT NULL COMMENT 'Date when last reminder was sent',
  `reminder_count` int NOT NULL COMMENT 'Number of reminders sent for this period',
  `is_bja_applicable` tinyint(1) NOT NULL COMMENT 'Flag indicating if Best Judgment Assessment should be applied\nfor non-filing. Set automatically after final reminder.\n',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Username of person who created this record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when record was created',
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Username of person who last modified',
  `modified_date` timestamp NULL DEFAULT NULL COMMENT 'Timestamp when record was last modified',
  PRIMARY KEY (`tax_account_period_id`),
  KEY `uk_tax_acct_period` (`tax_account_id`,`tax_period_id`),
  KEY `idx_tax_acct_period_filing_status` (`filing_status_code`,`filing_due_date`),
  KEY `idx_tax_acct_period_payment_status` (`payment_status_code`,`payment_due_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Tracks filing and payment obligations for each period of each\ntax account. Created automatically when tax period is opened\nfor applicable taxpayers based on their filing frequency.\n\nThis table implements the Period component of TTT principle:\nTIN (party) ';

-- ------------------------------------------------------------------------------
-- Table: taxable_object
-- ------------------------------------------------------------------------------

CREATE TABLE `taxable_object` (
  `taxable_object_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for taxable object',
  `object_type_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Type of taxable object:\n- REAL_ESTATE: Land and buildings\n- VEHICLE: Motor vehicles\n- EQUIPMENT: Business equipment\n- INVENTORY: Business inventory\n- OTHER: Other taxable assets\n',
  `object_number` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Official identification number for the object.\nFor property: cadastral number\nFor vehicles: VIN or registration number\n',
  `owner_party_id` bigint NOT NULL COMMENT 'Current owner of the object',
  `ownership_start_date` date NOT NULL COMMENT 'Date current owner acquired the object',
  `ownership_end_date` date DEFAULT NULL COMMENT 'Date ownership ended (for historical records)',
  `object_description` text COLLATE utf8mb4_unicode_ci COMMENT 'Detailed description of the object',
  `location_address_id` bigint DEFAULT NULL COMMENT 'Physical location of the object (for property and equipment).\nForeign key to address table.\n',
  `assessed_value` decimal(19,2) DEFAULT NULL COMMENT 'Official assessed value for tax purposes. Used as base\nfor calculating property tax, vehicle tax, etc.\n',
  `assessed_value_currency` char(3) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'ISO 4217 currency code for assessed value',
  `assessment_date` date DEFAULT NULL COMMENT 'Date when value was assessed',
  `next_assessment_date` date DEFAULT NULL COMMENT 'Date when value should be reassessed',
  `market_value` decimal(19,2) DEFAULT NULL COMMENT 'Current market value (may differ from assessed value).\nUsed for comparison and quality assurance.\n',
  `acquisition_date` date DEFAULT NULL COMMENT 'Date object was originally acquired/built/purchased',
  `acquisition_cost` decimal(19,2) DEFAULT NULL COMMENT 'Original cost of acquisition',
  `object_status_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Current status: ACTIVE, INACTIVE, DISPOSED, DESTROYED\n',
  `disposal_date` date DEFAULT NULL COMMENT 'Date object was disposed of or destroyed',
  `disposal_reason` text COLLATE utf8mb4_unicode_ci COMMENT 'Reason for disposal',
  `property_type_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'For REAL_ESTATE objects: type of property\n(RESIDENTIAL, COMMERCIAL, INDUSTRIAL, AGRICULTURAL, LAND)\n',
  `property_area_sqm` decimal(10,2) DEFAULT NULL COMMENT 'Area in square meters (for real estate)',
  `vehicle_make` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Manufacturer of vehicle (for VEHICLE objects)',
  `vehicle_model` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Model of vehicle',
  `vehicle_year` int DEFAULT NULL COMMENT 'Year of manufacture',
  `vehicle_vin` varchar(17) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Vehicle Identification Number',
  `vehicle_registration_number` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Official vehicle registration/license plate number',
  `vehicle_engine_capacity_cc` int DEFAULT NULL COMMENT 'Engine displacement in cubic centimeters',
  `notes` text COLLATE utf8mb4_unicode_ci COMMENT 'Additional notes about the object',
  `valid_from` date NOT NULL COMMENT 'Date from which this record version is valid',
  `valid_to` date DEFAULT NULL COMMENT 'Date until which this record version is valid',
  `is_current` tinyint(1) NOT NULL COMMENT 'Flag indicating if this is the current version',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Username of person who created this record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when record was created',
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Username of person who last modified',
  `modified_date` timestamp NULL DEFAULT NULL COMMENT 'Timestamp when record was last modified',
  PRIMARY KEY (`taxable_object_id`),
  KEY `idx_taxable_obj_owner_type` (`owner_party_id`,`object_type_code`),
  KEY `idx_taxable_obj_status` (`object_status_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Represents physical objects or assets that are subject to taxation.\nExamples include real estate (property tax), motor vehicles (vehicle\ntax), and business equipment.\n\nTaxable objects link taxpayers to specific items being taxed, and\noften trigger automat';

-- ------------------------------------------------------------------------------
-- Table: taxpayer_segment_assignment
-- ------------------------------------------------------------------------------

CREATE TABLE `taxpayer_segment_assignment` (
  `segment_assignment_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for segment assignment',
  `party_id` bigint NOT NULL COMMENT 'Taxpayer being segmented',
  `tax_type_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Specific tax type if segmentation is tax-specific.\nNULL means applies to all tax types.\n',
  `segment_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Segment classification:\nLARGE, MEDIUM, SMALL, HIGH_RISK, VIP, etc.\n',
  `segment_criteria` text COLLATE utf8mb4_unicode_ci COMMENT 'Description of criteria that qualified taxpayer for segment.\nExample: "Annual turnover > $10M for 2 consecutive years"\n',
  `effective_from_date` date NOT NULL COMMENT 'Date from which segmentation is effective',
  `effective_to_date` date DEFAULT NULL COMMENT 'Date until which segmentation is effective',
  `assigned_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Officer who made the segmentation decision',
  `assignment_date` date NOT NULL COMMENT 'Date when segmentation was decided',
  `assignment_reason` text COLLATE utf8mb4_unicode_ci COMMENT 'Explanation for segment assignment',
  `review_date` date DEFAULT NULL COMMENT 'Date when segment should be reviewed next',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Username of person who created this record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when record was created',
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Username of person who last modified',
  `modified_date` timestamp NULL DEFAULT NULL COMMENT 'Timestamp when record was last modified',
  PRIMARY KEY (`segment_assignment_id`),
  KEY `idx_segment_assign_current` (`party_id`,`segment_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Tracks taxpayer segmentation over time. Segmentation determines\nservice level, compliance approach, and resource allocation.\n\nSegments typically include:\n- Large taxpayers (dedicated service, key account management)\n- Medium taxpayers (standard service)\n-';

-- ------------------------------------------------------------------------------
-- Table: vat_certificate
-- ------------------------------------------------------------------------------

CREATE TABLE `vat_certificate` (
  `vat_certificate_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for certificate',
  `vat_registration_id` bigint NOT NULL COMMENT 'Foreign key to VAT registration',
  `certificate_number` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Unique certificate number',
  `issue_date` date NOT NULL COMMENT 'Date certificate was issued',
  `expiry_date` date DEFAULT NULL COMMENT 'Date certificate expires (NULL if permanent)',
  `certificate_status_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Current status: ACTIVE, EXPIRED, REVOKED, RENEWED\n',
  `issued_by_office_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Tax office that issued the certificate',
  `issued_by_officer` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Officer who issued the certificate',
  `revocation_date` date DEFAULT NULL COMMENT 'Date certificate was revoked',
  `revocation_reason` text COLLATE utf8mb4_unicode_ci COMMENT 'Reason for revoking certificate',
  `print_date` date DEFAULT NULL COMMENT 'Date certificate was printed/generated',
  `delivery_date` date DEFAULT NULL COMMENT 'Date certificate was delivered to taxpayer',
  `delivery_method_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'How certificate was delivered: MAIL, IN_PERSON, ELECTRONIC',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Username of person who created this record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when record was created',
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Username of person who last modified',
  `modified_date` timestamp NULL DEFAULT NULL COMMENT 'Timestamp when record was last modified',
  PRIMARY KEY (`vat_certificate_id`),
  UNIQUE KEY `certificate_number` (`certificate_number`),
  KEY `idx_vat_cert_status` (`certificate_status_code`,`expiry_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Tracks VAT registration certificates issued to taxpayers.\nCertificates serve as official proof of VAT registration and\nmay need periodic renewal.\n\nSIGTAS Source: VAT_CERTIF table\n';

-- ------------------------------------------------------------------------------
-- Table: vat_registration
-- ------------------------------------------------------------------------------

CREATE TABLE `vat_registration` (
  `vat_registration_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for VAT registration',
  `tax_account_id` bigint NOT NULL COMMENT 'Foreign key to tax_account (must be VAT tax type)',
  `vat_number` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Official VAT identification number issued to taxpayer.\nFormat varies by country. Often includes country prefix.\n',
  `registration_type_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Type of VAT registration:\n- MANDATORY: Above threshold, required by law\n- VOLUNTARY: Below threshold, taxpayer opted in\n- GROUP: Part of VAT group\n- DIPLOMATIC: Diplomatic mission (no filing, refund only)\n- NGO: Non-governmental organization\n',
  `threshold_amount` decimal(19,2) DEFAULT NULL COMMENT 'VAT registration threshold applicable at time of registration.\nPreserved for historical analysis even if threshold changes.\n',
  `turnover_at_registration` decimal(19,2) DEFAULT NULL COMMENT 'Taxpayer''s annual turnover at time of VAT registration.\nUsed to validate threshold compliance.\n',
  `is_group_vat` tinyint(1) NOT NULL COMMENT 'Flag indicating if this is part of VAT group registration\nwhere one entity files for multiple related entities.\n',
  `vat_group_id` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Identifier for VAT group if is_group_vat=true.\nAll entities in same group share this ID.\n',
  `is_group_head` tinyint(1) NOT NULL COMMENT 'Flag indicating if this entity is the head of VAT group\nresponsible for consolidated filing.\n',
  `certificate_number` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'VAT registration certificate number. Physical or electronic\ncertificate issued to taxpayer as proof of registration.\n',
  `certificate_issue_date` date DEFAULT NULL COMMENT 'Date when VAT certificate was issued',
  `certificate_expiry_date` date DEFAULT NULL COMMENT 'Expiry date of certificate if renewal required.\nNULL for permanent certificates.\n',
  `is_exporter` tinyint(1) NOT NULL COMMENT 'Flag indicating if taxpayer is significant exporter eligible\nfor expedited VAT refund processing.\n',
  `export_percentage` decimal(5,2) DEFAULT NULL COMMENT 'Percentage of sales that are exports. Must be >50% for\nexporter status in most jurisdictions.\n',
  `refund_processing_period_days` int NOT NULL COMMENT 'Number of days within which VAT refund must be processed.\nShorter for exporters (e.g., 30 days vs 90 days standard).\n',
  `special_regime_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Special VAT regime if applicable:\n- DIPLOMATIC: Diplomatic missions\n- NGO: Non-profit organizations\n- AGRICULTURE: Agricultural flat rate\n- SMALL_BUSINESS: Simplified scheme\n- MARGIN: Margin scheme (used goods)\n',
  `intra_community_flag` tinyint(1) NOT NULL COMMENT 'Flag indicating if taxpayer engages in intra-community\nsupplies (relevant for EU countries).\n',
  `vies_registration_date` date DEFAULT NULL COMMENT 'Date when VAT number was registered in VIES (VAT Information\nExchange System) for EU intra-community transactions.\n',
  `previous_vat_number` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Previous VAT number if taxpayer was re-registered after\nderegistration. Used for compliance history tracking.\n',
  `previous_deregistration_date` date DEFAULT NULL COMMENT 'Date of previous VAT deregistration if applicable.\nUsed to detect repeat registrations which may indicate risk.\n',
  `application_date` date DEFAULT NULL COMMENT 'Date when taxpayer applied for VAT registration.\nFor voluntary registrations.\n',
  `approval_date` date DEFAULT NULL COMMENT 'Date when VAT registration was approved',
  `approved_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Officer who approved VAT registration',
  `rejection_date` date DEFAULT NULL COMMENT 'Date when application was rejected (if applicable)',
  `rejection_reason` text COLLATE utf8mb4_unicode_ci COMMENT 'Reason for rejecting VAT registration application',
  `valid_from` date NOT NULL COMMENT 'Date from which this record version is valid',
  `valid_to` date DEFAULT NULL COMMENT 'Date until which this record version is valid',
  `is_current` tinyint(1) NOT NULL COMMENT 'Flag indicating if this is the current version',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Username of person who created this record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when record was created',
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Username of person who last modified',
  `modified_date` timestamp NULL DEFAULT NULL COMMENT 'Timestamp when record was last modified',
  PRIMARY KEY (`vat_registration_id`),
  UNIQUE KEY `tax_account_id` (`tax_account_id`),
  UNIQUE KEY `vat_number` (`vat_number`),
  KEY `idx_vat_reg_group` (`vat_group_id`),
  KEY `idx_vat_reg_exporter` (`is_exporter`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='VAT-specific registration data extending the tax_account table.\nVAT is typically the most complex tax type with special rules\naround thresholds, registration methods, group filing, and\ncertificate issuance.\n\nThis table captures VAT-specific attributes tha';


-- ==============================================================================
-- Schema: tax_framework
-- ==============================================================================

CREATE DATABASE IF NOT EXISTS `tax_framework`;
USE `tax_framework`;

-- ------------------------------------------------------------------------------
-- Table: calculation_rule
-- ------------------------------------------------------------------------------

CREATE TABLE `calculation_rule` (
  `calculation_rule_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for the calculation rule',
  `tax_type_id` bigint NOT NULL COMMENT 'Tax type this rule applies to',
  `rule_code` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Unique code for the rule.\nExample: "STANDARD_DEDUCTION", "DEPENDENT_CREDIT", "AMT_CALC"\n',
  `rule_name` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Descriptive name.\nExample: "Standard Deduction for Single Filers"\n',
  `description` text COLLATE utf8mb4_unicode_ci COMMENT 'Detailed description of what the rule does and when it applies',
  `rule_category` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Category: DEDUCTION, CREDIT, EXEMPTION, THRESHOLD, RELIEF,\nADJUSTMENT, CALCULATION\n',
  `calculation_method` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'How to calculate: FORMULA, LOOKUP_TABLE, PERCENTAGE, FIXED_AMOUNT,\nCONDITIONAL, EXTERNAL_API\n',
  `formula` text COLLATE utf8mb4_unicode_ci COMMENT 'Calculation formula using standard notation.\nExample: "IF(FILING_STATUS=''SINGLE'', 12000, 24000)"\nCan reference form lines, other rules, taxpayer attributes.\n',
  `lookup_table_json` json DEFAULT NULL COMMENT 'Lookup table for table-based calculations.\nExample: [{"from": 0, "to": 50000, "value": 5000}, ...]\n',
  `applies_when` text COLLATE utf8mb4_unicode_ci COMMENT 'Condition for when rule applies.\nExample: "TAXPAYER_AGE >= 65 AND FILING_STATUS = ''SINGLE''"\n',
  `sequence_order` int NOT NULL COMMENT 'Order in which rules are applied (important when rules interact).\nLower numbers execute first.\n',
  `min_value` decimal(19,2) DEFAULT NULL COMMENT 'Floor value (result cannot be less than this)',
  `max_value` decimal(19,2) DEFAULT NULL COMMENT 'Cap value (result cannot exceed this)',
  `rounding_rule` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'How to round result: NONE, NEAREST_INTEGER, UP, DOWN,\nNEAREST_10, NEAREST_100\n',
  `legal_reference` text COLLATE utf8mb4_unicode_ci COMMENT 'Legal basis for this rule.\nExample: "Tax Code Section 63(c)(2)"\n',
  `effective_from` date NOT NULL COMMENT 'Date from which this rule becomes effective',
  `effective_to` date DEFAULT NULL COMMENT 'Date until which this rule is valid (NULL if current)',
  `is_current` tinyint(1) NOT NULL COMMENT 'True if this is the currently active version',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_date` timestamp NOT NULL,
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `modified_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`calculation_rule_id`),
  KEY `idx_calc_rule_code` (`tax_type_id`,`rule_code`),
  KEY `idx_calc_rule_category` (`rule_category`),
  KEY `idx_calc_rule_effective` (`effective_from`,`effective_to`),
  KEY `idx_calc_rule_sequence` (`tax_type_id`,`sequence_order`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Defines configurable business rules for tax calculations beyond\nsimple rate application. These rules encode complex tax logic that\nvaries by jurisdiction and changes over time.\n\nRule types:\n- Deduction rules (standard deductions, itemized deductions)\n- Cr';

-- ------------------------------------------------------------------------------
-- Table: form_line
-- ------------------------------------------------------------------------------

CREATE TABLE `form_line` (
  `form_line_id` bigint NOT NULL AUTO_INCREMENT,
  `tax_form_id` bigint NOT NULL,
  `form_version_id` bigint DEFAULT NULL,
  `line_number` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL,
  `line_sequence` int NOT NULL,
  `line_label` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL,
  `line_description` text COLLATE utf8mb4_unicode_ci,
  `line_type` enum('INPUT','CALCULATED','REFERENCE','HEADER') COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'INPUT',
  `data_type` enum('NUMERIC','TEXT','DATE','BOOLEAN') COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'NUMERIC',
  `is_mandatory` tinyint(1) NOT NULL DEFAULT '0',
  `min_value` decimal(19,2) DEFAULT NULL,
  `max_value` decimal(19,2) DEFAULT NULL,
  `calculation_formula` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `validation_rules` text COLLATE utf8mb4_unicode_ci,
  `parent_line_id` bigint DEFAULT NULL,
  `sort_order` int DEFAULT NULL,
  `is_active` tinyint(1) NOT NULL DEFAULT '1',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `modified_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`form_line_id`),
  UNIQUE KEY `uk_form_line` (`tax_form_id`,`form_version_id`,`line_number`),
  KEY `idx_form_line_form_id` (`tax_form_id`),
  KEY `idx_form_line_version_id` (`form_version_id`),
  KEY `idx_form_line_parent` (`parent_line_id`),
  KEY `idx_form_line_number` (`line_number`),
  CONSTRAINT `fk_form_line_form_version_id` FOREIGN KEY (`form_version_id`) REFERENCES `form_version` (`form_version_id`),
  CONSTRAINT `fk_form_line_parent_line_id` FOREIGN KEY (`parent_line_id`) REFERENCES `form_line` (`form_line_id`),
  CONSTRAINT `fk_form_line_tax_form_id` FOREIGN KEY (`tax_form_id`) REFERENCES `tax_form` (`tax_form_id`)
) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ------------------------------------------------------------------------------
-- Table: form_version
-- ------------------------------------------------------------------------------

CREATE TABLE `form_version` (
  `form_version_id` bigint NOT NULL AUTO_INCREMENT,
  `tax_form_id` bigint NOT NULL,
  `version_number` int NOT NULL,
  `version_name` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL,
  `effective_from_date` date NOT NULL,
  `effective_to_date` date DEFAULT NULL,
  `is_current_version` tinyint(1) NOT NULL DEFAULT '1',
  `version_notes` text COLLATE utf8mb4_unicode_ci,
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `modified_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`form_version_id`),
  UNIQUE KEY `uk_form_version` (`tax_form_id`,`version_number`),
  KEY `idx_form_version_form_id` (`tax_form_id`),
  KEY `idx_form_version_effective` (`effective_from_date`,`effective_to_date`),
  CONSTRAINT `fk_form_version_tax_form_id` FOREIGN KEY (`tax_form_id`) REFERENCES `tax_form` (`tax_form_id`)
) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ------------------------------------------------------------------------------
-- Table: paye_tax_bracket
-- ------------------------------------------------------------------------------

CREATE TABLE `paye_tax_bracket` (
  `paye_tax_bracket_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for the tax bracket',
  `paye_tax_scale_id` bigint NOT NULL COMMENT 'Tax scale this bracket belongs to',
  `bracket_number` int NOT NULL COMMENT 'Sequential bracket number (1, 2, 3, etc.)',
  `bracket_name` varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Descriptive name.\nExample: "Bracket 1: 0 - 50,000"\n',
  `threshold_from` decimal(19,2) NOT NULL COMMENT 'Lower bound of income range for this bracket (inclusive).\nFirst bracket typically starts at 0.\n',
  `threshold_to` decimal(19,2) DEFAULT NULL COMMENT 'Upper bound of income range for this bracket (inclusive).\nNULL for the top bracket (unlimited).\n',
  `tax_rate` decimal(7,4) NOT NULL COMMENT 'Tax rate as percentage for income in this bracket.\nExample: 12.0000 for 12%\n',
  `fixed_amount` decimal(19,2) DEFAULT NULL COMMENT 'Fixed tax amount for all income up to the start of this bracket.\nOptimization for cumulative calculation.\nExample: Bracket 3 fixed amount = tax on brackets 1 & 2\n',
  `description` text COLLATE utf8mb4_unicode_ci COMMENT 'Additional notes about this bracket',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_date` timestamp NOT NULL,
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `modified_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`paye_tax_bracket_id`),
  KEY `idx_paye_bracket_scale` (`paye_tax_scale_id`,`bracket_number`),
  KEY `idx_paye_bracket_thresholds` (`paye_tax_scale_id`,`threshold_from`,`threshold_to`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Individual tax bracket within a PAYE tax scale. Defines the\nprogressive rate structure for withholding tax calculations.\n\nTypical progressive structure:\n- Bracket 1: 0 - 50,000 at 0% (personal allowance)\n- Bracket 2: 50,001 - 150,000 at 12%\n- Bracket 3: 1';

-- ------------------------------------------------------------------------------
-- Table: paye_tax_scale
-- ------------------------------------------------------------------------------

CREATE TABLE `paye_tax_scale` (
  `paye_tax_scale_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for the PAYE tax scale',
  `tax_type_id` bigint NOT NULL COMMENT 'Tax type (typically PIT or withholding tax type)',
  `scale_code` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Unique code for the scale.\nExample: "PAYE-2024", "WHT-SALARY-2024"\n',
  `scale_name` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Descriptive name.\nExample: "PAYE Tax Scale 2024"\n',
  `description` text COLLATE utf8mb4_unicode_ci COMMENT 'Detailed description of the scale and how it applies',
  `calculation_period` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Period basis for calculation: MONTHLY, WEEKLY, ANNUAL\nMost common is MONTHLY with annualization.\n',
  `effective_from` date NOT NULL COMMENT 'Date from which this scale becomes effective',
  `effective_to` date DEFAULT NULL COMMENT 'Date until which this scale is valid (NULL if current)',
  `is_current` tinyint(1) NOT NULL COMMENT 'True if this is the currently active scale',
  `personal_allowance_annual` decimal(19,2) DEFAULT NULL COMMENT 'Annual personal allowance (tax-free amount) in local currency.\nExample: 12000 (first 12,000 is tax-free)\n',
  `dependent_allowance` decimal(19,2) DEFAULT NULL COMMENT 'Additional allowance per dependent in local currency.\nExample: 2000 per dependent child\n',
  `max_dependents` int DEFAULT NULL COMMENT 'Maximum number of dependents allowed for allowance calculation',
  `standard_deduction` decimal(19,2) DEFAULT NULL COMMENT 'Standard deduction amount applied before tax calculation',
  `uses_cumulative_basis` tinyint(1) NOT NULL COMMENT 'True if tax calculated on cumulative year-to-date basis.\nFalse for monthly basis only.\n',
  `legal_reference` text COLLATE utf8mb4_unicode_ci COMMENT 'Legal basis for this scale.\nExample: "Tax Code Schedule A, as amended 2024"\n',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_date` timestamp NOT NULL,
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `modified_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`paye_tax_scale_id`),
  UNIQUE KEY `scale_code` (`scale_code`),
  KEY `idx_paye_scale_code` (`scale_code`),
  KEY `idx_paye_scale_tax_type` (`tax_type_id`),
  KEY `idx_paye_scale_effective` (`effective_from`,`effective_to`),
  KEY `idx_paye_scale_current` (`tax_type_id`,`is_current`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Defines Pay-As-You-Earn (PAYE) tax scales for withholding taxes\non employment income. PAYE systems require employers to withhold\ntax from employee salaries based on progressive scales.\n\nPAYE scales typically include:\n- Monthly income thresholds\n- Tax rate';

-- ------------------------------------------------------------------------------
-- Table: tax_base_definition
-- ------------------------------------------------------------------------------

CREATE TABLE `tax_base_definition` (
  `tax_base_definition_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for the tax base definition',
  `tax_type_id` bigint NOT NULL COMMENT 'Tax type this base definition applies to',
  `base_code` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Code identifying this base definition.\nExample: "TAXABLE_INCOME", "VAT_BASE", "ASSESSED_VALUE"\n',
  `base_name` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Descriptive name.\nExample: "Taxable Income for Residents"\n',
  `description` text COLLATE utf8mb4_unicode_ci COMMENT 'Detailed description of how base is determined',
  `base_category` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Category: INCOME, VALUE, QUANTITY, AREA, ASSESSED_VALUE,\nTRANSACTION_VALUE\n',
  `calculation_method` text COLLATE utf8mb4_unicode_ci COMMENT 'How to calculate the base (formula or rules).\nExample: "GROSS_INCOME - DEDUCTIONS - EXEMPTIONS"\n',
  `included_items_json` json DEFAULT NULL COMMENT 'JSON array of items included in base.\nExample: ["salary", "bonuses", "benefits", "other_income"]\n',
  `excluded_items_json` json DEFAULT NULL COMMENT 'JSON array of items excluded from base.\nExample: ["exempt_income", "foreign_dividends"]\n',
  `valuation_method` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Method for valuing base.\nExample: FAIR_MARKET_VALUE, HISTORICAL_COST, ASSESSED_VALUE,\nTRANSACTION_VALUE, CIF_VALUE\n',
  `rounding_rule` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'How to round the calculated base.\nExample: NEAREST_INTEGER, DOWN, UP, NEAREST_10\n',
  `min_base` decimal(19,2) DEFAULT NULL COMMENT 'Minimum tax base (floor value)',
  `max_base` decimal(19,2) DEFAULT NULL COMMENT 'Maximum tax base (cap value)',
  `applies_to_resident` tinyint(1) NOT NULL COMMENT 'True if this definition applies to resident taxpayers',
  `applies_to_non_resident` tinyint(1) NOT NULL COMMENT 'True if this definition applies to non-resident taxpayers',
  `applies_to_individuals` tinyint(1) NOT NULL COMMENT 'True if this definition applies to individuals',
  `applies_to_enterprises` tinyint(1) NOT NULL COMMENT 'True if this definition applies to enterprises',
  `is_default` tinyint(1) NOT NULL COMMENT 'True if this is the default definition for the tax type',
  `effective_from` date NOT NULL COMMENT 'Date from which this definition becomes effective',
  `effective_to` date DEFAULT NULL COMMENT 'Date until which this definition is valid (NULL if current)',
  `is_current` tinyint(1) NOT NULL COMMENT 'True if this is the currently active version',
  `legal_reference` text COLLATE utf8mb4_unicode_ci COMMENT 'Legal basis for this base definition',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_date` timestamp NOT NULL,
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `modified_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`tax_base_definition_id`),
  KEY `idx_tax_base_code` (`tax_type_id`,`base_code`),
  KEY `idx_tax_base_category` (`base_category`),
  KEY `idx_tax_base_effective` (`effective_from`,`effective_to`),
  KEY `idx_tax_base_default` (`tax_type_id`,`is_default`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Defines how the tax base (taxable amount) is determined for a\ntax type. The tax base is the value to which tax rates are applied.\n\nTax base varies by tax type:\n- Income Tax: Taxable income (gross income - deductions)\n- VAT: Value of taxable supplies\n- Pro';

-- ------------------------------------------------------------------------------
-- Table: tax_form
-- ------------------------------------------------------------------------------

CREATE TABLE `tax_form` (
  `tax_form_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for the tax form',
  `tax_type_id` bigint NOT NULL COMMENT 'Tax type this form is used for',
  `form_code` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Unique code identifying the form (e.g., ''CIT-1'', ''VAT-12'', ''PIT-ANN'').\nUsed in APIs, reports, and external integrations.\n',
  `form_name` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Full name of the form in the default language',
  `form_name_i18n` json DEFAULT NULL COMMENT 'Internationalized names in multiple languages.\nStructure: {"en": "Corporate Income Tax Return", "ru": "Ð”ÐµÐºÐ»Ð°Ñ€Ð°Ñ†Ð¸Ñ"}\n',
  `description` text COLLATE utf8mb4_unicode_ci COMMENT 'Detailed description of the form''s purpose and when it should be used',
  `form_category` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'High-level category: RETURN, DECLARATION, SCHEDULE, ATTACHMENT,\nNOTICE, CERTIFICATE, APPLICATION\n',
  `filing_frequency` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'How often this form is filed: MONTHLY, QUARTERLY, ANNUAL,\nTRANSACTION, EVENT_DRIVEN\n',
  `is_mandatory` tinyint(1) NOT NULL COMMENT 'True if all registered taxpayers must file this form.\nFalse if optional or conditional.\n',
  `is_electronic_filing_enabled` tinyint(1) NOT NULL COMMENT 'True if form can be filed electronically through e-tax portal',
  `is_paper_filing_enabled` tinyint(1) NOT NULL COMMENT 'True if form can be filed on paper (legacy support)',
  `applies_to_individuals` tinyint(1) NOT NULL COMMENT 'True if individuals must file this form',
  `applies_to_enterprises` tinyint(1) NOT NULL COMMENT 'True if enterprises must file this form',
  `requires_attachment` tinyint(1) NOT NULL COMMENT 'True if this form requires supporting attachments\n(financial statements, schedules, etc.)\n',
  `allows_amendments` tinyint(1) NOT NULL COMMENT 'True if taxpayer can file amended versions of this form',
  `max_amendments_allowed` int DEFAULT NULL COMMENT 'Maximum number of amendments allowed. NULL means unlimited.\n',
  `sort_order` int DEFAULT NULL COMMENT 'Display order for UI lists and reports',
  `is_active` tinyint(1) NOT NULL COMMENT 'False if form is obsolete (no new filings accepted)',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_date` timestamp NOT NULL,
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `modified_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`tax_form_id`),
  KEY `idx_tax_form_code` (`form_code`),
  KEY `idx_tax_form_tax_type` (`tax_type_id`),
  KEY `idx_tax_form_category` (`form_category`),
  KEY `idx_tax_form_active` (`is_active`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Defines the structure and metadata for all tax forms used in the\ntax administration. Forms are the primary means by which taxpayers\nreport their tax obligations. Each form is specific to a tax type\nand may have multiple versions over time as tax law chang';

-- ------------------------------------------------------------------------------
-- Table: tax_period
-- ------------------------------------------------------------------------------

CREATE TABLE `tax_period` (
  `tax_period_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for the tax period',
  `tax_type_id` bigint NOT NULL COMMENT 'Tax type this period applies to',
  `period_year` int NOT NULL COMMENT 'Calendar year or fiscal year this period belongs to',
  `period_month` int DEFAULT NULL COMMENT 'Month number (1-12) for monthly periods, or quarter start month\nfor quarterly periods. NULL for annual periods.\n',
  `period_number` int NOT NULL COMMENT 'Sequential period number within the year.\nFor monthly: 1-12, for quarterly: 1-4, for annual: 1\n',
  `period_name` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Human-readable period name.\nExamples: "January 2024", "Q1 2024", "FY 2024"\n',
  `period_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Short code for the period, used in APIs and reports.\nExamples: "2024-01", "2024-Q1", "FY2024"\n',
  `period_start_date` date NOT NULL COMMENT 'First day of the tax period',
  `period_end_date` date NOT NULL COMMENT 'Last day of the tax period',
  `filing_due_date` date NOT NULL COMMENT 'Deadline for filing returns for this period.\nCan be adjusted for weekends/holidays by the system.\n',
  `payment_due_date` date NOT NULL COMMENT 'Deadline for payment of tax liability for this period.\nMay be same as filing_due_date or different.\n',
  `period_status` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Status of the period: FUTURE, OPEN, CLOSED, LOCKED\n- FUTURE: Not yet open for filing\n- OPEN: Accepting filings\n- CLOSED: Past due date, late filings allowed\n- LOCKED: No more filings accepted (used for archival)\n',
  `is_year_end_period` tinyint(1) NOT NULL COMMENT 'True if this is the final period of the fiscal/calendar year.\nUsed for annual reconciliations and year-end adjustments.\n',
  `grace_period_days` int DEFAULT NULL COMMENT 'Number of days after due date before penalties apply.\nNull means use system default.\n',
  `extension_deadline` date DEFAULT NULL COMMENT 'Extended deadline if general extension was granted\n(e.g., due to emergency, system outage)\n',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'User who created this record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when record was created',
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'User who last modified this record',
  `modified_date` timestamp NULL DEFAULT NULL COMMENT 'Timestamp when record was last modified',
  PRIMARY KEY (`tax_period_id`),
  KEY `idx_tax_period_composite` (`tax_type_id`,`period_year`,`period_number`),
  KEY `idx_tax_period_dates` (`period_start_date`,`period_end_date`),
  KEY `idx_tax_period_due_date` (`filing_due_date`),
  KEY `idx_tax_period_status` (`period_status`)
) ENGINE=InnoDB AUTO_INCREMENT=91 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Defines the periods for which taxes must be filed and paid.\nTax periods vary by tax type:\n\n- Monthly periods (VAT, withholding taxes)\n- Quarterly periods (estimated tax payments)\n- Annual periods (income taxes)\n- Custom periods (excise, property tax)\n\nThe';

-- ------------------------------------------------------------------------------
-- Table: tax_period_date_base
-- ------------------------------------------------------------------------------

CREATE TABLE `tax_period_date_base` (
  `tax_period_date_base_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for the period date base',
  `tax_type_id` bigint NOT NULL COMMENT 'Tax type this date base applies to',
  `date_base_code` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Code identifying this date base.\nExample: "VAT_MONTHLY", "CIT_QUARTERLY"\n',
  `date_base_name` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Descriptive name',
  `description` text COLLATE utf8mb4_unicode_ci COMMENT 'Detailed description of the date calculation rules',
  `period_frequency` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Frequency of periods: MONTHLY, QUARTERLY, SEMI_ANNUAL, ANNUAL,\nCUSTOM\n',
  `period_start_rule` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Rule for determining period start date.\nExample: FIRST_DAY_OF_MONTH, FIRST_DAY_OF_QUARTER, FISCAL_YEAR_START\n',
  `period_end_rule` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Rule for determining period end date.\nExample: LAST_DAY_OF_MONTH, LAST_DAY_OF_QUARTER, FISCAL_YEAR_END\n',
  `fiscal_year_start_month` int DEFAULT NULL COMMENT 'Month fiscal year starts (1-12) if using fiscal year.\nNULL for calendar year. Example: 7 for July 1 fiscal start.\n',
  `filing_due_date_offset_days` int NOT NULL COMMENT 'Number of days after period end that filing is due.\nExample: 20 (filing due 20 days after period ends)\n',
  `payment_due_date_offset_days` int NOT NULL COMMENT 'Number of days after period end that payment is due.\nCan be same as or different from filing due date.\n',
  `grace_period_days` int DEFAULT NULL COMMENT 'Grace period in days after due date before penalties apply.\nExample: 5 (5 day grace period)\n',
  `adjust_for_weekends` tinyint(1) NOT NULL COMMENT 'True if due dates falling on weekends should be moved to\nnext business day\n',
  `adjust_for_holidays` tinyint(1) NOT NULL COMMENT 'True if due dates falling on public holidays should be moved\nto next business day\n',
  `weekend_adjustment_rule` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'How to adjust weekend dates: NEXT_BUSINESS_DAY, PRIOR_BUSINESS_DAY,\nNEXT_MONDAY\n',
  `auto_create_periods` tinyint(1) NOT NULL COMMENT 'True if system should automatically create periods as time passes.\nFalse if periods are manually created.\n',
  `periods_to_create_ahead` int DEFAULT NULL COMMENT 'Number of periods to create in advance.\nExample: 3 (create next 3 periods ahead of time)\n',
  `effective_from` date NOT NULL COMMENT 'Date from which this date base becomes effective',
  `effective_to` date DEFAULT NULL COMMENT 'Date until which this date base is valid (NULL if current)',
  `is_current` tinyint(1) NOT NULL COMMENT 'True if this is the currently active date base',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_date` timestamp NOT NULL,
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `modified_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`tax_period_date_base_id`),
  KEY `idx_period_date_base_code` (`tax_type_id`,`date_base_code`),
  KEY `idx_period_date_base_effective` (`effective_from`,`effective_to`),
  KEY `idx_period_date_base_current` (`tax_type_id`,`is_current`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Defines rules for automatically calculating tax period dates\nbased on parameters. Used by the system to generate period\nrecords automatically when new periods begin.\n\nDate base rules specify:\n- Period frequency (monthly, quarterly, annual)\n- Start date ca';

-- ------------------------------------------------------------------------------
-- Table: tax_rate
-- ------------------------------------------------------------------------------

CREATE TABLE `tax_rate` (
  `tax_rate_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for the tax rate',
  `tax_rate_schedule_id` bigint NOT NULL COMMENT 'Rate schedule this rate belongs to',
  `rate_code` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Code identifying this rate.\nExample: "STD" (standard), "RED" (reduced), "ZERO", "BRACKET_1"\n',
  `rate_name` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Descriptive name.\nExample: "Standard Rate", "Bracket 1: 0-50,000"\n',
  `description` text COLLATE utf8mb4_unicode_ci COMMENT 'Detailed description of when/how this rate applies',
  `rate_type` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Type of rate: PERCENTAGE, SPECIFIC_AMOUNT, COMPOUND\n',
  `rate_percentage` decimal(7,4) DEFAULT NULL COMMENT 'Tax rate as percentage (e.g., 15.5 for 15.5%).\nNULL for specific duty rates.\n',
  `specific_amount` decimal(19,2) DEFAULT NULL COMMENT 'Specific duty amount (e.g., 5.50 per unit).\nNULL for percentage rates.\n',
  `amount_per_unit` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Unit for specific duties.\nExample: "per liter", "per kilogram", "per unit"\n',
  `threshold_from` decimal(19,2) DEFAULT NULL COMMENT 'Lower bound of bracket (for progressive rates).\nExample: 50000 for "50,001 - 200,000" bracket.\nNULL for flat rates or first bracket.\n',
  `threshold_to` decimal(19,2) DEFAULT NULL COMMENT 'Upper bound of bracket (for progressive rates).\nNULL for unlimited top bracket or flat rates.\n',
  `applies_to_category` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Category this rate applies to (for differentiated rates).\nExample: "FOOD", "BOOKS", "EXPORTS", "LUXURY_GOODS"\nNULL for universal rates.\n',
  `sequence_order` int NOT NULL COMMENT 'Order for displaying rates (e.g., bracket 1, 2, 3)',
  `is_default` tinyint(1) NOT NULL COMMENT 'True if this is the default rate when category not specified.\nOnly one rate per schedule can be default.\n',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_date` timestamp NOT NULL,
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `modified_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`tax_rate_id`),
  KEY `idx_tax_rate_schedule` (`tax_rate_schedule_id`,`sequence_order`),
  KEY `idx_tax_rate_code` (`tax_rate_schedule_id`,`rate_code`),
  KEY `idx_tax_rate_thresholds` (`threshold_from`,`threshold_to`),
  KEY `idx_tax_rate_category` (`applies_to_category`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Specific tax rate within a rate schedule. For progressive/tiered\nschedules, multiple rates exist at different threshold levels.\nFor differentiated rates (like VAT), multiple rates exist for\ndifferent categories.\n\nRate types:\n- Percentage rates (most commo';

-- ------------------------------------------------------------------------------
-- Table: tax_rate_schedule
-- ------------------------------------------------------------------------------

CREATE TABLE `tax_rate_schedule` (
  `tax_rate_schedule_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for the rate schedule',
  `tax_type_id` bigint NOT NULL COMMENT 'Tax type this schedule applies to',
  `schedule_code` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Unique code for the schedule.\nExample: "CIT-2024", "PIT-PROG-2023", "VAT-STD-2024"\n',
  `schedule_name` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Descriptive name of the schedule.\nExample: "Corporate Income Tax Rates 2024"\n',
  `description` text COLLATE utf8mb4_unicode_ci COMMENT 'Detailed description of the rate schedule and how it applies',
  `schedule_type` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Type of rate structure: FLAT, PROGRESSIVE, TIERED,\nDIFFERENTIATED, SPECIFIC_DUTY\n',
  `effective_from` date NOT NULL COMMENT 'Date from which this schedule becomes effective',
  `effective_to` date DEFAULT NULL COMMENT 'Date until which this schedule is valid (NULL if current)',
  `is_current` tinyint(1) NOT NULL COMMENT 'True if this is the currently active schedule',
  `legal_reference` text COLLATE utf8mb4_unicode_ci COMMENT 'Legal basis for these rates (law, regulation).\nExample: "Tax Code Article 123, as amended by Law 45/2024"\n',
  `applies_to_resident` tinyint(1) NOT NULL COMMENT 'True if schedule applies to resident taxpayers',
  `applies_to_non_resident` tinyint(1) NOT NULL COMMENT 'True if schedule applies to non-resident taxpayers',
  `currency_code` char(3) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'ISO currency code if rates are amount-based (specific duties).\nNULL for percentage-based rates.\n',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_date` timestamp NOT NULL,
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `modified_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`tax_rate_schedule_id`),
  UNIQUE KEY `schedule_code` (`schedule_code`),
  KEY `idx_rate_schedule_code` (`schedule_code`),
  KEY `idx_rate_schedule_tax_type` (`tax_type_id`),
  KEY `idx_rate_schedule_effective` (`effective_from`,`effective_to`),
  KEY `idx_rate_schedule_current` (`tax_type_id`,`is_current`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Defines a collection of tax rates that apply for a specific tax\ntype during a defined period. Rate schedules can be:\n\n- Flat rates (single percentage applies to all)\n- Progressive/graduated rates (different rates for income brackets)\n- Tiered rates (first';

-- ------------------------------------------------------------------------------
-- Table: tax_type
-- ------------------------------------------------------------------------------

CREATE TABLE `tax_type` (
  `tax_type_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for the tax type',
  `tax_type_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Short code identifying the tax type (e.g., ''CIT'', ''VAT'', ''PIT'').\nImmutable once created, used in external integrations.\n',
  `tax_type_name` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Full name of the tax type in the default language',
  `tax_type_name_i18n` json DEFAULT NULL COMMENT 'Internationalized names in multiple languages.\nStructure: {"en": "Corporate Income Tax", "ru": "ÐÐ°Ð»Ð¾Ð³ Ð½Ð° Ð¿Ñ€Ð¸Ð±Ñ‹Ð»ÑŒ"}\n',
  `description` text COLLATE utf8mb4_unicode_ci COMMENT 'Detailed description of the tax type, its purpose and scope',
  `tax_category` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'High-level tax category for grouping and reporting.\nValues: INCOME, CONSUMPTION, PROPERTY, WEALTH, TRADE, OTHER\n',
  `legal_basis` text COLLATE utf8mb4_unicode_ci COMMENT 'Legal reference (law, code, regulation) establishing this tax.\nExample: "Tax Code Chapter 25, Articles 246-333"\n',
  `administering_authority` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'The authority responsible for this tax (usually the tax authority,\nbut could be customs, local government, etc.)\n',
  `is_federal` tinyint(1) NOT NULL COMMENT 'True if this is a federal/national tax, false if local/regional',
  `is_withholding_tax` tinyint(1) NOT NULL COMMENT 'True if this tax is collected through withholding at source\n(e.g., PAYE, dividend withholding)\n',
  `requires_registration` tinyint(1) NOT NULL COMMENT 'True if taxpayers must explicitly register for this tax type.\nFalse if registration is automatic (e.g., based on business registry)\n',
  `requires_periodic_filing` tinyint(1) NOT NULL COMMENT 'True if this tax requires regular period filings.\nFalse if transaction-based or annual only.\n',
  `default_filing_frequency` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Default filing frequency: MONTHLY, QUARTERLY, ANNUAL, TRANSACTION\nCan be overridden per taxpayer based on thresholds.\n',
  `is_self_assessment` tinyint(1) NOT NULL COMMENT 'True if taxpayers self-assess and pay.\nFalse if tax authority assesses (e.g., property tax).\n',
  `supports_installments` tinyint(1) NOT NULL COMMENT 'True if payment plans are allowed for this tax type',
  `applies_to_individuals` tinyint(1) NOT NULL COMMENT 'True if individuals can be liable for this tax',
  `applies_to_enterprises` tinyint(1) NOT NULL COMMENT 'True if enterprises can be liable for this tax',
  `penalty_rate_default` decimal(5,2) DEFAULT NULL COMMENT 'Default penalty rate as percentage for late filing/payment.\nCan be overridden in penalty configuration.\n',
  `interest_rate_default` decimal(5,2) DEFAULT NULL COMMENT 'Default interest rate as annual percentage for unpaid amounts.\nCan be overridden in interest configuration.\n',
  `valid_from` date NOT NULL COMMENT 'Date from which this tax type version is effective',
  `valid_to` date DEFAULT NULL COMMENT 'Date until which this version is valid (null if currently valid)\n',
  `is_current` tinyint(1) NOT NULL COMMENT 'True if this is the currently active version',
  `is_active` tinyint(1) NOT NULL COMMENT 'False if tax type is discontinued (no new registrations,\nexisting accounts in run-off)\n',
  `predecessor_tax_type_id` bigint DEFAULT NULL COMMENT 'Reference to previous tax type if this replaces another\n(e.g., new tax replacing old tax due to law change)\n',
  `sort_order` int DEFAULT NULL COMMENT 'Display order for UI lists and reports',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'User who created this record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when record was created',
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'User who last modified this record',
  `modified_date` timestamp NULL DEFAULT NULL COMMENT 'Timestamp when record was last modified',
  PRIMARY KEY (`tax_type_id`),
  UNIQUE KEY `tax_type_code` (`tax_type_code`),
  KEY `idx_tax_type_code` (`tax_type_code`),
  KEY `idx_tax_type_current` (`is_current`),
  KEY `idx_tax_type_category` (`tax_category`),
  KEY `idx_tax_type_valid` (`valid_from`,`valid_to`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Defines all types of taxes administered by the tax authority.\nEach tax type has unique registration, filing, assessment, and\npayment rules. Common tax types include:\n\n- Corporate Income Tax (CIT)\n- Personal Income Tax (PIT)\n- Value Added Tax (VAT)\n- Excis';


-- ==============================================================================
-- Schema: filing_assessment
-- ==============================================================================

CREATE DATABASE IF NOT EXISTS `filing_assessment`;
USE `filing_assessment`;

-- ------------------------------------------------------------------------------
-- Table: assessment
-- ------------------------------------------------------------------------------

CREATE TABLE `assessment` (
  `assessment_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for assessment (primary key)',
  `tax_account_id` bigint NOT NULL COMMENT 'Foreign key to tax account (TIN-Tax Type linkage)',
  `tax_period_id` bigint NOT NULL COMMENT 'Foreign key to tax period being assessed',
  `tax_return_id` bigint DEFAULT NULL COMMENT 'Foreign key to tax return (for self-assessments).\nNull for administrative, audit, or BJA assessments.\n',
  `assessment_number` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'System-generated unique assessment number for tracking.\nFormat: {TYPE}-{TAX_TYPE}-{TIN}-{PERIOD}-{SEQUENCE}\nExample: SELF-CIT-123456789-202403-01\n',
  `assessment_type_code` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Type of assessment per tax administration methodology:\n- SELF: Self-assessment from taxpayer return\n- ADMINISTRATIVE: Assessment by tax authority\n- AUDIT: Additional assessment from audit\n- BEST_JUDGMENT: Automatic assessment for non-filers\n- ESTIMATED: P',
  `assessment_version` int NOT NULL COMMENT 'Version number of assessment. Original = 1, amendments = 2, 3, etc.\nUsed to track assessment amendment history.\n',
  `previous_assessment_id` bigint DEFAULT NULL COMMENT 'Foreign key to previous version of assessment (if amended).\nNull for original assessments. Creates amendment chain.\n',
  `is_current_version` tinyint(1) NOT NULL COMMENT 'Flag indicating if this is the current (latest) version',
  `audit_case_id` bigint DEFAULT NULL COMMENT 'Foreign key to audit case (for audit assessments).\nNull for non-audit assessments.\n',
  `assessment_status_code` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Current status of assessment in workflow:\n- DRAFT: Being prepared\n- PRELIMINARY: Preliminary calculation done\n- PROPOSED: Proposed to taxpayer (for administrative)\n- OBJECTION_PERIOD: Within objection period\n- FINAL: Finalized (no longer amendable)\n- APPE',
  `assessment_date` date NOT NULL COMMENT 'Official date of assessment. This is when assessment is\nformally created and becomes part of official record.\n',
  `finalization_date` date DEFAULT NULL COMMENT 'Date when assessment became final (objection period expired).\nNull until assessment is finalized.\n',
  `payment_due_date` date NOT NULL COMMENT 'Date by which assessed amount must be paid',
  `assessed_tax_amount` decimal(19,2) NOT NULL COMMENT 'Base tax amount assessed (before penalties and interest).\nPositive = tax owed, Negative = refund due.\n',
  `penalty_amount` decimal(19,2) NOT NULL COMMENT 'Total penalties assessed (late filing, late payment, etc.)',
  `interest_amount` decimal(19,2) NOT NULL COMMENT 'Total interest assessed on unpaid tax',
  `credit_amount` decimal(19,2) NOT NULL COMMENT 'Total tax credits applied (prepayments, withholding, etc.).\nReduces net amount payable.\n',
  `net_assessment_amount` decimal(19,2) NOT NULL COMMENT 'Net amount to be paid (or refunded).\nFormula: assessed_tax + penalty + interest - credits\nPositive = payment due, Negative = refund due, Zero = no action\n',
  `amount_paid` decimal(19,2) NOT NULL COMMENT 'Total amount paid against this assessment.\nUpdated by payment_refund domain.\n',
  `balance_outstanding` decimal(19,2) NOT NULL COMMENT 'Outstanding balance on assessment.\nFormula: net_assessment_amount - amount_paid\nPositive = still owes, Zero = fully paid, Negative = overpaid\n',
  `objection_deadline_date` date DEFAULT NULL COMMENT 'Last date taxpayer can file objection to assessment.\nTypically 30-90 days from assessment_date.\n',
  `has_objection` tinyint(1) NOT NULL COMMENT 'Flag indicating if taxpayer filed objection',
  `last_interest_calculation_date` date DEFAULT NULL COMMENT 'Date when interest was last calculated and added',
  `bja_method_code` varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Method used for Best Judgment Assessment:\n- PRIOR_PERIOD: Based on prior period assessment\n- INDUSTRY_AVERAGE: Based on industry benchmarks\n- MINIMUM_TAX: Minimum tax for category\n- MANUAL: Manually determined by officer\nNull for non-BJA assessments.\n',
  `is_refund_due` tinyint(1) NOT NULL COMMENT 'Flag indicating if assessment results in refund to taxpayer.\nSet true if net_assessment_amount < 0.\n',
  `refund_approved` tinyint(1) DEFAULT NULL COMMENT 'Flag indicating if refund has been approved',
  `refund_approval_date` date DEFAULT NULL COMMENT 'Date when refund was approved',
  `posted_to_accounting` tinyint(1) NOT NULL COMMENT 'Flag indicating if assessment has been posted to accounting\nledgers (taxpayer account and revenue account).\n',
  `accounting_post_date` date DEFAULT NULL COMMENT 'Date when posted to accounting',
  `assessed_by_user_id` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'User ID of tax officer who performed assessment\n(for administrative, audit, BJA assessments).\nSystem for self-assessments.\n',
  `approved_by_user_id` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'User ID of supervisor who approved assessment',
  `approval_date` date DEFAULT NULL COMMENT 'Date when assessment was approved',
  `assessment_notes` text COLLATE utf8mb4_unicode_ci COMMENT 'Detailed notes explaining assessment rationale and calculations',
  `internal_notes` text COLLATE utf8mb4_unicode_ci COMMENT 'Internal notes not visible to taxpayer',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'User who created the record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when record was created',
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'User who last modified the record',
  `modified_date` timestamp NULL DEFAULT NULL COMMENT 'Timestamp when record was last modified',
  PRIMARY KEY (`assessment_id`),
  UNIQUE KEY `assessment_number` (`assessment_number`),
  KEY `idx_assessment_pk` (`assessment_id`),
  KEY `idx_assessment_account_period` (`tax_account_id`,`tax_period_id`,`is_current_version`),
  KEY `idx_assessment_number` (`assessment_number`),
  KEY `idx_assessment_return` (`tax_return_id`),
  KEY `idx_assessment_status` (`assessment_status_code`,`assessment_date`),
  KEY `idx_assessment_type` (`assessment_type_code`,`assessment_date`),
  KEY `idx_assessment_payment_due` (`payment_due_date`,`balance_outstanding`),
  KEY `idx_assessment_refund` (`is_refund_due`,`refund_approved`),
  KEY `idx_assessment_accounting` (`posted_to_accounting`,`accounting_post_date`),
  KEY `idx_assessment_objection` (`has_objection`,`objection_deadline_date`),
  KEY `fk_assessment_audit_case_id` (`audit_case_id`),
  KEY `fk_assessment_tax_period_id` (`tax_period_id`),
  KEY `fk_assessment_previous_assessment_id` (`previous_assessment_id`),
  CONSTRAINT `fk_assessment_previous_assessment_id` FOREIGN KEY (`previous_assessment_id`) REFERENCES `assessment` (`assessment_id`),
  CONSTRAINT `fk_assessment_tax_account_id` FOREIGN KEY (`tax_account_id`) REFERENCES `registration`.`tax_account` (`tax_account_id`),
  CONSTRAINT `fk_assessment_tax_period_id` FOREIGN KEY (`tax_period_id`) REFERENCES `tax_framework`.`tax_period` (`tax_period_id`),
  CONSTRAINT `fk_assessment_tax_return_id` FOREIGN KEY (`tax_return_id`) REFERENCES `tax_return` (`tax_return_id`)
) ENGINE=InnoDB AUTO_INCREMENT=72 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Core assessment table representing tax liability determinations.\nAn assessment is the official determination of tax owed (or refundable)\nfor a specific TIN-Tax Type-Tax Period combination.\n\nSIGTAS Source: ASSESSMENT table\n\nAssessment Types:\n1. Self-Assess';

-- ------------------------------------------------------------------------------
-- Table: assessment_line
-- ------------------------------------------------------------------------------

CREATE TABLE `assessment_line` (
  `assessment_line_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for assessment line',
  `assessment_id` bigint NOT NULL COMMENT 'Foreign key to parent assessment',
  `form_line_id` bigint DEFAULT NULL COMMENT 'Foreign key to form line (if assessment based on return)',
  `line_number` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Line number reference',
  `line_sequence` int NOT NULL COMMENT 'Sequence for ordering lines',
  `line_description` varchar(500) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Description of what this line represents',
  `submitted_amount` decimal(19,2) DEFAULT NULL COMMENT 'Amount submitted by taxpayer (from return)',
  `assessed_amount` decimal(19,2) NOT NULL COMMENT 'Amount determined by assessment',
  `adjustment_amount` decimal(19,2) DEFAULT NULL COMMENT 'Difference between submitted and assessed (if applicable)',
  `adjustment_reason_code` varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Reason for adjustment if assessed differs from submitted',
  `adjustment_notes` text COLLATE utf8mb4_unicode_ci COMMENT 'Detailed explanation of adjustment',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_date` timestamp NOT NULL,
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `modified_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`assessment_line_id`),
  KEY `idx_assessment_line_pk` (`assessment_line_id`),
  KEY `idx_assessment_line_assessment` (`assessment_id`,`line_sequence`),
  KEY `idx_assessment_line_form` (`form_line_id`),
  CONSTRAINT `fk_assessment_line_assessment_id` FOREIGN KEY (`assessment_id`) REFERENCES `assessment` (`assessment_id`),
  CONSTRAINT `fk_assessment_line_form_line_id` FOREIGN KEY (`form_line_id`) REFERENCES `tax_framework`.`form_line` (`form_line_id`)
) ENGINE=InnoDB AUTO_INCREMENT=71 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Detailed line-item breakdown of assessment showing how assessed\namount was calculated. Links assessment to specific form lines\nand calculation steps.\n\nSIGTAS Source: ASSESS_LINE table\n\nBusiness Rules:\n- Assessment lines must reconcile to total assessment ';

-- ------------------------------------------------------------------------------
-- Table: interest_assessment
-- ------------------------------------------------------------------------------

CREATE TABLE `interest_assessment` (
  `interest_assessment_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for interest assessment',
  `assessment_id` bigint NOT NULL COMMENT 'Foreign key to related assessment',
  `interest_period_start_date` date NOT NULL COMMENT 'Start date for interest calculation period.\nTypically payment_due_date of assessment.\n',
  `interest_period_end_date` date NOT NULL COMMENT 'End date for interest calculation period.\nTypically payment date or assessment date.\n',
  `days_overdue` int NOT NULL COMMENT 'Number of days interest is calculated for.\nFormula: interest_period_end_date - interest_period_start_date\n',
  `principal_amount` decimal(19,2) NOT NULL COMMENT 'Principal amount on which interest is calculated.\nTypically unpaid tax amount from assessment.\n',
  `annual_interest_rate` decimal(5,2) NOT NULL COMMENT 'Annual interest rate as percentage.\nE.g., 8.00 = 8% per annum\n',
  `daily_interest_rate` decimal(8,6) NOT NULL COMMENT 'Daily interest rate calculated from annual rate.\nFormula: annual_rate / 365\n',
  `calculated_interest_amount` decimal(19,2) NOT NULL COMMENT 'Interest amount calculated using formula:\nprincipal Ã— daily_rate Ã— days_overdue\n',
  `maximum_interest_amount` decimal(19,2) DEFAULT NULL COMMENT 'Maximum interest that can be charged per regulations.\nMay be capped at X% of principal.\n',
  `final_interest_amount` decimal(19,2) NOT NULL COMMENT 'Final interest amount after applying maximum cap if applicable.\nThis is the amount charged to taxpayer.\n',
  `interest_status_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Status of interest:\n- CALCULATED: Calculated but not yet assessed\n- ASSESSED: Formally assessed\n- PAID: Paid in full\n- PARTIALLY_PAID: Partially paid\n- WAIVED: Waived by authority\n- WRITTEN_OFF: Written off\n',
  `is_compounded` tinyint(1) NOT NULL COMMENT 'Flag indicating if this interest calculation includes\ncompounding (interest on interest).\n',
  `compounded_interest_amount` decimal(19,2) DEFAULT NULL COMMENT 'Portion of interest that is compounded interest.\nZero if is_compounded = false.\n',
  `waiver_requested` tinyint(1) NOT NULL COMMENT 'Flag indicating if taxpayer requested interest waiver',
  `waiver_approved` tinyint(1) DEFAULT NULL COMMENT 'Flag indicating if waiver was approved',
  `calculation_date` date NOT NULL COMMENT 'Date when interest was calculated',
  `payment_due_date` date NOT NULL COMMENT 'Date by which interest must be paid',
  `calculation_notes` text COLLATE utf8mb4_unicode_ci COMMENT 'Detailed explanation of interest calculation',
  `legal_reference` varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Legal reference for interest rate and rules',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_date` timestamp NOT NULL,
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `modified_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`interest_assessment_id`),
  KEY `idx_interest_assessment_pk` (`interest_assessment_id`),
  KEY `idx_interest_assessment_assessment` (`assessment_id`,`calculation_date`),
  KEY `idx_interest_assessment_period` (`interest_period_start_date`,`interest_period_end_date`),
  KEY `idx_interest_assessment_status` (`interest_status_code`,`payment_due_date`),
  CONSTRAINT `fk_interest_assessment_assessment_id` FOREIGN KEY (`assessment_id`) REFERENCES `assessment` (`assessment_id`)
) ENGINE=InnoDB AUTO_INCREMENT=52 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Records interest assessments on unpaid tax amounts. Interest\naccrues daily on unpaid balances from due date until payment date.\n\nSIGTAS Source: INTEREST table from PENALTIES__INTERESTS schema\n\nBusiness Rules:\n- Interest calculated automatically based on i';

-- ------------------------------------------------------------------------------
-- Table: penalty_assessment
-- ------------------------------------------------------------------------------

CREATE TABLE `penalty_assessment` (
  `penalty_assessment_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for penalty assessment',
  `assessment_id` bigint NOT NULL COMMENT 'Foreign key to related assessment',
  `penalty_type_code` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Type of penalty:\n- LATE_FILING: Penalty for filing after due date\n- LATE_PAYMENT: Penalty for paying after due date\n- UNDERSTATEMENT: Penalty for underreporting tax\n- NON_FILING: Penalty for not filing return\n- INACCURATE_INFO: Penalty for providing false',
  `penalty_calculation_method_code` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'How penalty was calculated:\n- FIXED_AMOUNT: Fixed amount per regulations\n- PERCENTAGE_TAX: Percentage of tax amount\n- PERCENTAGE_TURNOVER: Percentage of turnover\n- DAILY_RATE: Daily rate for period late\n- PROGRESSIVE: Progressive based on violation count\n',
  `base_amount` decimal(19,2) DEFAULT NULL COMMENT 'Base amount used for percentage calculations.\nE.g., tax amount for percentage-of-tax penalty.\n',
  `penalty_rate` decimal(5,2) DEFAULT NULL COMMENT 'Penalty rate as percentage (for percentage-based penalties).\nE.g., 5.00 = 5%\n',
  `days_late` int DEFAULT NULL COMMENT 'Number of days late (for time-based penalties).\nUsed with daily rate penalties.\n',
  `calculated_penalty_amount` decimal(19,2) NOT NULL COMMENT 'Penalty amount before any adjustments (minimum, maximum, discount).\nRaw calculation result.\n',
  `minimum_penalty_amount` decimal(19,2) DEFAULT NULL COMMENT 'Minimum penalty amount per regulations.\nFinal penalty cannot be less than this.\n',
  `maximum_penalty_amount` decimal(19,2) DEFAULT NULL COMMENT 'Maximum penalty amount per regulations.\nFinal penalty cannot exceed this.\n',
  `discount_percentage` decimal(5,2) DEFAULT NULL COMMENT 'Discount percentage for early payment or first violation.\nE.g., 50.00 = 50% discount\n',
  `discount_amount` decimal(19,2) DEFAULT NULL COMMENT 'Absolute discount amount applied',
  `final_penalty_amount` decimal(19,2) NOT NULL COMMENT 'Final penalty amount after applying min/max/discounts.\nThis is the amount charged to taxpayer.\n',
  `penalty_status_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Status of penalty:\n- CALCULATED: Calculated but not yet assessed\n- ASSESSED: Formally assessed\n- APPEALED: Under appeal\n- WAIVED: Waived by authority\n- PAID: Paid in full\n- PARTIALLY_PAID: Partially paid\n- WRITTEN_OFF: Written off\n',
  `is_automatic` tinyint(1) NOT NULL COMMENT 'Flag indicating if penalty was automatically calculated\nor manually assessed by officer.\n',
  `violation_count` int DEFAULT NULL COMMENT 'Count of previous similar violations (for progressive penalties).\nUsed to increase penalty for repeat offenders.\n',
  `waiver_requested` tinyint(1) NOT NULL COMMENT 'Flag indicating if taxpayer requested penalty waiver',
  `waiver_reason` text COLLATE utf8mb4_unicode_ci COMMENT 'Reason provided for waiver request',
  `waiver_approved` tinyint(1) DEFAULT NULL COMMENT 'Flag indicating if waiver was approved',
  `waiver_approval_date` date DEFAULT NULL COMMENT 'Date when waiver was approved',
  `waiver_approved_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'User who approved waiver',
  `assessment_date` date NOT NULL COMMENT 'Date when penalty was assessed',
  `payment_due_date` date NOT NULL COMMENT 'Date by which penalty must be paid',
  `discount_deadline_date` date DEFAULT NULL COMMENT 'Last date to pay and receive early payment discount.\nNull if no discount available.\n',
  `calculation_notes` text COLLATE utf8mb4_unicode_ci COMMENT 'Detailed explanation of penalty calculation',
  `legal_reference` varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Legal reference (article/section) authorizing this penalty.\nExample: "Tax Code Article 123.4"\n',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_date` timestamp NOT NULL,
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `modified_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`penalty_assessment_id`),
  KEY `idx_penalty_assessment_pk` (`penalty_assessment_id`),
  KEY `idx_penalty_assessment_assessment` (`assessment_id`,`penalty_type_code`),
  KEY `idx_penalty_assessment_status` (`penalty_status_code`,`payment_due_date`),
  KEY `idx_penalty_assessment_waiver` (`waiver_requested`,`waiver_approved`),
  CONSTRAINT `fk_penalty_assessment_assessment_id` FOREIGN KEY (`assessment_id`) REFERENCES `assessment` (`assessment_id`)
) ENGINE=InnoDB AUTO_INCREMENT=22 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Records individual penalty assessments applied to assessments or\nreturns. Penalties can be for late filing, late payment, incorrect\ninformation, non-compliance, etc.\n\nSIGTAS Source: PENALTY table from PENALTIES__INTERESTS schema\n\nBusiness Rules:\n- Penalti';

-- ------------------------------------------------------------------------------
-- Table: tax_return
-- ------------------------------------------------------------------------------

CREATE TABLE `tax_return` (
  `tax_return_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for tax return (primary key)',
  `tax_account_id` bigint NOT NULL COMMENT 'Foreign key to tax account (implements TIN-Tax Type linkage).\nEvery return belongs to specific tax account.\n',
  `tax_period_id` bigint NOT NULL COMMENT 'Foreign key to tax period for which return is filed.\nImplements TTT principle (TIN-Tax Type-Tax Period).\n',
  `form_id` bigint NOT NULL COMMENT 'Foreign key to tax form used for this return. Form defines\nstructure and validation rules.\n',
  `form_version_id` bigint NOT NULL COMMENT 'Foreign key to specific version of form (forms evolve over time).\nCritical for maintaining historical accuracy.\n',
  `return_number` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'System-generated unique return number for tracking and reference.\nFormat: {TAX_TYPE}-{TIN}-{PERIOD}-{SEQUENCE}\nExample: CIT-123456789-202403-01\n',
  `return_version` int NOT NULL COMMENT 'Version number of return. Original return = 1, first amendment = 2, etc.\nUsed to track return amendment history.\n',
  `previous_return_id` bigint DEFAULT NULL COMMENT 'Foreign key to previous version of return (if this is amendment).\nNull for original returns. Creates amendment chain.\n',
  `filing_method_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Method used to file return:\n- E_PORTAL: Filed via online portal\n- WEB_SERVICE: Uploaded via web service/API\n- PAPER: Paper return (manual data entry)\n- PHONE: Filed via phone (rare)\n- TAX_OFFICE: Filed in person at tax office\n',
  `filing_date` timestamp NOT NULL COMMENT 'Actual date and time when return was received by tax system.\nUsed to determine if filing is late.\n',
  `filing_due_date` date NOT NULL COMMENT 'Statutory due date for filing this return. Copied from tax_period\nbut stored here for historical accuracy (laws may change).\n',
  `is_filing_late` tinyint(1) NOT NULL COMMENT 'Flag indicating if return was filed after due date.\nAutomatically set by comparing filing_date with filing_due_date.\nTriggers late filing penalty assessment.\n',
  `days_late` int DEFAULT NULL COMMENT 'Number of days late if filing_date > filing_due_date.\nUsed for progressive penalty calculations.\n',
  `return_status_code` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Current status of return in processing workflow:\n- DRAFT: Being prepared (not yet submitted)\n- SUBMITTED: Submitted and awaiting validation\n- VALIDATING: Undergoing validation checks\n- VALIDATION_FAILED: Failed validation (returned to taxpayer)\n- ACCEPTED',
  `is_amended` tinyint(1) NOT NULL COMMENT 'Flag indicating if this return has been superseded by an\namended version. Set to true when newer version filed.\n',
  `is_current_version` tinyint(1) NOT NULL COMMENT 'Flag indicating if this is the current (latest) version of return.\nOnly one version should have this set to true.\n',
  `amendment_reason_code` varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Reason for amendment if this is amended return:\n- ERROR_CORRECTION: Correcting arithmetic/data error\n- TAX_LAW_CHANGE: Reflecting change in tax law interpretation\n- ADDITIONAL_INFORMATION: Including previously omitted information\n- AUDIT_ADJUSTMENT: Requi',
  `amendment_description` text COLLATE utf8mb4_unicode_ci COMMENT 'Detailed explanation of what was amended and why',
  `risk_score` decimal(5,2) DEFAULT NULL COMMENT 'Calculated risk score for this return (0-100 scale).\nHigher score = higher risk of non-compliance.\nUsed to flag returns for review or audit.\n',
  `risk_category_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Risk category based on risk score:\n- LOW: 0-33 (routine processing)\n- MEDIUM: 34-66 (may require review)\n- HIGH: 67-100 (requires review or audit)\n',
  `is_flagged_for_review` tinyint(1) NOT NULL COMMENT 'Flag indicating return is flagged for desk review before\nfinal processing. Set by risk assessment or manual flag.\n',
  `review_notes` text COLLATE utf8mb4_unicode_ci COMMENT 'Notes from desk review of return',
  `prepared_by_party_id` bigint DEFAULT NULL COMMENT 'Party ID of tax preparer/agent who prepared return (if applicable).\nNull if taxpayer prepared return themselves.\n',
  `ip_address` varchar(45) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'IP address from which return was filed (for e-filings)',
  `submission_file_reference` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Reference to original submission file (PDF, XML, etc.)',
  `calculation_checksum` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'SHA-256 hash of all calculation values in return.\nUsed to detect tampering or calculation errors.\n',
  `notes` text COLLATE utf8mb4_unicode_ci COMMENT 'Additional notes or comments about return',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'User who created the record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when record was created',
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'User who last modified the record',
  `modified_date` timestamp NULL DEFAULT NULL COMMENT 'Timestamp when record was last modified',
  PRIMARY KEY (`tax_return_id`),
  UNIQUE KEY `return_number` (`return_number`),
  KEY `idx_tax_return_pk` (`tax_return_id`),
  KEY `idx_tax_return_account` (`tax_account_id`,`tax_period_id`),
  KEY `idx_tax_return_period` (`tax_period_id`,`return_status_code`),
  KEY `idx_tax_return_number` (`return_number`),
  KEY `idx_tax_return_filing_date` (`filing_date`),
  KEY `idx_tax_return_risk` (`risk_category_code`,`is_flagged_for_review`),
  KEY `idx_tax_return_status` (`return_status_code`,`filing_date`),
  KEY `idx_tax_return_version` (`tax_account_id`,`tax_period_id`,`return_version`),
  KEY `idx_tax_return_current` (`tax_account_id`,`tax_period_id`,`is_current_version`),
  KEY `fk_tax_return_form_id` (`form_id`),
  KEY `fk_tax_return_form_version_id` (`form_version_id`),
  KEY `fk_tax_return_prepared_by_party_id` (`prepared_by_party_id`),
  KEY `fk_tax_return_previous_return_id` (`previous_return_id`),
  CONSTRAINT `fk_tax_return_form_id` FOREIGN KEY (`form_id`) REFERENCES `tax_framework`.`tax_form` (`tax_form_id`),
  CONSTRAINT `fk_tax_return_form_version_id` FOREIGN KEY (`form_version_id`) REFERENCES `tax_framework`.`form_version` (`form_version_id`),
  CONSTRAINT `fk_tax_return_prepared_by_party_id` FOREIGN KEY (`prepared_by_party_id`) REFERENCES `party`.`party` (`party_id`),
  CONSTRAINT `fk_tax_return_previous_return_id` FOREIGN KEY (`previous_return_id`) REFERENCES `tax_return` (`tax_return_id`),
  CONSTRAINT `fk_tax_return_tax_account_id` FOREIGN KEY (`tax_account_id`) REFERENCES `registration`.`tax_account` (`tax_account_id`),
  CONSTRAINT `fk_tax_return_tax_period_id` FOREIGN KEY (`tax_period_id`) REFERENCES `tax_framework`.`tax_period` (`tax_period_id`)
) ENGINE=InnoDB AUTO_INCREMENT=77 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Master table for tax returns filed by taxpayers. A tax return is\nthe primary document through which taxpayers self-assess their tax\nliability for a specific tax period. Returns contain detailed\ninformation about income, deductions, credits, and calculated';

-- ------------------------------------------------------------------------------
-- Table: tax_return_line
-- ------------------------------------------------------------------------------

CREATE TABLE `tax_return_line` (
  `tax_return_line_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for return line (primary key)',
  `tax_return_id` bigint NOT NULL COMMENT 'Foreign key to parent tax return',
  `form_line_id` bigint NOT NULL COMMENT 'Foreign key to form line definition. Defines what this line\nrepresents and validation rules.\n',
  `line_number` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Line number from form (e.g., "1a", "5b", "10").\nFor reference and sorting.\n',
  `line_sequence` int NOT NULL COMMENT 'Sequence number for ordering lines within return',
  `line_value_numeric` decimal(19,2) DEFAULT NULL COMMENT 'Numeric value for this line (amounts, quantities, percentages).\nNull if line is text-based.\n',
  `line_value_text` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Text value for this line (descriptions, explanations).\nNull if line is numeric.\n',
  `line_value_date` date DEFAULT NULL COMMENT 'Date value for this line if applicable',
  `line_value_boolean` tinyint(1) DEFAULT NULL COMMENT 'Boolean value for yes/no questions',
  `is_calculated` tinyint(1) NOT NULL COMMENT 'Flag indicating if value was auto-calculated by system\nor entered by taxpayer.\n',
  `calculation_formula` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Formula used to calculate value if is_calculated = true.\nExample: "LINE_3 - LINE_4 + LINE_5"\n',
  `is_overridden` tinyint(1) NOT NULL COMMENT 'Flag indicating if calculated value was manually overridden\nby taxpayer (requires explanation).\n',
  `override_reason` text COLLATE utf8mb4_unicode_ci COMMENT 'Explanation for why calculated value was overridden',
  `validation_status_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Validation status for this line:\n- VALID: Passed all validations\n- WARNING: Passed with warnings\n- ERROR: Failed validation\n- NOT_VALIDATED: Not yet validated\n',
  `validation_messages` text COLLATE utf8mb4_unicode_ci COMMENT 'JSON array of validation warnings or errors',
  `notes` text COLLATE utf8mb4_unicode_ci COMMENT 'Additional notes or explanations for this line',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'User who created the record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when record was created',
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'User who last modified the record',
  `modified_date` timestamp NULL DEFAULT NULL COMMENT 'Timestamp when record was last modified',
  PRIMARY KEY (`tax_return_line_id`),
  KEY `idx_tax_return_line_pk` (`tax_return_line_id`),
  KEY `idx_return_line_return` (`tax_return_id`,`line_sequence`),
  KEY `idx_return_line_form_line` (`form_line_id`,`tax_return_id`),
  KEY `idx_return_line_validation` (`validation_status_code`),
  CONSTRAINT `fk_tax_return_line_form_line_id` FOREIGN KEY (`form_line_id`) REFERENCES `tax_framework`.`form_line` (`form_line_id`),
  CONSTRAINT `fk_tax_return_line_tax_return_id` FOREIGN KEY (`tax_return_id`) REFERENCES `tax_return` (`tax_return_id`)
) ENGINE=InnoDB AUTO_INCREMENT=176 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Detailed line items of tax return corresponding to form lines.\nEach return consists of multiple lines capturing income, deductions,\ncredits, and calculations per the form structure.\n\nSIGTAS Source: ASSESS_LINE table\n\nBusiness Rules:\n- Lines must match for';


-- ==============================================================================
-- Schema: payment_refund
-- ==============================================================================

CREATE DATABASE IF NOT EXISTS `payment_refund`;
USE `payment_refund`;

-- ------------------------------------------------------------------------------
-- Table: bank
-- ------------------------------------------------------------------------------

CREATE TABLE `bank` (
  `bank_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for bank',
  `bank_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Official bank code (central bank registry)',
  `bank_name` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Full legal name of bank',
  `bank_short_name` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Abbreviated name for display',
  `swift_code` varchar(11) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'SWIFT/BIC code for international transfers',
  `tax_collection_account` varchar(34) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Bank account number where taxpayers deposit tax payments (IBAN).\nThis is the single tax account per bank.\n',
  `is_active` tinyint(1) NOT NULL COMMENT 'Flag indicating if bank is currently active for tax transactions',
  `payment_interface_type` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Type of integration: FILE_IMPORT, API, MANUAL\n',
  `contact_name` varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Primary contact person at bank',
  `contact_email` varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Email for bank contact',
  `contact_phone` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Phone number for bank contact',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_date` timestamp NOT NULL,
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `modified_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`bank_id`),
  UNIQUE KEY `bank_code` (`bank_code`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Master data for banks that collect tax payments or receive refunds.\nIncludes commercial banks, payment service providers, and treasury.\n\nBanks interface with tax system through:\n- Payment file imports (batch or real-time)\n- Refund payment instructions (ou';

-- ------------------------------------------------------------------------------
-- Table: bank_interface_log
-- ------------------------------------------------------------------------------

CREATE TABLE `bank_interface_log` (
  `bank_interface_log_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for log entry',
  `bank_id` bigint NOT NULL COMMENT 'Foreign key to bank',
  `interface_type` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Type: PAYMENT_IMPORT, REFUND_EXPORT, RECONCILIATION',
  `file_name` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Name of imported/exported file',
  `file_date` date DEFAULT NULL COMMENT 'Date on file (may differ from processing date)',
  `processing_date` timestamp NOT NULL COMMENT 'Timestamp when file was processed',
  `total_records` int NOT NULL COMMENT 'Total number of records in file',
  `successful_records` int NOT NULL COMMENT 'Number of records processed successfully',
  `failed_records` int NOT NULL COMMENT 'Number of records that failed processing',
  `total_amount` decimal(19,2) DEFAULT NULL COMMENT 'Total amount in file',
  `processing_status` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Status: SUCCESS, PARTIAL, FAILED',
  `error_message` text COLLATE utf8mb4_unicode_ci COMMENT 'Error details if processing failed',
  `processing_duration_seconds` int DEFAULT NULL COMMENT 'Time taken to process file in seconds',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_date` timestamp NOT NULL,
  PRIMARY KEY (`bank_interface_log_id`),
  KEY `fk_bank_interface_log_bank_id` (`bank_id`),
  CONSTRAINT `fk_bank_interface_log_bank_id` FOREIGN KEY (`bank_id`) REFERENCES `payment_refund.bank` (`bank_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Logs all bank file imports and payment batch processing.\nCritical for auditing and troubleshooting bank integration issues.\n\nEach bank file import creates one log record with:\n- File details (name, size, records)\n- Processing results (success, errors)\n- R';

-- ------------------------------------------------------------------------------
-- Table: bank_reconciliation
-- ------------------------------------------------------------------------------

CREATE TABLE `bank_reconciliation` (
  `bank_reconciliation_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for reconciliation record',
  `bank_id` bigint NOT NULL COMMENT 'Foreign key to bank being reconciled',
  `reconciliation_date` date NOT NULL COMMENT 'Date of reconciliation',
  `statement_date` date NOT NULL COMMENT 'Date of bank statement being reconciled',
  `opening_balance` decimal(19,2) NOT NULL COMMENT 'Opening balance per bank statement',
  `total_receipts` decimal(19,2) NOT NULL COMMENT 'Total receipts per bank statement',
  `total_disbursements` decimal(19,2) NOT NULL COMMENT 'Total disbursements per bank statement',
  `closing_balance` decimal(19,2) NOT NULL COMMENT 'Closing balance per bank statement',
  `system_receipts` decimal(19,2) NOT NULL COMMENT 'Total receipts per tax system records',
  `system_disbursements` decimal(19,2) NOT NULL COMMENT 'Total disbursements per tax system records',
  `variance_amount` decimal(19,2) NOT NULL COMMENT 'Difference between bank and system balances.\nPositive = bank higher, Negative = system higher\n',
  `reconciliation_status` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Status: IN_PROGRESS, RECONCILED, DISCREPANCY, APPROVED\n',
  `variance_explanation` text COLLATE utf8mb4_unicode_ci COMMENT 'Explanation for any variances identified',
  `reconciled_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'User who performed reconciliation',
  `approved_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'User who approved reconciliation',
  `approval_date` date DEFAULT NULL COMMENT 'Date when reconciliation was approved',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_date` timestamp NOT NULL,
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `modified_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`bank_reconciliation_id`),
  KEY `fk_bank_reconciliation_bank_id` (`bank_id`),
  CONSTRAINT `fk_bank_reconciliation_bank_id` FOREIGN KEY (`bank_id`) REFERENCES `payment_refund.bank` (`bank_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Bank reconciliation records matching payments received with\nbank statements. Critical control for cash management.\n\nReconciliation Process:\n1. Bank statement received (daily or monthly)\n2. Payments in system matched to statement transactions\n3. Difference';

-- ------------------------------------------------------------------------------
-- Table: installment_plan
-- ------------------------------------------------------------------------------

CREATE TABLE `installment_plan` (
  `installment_plan_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for installment plan',
  `agreement_number` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Unique agreement number for tracking',
  `tax_account_id` bigint NOT NULL COMMENT 'Foreign key to tax account covered by plan',
  `party_id` bigint NOT NULL COMMENT 'Foreign key to party (taxpayer) in agreement',
  `installment_method_code` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Method for calculating installments:\n- EQUAL_PRINCIPAL: Equal principal each period\n- EQUAL_PAYMENT: Equal total payment each period\n- CUSTOM: Custom schedule\n',
  `agreement_date` date NOT NULL COMMENT 'Date when agreement was signed/approved',
  `start_date` date NOT NULL COMMENT 'Date when first installment is due',
  `end_date` date NOT NULL COMMENT 'Expected end date when all installments paid.\nCalculated based on number of installments.\n',
  `total_amount` decimal(19,2) NOT NULL COMMENT 'Total amount covered by agreement (principal + interest).\nSum of all scheduled installments.\n',
  `principal_amount` decimal(19,2) NOT NULL COMMENT 'Principal amount (without interest)',
  `interest_amount` decimal(19,2) NOT NULL COMMENT 'Total interest charged over life of plan',
  `down_payment_required` decimal(19,2) NOT NULL COMMENT 'Required down payment (e.g., 10% of principal).\nMust be paid before agreement activated.\n',
  `down_payment_received` decimal(19,2) NOT NULL COMMENT 'Actual down payment received from taxpayer',
  `down_payment_date` date DEFAULT NULL COMMENT 'Date when down payment was received',
  `number_of_installments` int NOT NULL COMMENT 'Total number of scheduled installments',
  `installment_frequency_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Payment frequency: MONTHLY, QUARTERLY, SEMI_ANNUAL\n',
  `annual_interest_rate` decimal(5,2) NOT NULL COMMENT 'Annual interest rate as percentage (e.g., 8.00 = 8%)',
  `plan_status_code` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Status: REQUESTED, APPROVED, ACTIVE, COMPLETED, DEFAULTED,\nCANCELLED, REJECTED\n',
  `approved_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'User ID of person who approved agreement',
  `approval_date` date DEFAULT NULL COMMENT 'Date when agreement was approved',
  `amount_paid_to_date` decimal(19,2) NOT NULL COMMENT 'Total amount paid so far (including down payment).\nUpdated with each installment payment.\n',
  `outstanding_balance` decimal(19,2) NOT NULL COMMENT 'Remaining balance to be paid.\nFormula: total_amount - amount_paid_to_date\n',
  `missed_payments_count` int NOT NULL COMMENT 'Number of missed installment payments.\nTriggers cancellation if exceeds threshold.\n',
  `last_payment_date` date DEFAULT NULL COMMENT 'Date of most recent installment payment',
  `next_payment_due_date` date DEFAULT NULL COMMENT 'Due date for next installment payment.\nUsed for monitoring and reminder notices.\n',
  `default_date` date DEFAULT NULL COMMENT 'Date when agreement went into default status',
  `cancellation_date` date DEFAULT NULL COMMENT 'Date when agreement was cancelled',
  `cancellation_reason` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Reason for cancellation (default, taxpayer request, etc.)',
  `completion_date` date DEFAULT NULL COMMENT 'Date when all installments paid and agreement completed',
  `agreement_terms` text COLLATE utf8mb4_unicode_ci COMMENT 'Full text of agreement terms and conditions',
  `agreement_notes` text COLLATE utf8mb4_unicode_ci COMMENT 'Additional notes about agreement',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_date` timestamp NOT NULL,
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `modified_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`installment_plan_id`),
  UNIQUE KEY `agreement_number` (`agreement_number`),
  KEY `idx_installment_active` (`plan_status_code`,`next_payment_due_date`),
  KEY `fk_installment_plan_tax_account_id` (`tax_account_id`),
  KEY `fk_installment_plan_party_id` (`party_id`),
  KEY `fk_installment_plan_installment_method_code` (`installment_method_code`),
  CONSTRAINT `fk_installment_plan_installment_method_code` FOREIGN KEY (`installment_method_code`) REFERENCES `reference_data.ref_installment_method` (`installment_method_code`),
  CONSTRAINT `fk_installment_plan_party_id` FOREIGN KEY (`party_id`) REFERENCES `party_management.party` (`party_id`),
  CONSTRAINT `fk_installment_plan_tax_account_id` FOREIGN KEY (`tax_account_id`) REFERENCES `registration.tax_account` (`tax_account_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Payment agreements allowing taxpayers to pay liabilities in\ninstallments over time. Critical for enforced collection function\nto encourage compliance and avoid escalation to enforcement actions.\n\nInstallment Plan Process:\n1. Taxpayer requests payment agre';

-- ------------------------------------------------------------------------------
-- Table: installment_schedule
-- ------------------------------------------------------------------------------

CREATE TABLE `installment_schedule` (
  `installment_schedule_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for scheduled installment',
  `installment_plan_id` bigint NOT NULL COMMENT 'Foreign key to parent installment plan',
  `installment_number` int NOT NULL COMMENT 'Sequential installment number (1, 2, 3, ...)',
  `due_date` date NOT NULL COMMENT 'Date when this installment is due',
  `principal_amount` decimal(19,2) NOT NULL COMMENT 'Principal portion of this installment',
  `interest_amount` decimal(19,2) NOT NULL COMMENT 'Interest portion of this installment',
  `total_installment_amount` decimal(19,2) NOT NULL COMMENT 'Total amount due for this installment.\nFormula: principal_amount + interest_amount\n',
  `payment_status_code` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Status: SCHEDULED, PAID, PARTIALLY_PAID, OVERDUE, WAIVED\n',
  `amount_paid` decimal(19,2) NOT NULL COMMENT 'Actual amount paid for this installment',
  `payment_date` date DEFAULT NULL COMMENT 'Date when payment was received',
  `payment_id` bigint DEFAULT NULL COMMENT 'Foreign key to payment that satisfied this installment',
  `days_overdue` int NOT NULL COMMENT 'Number of days payment is overdue (calculated)',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_date` timestamp NOT NULL,
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `modified_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`installment_schedule_id`),
  KEY `fk_installment_schedule_installment_plan_id` (`installment_plan_id`),
  KEY `fk_installment_schedule_payment_id` (`payment_id`),
  CONSTRAINT `fk_installment_schedule_installment_plan_id` FOREIGN KEY (`installment_plan_id`) REFERENCES `payment_refund.installment_plan` (`installment_plan_id`),
  CONSTRAINT `fk_installment_schedule_payment_id` FOREIGN KEY (`payment_id`) REFERENCES `payment_refund.payment` (`payment_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Detailed schedule of each installment payment in a payment plan.\nOne row per scheduled installment.\n\nAutomatically generated when plan approved based on:\n- Total amount\n- Number of installments\n- Frequency\n- Interest rate\n- Calculation method\n\nSIGTAS Sour';

-- ------------------------------------------------------------------------------
-- Table: payment
-- ------------------------------------------------------------------------------

CREATE TABLE `payment` (
  `payment_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for payment record',
  `payment_reference_number` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Unique reference number for payment. Could be bank transaction\nreference, payment receipt number, or system-generated reference.\nUsed for reconciliation and taxpayer inquiries.\n',
  `tax_account_id` bigint DEFAULT NULL COMMENT 'Foreign key to tax_account receiving the payment.\nNULL if taxpayer not identified (suspended payment).\n',
  `party_id` bigint DEFAULT NULL COMMENT 'Foreign key to party making the payment. Usually the taxpayer,\nbut could be third party (agent, bank, employer for withholding).\n',
  `payment_type_code` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Type of payment method used:\n- BANK_TRANSFER: Electronic bank transfer\n- CASH: Cash payment at tax office\n- CHECK: Check payment\n- CREDIT_CARD: Credit card payment\n- DEBIT_CARD: Debit card payment\n- DIRECT_DEBIT: Automatic bank debit\n- MOBILE_PAYMENT: Mob',
  `payment_location_code` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Location or channel where payment was received:\n- BANK_BRANCH: Commercial bank branch\n- TAX_OFFICE: Tax administration office\n- E_PORTAL: Online e-services portal\n- MOBILE_APP: Mobile application\n- POS_TERMINAL: Point-of-sale terminal\n- EMPLOYER: Withheld',
  `payment_date` date NOT NULL COMMENT 'Date when payment was made by taxpayer. This is the effective\ndate for accounting and interest calculation purposes.\n',
  `received_date` date NOT NULL COMMENT 'Date when payment was received and recorded by tax administration.\nMay differ from payment_date if there''s processing delay.\n',
  `payment_amount` decimal(19,2) NOT NULL COMMENT 'Total payment amount in local currency. Must be positive.\nThis is the gross amount before any allocation.\n',
  `currency_code` char(3) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'ISO 4217 currency code. Usually domestic currency, but foreign\ncurrency payments may be accepted and converted.\n',
  `exchange_rate` decimal(12,6) DEFAULT NULL COMMENT 'Exchange rate applied if payment in foreign currency.\nNULL if payment in domestic currency.\n',
  `amount_in_domestic_currency` decimal(19,2) NOT NULL COMMENT 'Payment amount converted to domestic currency.\nSame as payment_amount if already in domestic currency.\n',
  `bank_id` bigint DEFAULT NULL COMMENT 'Foreign key to bank through which payment was received.\nNULL for non-bank payments (cash, e-portal, etc.).\n',
  `bank_transaction_reference` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Bank''s own transaction reference number.\nUsed for reconciliation with bank statements.\n',
  `payer_account_number` varchar(34) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Bank account number from which payment originated (IBAN format).\nUsed for verification and audit.\n',
  `payment_status_code` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Current status of payment:\n- RECEIVED: Payment received but not allocated\n- ALLOCATED: Payment allocated to liabilities\n- SUSPENDED: Taxpayer not identified\n- REVERSED: Payment reversed\n- RECONCILED: Reconciled with bank\n- CANCELLED: Payment cancelled\n',
  `is_allocated` tinyint(1) NOT NULL COMMENT 'Flag indicating if payment has been allocated to liabilities.\nFalse for suspended payments awaiting identification.\n',
  `allocated_date` timestamp NULL DEFAULT NULL COMMENT 'Timestamp when payment was allocated',
  `is_suspended` tinyint(1) NOT NULL COMMENT 'Flag indicating if payment is suspended due to taxpayer\nidentification issues. Suspended payments held in queue.\n',
  `suspended_reason` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Reason why payment is suspended:\n- TIN not found\n- TIN invalid\n- Multiple potential matches\n- Awaiting taxpayer contact\n',
  `is_reconciled` tinyint(1) NOT NULL COMMENT 'Flag indicating if payment has been reconciled with bank statement.\nCritical for cash management control.\n',
  `reconciliation_date` date DEFAULT NULL COMMENT 'Date when payment was reconciled with bank',
  `receipt_number` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Official tax receipt number issued to taxpayer.\nPrinted on receipt and used for inquiries.\n',
  `receipt_issued_date` timestamp NULL DEFAULT NULL COMMENT 'Date and time when receipt was issued to taxpayer',
  `payment_notes` text COLLATE utf8mb4_unicode_ci COMMENT 'Additional notes about payment. Could include:\n- Taxpayer''s allocation instructions\n- Special circumstances\n- Investigation notes for suspended payments\n',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_date` timestamp NOT NULL,
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `modified_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`payment_id`),
  UNIQUE KEY `payment_reference_number` (`payment_reference_number`),
  UNIQUE KEY `receipt_number` (`receipt_number`),
  KEY `idx_payment_composite` (`tax_account_id`,`payment_date`,`payment_status_code`),
  KEY `idx_payment_bank` (`bank_id`,`payment_date`),
  KEY `idx_payment_suspended` (`is_suspended`,`received_date`),
  KEY `idx_payment_unallocated` (`is_allocated`,`received_date`),
  KEY `fk_payment_party_id` (`party_id`),
  KEY `fk_payment_payment_type_code` (`payment_type_code`),
  KEY `fk_payment_payment_location_code` (`payment_location_code`),
  CONSTRAINT `fk_payment_bank_id` FOREIGN KEY (`bank_id`) REFERENCES `payment_refund.bank` (`bank_id`),
  CONSTRAINT `fk_payment_party_id` FOREIGN KEY (`party_id`) REFERENCES `party_management.party` (`party_id`),
  CONSTRAINT `fk_payment_payment_location_code` FOREIGN KEY (`payment_location_code`) REFERENCES `reference_data.ref_payment_location` (`payment_location_code`),
  CONSTRAINT `fk_payment_payment_type_code` FOREIGN KEY (`payment_type_code`) REFERENCES `reference_data.ref_payment_type` (`payment_type_code`),
  CONSTRAINT `fk_payment_tax_account_id` FOREIGN KEY (`tax_account_id`) REFERENCES `registration.tax_account` (`tax_account_id`)
) ENGINE=InnoDB AUTO_INCREMENT=80 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Records all payments received from taxpayers through any channel.\nThis is the primary table for payment processing and serves as the\nsource for payment allocation to taxpayer accounts and liabilities.\n\nEach payment record represents a financial transactio';

-- ------------------------------------------------------------------------------
-- Table: payment_allocation
-- ------------------------------------------------------------------------------

CREATE TABLE `payment_allocation` (
  `payment_allocation_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for payment allocation record',
  `payment_id` bigint NOT NULL COMMENT 'Foreign key to payment being allocated',
  `assessment_id` bigint DEFAULT NULL COMMENT 'Foreign key to assessment liability being paid.\nNULL if allocation to general account credit.\n',
  `charge_type_code` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Type of charge being paid:\n- PRINCIPAL_TAX: Main tax liability\n- PENALTY: Penalty amount\n- INTEREST: Interest charges\n- SURCHARGE: Additional surcharges\n- FEE: Administrative fees\n',
  `allocated_amount` decimal(19,2) NOT NULL COMMENT 'Amount allocated to this specific charge type and liability.\nSum of all allocations for payment must equal payment amount.\n',
  `allocation_sequence` int NOT NULL COMMENT 'Order in which this allocation was made (1, 2, 3...).\nUsed to determine priority when payment insufficient for all.\n',
  `allocation_date` timestamp NOT NULL COMMENT 'Timestamp when allocation was performed',
  `accounting_transaction_id` bigint DEFAULT NULL COMMENT 'Foreign key to accounting transaction created by this allocation.\nLinks payment allocation to accounting system.\n',
  `is_automatic` tinyint(1) NOT NULL COMMENT 'Flag indicating if allocation was automatic (per rules) or manual.\nMost allocations should be automatic.\n',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_date` timestamp NOT NULL,
  PRIMARY KEY (`payment_allocation_id`),
  KEY `idx_alloc_composite` (`payment_id`,`allocation_sequence`),
  KEY `fk_payment_allocation_assessment_id` (`assessment_id`),
  KEY `fk_payment_allocation_charge_type_code` (`charge_type_code`),
  KEY `fk_payment_allocation_accounting_transaction_id` (`accounting_transaction_id`),
  CONSTRAINT `fk_payment_allocation_accounting_transaction_id` FOREIGN KEY (`accounting_transaction_id`) REFERENCES `accounting.tax_transaction` (`transaction_id`),
  CONSTRAINT `fk_payment_allocation_assessment_id` FOREIGN KEY (`assessment_id`) REFERENCES `filing_assessment.assessment` (`assessment_id`),
  CONSTRAINT `fk_payment_allocation_charge_type_code` FOREIGN KEY (`charge_type_code`) REFERENCES `reference_data.ref_charge_type` (`charge_type_code`),
  CONSTRAINT `fk_payment_allocation_payment_id` FOREIGN KEY (`payment_id`) REFERENCES `payment_refund.payment` (`payment_id`)
) ENGINE=InnoDB AUTO_INCREMENT=159 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Records how payments are allocated to specific tax liabilities and\ncharge types. This table implements the payment allocation rules\nthat determine which liabilities are paid first when a payment is\nreceived.\n\nPayment Allocation Process:\n1. Payment receive';

-- ------------------------------------------------------------------------------
-- Table: refund
-- ------------------------------------------------------------------------------

CREATE TABLE `refund` (
  `refund_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for refund record',
  `refund_number` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Unique refund number for tracking and taxpayer reference.\nFormat: RFD-YYYY-NNNNNN\n',
  `tax_account_id` bigint NOT NULL COMMENT 'Foreign key to tax account receiving refund',
  `party_id` bigint NOT NULL COMMENT 'Foreign key to party (taxpayer) receiving refund',
  `tax_period_id` bigint DEFAULT NULL COMMENT 'Foreign key to tax period for which refund is claimed.\nNULL for refunds not tied to specific period.\n',
  `refund_type_code` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Type of refund:\n- OVERPAYMENT: Excess payment\n- EXPORT_VAT: Export VAT refund\n- AUDIT_ADJUSTMENT: From audit\n- AMENDED_RETURN: Amended filing\n- PENALTY_WAIVER: Waived penalty\n- ERRONEOUS_ASSESSMENT: Correction\n- WITHHOLDING_EXCESS: Excess withholding\n',
  `refund_status_code` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Current workflow status:\n- CLAIMED: Claim received/identified\n- UNDER_REVIEW: Being reviewed\n- PENDING_APPROVAL: Awaiting approval\n- APPROVED: Approved for refund\n- OFFSET_APPLIED: Offset against liabilities\n- VOUCHER_PREPARED: Refund voucher created\n- SE',
  `claim_date` date NOT NULL COMMENT 'Date when refund claim was filed by taxpayer or identified\nby system (for overpayments).\n',
  `gross_refund_amount` decimal(19,2) NOT NULL COMMENT 'Gross refund amount before any offsetting.\nThis is the total amount taxpayer is entitled to.\n',
  `offset_amount` decimal(19,2) NOT NULL COMMENT 'Amount offset against outstanding liabilities.\nZero if no offsetting performed.\n',
  `net_refund_amount` decimal(19,2) NOT NULL COMMENT 'Net amount to be refunded to taxpayer.\nFormula: gross_refund_amount - offset_amount\n',
  `interest_amount` decimal(19,2) NOT NULL COMMENT 'Interest on late refund per regulations.\nCalculated from due date to refund date.\n',
  `total_refund_amount` decimal(19,2) NOT NULL COMMENT 'Total amount including interest.\nFormula: net_refund_amount + interest_amount\n',
  `earliest_refund_date` date DEFAULT NULL COMMENT 'Earliest date refund can be made per regulations.\nOften X days after claim date (e.g., 30 days for exporters).\n',
  `review_assigned_to` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'User ID of reviewer assigned to review claim',
  `review_start_date` date DEFAULT NULL COMMENT 'Date when review commenced',
  `review_completed_date` date DEFAULT NULL COMMENT 'Date when review was completed',
  `approval_required` tinyint(1) NOT NULL COMMENT 'Flag indicating if manual approval required.\nBased on amount threshold and risk score.\n',
  `approved_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'User ID of approver who approved refund',
  `approval_date` date DEFAULT NULL COMMENT 'Date when refund was approved',
  `rejection_reason` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Reason for rejection if status = REJECTED',
  `refund_voucher_id` bigint DEFAULT NULL COMMENT 'Foreign key to refund voucher (payment instruction).\nNULL until voucher prepared.\n',
  `treasury_instruction_date` date DEFAULT NULL COMMENT 'Date when instruction sent to treasury for payment',
  `refund_date` date DEFAULT NULL COMMENT 'Actual date when refund was transferred to taxpayer.\nConfirmed by bank/treasury.\n',
  `bank_confirmation_received` tinyint(1) NOT NULL COMMENT 'Flag indicating if bank confirmation received',
  `bank_confirmation_date` date DEFAULT NULL COMMENT 'Date when bank confirmed refund transfer',
  `refund_bank_account` varchar(34) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Taxpayer''s bank account for refund (IBAN format).\nVerified before refund processed.\n',
  `refund_notes` text COLLATE utf8mb4_unicode_ci COMMENT 'Additional notes about refund processing',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_date` timestamp NOT NULL,
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `modified_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`refund_id`),
  UNIQUE KEY `refund_number` (`refund_number`),
  KEY `idx_refund_composite` (`tax_account_id`,`refund_status_code`,`claim_date`),
  KEY `idx_refund_pending` (`refund_status_code`,`claim_date`),
  KEY `fk_refund_party_id` (`party_id`),
  KEY `fk_refund_tax_period_id` (`tax_period_id`),
  KEY `fk_refund_refund_type_code` (`refund_type_code`),
  KEY `fk_refund_refund_voucher_id` (`refund_voucher_id`),
  CONSTRAINT `fk_refund_party_id` FOREIGN KEY (`party_id`) REFERENCES `party_management.party` (`party_id`),
  CONSTRAINT `fk_refund_refund_type_code` FOREIGN KEY (`refund_type_code`) REFERENCES `reference_data.ref_refund_type` (`refund_type_code`),
  CONSTRAINT `fk_refund_refund_voucher_id` FOREIGN KEY (`refund_voucher_id`) REFERENCES `payment_refund.refund_voucher` (`refund_voucher_id`),
  CONSTRAINT `fk_refund_tax_account_id` FOREIGN KEY (`tax_account_id`) REFERENCES `registration.tax_account` (`tax_account_id`),
  CONSTRAINT `fk_refund_tax_period_id` FOREIGN KEY (`tax_period_id`) REFERENCES `tax_framework.tax_period` (`tax_period_id`)
) ENGINE=InnoDB AUTO_INCREMENT=29 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Records tax refunds due to taxpayers. Refunds arise from various\nsituations: overpayments, export VAT refunds, audit adjustments,\npenalty waivers, or erroneous assessments.\n\nRefund Processing Workflow:\n1. Refund claim identified or filed by taxpayer\n2. In';

-- ------------------------------------------------------------------------------
-- Table: refund_approval
-- ------------------------------------------------------------------------------

CREATE TABLE `refund_approval` (
  `refund_approval_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for approval record',
  `refund_id` bigint NOT NULL COMMENT 'Foreign key to refund being approved',
  `approval_level` int NOT NULL COMMENT 'Level in approval chain (1 = first approver, 2 = second, etc.).\nHigher amounts require more approval levels.\n',
  `approver_user_id` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'User ID of person who approved at this level',
  `approver_role` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Role of approver (SUPERVISOR, MANAGER, DIRECTOR, etc.)',
  `approval_date` timestamp NOT NULL COMMENT 'Timestamp when approval was granted',
  `approval_decision` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Decision: APPROVED, REJECTED, SENT_BACK',
  `approval_comments` text COLLATE utf8mb4_unicode_ci COMMENT 'Comments from approver',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_date` timestamp NOT NULL,
  PRIMARY KEY (`refund_approval_id`),
  KEY `fk_refund_approval_refund_id` (`refund_id`),
  CONSTRAINT `fk_refund_approval_refund_id` FOREIGN KEY (`refund_id`) REFERENCES `payment_refund.refund` (`refund_id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Records approval workflow for refunds requiring manual approval.\nTracks approval chain for audit and compliance.\n\nSIGTAS Source: REFUND.RECORD_APPROVAL\n';

-- ------------------------------------------------------------------------------
-- Table: refund_line
-- ------------------------------------------------------------------------------

CREATE TABLE `refund_line` (
  `refund_line_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for refund line',
  `refund_id` bigint NOT NULL COMMENT 'Foreign key to parent refund',
  `tax_period_id` bigint DEFAULT NULL COMMENT 'Foreign key to tax period for this line item',
  `charge_type_code` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Charge type for this refund component:\n- PRINCIPAL_TAX\n- PENALTY\n- INTEREST\n',
  `line_amount` decimal(19,2) NOT NULL COMMENT 'Refund amount for this specific charge type and period',
  `original_assessment_id` bigint DEFAULT NULL COMMENT 'Foreign key to original assessment that is being refunded.\nLinks refund to source transaction.\n',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_date` timestamp NOT NULL,
  PRIMARY KEY (`refund_line_id`),
  KEY `fk_refund_line_refund_id` (`refund_id`),
  KEY `fk_refund_line_tax_period_id` (`tax_period_id`),
  KEY `fk_refund_line_charge_type_code` (`charge_type_code`),
  KEY `fk_refund_line_original_assessment_id` (`original_assessment_id`),
  CONSTRAINT `fk_refund_line_charge_type_code` FOREIGN KEY (`charge_type_code`) REFERENCES `reference_data.ref_charge_type` (`charge_type_code`),
  CONSTRAINT `fk_refund_line_original_assessment_id` FOREIGN KEY (`original_assessment_id`) REFERENCES `filing_assessment.assessment` (`assessment_id`),
  CONSTRAINT `fk_refund_line_refund_id` FOREIGN KEY (`refund_id`) REFERENCES `payment_refund.refund` (`refund_id`),
  CONSTRAINT `fk_refund_line_tax_period_id` FOREIGN KEY (`tax_period_id`) REFERENCES `tax_framework.tax_period` (`tax_period_id`)
) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Details the breakdown of refund amounts by charge type and tax period.\nA single refund may consist of multiple line items representing\ndifferent components (principal tax, penalties, interest) and\ndifferent tax periods.\n\nSIGTAS Source: REFUND.REFUND_LINE\n';

-- ------------------------------------------------------------------------------
-- Table: refund_voucher
-- ------------------------------------------------------------------------------

CREATE TABLE `refund_voucher` (
  `refund_voucher_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for refund voucher',
  `voucher_number` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Unique voucher number for tracking and treasury reference',
  `voucher_date` date NOT NULL COMMENT 'Date when voucher was prepared',
  `total_voucher_amount` decimal(19,2) NOT NULL COMMENT 'Total amount covered by this voucher.\nMay include multiple refunds batched together.\n',
  `bank_id` bigint NOT NULL COMMENT 'Foreign key to bank where payment will be made',
  `voucher_status_code` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Status: PREPARED, SENT_TO_TREASURY, EXECUTED, CANCELLED\n',
  `sent_to_treasury_date` date DEFAULT NULL COMMENT 'Date when voucher was sent to treasury',
  `execution_date` date DEFAULT NULL COMMENT 'Date when treasury executed the payment',
  `created_by` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_date` timestamp NOT NULL,
  `modified_by` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `modified_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`refund_voucher_id`),
  UNIQUE KEY `voucher_number` (`voucher_number`),
  KEY `fk_refund_voucher_bank_id` (`bank_id`),
  CONSTRAINT `fk_refund_voucher_bank_id` FOREIGN KEY (`bank_id`) REFERENCES `payment_refund.bank` (`bank_id`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Refund payment vouchers prepared for treasury to execute refund\npayments. Each voucher contains bank transfer instructions.\n\nSIGTAS Source: REFUND.REF_VOUCHER\n';


-- ==============================================================================
-- Schema: accounting
-- ==============================================================================

CREATE DATABASE IF NOT EXISTS `accounting`;
USE `accounting`;

-- ------------------------------------------------------------------------------
-- Table: account_balance
-- ------------------------------------------------------------------------------

CREATE TABLE `account_balance` (
  `account_balance_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for the account balance record',
  `tax_account_id` bigint NOT NULL COMMENT 'Foreign key to tax_account (TIN-Tax Type)',
  `tax_period_id` bigint NOT NULL COMMENT 'Foreign key to tax_period',
  `charge_type_code` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Foreign key to ref_charge_type. Identifies which charge type\nthis balance represents (PRINCIPAL_TAX, PENALTY, INTEREST, etc.).\n',
  `opening_balance` decimal(19,2) NOT NULL COMMENT 'Balance at the start of the period (brought forward from previous\nperiod closing balance). Zero for first period of tax account.\n',
  `debit_amount` decimal(19,2) NOT NULL COMMENT 'Sum of all debit transactions in the period (increases liability).\nIncludes assessments, penalties, interest, adjustments.\n',
  `credit_amount` decimal(19,2) NOT NULL COMMENT 'Sum of all credit transactions in the period (decreases liability).\nIncludes payments, refunds, write-offs, adjustments.\n',
  `closing_balance` decimal(19,2) NOT NULL COMMENT 'Balance at end of period. Calculated as: opening_balance +\ndebit_amount - credit_amount. Positive = owe, Negative = overpaid.\n',
  `is_current` tinyint(1) NOT NULL COMMENT 'Flag indicating if this is the current period balance (true) or\nhistorical period (false). Only one current balance per account\nand charge type.\n',
  `period_closed_date` date DEFAULT NULL COMMENT 'Date when this period was closed. Null if period still open.\nAfter closing, no more transactions can be posted to this period.\n',
  `last_transaction_id` bigint DEFAULT NULL COMMENT 'Foreign key to tax_transaction. References the most recent\ntransaction that affected this balance. Used for audit trail.\n',
  `last_updated_date` timestamp NOT NULL COMMENT 'Timestamp when balance was last updated',
  `created_by` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'User or system that created the balance record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when balance record was created',
  `modified_by` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'User or system that last modified the balance',
  `modified_date` timestamp NULL DEFAULT NULL COMMENT 'Timestamp when balance was last modified',
  PRIMARY KEY (`account_balance_id`),
  KEY `idx_acct_bal_composite` (`tax_account_id`,`tax_period_id`,`charge_type_code`),
  KEY `idx_acct_bal_current_query` (`tax_account_id`,`is_current`)
) ENGINE=InnoDB AUTO_INCREMENT=223 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Maintains current and historical balances for each tax account by\nperiod and charge type. This is a critical table for real-time\nbalance queries and balance forward accounting.\n\nBalance Types:\n- PRINCIPAL: Main tax balance\n- PENALTY: Accumulated penalties';

-- ------------------------------------------------------------------------------
-- Table: account_reconciliation
-- ------------------------------------------------------------------------------

CREATE TABLE `account_reconciliation` (
  `reconciliation_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for reconciliation record',
  `tax_account_id` bigint NOT NULL COMMENT 'Foreign key to tax_account being reconciled',
  `tax_period_id` bigint NOT NULL COMMENT 'Foreign key to tax_period being reconciled',
  `reconciliation_type` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Type of reconciliation: PERIODIC, AD_HOC, YEAR_END, SYSTEM,\nINVESTIGATION.\n',
  `reconciliation_date` date NOT NULL COMMENT 'Date when reconciliation was performed',
  `opening_balance_expected` decimal(19,2) NOT NULL COMMENT 'Expected opening balance from previous period closing balance.\n',
  `opening_balance_actual` decimal(19,2) NOT NULL COMMENT 'Actual opening balance in account_balance table.\n',
  `total_debits_expected` decimal(19,2) NOT NULL COMMENT 'Expected total debits based on summing transactions.\n',
  `total_debits_actual` decimal(19,2) NOT NULL COMMENT 'Actual total debits in account_balance table.\n',
  `total_credits_expected` decimal(19,2) NOT NULL COMMENT 'Expected total credits based on summing transactions.\n',
  `total_credits_actual` decimal(19,2) NOT NULL COMMENT 'Actual total credits in account_balance table.\n',
  `closing_balance_expected` decimal(19,2) NOT NULL COMMENT 'Expected closing balance: opening + debits - credits.\n',
  `closing_balance_actual` decimal(19,2) NOT NULL COMMENT 'Actual closing balance in account_balance table.\n',
  `variance_amount` decimal(19,2) NOT NULL COMMENT 'Difference between expected and actual closing balance.\nZero = reconciled, non-zero = discrepancy.\n',
  `reconciliation_status` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Status: PENDING (not started), IN_PROGRESS (being reconciled),\nRECONCILED (no discrepancy or resolved), DISCREPANCY (unresolved\nvariance), APPROVED (reconciliation approved).\n',
  `variance_explanation` text COLLATE utf8mb4_unicode_ci COMMENT 'Explanation of any variance found. Required if variance is non-zero.\nDocuments the cause and resolution of discrepancy.\n',
  `reconciled_by` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'User who performed the reconciliation',
  `approved_by` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Supervisor who approved the reconciliation',
  `approved_date` date DEFAULT NULL COMMENT 'Date when reconciliation was approved',
  `created_by` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'User or system that created reconciliation record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when reconciliation record was created',
  `modified_by` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'User who last modified reconciliation',
  `modified_date` timestamp NULL DEFAULT NULL COMMENT 'Timestamp when reconciliation was last modified',
  PRIMARY KEY (`reconciliation_id`),
  KEY `idx_recon_composite` (`tax_account_id`,`tax_period_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Tracks account reconciliation activities - comparing calculated balances\nagainst actual balances and resolving discrepancies. Essential for\nmaintaining financial integrity and identifying data quality issues.\n\nReconciliation Types:\n- PERIODIC: Regular sch';

-- ------------------------------------------------------------------------------
-- Table: account_statement
-- ------------------------------------------------------------------------------

CREATE TABLE `account_statement` (
  `statement_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for account statement',
  `tax_account_id` bigint NOT NULL COMMENT 'Foreign key to tax_account',
  `statement_date` date NOT NULL COMMENT 'Date when statement was generated',
  `period_from_date` date NOT NULL COMMENT 'Start date of period covered by statement',
  `period_to_date` date NOT NULL COMMENT 'End date of period covered by statement',
  `opening_balance` decimal(19,2) NOT NULL COMMENT 'Balance at start of statement period',
  `total_debits` decimal(19,2) NOT NULL COMMENT 'Sum of all debit transactions in period',
  `total_credits` decimal(19,2) NOT NULL COMMENT 'Sum of all credit transactions in period',
  `closing_balance` decimal(19,2) NOT NULL COMMENT 'Balance at end of statement period',
  `transaction_count` int NOT NULL COMMENT 'Number of transactions included in statement',
  `statement_format` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Format of statement: PDF, HTML, XML, JSON',
  `statement_file_path` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Path to generated statement file in document storage system.\n',
  `digital_signature` text COLLATE utf8mb4_unicode_ci COMMENT 'Digital signature of statement for authentication and non-repudiation.\n',
  `generation_method` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'How statement was generated: TAXPAYER_REQUEST (self-service),\nAUTOMATIC (system-generated), OFFICER_REQUEST (manual), API (third-party).\n',
  `delivery_method` varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'How statement delivered: PORTAL_DOWNLOAD, EMAIL, POSTAL_MAIL, API\n',
  `delivered_date` timestamp NULL DEFAULT NULL COMMENT 'Timestamp when statement was delivered to taxpayer',
  `created_by` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'User or system that generated the statement',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when statement was generated',
  PRIMARY KEY (`statement_id`),
  KEY `idx_statement_composite` (`tax_account_id`,`statement_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Generates and stores account statements for taxpayers. Account statements\nprovide a complete history of transactions, showing all debits (liabilities)\nand credits (payments) for a tax account over a specified period.\n\nAccount statements are:\n- Generated o';

-- ------------------------------------------------------------------------------
-- Table: fiscal_year
-- ------------------------------------------------------------------------------

CREATE TABLE `fiscal_year` (
  `fiscal_year_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for fiscal year',
  `fiscal_year` int NOT NULL COMMENT 'Calendar year of fiscal year (e.g., 2025). If fiscal year spans\ncalendar years, use year when fiscal year starts.\n',
  `start_date` date NOT NULL COMMENT 'First day of fiscal year',
  `end_date` date NOT NULL COMMENT 'Last day of fiscal year',
  `fiscal_year_status` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Status of fiscal year: PLANNED (not yet started), OPEN (current\nyear, transactions allowed), CLOSING (year-end procedures in\nprogress), CLOSED (locked, no more transactions).\n',
  `closed_date` date DEFAULT NULL COMMENT 'Date when fiscal year was officially closed. Null if still open.\n',
  `description` text COLLATE utf8mb4_unicode_ci COMMENT 'Additional information about this fiscal year',
  `created_by` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'User who created fiscal year record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when fiscal year record was created',
  `modified_by` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'User who last modified fiscal year',
  `modified_date` timestamp NULL DEFAULT NULL COMMENT 'Timestamp when fiscal year was last modified',
  PRIMARY KEY (`fiscal_year_id`),
  UNIQUE KEY `fiscal_year` (`fiscal_year`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Defines fiscal years for accounting purposes. Fiscal year may or may\nnot align with calendar year depending on jurisdiction.\n\nFiscal year management controls:\n- When revenue accounting starts\n- When revenue accounting closes\n- Period structure within fisc';

-- ------------------------------------------------------------------------------
-- Table: opening_balance
-- ------------------------------------------------------------------------------

CREATE TABLE `opening_balance` (
  `opening_balance_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for opening balance record',
  `tax_account_id` bigint NOT NULL COMMENT 'Foreign key to tax_account',
  `tax_period_id` bigint NOT NULL COMMENT 'Foreign key to tax_period (period being opened)',
  `charge_type_code` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Foreign key to ref_charge_type',
  `opening_amount` decimal(19,2) NOT NULL COMMENT 'Opening balance amount for this account/period/charge type.\nPositive = debit (owes), negative = credit (overpaid), zero = cleared.\n',
  `source` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Source of opening balance: CLOSING_BALANCE (from prior period),\nMANUAL_ENTRY (data migration), SYSTEM_GENERATED (first period),\nADJUSTMENT (corrected balance).\n',
  `reference_period_id` bigint DEFAULT NULL COMMENT 'Foreign key to tax_period. If opening balance came from closing\nbalance of previous period, this references that period.\n',
  `created_by` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'User or system that created opening balance',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when opening balance was created',
  `modified_by` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'User or system that last modified opening balance',
  `modified_date` timestamp NULL DEFAULT NULL COMMENT 'Timestamp when opening balance was last modified',
  PRIMARY KEY (`opening_balance_id`),
  KEY `idx_opening_bal_composite` (`tax_account_id`,`tax_period_id`,`charge_type_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Stores opening balances for tax accounts at the start of fiscal years\nor when accounts are first activated. Essential for fiscal year\nmanagement and historical balance tracking.\n\nOpening balances are typically:\n- Brought forward from previous year closing';

-- ------------------------------------------------------------------------------
-- Table: revenue_account
-- ------------------------------------------------------------------------------

CREATE TABLE `revenue_account` (
  `revenue_account_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for revenue account',
  `revenue_account_code` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Unique code for revenue account, typically aligned with government\nchart of accounts or budget codes.\n',
  `revenue_account_name` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Descriptive name for the revenue account',
  `tax_type_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Foreign key to tax_type. Identifies which tax type this revenue\naccount tracks.\n',
  `jurisdiction_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Code identifying jurisdiction that receives this revenue (NATIONAL,\nREGIONAL_XX, MUNICIPAL_XXXX). Supports revenue distribution.\n',
  `budget_account_code` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'External budget account code in government financial management\nsystem where revenue is disbursed.\n',
  `is_active` tinyint(1) NOT NULL COMMENT 'Flag indicating if revenue account is currently active',
  `created_by` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'User who created revenue account',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when revenue account was created',
  `modified_by` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'User who last modified revenue account',
  `modified_date` timestamp NULL DEFAULT NULL COMMENT 'Timestamp when revenue account was last modified',
  PRIMARY KEY (`revenue_account_id`),
  UNIQUE KEY `revenue_account_code` (`revenue_account_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Master table for revenue accounts (government accounts). Each revenue\naccount represents a specific tax type''s revenue tracking for a\njurisdiction (national, regional, municipal).\n\nRevenue accounts track:\n- Expected revenue (from assessments)\n- Collected';

-- ------------------------------------------------------------------------------
-- Table: revenue_balance
-- ------------------------------------------------------------------------------

CREATE TABLE `revenue_balance` (
  `revenue_balance_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for revenue balance record',
  `revenue_account_id` bigint NOT NULL COMMENT 'Foreign key to revenue_account',
  `fiscal_year_id` bigint NOT NULL COMMENT 'Foreign key to fiscal_year',
  `tax_period_id` bigint DEFAULT NULL COMMENT 'Foreign key to tax_period. Allows tracking revenue by period\nwithin fiscal year for detailed analysis.\n',
  `expected_revenue` decimal(19,2) NOT NULL COMMENT 'Total revenue expected based on assessments posted in this\nfiscal year/period.\n',
  `collected_revenue` decimal(19,2) NOT NULL COMMENT 'Total revenue actually collected (payments received) in this\nfiscal year/period.\n',
  `refunded_revenue` decimal(19,2) NOT NULL COMMENT 'Total revenue refunded to taxpayers in this fiscal year/period.\n',
  `written_off_revenue` decimal(19,2) NOT NULL COMMENT 'Total revenue written off as uncollectible in this fiscal\nyear/period.\n',
  `disbursed_revenue` decimal(19,2) NOT NULL COMMENT 'Total revenue transferred to budget accounts (disbursed to\ngovernment) in this fiscal year/period.\n',
  `outstanding_revenue` decimal(19,2) NOT NULL COMMENT 'Revenue still to be collected. Calculated as: expected_revenue -\ncollected_revenue - refunded_revenue - written_off_revenue\n',
  `collection_rate_percent` decimal(5,2) DEFAULT NULL COMMENT 'Collection rate percentage: (collected / expected) * 100.\nKey performance indicator for revenue administration.\n',
  `last_updated_date` timestamp NOT NULL COMMENT 'Timestamp when revenue balance was last updated',
  `created_by` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'User or system that created revenue balance record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when revenue balance record was created',
  `modified_by` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'User or system that last modified revenue balance',
  `modified_date` timestamp NULL DEFAULT NULL COMMENT 'Timestamp when revenue balance was last modified',
  PRIMARY KEY (`revenue_balance_id`),
  KEY `idx_rev_bal_composite` (`revenue_account_id`,`fiscal_year_id`,`tax_period_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Tracks revenue account balances by fiscal year and period. Maintains\nexpected revenue (from assessments), collected revenue (from payments),\nand refunded revenue.\n\nRevenue balance provides real-time visibility into:\n- How much revenue expected for the yea';

-- ------------------------------------------------------------------------------
-- Table: tax_sub_transaction
-- ------------------------------------------------------------------------------

CREATE TABLE `tax_sub_transaction` (
  `sub_transaction_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for the sub-transaction',
  `transaction_id` bigint NOT NULL COMMENT 'Foreign key to parent tax_transaction',
  `charge_type_code` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Foreign key to ref_charge_type. Identifies which charge type\nthis sub-transaction affects (PRINCIPAL_TAX, PENALTY, INTEREST,\nSURCHARGE, FEE).\n',
  `amount` decimal(19,2) NOT NULL COMMENT 'Amount allocated to this charge type. Must be same sign as parent\ntransaction amount.\n',
  `assessment_id` bigint DEFAULT NULL COMMENT 'Foreign key to assessment table (if this sub-transaction is paying\na specific assessment). Used for detailed allocation tracking.\n',
  `created_by` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'User or system process that created the sub-transaction',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when sub-transaction was created',
  PRIMARY KEY (`sub_transaction_id`),
  KEY `idx_sub_trans_composite` (`transaction_id`,`charge_type_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Detail breakdown of tax transactions by charge type. A single\ntransaction may have multiple sub-transactions if it affects\ndifferent charge types (e.g., principal, penalty, and interest).\n\nThis table enables detailed tracking of how amounts are distribute';

-- ------------------------------------------------------------------------------
-- Table: tax_transaction
-- ------------------------------------------------------------------------------

CREATE TABLE `tax_transaction` (
  `transaction_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for the transaction',
  `tax_account_id` bigint NOT NULL COMMENT 'Foreign key to tax_account. Links transaction to specific TIN-Tax Type\ncombination. This is the taxpayer account being debited or credited.\n',
  `tax_period_id` bigint NOT NULL COMMENT 'Foreign key to tax_period. Links transaction to specific period.\nCritical for period-based accounting and balance forward method.\n',
  `transaction_type_code` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Foreign key to ref_transaction_type. Identifies the type of financial\ntransaction (LIABILITY, PAYMENT, REFUND, TRANSFER, WRITE_OFF, etc.).\nDetermines debit vs. credit treatment.\n',
  `charge_type_code` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Foreign key to ref_charge_type. Identifies what is being charged\n(PRINCIPAL_TAX, PENALTY, INTEREST, SURCHARGE, FEE). Used for\nallocation rules and reporting.\n',
  `transaction_date` date NOT NULL COMMENT 'Date when transaction occurred (business date). This is the date\nthe transaction is effective for accounting purposes, not necessarily\nwhen it was recorded in the system.\n',
  `posting_date` date NOT NULL COMMENT 'Date when transaction was posted to the account. May differ from\ntransaction_date if transaction was entered retroactively or\nforward-dated.\n',
  `amount` decimal(19,2) NOT NULL COMMENT 'Transaction amount in local currency. Positive amounts are debits\n(increase liability), negative amounts are credits (reduce liability).\nZero amounts not allowed.\n',
  `balance_after_transaction` decimal(19,2) NOT NULL COMMENT 'Running balance of the account after this transaction was posted.\nPositive = taxpayer owes, Negative = overpayment, Zero = settled.\nComputed when transaction posted.\n',
  `reference_number` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Unique reference number for the transaction. Could be payment receipt\nnumber, assessment number, refund voucher number, etc. Used for\nreconciliation and audit trail.\n',
  `source_document_type` varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Type of source document that generated this transaction (ASSESSMENT,\nPAYMENT_RECEIPT, REFUND_VOUCHER, AUDIT_NOTICE, COLLECTION_ORDER, etc.).\nLinks to document management system.\n',
  `source_document_id` bigint DEFAULT NULL COMMENT 'ID of source document in originating system. Could be assessment_id,\npayment_id, refund_id, etc. Used to trace back to source.\n',
  `reversal_of_transaction_id` bigint DEFAULT NULL COMMENT 'Foreign key to tax_transaction. If this transaction is a reversal\nof a previous transaction, this field contains the ID of the\noriginal transaction being reversed.\n',
  `reversed_by_transaction_id` bigint DEFAULT NULL COMMENT 'Foreign key to tax_transaction. If this transaction has been reversed,\nthis field contains the ID of the reversal transaction. Null if not\nyet reversed.\n',
  `narrative` text COLLATE utf8mb4_unicode_ci COMMENT 'Free-text description of the transaction. Provides context for\nauditing and customer service. Should describe what this transaction\nrepresents in business terms.\n',
  `transaction_status_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Current status of transaction: DRAFT (not yet posted), POSTED\n(posted but can be reversed), FINAL (locked, cannot be reversed),\nREVERSED (has been reversed), ERROR (posting failed).\n',
  `fiscal_year_id` bigint NOT NULL COMMENT 'Foreign key to fiscal_year. Links transaction to fiscal year for\nyear-end closing and reporting.\n',
  `is_automatically_allocated` tinyint(1) NOT NULL COMMENT 'Flag indicating if payment was automatically allocated by the system\nper allocation rules (true) or manually allocated by user (false).\n',
  `allocation_sequence` int DEFAULT NULL COMMENT 'For payment transactions, the order in which payments were allocated\nacross multiple liabilities. Used to determine which liability was\npaid first when payment insufficient for all obligations.\n',
  `created_by` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'User or system process that created the transaction',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when transaction record was created',
  `modified_by` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'User or system process that last modified the transaction',
  `modified_date` timestamp NULL DEFAULT NULL COMMENT 'Timestamp when transaction was last modified',
  PRIMARY KEY (`transaction_id`),
  UNIQUE KEY `reference_number` (`reference_number`),
  KEY `idx_tax_trans_composite` (`tax_account_id`,`tax_period_id`,`transaction_date`),
  KEY `idx_tax_trans_status_date` (`transaction_status_code`,`transaction_date`),
  KEY `idx_tax_trans_fiscal` (`fiscal_year_id`,`transaction_type_code`)
) ENGINE=InnoDB AUTO_INCREMENT=593 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Master table for all financial transactions in the tax system. Every\nfinancial event (assessment, payment, refund, transfer, write-off, etc.)\ngenerates a transaction record. Transactions are immutable once posted\nand can only be reversed by creating offse';


-- ==============================================================================
-- Schema: compliance_control
-- ==============================================================================

CREATE DATABASE IF NOT EXISTS `compliance_control`;
USE `compliance_control`;

-- ------------------------------------------------------------------------------
-- Table: appeal_case
-- ------------------------------------------------------------------------------

CREATE TABLE `appeal_case` (
  `appeal_case_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for appeal case',
  `appeal_number` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Human-readable appeal reference number',
  `objection_case_id` bigint NOT NULL COMMENT 'Original objection case being appealed',
  `party_id` bigint NOT NULL COMMENT 'Party filing the appeal (usually taxpayer)',
  `appeal_level_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Level: APPEALS_BOARD, TAX_TRIBUNAL, HIGH_COURT,\nSUPREME_COURT\n',
  `appeal_status_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Status: FILED, ACCEPTED, HEARING_SCHEDULED, HEARING_HELD,\nUNDER_DELIBERATION, DECISION_ISSUED, CLOSED\n',
  `filing_date` date NOT NULL COMMENT 'Date appeal was filed',
  `amount_at_stake` decimal(19,2) DEFAULT NULL COMMENT 'Total amount in dispute',
  `appeal_grounds` text COLLATE utf8mb4_unicode_ci COMMENT 'Grounds for appeal',
  `hearing_date` date DEFAULT NULL COMMENT 'Date of appeal hearing',
  `decision_date` date DEFAULT NULL COMMENT 'Date decision was issued',
  `decision_outcome_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Decision: APPEAL_UPHELD, APPEAL_DISMISSED,\nREMANDED, SETTLED\n',
  `decision_summary` text COLLATE utf8mb4_unicode_ci COMMENT 'Summary of appeal decision',
  `amount_adjusted` decimal(19,2) DEFAULT NULL COMMENT 'Financial impact of appeal decision',
  `created_by` bigint NOT NULL COMMENT 'User who created the record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when record was created',
  `modified_by` bigint NOT NULL COMMENT 'User who last modified the record',
  `modified_date` timestamp NOT NULL COMMENT 'Timestamp when record was last modified',
  PRIMARY KEY (`appeal_case_id`),
  UNIQUE KEY `appeal_number` (`appeal_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Higher-level appeals to Appeals Board, Commission, Tribunal,\nor courts. Appeals represent escalation of objections when\ntaxpayer disagrees with first-level administrative decision.\n\nAppeal levels (jurisdiction-specific):\n- Appeals Board/Commission (admini';

-- ------------------------------------------------------------------------------
-- Table: audit_case
-- ------------------------------------------------------------------------------

CREATE TABLE `audit_case` (
  `audit_case_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for audit case',
  `case_number` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Human-readable audit case reference number',
  `audit_plan_id` bigint DEFAULT NULL COMMENT 'Link to audit plan under which case was created',
  `party_id` bigint NOT NULL COMMENT 'Taxpayer being audited',
  `tax_account_id` bigint DEFAULT NULL COMMENT 'Tax account under audit',
  `tax_type_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Tax type being audited',
  `audit_type_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Type of audit: COMPREHENSIVE, DESK_AUDIT, FIELD_AUDIT,\nISSUE_ORIENTED, REFUND_VERIFICATION, VAT_VERIFICATION,\nTRANSFER_PRICING, SPECIAL_INVESTIGATION\n',
  `audit_scope_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Scope: FULL_AUDIT, PARTIAL_AUDIT, SINGLE_ISSUE,\nMULTI_TAX, SINGLE_TAX\n',
  `selection_method_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'How case was selected: RISK_BASED, RANDOM,\nMANUAL_SELECTION, THIRD_PARTY_INFO, CAMPAIGN\n',
  `risk_score` decimal(5,2) DEFAULT NULL COMMENT 'Risk score at time of selection (0-100)',
  `audit_period_from` date NOT NULL COMMENT 'Start of period under audit',
  `audit_period_to` date NOT NULL COMMENT 'End of period under audit',
  `case_status_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Current status: SELECTED, ASSIGNED, NOTIFIED, IN_PROGRESS,\nFINDINGS_REVIEW, TAXPAYER_RESPONSE, FINALIZED, CLOSED,\nSUSPENDED, CANCELLED\n',
  `priority_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Case priority: LOW, MEDIUM, HIGH, URGENT',
  `assigned_officer_id` bigint DEFAULT NULL COMMENT 'Primary auditor assigned to case',
  `assignment_date` date DEFAULT NULL COMMENT 'Date case was assigned to auditor',
  `notification_date` date DEFAULT NULL COMMENT 'Date taxpayer was notified of audit',
  `planned_start_date` date DEFAULT NULL COMMENT 'Planned date to begin audit',
  `actual_start_date` date DEFAULT NULL COMMENT 'Actual date audit began',
  `planned_completion_date` date DEFAULT NULL COMMENT 'Planned date to complete audit',
  `actual_completion_date` date DEFAULT NULL COMMENT 'Actual date audit was completed',
  `audit_location_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Where audit conducted: OFFICE_DESK, TAXPAYER_PREMISES,\nREMOTE_DIGITAL, HYBRID\n',
  `estimated_hours` decimal(8,2) DEFAULT NULL COMMENT 'Estimated audit hours required',
  `actual_hours` decimal(8,2) DEFAULT NULL COMMENT 'Actual audit hours spent',
  `adjustment_amount` decimal(19,2) DEFAULT NULL COMMENT 'Total tax adjustment resulting from audit',
  `penalty_amount` decimal(19,2) DEFAULT NULL COMMENT 'Total penalties assessed',
  `interest_amount` decimal(19,2) DEFAULT NULL COMMENT 'Total interest assessed',
  `total_amount_assessed` decimal(19,2) DEFAULT NULL COMMENT 'Total amount assessed (tax + penalty + interest)',
  `taxpayer_agreed` tinyint(1) DEFAULT NULL COMMENT 'Whether taxpayer agreed with audit findings',
  `objection_filed` tinyint(1) DEFAULT NULL COMMENT 'Whether taxpayer filed objection',
  `case_summary` text COLLATE utf8mb4_unicode_ci COMMENT 'Summary of audit findings and outcomes',
  `closure_reason_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Reason for closure: COMPLETED_WITH_ADJUSTMENT,\nCOMPLETED_NO_ADJUSTMENT, CANCELLED_DUPLICATE,\nCANCELLED_OUT_OF_SCOPE, SUSPENDED_PENDING_OBJECTION\n',
  `created_by` bigint NOT NULL COMMENT 'User who created the record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when record was created',
  `modified_by` bigint NOT NULL COMMENT 'User who last modified the record',
  `modified_date` timestamp NOT NULL COMMENT 'Timestamp when record was last modified',
  PRIMARY KEY (`audit_case_id`),
  UNIQUE KEY `case_number` (`case_number`),
  KEY `idx_audit_case_composite` (`case_status_code`,`assigned_officer_id`,`planned_completion_date`),
  KEY `idx_audit_case_dates` (`actual_start_date`,`actual_completion_date`),
  KEY `idx_audit_case_party_tax` (`party_id`,`tax_type_code`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Individual audit cases representing examinations of taxpayer\ncompliance for specific tax periods and tax types. Each audit\ncase tracks the complete audit lifecycle from selection through\ncompletion, including findings, adjustments, and outcomes.\n\nCases ca';

-- ------------------------------------------------------------------------------
-- Table: audit_case_officer
-- ------------------------------------------------------------------------------

CREATE TABLE `audit_case_officer` (
  `audit_case_officer_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for assignment',
  `audit_case_id` bigint NOT NULL COMMENT 'Audit case this officer is assigned to',
  `officer_id` bigint NOT NULL COMMENT 'Staff member assigned to audit',
  `role_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Officer''s role: LEAD_AUDITOR, TEAM_MEMBER, SUPERVISOR,\nTECHNICAL_SPECIALIST, REVIEWER, QUALITY_ASSURANCE\n',
  `assignment_start_date` date NOT NULL COMMENT 'Date officer assigned to case',
  `assignment_end_date` date DEFAULT NULL COMMENT 'Date officer completed work on case',
  `hours_spent` decimal(8,2) DEFAULT NULL COMMENT 'Total hours this officer spent on case',
  `created_by` bigint NOT NULL COMMENT 'User who created the record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when record was created',
  `modified_by` bigint NOT NULL COMMENT 'User who last modified the record',
  `modified_date` timestamp NOT NULL COMMENT 'Timestamp when record was last modified',
  PRIMARY KEY (`audit_case_officer_id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Many-to-many relationship table tracking all officers involved\nin an audit case. While each case has a lead auditor, complex\naudits may involve multiple team members with different roles\n(technical specialists, industry experts, supervisors, reviewers).\n\n';

-- ------------------------------------------------------------------------------
-- Table: audit_finding
-- ------------------------------------------------------------------------------

CREATE TABLE `audit_finding` (
  `audit_finding_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for finding',
  `audit_case_id` bigint NOT NULL COMMENT 'Audit case this finding belongs to',
  `finding_number` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Sequential number within audit case',
  `tax_period_id` bigint DEFAULT NULL COMMENT 'Tax period affected by this finding',
  `finding_category_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Category: UNREPORTED_INCOME, OVERSTATED_EXPENSE,\nMISCLASSIFICATION, CALCULATION_ERROR, MISSING_RECORDS,\nPROCEDURAL_NONCOMPLIANCE, FRAUD_INDICATOR\n',
  `severity_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Severity: MINOR, MODERATE, MAJOR, CRITICAL',
  `finding_description` text COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Detailed description of the finding',
  `legal_basis` text COLLATE utf8mb4_unicode_ci COMMENT 'Tax law or regulation violated or misapplied',
  `taxpayer_position` text COLLATE utf8mb4_unicode_ci COMMENT 'How taxpayer originally treated this issue',
  `auditor_position` text COLLATE utf8mb4_unicode_ci COMMENT 'Correct treatment according to auditor',
  `tax_base_adjustment` decimal(19,2) DEFAULT NULL COMMENT 'Adjustment to taxable base',
  `tax_adjustment` decimal(19,2) NOT NULL COMMENT 'Additional tax or credit',
  `penalty_applicable` tinyint(1) DEFAULT NULL COMMENT 'Whether penalty applies to this finding',
  `penalty_rate` decimal(5,2) DEFAULT NULL COMMENT 'Penalty rate as percentage',
  `penalty_amount` decimal(19,2) DEFAULT NULL COMMENT 'Penalty assessed for this finding',
  `interest_amount` decimal(19,2) DEFAULT NULL COMMENT 'Interest assessed for this finding',
  `taxpayer_response` text COLLATE utf8mb4_unicode_ci COMMENT 'Taxpayer''s response to finding',
  `taxpayer_accepted` tinyint(1) DEFAULT NULL COMMENT 'Whether taxpayer accepted this finding',
  `adjustment_posted` tinyint(1) DEFAULT NULL COMMENT 'Whether adjustment has been posted to account',
  `assessment_id` bigint DEFAULT NULL COMMENT 'Assessment created from this finding',
  `created_by` bigint NOT NULL COMMENT 'User who created the record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when record was created',
  `modified_by` bigint NOT NULL COMMENT 'User who last modified the record',
  `modified_date` timestamp NOT NULL COMMENT 'Timestamp when record was last modified',
  PRIMARY KEY (`audit_finding_id`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Individual findings discovered during audit examination.\nEach finding represents a specific compliance issue, error,\nor discrepancy identified by the auditor. Findings lead to\nadjustments in tax liability, penalties, or recommendations\nfor corrective acti';

-- ------------------------------------------------------------------------------
-- Table: audit_plan
-- ------------------------------------------------------------------------------

CREATE TABLE `audit_plan` (
  `audit_plan_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for audit plan',
  `plan_code` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Human-readable audit plan code',
  `plan_name` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Descriptive name of the audit plan',
  `plan_type_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Type of plan: ANNUAL, QUARTERLY, SPECIAL_PROJECT,\nTARGETED_CAMPAIGN, INDUSTRY_FOCUS\n',
  `plan_period_year` int NOT NULL COMMENT 'Year for which plan applies',
  `plan_period_quarter` int DEFAULT NULL COMMENT 'Quarter (1-4) if applicable',
  `plan_status_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Current status: DRAFT, APPROVED, ACTIVE, SUSPENDED,\nCOMPLETED, CANCELLED\n',
  `target_audit_count` int NOT NULL COMMENT 'Planned number of audits to complete',
  `target_revenue_impact` decimal(19,2) DEFAULT NULL COMMENT 'Expected revenue impact from audits',
  `allocated_resources_count` int DEFAULT NULL COMMENT 'Number of auditors allocated to plan',
  `approved_by` bigint DEFAULT NULL COMMENT 'Officer who approved the plan',
  `approval_date` date DEFAULT NULL COMMENT 'Date plan was approved',
  `plan_description` text COLLATE utf8mb4_unicode_ci COMMENT 'Detailed description of plan objectives and strategy',
  `created_by` bigint NOT NULL COMMENT 'User who created the record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when record was created',
  `modified_by` bigint NOT NULL COMMENT 'User who last modified the record',
  `modified_date` timestamp NOT NULL COMMENT 'Timestamp when record was last modified',
  PRIMARY KEY (`audit_plan_id`),
  UNIQUE KEY `plan_code` (`plan_code`),
  KEY `idx_audit_plan_status` (`plan_status_code`),
  KEY `idx_audit_plan_period` (`plan_period_year`,`plan_period_quarter`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Annual or periodic audit plans that define audit strategies,\ntarget numbers, resources, and priorities for a planning period.\nAudit plans are developed based on risk analysis, compliance\ngaps, revenue risks, and strategic priorities. Each plan covers\na sp';

-- ------------------------------------------------------------------------------
-- Table: collection_case
-- ------------------------------------------------------------------------------

CREATE TABLE `collection_case` (
  `collection_case_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for collection case',
  `case_number` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Human-readable collection case reference',
  `party_id` bigint NOT NULL COMMENT 'Taxpayer with outstanding debt',
  `tax_account_id` bigint NOT NULL COMMENT 'Tax account with outstanding balance',
  `case_type_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Type: STANDARD_COLLECTION, HIGH_VALUE, CRIMINAL_REFERRAL,\nBANKRUPTCY, ESTATE, UNCOLLECTABLE\n',
  `case_status_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Status: NEW, UNDER_REVIEW, ACTIVE_COLLECTION, PAYMENT_PLAN,\nENFORCED_ACTION, SUSPENDED, RESOLVED, CLOSED, WRITTEN_OFF\n',
  `priority_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Collection priority: LOW, MEDIUM, HIGH, URGENT\nBased on age, amount, collectability\n',
  `assigned_officer_id` bigint DEFAULT NULL COMMENT 'Officer assigned to case',
  `assignment_date` date DEFAULT NULL COMMENT 'Date case assigned to officer',
  `case_opened_date` date NOT NULL COMMENT 'Date case was opened',
  `case_closed_date` date DEFAULT NULL COMMENT 'Date case was closed',
  `original_debt_amount` decimal(19,2) NOT NULL COMMENT 'Original debt amount when case opened',
  `current_debt_amount` decimal(19,2) NOT NULL COMMENT 'Current outstanding debt amount',
  `amount_collected` decimal(19,2) DEFAULT NULL COMMENT 'Total amount collected on this case',
  `debt_age_days` int DEFAULT NULL COMMENT 'Number of days debt has been outstanding',
  `collectability_score` decimal(5,2) DEFAULT NULL COMMENT 'Likelihood of successful collection (0-100)',
  `enforcement_level_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Current enforcement level: NOTICE, DEMAND, GARNISHMENT,\nSEIZURE, LEGAL_ACTION\n',
  `has_payment_agreement` tinyint(1) DEFAULT NULL COMMENT 'Whether taxpayer has active payment plan',
  `payment_agreement_id` bigint DEFAULT NULL COMMENT 'Link to active payment agreement',
  `taxpayer_contact_attempted` tinyint(1) DEFAULT NULL COMMENT 'Whether contact with taxpayer attempted',
  `taxpayer_contact_successful` tinyint(1) DEFAULT NULL COMMENT 'Whether contact with taxpayer successful',
  `last_contact_date` date DEFAULT NULL COMMENT 'Date of last contact with taxpayer',
  `next_action_due_date` date DEFAULT NULL COMMENT 'Date next collection action is due',
  `closure_reason_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Reason: PAID_IN_FULL, PAYMENT_PLAN_ACTIVE, BANKRUPTCY,\nUNCOLLECTABLE, STATUTE_EXPIRED, WRITE_OFF\n',
  `case_notes` text COLLATE utf8mb4_unicode_ci COMMENT 'General notes about collection efforts',
  `created_by` bigint NOT NULL COMMENT 'User who created the record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when record was created',
  `modified_by` bigint NOT NULL COMMENT 'User who last modified the record',
  `modified_date` timestamp NOT NULL COMMENT 'Timestamp when record was last modified',
  PRIMARY KEY (`collection_case_id`),
  UNIQUE KEY `case_number` (`case_number`),
  KEY `idx_collection_composite` (`case_status_code`,`priority_code`,`next_action_due_date`),
  KEY `idx_collection_amount_age` (`current_debt_amount`,`debt_age_days`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Individual debt collection cases tracking enforcement actions\nto recover unpaid tax liabilities. Each case represents one\ntaxpayer''s outstanding debt for specific tax types and periods,\nwith comprehensive tracking of all collection activities,\nenforcemen';

-- ------------------------------------------------------------------------------
-- Table: compliance_program
-- ------------------------------------------------------------------------------

CREATE TABLE `compliance_program` (
  `compliance_program_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for compliance program',
  `program_code` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Human-readable program code',
  `program_name` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Descriptive name of compliance program',
  `program_year` int NOT NULL COMMENT 'Fiscal year for program',
  `taxpayer_segment_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Target segment: LARGE_BUSINESS, MEDIUM_BUSINESS,\nSMALL_BUSINESS, MICRO_BUSINESS, INDIVIDUALS,\nHIGH_NET_WORTH, NON_PROFIT, GOVERNMENT\n',
  `tax_type_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Primary tax type if program is tax-specific',
  `compliance_risk_category` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Risk being addressed: NON_REGISTRATION, NON_FILING,\nUNDERREPORTING, EVASION, FRAUD, AVOIDANCE\n',
  `program_objectives` text COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Detailed objectives and goals',
  `treatment_strategy` text COLLATE utf8mb4_unicode_ci COMMENT 'Strategy: SERVICE_FOCUSED, ENFORCEMENT_FOCUSED,\nBALANCED_APPROACH, EDUCATION_CAMPAIGN\n',
  `target_population_size` int DEFAULT NULL COMMENT 'Number of taxpayers in scope',
  `planned_interventions` int DEFAULT NULL COMMENT 'Number of planned compliance interventions',
  `allocated_resources` int DEFAULT NULL COMMENT 'Number of staff allocated',
  `target_revenue_impact` decimal(19,2) DEFAULT NULL COMMENT 'Expected revenue impact',
  `program_status_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Status: PLANNING, APPROVED, ACTIVE, SUSPENDED,\nCOMPLETED, CANCELLED\n',
  `start_date` date DEFAULT NULL COMMENT 'Program start date',
  `end_date` date DEFAULT NULL COMMENT 'Program end date',
  `created_by` bigint NOT NULL COMMENT 'User who created the record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when record was created',
  `modified_by` bigint NOT NULL COMMENT 'User who last modified the record',
  `modified_date` timestamp NOT NULL COMMENT 'Timestamp when record was last modified',
  PRIMARY KEY (`compliance_program_id`),
  UNIQUE KEY `program_code` (`program_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Strategic compliance programs targeting specific taxpayer\nsegments, compliance risks, or tax types. Programs define\nhigh-level compliance strategies, resource allocation, and\nexpected outcomes for a planning period.\n\nPrograms organized around:\n- Taxpayer ';

-- ------------------------------------------------------------------------------
-- Table: enforcement_action
-- ------------------------------------------------------------------------------

CREATE TABLE `enforcement_action` (
  `enforcement_action_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for enforcement action',
  `collection_case_id` bigint NOT NULL COMMENT 'Collection case this action relates to',
  `action_type_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Type of enforcement action: REMINDER_NOTICE, DEMAND_NOTICE,\nPHONE_CONTACT, FIELD_VISIT, REFUND_OFFSET, BANK_GARNISHMENT,\nSALARY_GARNISHMENT, ASSET_SEIZURE, ASSET_SALE, LEGAL_FILING\n',
  `action_date` date NOT NULL COMMENT 'Date action was taken or initiated',
  `action_status_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Status: PLANNED, IN_PROGRESS, COMPLETED, UNSUCCESSFUL,\nCANCELLED, PENDING_APPROVAL\n',
  `performed_by` bigint NOT NULL COMMENT 'Officer who performed the action',
  `target_amount` decimal(19,2) DEFAULT NULL COMMENT 'Amount targeted for collection by this action',
  `amount_collected` decimal(19,2) DEFAULT NULL COMMENT 'Amount actually collected from this action',
  `action_outcome_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Outcome: SUCCESSFUL, PARTIAL, UNSUCCESSFUL,\nTAXPAYER_RESPONSIVE, TAXPAYER_UNRESPONSIVE\n',
  `action_description` text COLLATE utf8mb4_unicode_ci COMMENT 'Detailed description of action taken',
  `taxpayer_response` text COLLATE utf8mb4_unicode_ci COMMENT 'How taxpayer responded to action',
  `document_id` bigint DEFAULT NULL COMMENT 'Document generated or received for this action',
  `next_action_recommended` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Recommended next enforcement step',
  `next_action_due_date` date DEFAULT NULL COMMENT 'Date recommended action should be taken',
  `created_by` bigint NOT NULL COMMENT 'User who created the record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when record was created',
  `modified_by` bigint NOT NULL COMMENT 'User who last modified the record',
  `modified_date` timestamp NOT NULL COMMENT 'Timestamp when record was last modified',
  PRIMARY KEY (`enforcement_action_id`)
) ENGINE=InnoDB AUTO_INCREMENT=38 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Specific enforcement measures taken to collect outstanding debt.\nEach action represents one step in the enforcement process,\nfrom initial notices through to asset seizure and legal proceedings.\n\nAction types progress through enforcement continuum:\n- REMIN';

-- ------------------------------------------------------------------------------
-- Table: objection_case
-- ------------------------------------------------------------------------------

CREATE TABLE `objection_case` (
  `objection_case_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for objection case',
  `case_number` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Human-readable objection case reference',
  `party_id` bigint NOT NULL COMMENT 'Taxpayer filing the objection',
  `objection_type_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Type: ASSESSMENT_OBJECTION, AUDIT_OBJECTION,\nPENALTY_OBJECTION, REGISTRATION_OBJECTION,\nREFUND_DENIAL_OBJECTION, COLLECTION_ACTION_OBJECTION\n',
  `objection_subject_type` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'What is being objected to: ASSESSMENT, AUDIT_CASE,\nPENALTY, REGISTRATION_DECISION, REFUND_DECISION\n',
  `assessment_id` bigint DEFAULT NULL COMMENT 'Assessment being objected to',
  `audit_case_id` bigint DEFAULT NULL COMMENT 'Audit case being objected to',
  `tax_type_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Tax type involved in objection',
  `tax_period_id` bigint DEFAULT NULL COMMENT 'Tax period involved in objection',
  `filing_date` date NOT NULL COMMENT 'Date objection was filed',
  `filing_method_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'How filed: ONLINE_PORTAL, EMAIL, POSTAL_MAIL,\nIN_PERSON, FAX\n',
  `case_status_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Status: FILED, UNDER_REVIEW, ASSIGNED, INVESTIGATION,\nHEARING_SCHEDULED, HEARING_HELD, DECISION_DRAFT,\nDECISION_REVIEW, DECISION_ISSUED, CLOSED, APPEALED\n',
  `priority_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Case priority: LOW, MEDIUM, HIGH, URGENT',
  `assigned_officer_id` bigint DEFAULT NULL COMMENT 'Officer assigned to review objection',
  `assignment_date` date DEFAULT NULL COMMENT 'Date case assigned to officer',
  `amount_disputed` decimal(19,2) NOT NULL COMMENT 'Total amount in dispute',
  `amount_at_stake_tax` decimal(19,2) DEFAULT NULL COMMENT 'Tax amount in dispute',
  `amount_at_stake_penalty` decimal(19,2) DEFAULT NULL COMMENT 'Penalty amount in dispute',
  `amount_at_stake_interest` decimal(19,2) DEFAULT NULL COMMENT 'Interest amount in dispute',
  `objection_grounds` text COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Taxpayer''s grounds/reasons for objection',
  `taxpayer_requested_hearing` tinyint(1) DEFAULT NULL COMMENT 'Whether taxpayer requested hearing',
  `hearing_scheduled_date` timestamp NULL DEFAULT NULL COMMENT 'Date and time of scheduled hearing',
  `hearing_held_date` date DEFAULT NULL COMMENT 'Date hearing was actually held',
  `legal_representative` varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Name of taxpayer''s legal representative',
  `decision_date` date DEFAULT NULL COMMENT 'Date decision was made',
  `decision_outcome_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Outcome: FULLY_ACCEPTED, PARTIALLY_ACCEPTED,\nREJECTED, WITHDRAWN, SETTLED\n',
  `decision_summary` text COLLATE utf8mb4_unicode_ci COMMENT 'Summary of decision and reasoning',
  `amount_adjusted` decimal(19,2) DEFAULT NULL COMMENT 'Amount of relief granted to taxpayer',
  `appeal_filed` tinyint(1) DEFAULT NULL COMMENT 'Whether taxpayer appealed to higher level',
  `appeal_deadline_date` date DEFAULT NULL COMMENT 'Last date taxpayer can file appeal',
  `suspension_of_collection` tinyint(1) DEFAULT NULL COMMENT 'Whether collection suspended during objection',
  `closure_date` date DEFAULT NULL COMMENT 'Date case was closed',
  `created_by` bigint NOT NULL COMMENT 'User who created the record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when record was created',
  `modified_by` bigint NOT NULL COMMENT 'User who last modified the record',
  `modified_date` timestamp NOT NULL COMMENT 'Timestamp when record was last modified',
  PRIMARY KEY (`objection_case_id`),
  UNIQUE KEY `case_number` (`case_number`),
  KEY `idx_objection_composite` (`case_status_code`,`assigned_officer_id`,`decision_date`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Legal proceedings initiated by taxpayers to challenge tax\nadministration decisions. Objections provide due process\nand administrative review before matters proceed to courts.\n\nTaxpayers can object to:\n- Tax assessments (self-assessed or administrative)\n- ';

-- ------------------------------------------------------------------------------
-- Table: taxpayer_risk_profile
-- ------------------------------------------------------------------------------

CREATE TABLE `taxpayer_risk_profile` (
  `taxpayer_risk_profile_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for risk profile',
  `party_id` bigint NOT NULL COMMENT 'Taxpayer this profile relates to',
  `overall_risk_score` decimal(5,2) NOT NULL COMMENT 'Composite risk score (0-100)',
  `risk_rating_code` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Risk category: VERY_LOW, LOW, MEDIUM, HIGH, VERY_HIGH,\nCRITICAL\n',
  `filing_risk_score` decimal(5,2) DEFAULT NULL COMMENT 'Risk score for filing compliance (0-100)',
  `payment_risk_score` decimal(5,2) DEFAULT NULL COMMENT 'Risk score for payment compliance (0-100)',
  `accuracy_risk_score` decimal(5,2) DEFAULT NULL COMMENT 'Risk score for reporting accuracy (0-100)',
  `industry_risk_score` decimal(5,2) DEFAULT NULL COMMENT 'Inherent risk of taxpayer''s industry (0-100)',
  `complexity_risk_score` decimal(5,2) DEFAULT NULL COMMENT 'Risk from business complexity (0-100)',
  `last_audit_date` date DEFAULT NULL COMMENT 'Date of most recent audit',
  `last_audit_outcome_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Outcome: NO_ADJUSTMENT, MINOR_ADJUSTMENT,\nMAJOR_ADJUSTMENT, FRAUD_DETECTED\n',
  `audit_adjustment_history` decimal(19,2) DEFAULT NULL COMMENT 'Total historical audit adjustments',
  `late_filing_count_12m` int DEFAULT NULL COMMENT 'Number of late filings in past 12 months',
  `non_filing_count_12m` int DEFAULT NULL COMMENT 'Number of missed filings in past 12 months',
  `late_payment_count_12m` int DEFAULT NULL COMMENT 'Number of late payments in past 12 months',
  `current_arrears_amount` decimal(19,2) DEFAULT NULL COMMENT 'Current outstanding arrears',
  `has_active_objection` tinyint(1) DEFAULT NULL COMMENT 'Whether taxpayer has active objection',
  `objection_count_history` int DEFAULT NULL COMMENT 'Total number of objections filed historically',
  `third_party_discrepancy_count` int DEFAULT NULL COMMENT 'Number of discrepancies from third-party data',
  `risk_factors` text COLLATE utf8mb4_unicode_ci COMMENT 'Narrative description of key risk factors',
  `treatment_recommendation` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Recommended treatment: ENHANCED_SERVICE, STANDARD_SERVICE,\nINCREASED_MONITORING, AUDIT_SELECTION, ENFORCEMENT_ACTION\n',
  `profile_last_updated` timestamp NOT NULL COMMENT 'When risk profile was last recalculated',
  `created_by` bigint NOT NULL COMMENT 'User who created the record',
  `created_date` timestamp NOT NULL COMMENT 'Timestamp when record was created',
  `modified_by` bigint NOT NULL COMMENT 'User who last modified the record',
  `modified_date` timestamp NOT NULL COMMENT 'Timestamp when record was last modified',
  PRIMARY KEY (`taxpayer_risk_profile_id`),
  UNIQUE KEY `party_id` (`party_id`),
  KEY `idx_risk_composite` (`risk_rating_code`,`overall_risk_score`),
  KEY `idx_risk_filing_payment` (`filing_risk_score`,`payment_risk_score`)
) ENGINE=InnoDB AUTO_INCREMENT=17 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Comprehensive risk profiles for taxpayers, updated dynamically\nbased on behavior, filing patterns, payment history, audit\nresults, and third-party data. Risk profiles drive case\nselection for audits, monitoring intensity, and service levels.\n\nRisk factors';

-- ==============================================================================
-- DDL SUMMARY
-- ==============================================================================
-- Total Schemas: 7
-- Total Tables: 59
--
-- Schema Breakdown:
--   - party: 2 tables (party, individual)
--   - registration: 9 tables (tax accounts, objects, compliance)
--   - tax_framework: 12 tables (tax types, periods, rates, forms)
--   - filing_assessment: 6 tables (returns, assessments, penalties, interest)
--   - payment_refund: 11 tables (payments, refunds, allocations, bank reconciliation)
--   - accounting: 9 tables (balances, transactions, revenue accounts)
--   - compliance_control: 10 tables (audits, collections, objections, appeals, risk)
--
-- Data Generation:
--   All test data can be regenerated using generators in generators/ directory
--   ETL pipelines in etl/ directory load data to ClickHouse L3 warehouse
--
-- ==============================================================================
