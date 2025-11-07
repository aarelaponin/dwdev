# TA-RDM Test Data Generator

**Phase A: Foundation Validation**

Generates realistic test data for the Tax Administration Reference Data Model (TA-RDM) Layer 2 MySQL database. This enables development and testing of Layer 2 → Layer 3 ETL transformations without dependency on actual source systems.

## Overview

### Phase A Scope

**Phase A.1: Reference Data** (~200 records)
- Countries (1 record: Sri Lanka)
- Regions (9 provinces)
- Districts (10 major districts)
- Localities (20+ major cities)
- Currency (LKR)
- Party types, segments, industry codes
- Risk ratings, status codes
- Filing types, payment methods

**Phase A.2: Party Data** (5 parties)
- 3 individuals with complete profiles
- 2 enterprises (stored as party records)
- Risk profiles for all parties
- Deterministic generation (seed=42)

## Prerequisites

### 1. MySQL Database Setup

Ensure the TA-RDM schema is installed on MySQL (port **3308**):

```bash
# Verify MySQL is running
mysql -h localhost -P 3308 -u root -p

# Check schemas exist
SHOW DATABASES LIKE 'reference';
SHOW DATABASES LIKE 'party';
SHOW DATABASES LIKE 'compliance_control';
```

### 2. Database User Configuration

Create a dedicated user for test data generation:

```sql
-- Create user
CREATE USER 'ta_testdata'@'localhost' IDENTIFIED BY 'your_secure_password';

-- Grant permissions on all 12 schemas
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

FLUSH PRIVILEGES;
```

### 3. Python Environment

Python 3.11+ required.

## Installation

### 1. Install Dependencies

```bash
cd test_data_generator
pip install -r requirements.txt
```

### 2. Configure Database Connection

Create `.env` file from template:

```bash
cp .env.example .env
```

Edit `.env` with your database credentials:

```env
DB_HOST=localhost
DB_PORT=3308
DB_USER=ta_testdata
DB_PASSWORD=your_secure_password_here

LOG_LEVEL=INFO
RANDOM_SEED=42
```

**Important:** Do NOT specify `DB_NAME` - this generator uses schema-qualified table names (e.g., `reference.ref_country`) to access all 12 schemas.

## Usage

### Basic Commands

```bash
# Generate all Phase A data (reference + 5 parties)
python main.py --phase foundation

# Generate reference data only
python main.py --phase reference

# Generate party data only (5 parties)
python main.py --phase party --count 5

# Generate more parties (e.g., 10)
python main.py --phase party --count 10
```

### Cleanup Commands (Re-running Generator)

If you need to regenerate data or encountered errors:

```bash
# Option 1: Python cleanup script (recommended)
python cleanup.py --verify-only    # Check what will be deleted
python cleanup.py                  # Interactive cleanup (asks confirmation)
python cleanup.py --force          # Force cleanup (no confirmation)

# Option 2: Direct SQL cleanup
mysql -h localhost -P 3308 -u ta_testdata -p < cleanup_test_data.sql
```

**After cleanup, re-run the generator:**
```bash
python main.py --phase foundation
```

### Advanced Options

```bash
# Dry run (validate configuration without generating data)
python main.py --phase foundation --dry-run

# Custom random seed
python main.py --phase foundation --seed 123

# Debug logging
python main.py --phase foundation --log-level DEBUG

# Help
python main.py --help
```

## Validation

### After Generation

Run these queries to validate generated data:

```sql
-- Reference data counts
SELECT COUNT(*) FROM reference.ref_country;     -- Should return 1
SELECT COUNT(*) FROM reference.ref_region;      -- Should return 9
SELECT COUNT(*) FROM reference.ref_district;    -- Should return 10
SELECT COUNT(*) FROM reference.ref_locality;    -- Should return ~20

-- Party data counts
SELECT COUNT(*) FROM party.party;               -- Should return 5
SELECT COUNT(*) FROM party.individual;          -- Should return 3
SELECT COUNT(*) FROM compliance_control.taxpayer_risk_profile; -- Should return 5

-- View generated parties
SELECT
    p.party_id,
    p.party_name,
    p.party_type_code,
    i.tax_identification_number AS tin,
    r.risk_rating_code
FROM party.party p
LEFT JOIN party.individual i ON p.party_id = i.party_id
LEFT JOIN compliance_control.taxpayer_risk_profile r ON p.party_id = r.party_id
ORDER BY p.party_id;

-- Check for orphaned records (should return 0)
SELECT COUNT(*)
FROM compliance_control.taxpayer_risk_profile r
WHERE NOT EXISTS (
    SELECT 1 FROM party.party p WHERE p.party_id = r.party_id
);
```

### Expected Results

**Reference Data:**
- 1 country (Sri Lanka)
- 9 regions (provinces)
- 10 districts
- ~20 localities
- 1 currency (LKR)
- 2 party types
- 4 party segments
- 25 industry codes
- 5 risk ratings
- ~15 status codes
- 3 filing types
- 5 payment methods

**Total: ~200 reference records**

**Party Data (5 parties):**
- 3 individuals (with NIC, birth date, TIN)
- 2 enterprises (company names, TIN)
- 5 risk profiles (varied risk ratings)

**Total: ~15 party records**

## Project Structure

```
test_data_generator/
├── config/
│   ├── __init__.py
│   ├── database_config.py      # MySQL connection settings
│   ├── tax_config.py            # Tax rates, rules, Sri Lankan config
│   └── party_profiles.py        # Party archetypes
│
├── generators/
│   ├── __init__.py
│   ├── reference_generator.py   # Phase A.1: Reference data
│   └── party_generator.py       # Phase A.2: Party data
│
├── utils/
│   ├── __init__.py
│   ├── db_utils.py              # Database operations
│   ├── faker_sri_lanka.py       # Custom Sri Lankan data provider
│   └── validators.py            # Data validation
│
├── main.py                      # Main orchestrator
├── requirements.txt             # Python dependencies
├── .env.example                 # Environment template
├── .env                         # Your config (DO NOT COMMIT)
└── README.md                    # This file
```

## Key Design Decisions

1. **Direct SQL, No ORM:** Uses `mysql-connector-python` with direct INSERT statements for simplicity and transparency
2. **Deterministic:** Uses `random.seed(42)` and `Faker.seed(42)` for reproducible results
3. **Transactional:** All inserts wrapped in transactions for rollback capability
4. **Schema-Qualified:** All table names use schema prefix (e.g., `party.party`)
5. **Sri Lankan Context:** Names, addresses, phone numbers, NICs are authentic Sri Lankan format
6. **Foreign Key Order:** Generates reference data before party data to satisfy FK constraints

## Troubleshooting

### Connection Refused

```
Error: Can't connect to MySQL server on 'localhost:3308'
```

**Solution:** Check MySQL is running and port is correct:
```bash
mysql -h localhost -P 3308 -u root -p
```

### Access Denied

```
Error: Access denied for user 'ta_testdata'@'localhost'
```

**Solution:** Verify user permissions:
```sql
SHOW GRANTS FOR 'ta_testdata'@'localhost';
```

### Table Doesn't Exist

```
Error: Table 'reference.ref_country' doesn't exist
```

**Solution:** Verify TA-RDM schema is installed:
```bash
mysql -h localhost -P 3308 -u ta_testdata -p
USE reference;
SHOW TABLES;
```

### Foreign Key Constraint Fails

```
Error: Cannot add or update a child row: a foreign key constraint fails
```

**Solution:** Generate reference data first:
```bash
python main.py --phase reference
python main.py --phase party
```

## Development

### Adding New Reference Tables

1. Add configuration to `config/tax_config.py`
2. Add generator method to `generators/reference_generator.py`
3. Call from `generate_all()` in dependency order

### Adding New Party Types

1. Add profile to `config/party_profiles.py`
2. Update `generators/party_generator.py` to use new profile

### Customizing Sri Lankan Data

Edit `utils/faker_sri_lanka.py` to add more:
- Names, provinces, cities
- NIC patterns, phone formats
- Enterprise naming patterns

## Next Steps (Phase B)

Phase B will add:
- Tax framework data (tax types, periods, rates, forms)
- Registration data (tax accounts, VAT registration)
- Filing data (tax returns for 2023-2024)
- Assessment and penalty data
- Accounting transactions
- Payment and refund data

**Not included in Phase A** - focus is foundation validation only.

## License

Internal use only - Sri Lanka Inland Revenue Department

## Support

For issues or questions, contact the TA-RDM development team.
