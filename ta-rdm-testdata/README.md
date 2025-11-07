# TA-RDM Test Data Generator

**Internal Tool** for generating realistic test data for Tax Administration Reference Data Model (TA-RDM) MySQL L2 database.

## ⚠️ Important Note

This is an **internal development tool** for generating test data. For production ETL operations, use the `ta-rdm-etl` package.

## Overview

Generates comprehensive, realistic test data across all tax administration domains:
- Party management (taxpayers, individuals, enterprises)
- Tax framework (tax types, periods, rates, forms)
- Filing and assessment
- Payments and refunds
- Account balances
- Collections and enforcement
- Audits
- Objections and appeals
- Risk assessments

## Quick Start

### 1. Installation

```bash
cd ta-rdm-testdata

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies (includes Faker)
pip install -r requirements.txt
```

### 2. Configuration

```bash
# Copy environment template
cp ../.env.example .env

# Edit with your MySQL L2 database credentials
nano .env
```

### 3. Generate Test Data

```bash
# Option 1: Generate all test data (recommended)
python main.py

# Option 2: Clean existing data
python cleanup.py

# Option 3: Run individual generators
python generators/party_generator.py
python generators/tax_framework_generator.py
# ... etc
```

## Generated Data

The tool generates:
- **5 parties** (2 individuals, 3 enterprises)
- **10 tax accounts**
- **5 tax types** (VAT, CIT, PIT, Withholding, Excise)
- **90 tax periods** (2023-2025)
- **69 tax returns and assessments**
- **78 payments** (~$4.7M)
- **222 account balance snapshots**
- **7 collection cases with 35 enforcement actions**
- **9 refunds** ($161K)
- **5 audit cases with 8 findings**
- **1 objection case**
- **5 risk assessment profiles**

## Generators

- `reference_generator.py` - Core reference data
- `party_generator.py` - Taxpayers (individuals and enterprises)
- `tax_framework_generator.py` - Tax types, periods, rates
- `filing_assessment_generator.py` - Tax returns and assessments
- `payment_generator.py` - Payment transactions
- `accounting_generator.py` - Account balances
- `collection_generator.py` - Collection cases and enforcement
- `refund_generator.py` - Refund transactions
- `audit_generator.py` - Audit cases and findings
- `objection_generator.py` - Objections and appeals
- `risk_assessment_generator.py` - Risk profiles

## Data Profiles

Test data uses realistic Sri Lankan profiles defined in:
- `config/party_profiles.py` - Individual and enterprise archetypes
- `config/tax_config.py` - Tax rates, rules, and configurations
- `utils/faker_sri_lanka.py` - Sri Lankan locale data generation

## Notes

- Uses deterministic seed (42) for reproducibility
- Maintains referential integrity
- Creates realistic business scenarios
- All amounts in Sri Lankan Rupees (LKR)
- Dates span 2023-2025

## After Data Generation

Once test data is generated, use the `ta-rdm-etl` package to load it into ClickHouse L3:

```bash
cd ../ta-rdm-etl
python etl/l2_to_l3_party.py
# ... run other ETL scripts
python etl/validate_etl.py
```

## Version

Version: 1.0.0
Date: 2025-11-06
