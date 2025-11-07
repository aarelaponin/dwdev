# TA-RDM ETL Pipeline

Production-ready ETL pipeline for transforming Tax Administration Reference Data Model (TA-RDM) data from MySQL L2 (operational database) to ClickHouse L3 (data warehouse).

## Overview

This package provides:
- **15 fact table ETL scripts** covering all tax administration domains
- **3 dimension ETL scripts** for core master data
- **Comprehensive validation framework** with 157+ automated checks
- **Production-ready utilities** for database connectivity and error handling

## Quick Start

### 1. Prerequisites

- Python 3.9 or higher
- MySQL 9.0+ (L2 operational database)
- ClickHouse 24.11+ (L3 data warehouse)
- Access credentials for both databases

### 2. Installation

```bash
# Clone or copy this directory
cd ta-rdm-etl

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Configuration

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your database credentials
nano .env  # or your preferred editor
```

### 4. Run ETL

```bash
# Option 1: Run complete ETL pipeline
./scripts/run_full_etl.sh

# Option 2: Run individual ETL scripts
python etl/l2_to_l3_party.py
python etl/l2_to_l3_tax_type.py
# ... etc

# Option 3: Run validation only
python etl/validate_etl.py
```

## Documentation

- **[Installation Guide](docs/INSTALLATION.md)** - Detailed setup instructions
- **[ETL Execution Guide](docs/ETL-EXECUTION.md)** - How to run ETL scripts
- **[Configuration Guide](docs/CONFIGURATION.md)** - Environment configuration
- **[ETL Validation Guide](docs/ETL-Validation-Guide.md)** - Validation framework
- **[L2-L3 Mapping](docs/L2-L3-Mapping.md)** - Data transformation mappings
- **[Troubleshooting](docs/TROUBLESHOOTING.md)** - Common issues and solutions
- **[Scheduling Guide](docs/SCHEDULING.md)** - Automation and scheduling

## ETL Scripts

### Dimensions
- `l2_to_l3_party.py` - Party master data (taxpayers)
- `l2_to_l3_tax_type.py` - Tax type reference data
- `l2_to_l3_tax_period.py` - Tax period calendar
- `generate_dim_time.py` - Time dimension (date calendar)

### Facts
- `l2_to_l3_fact_registration.py` - Tax account registrations
- `l2_to_l3_fact_filing.py` - Tax return filings
- `l2_to_l3_fact_assessment.py` - Tax assessments
- `l2_to_l3_fact_payment.py` - Payment transactions
- `l2_to_l3_fact_account_balance.py` - Account balance snapshots
- `l2_to_l3_fact_collection.py` - Collection and enforcement actions
- `l2_to_l3_fact_refund.py` - Refund transactions
- `l2_to_l3_fact_audit.py` - Audit cases
- `l2_to_l3_fact_objection.py` - Objections and appeals
- `l2_to_l3_fact_risk_assessment.py` - Risk assessment profiles
- `l2_to_l3_fact_taxpayer_activity.py` - Taxpayer activity aggregations

### Validation
- `validate_etl.py` - Comprehensive ETL validation framework

## Validation Framework

The package includes a robust validation framework with 4 validation types:

1. **Row Count Validation** - Ensures data completeness (L2 vs L3)
2. **Referential Integrity** - Validates foreign key relationships
3. **Data Quality** - Checks mandatory fields, formats, ranges
4. **Business Rules** - Validates business logic consistency

Run validation after ETL:
```bash
python etl/validate_etl.py --output-format console  # Default
python etl/validate_etl.py --output-format json > report.json
python etl/validate_etl.py --output-format html > report.html
```

## Directory Structure

```
ta-rdm-etl/
├── etl/                    # ETL scripts
│   ├── l2_to_l3_*.py       # ETL transformation scripts
│   ├── validate_etl.py     # Validation orchestrator
│   └── validators/         # Validation framework
├── config/                 # Database configuration
├── utils/                  # Database utilities
├── docs/                   # Documentation
├── scripts/                # Helper scripts
├── requirements.txt        # Python dependencies
├── .env.example            # Environment template
└── README.md               # This file
```

## Support

For issues, questions, or contributions:
- Review documentation in `docs/` directory
- Check troubleshooting guide for common issues
- Validate configuration with `scripts/test_connections.py`

## License

[Your License Here]

## Version

Version: 1.0.0
Date: 2025-11-06
