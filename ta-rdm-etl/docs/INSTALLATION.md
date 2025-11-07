# TA-RDM ETL Installation Guide

Complete setup guide for the TA-RDM ETL pipeline.

## Prerequisites

### Software Requirements

- **Python**: 3.9 or higher
- **MySQL**: 9.0+ (L2 operational database)
- **ClickHouse**: 24.11+ (L3 data warehouse)
- **Operating System**: Linux, macOS, or Windows

### Database Access

You need:
- MySQL connection credentials (host, port, username, password)
- ClickHouse connection credentials (host, port, username, password)
- Network access to both databases

### Minimum Hardware

- **CPU**: 2+ cores
- **RAM**: 4GB minimum, 8GB recommended
- **Disk**: 1GB free space for installation

## Installation Steps

### 1. Copy Package

```bash
# Option A: Download or copy the ta-rdm-etl directory
cp -r ta-rdm-etl /your/target/location/

# Navigate to directory
cd /your/target/location/ta-rdm-etl
```

### 2. Create Virtual Environment

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
# On Linux/macOS:
source venv/bin/activate

# On Windows:
venv\Scripts\activate
```

### 3. Install Dependencies

```bash
# Install required Python packages
pip install --upgrade pip
pip install -r requirements.txt

# Verify installation
pip list
```

Expected packages:
- mysql-connector-python (8.2.0+)
- clickhouse-connect (0.7.0+)
- python-dotenv (1.0.0+)
- colorama (0.4.6+)

### 4. Configure Environment

```bash
# Copy environment template
cp .env.example .env

# Edit configuration file
nano .env  # or vim, emacs, VS Code, etc.
```

**Edit `.env` with your database credentials:**

```env
# MySQL L2 Configuration
DB_HOST=your-mysql-host
DB_PORT=3308
DB_USER=your_mysql_user
DB_PASSWORD=your_secure_password

# ClickHouse L3 Configuration
CH_HOST=your-clickhouse-host
CH_PORT=8123
CH_DATABASE=ta_dw
CH_USER=default
CH_PASSWORD=your_clickhouse_password

# Logging
LOG_LEVEL=INFO
```

### 5. Test Connections

```bash
# Test database connectivity
python scripts/test_connections.py
```

Expected output:
```
✓ MySQL L2 connection successful
✓ ClickHouse L3 connection successful
✓ All database connections OK
```

If connections fail:
- Verify database credentials in `.env`
- Check network connectivity
- Verify databases are running
- Check firewall rules

### 6. Verify Schema

Ensure your databases have the correct schemas:

**MySQL L2 schemas required:**
- party
- registration
- tax_framework
- filing_assessment
- payment_refund
- accounting
- compliance_control

**ClickHouse L3 database:**
- ta_dw (with dim_* and fact_* tables)

Reference DDL files in `docs/`:
- `ta-rdm-mysql-ddl.sql` - MySQL L2 schema
- `ta_dw_clickhouse_schema.sql` - ClickHouse L3 schema

## First Run

### Run Sample ETL

```bash
# Run a simple dimension ETL first
python etl/l2_to_l3_tax_type.py

# Check output for success
# Expected: "ETL Completed Successfully"
```

### Run Full ETL Pipeline

```bash
# Run complete ETL
./scripts/run_full_etl.sh

# Or run manually in order:
python etl/generate_dim_time.py
python etl/l2_to_l3_party.py
python etl/l2_to_l3_tax_type.py
python etl/l2_to_l3_tax_period.py
python etl/l2_to_l3_fact_registration.py
python etl/l2_to_l3_fact_filing.py
python etl/l2_to_l3_fact_assessment.py
python etl/l2_to_l3_fact_payment.py
python etl/l2_to_l3_fact_account_balance.py
python etl/l2_to_l3_fact_collection.py
python etl/l2_to_l3_fact_refund.py
python etl/l2_to_l3_fact_audit.py
python etl/l2_to_l3_fact_objection.py
python etl/l2_to_l3_fact_risk_assessment.py
python etl/l2_to_l3_fact_taxpayer_activity.py
```

### Run Validation

```bash
# Validate ETL results
python etl/validate_etl.py

# Expected: "✓ VALIDATION PASSED - All checks successful"
```

## Configuration Options

### Environment Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| DB_HOST | MySQL hostname | localhost | Yes |
| DB_PORT | MySQL port | 3308 | Yes |
| DB_USER | MySQL username | ta_user | Yes |
| DB_PASSWORD | MySQL password | - | Yes |
| CH_HOST | ClickHouse hostname | localhost | Yes |
| CH_PORT | ClickHouse port | 8123 | Yes |
| CH_DATABASE | ClickHouse database | ta_dw | Yes |
| CH_USER | ClickHouse username | default | Yes |
| CH_PASSWORD | ClickHouse password | - | No |
| LOG_LEVEL | Logging level | INFO | No |
| ETL_BATCH_SIZE | Batch size for inserts | 1000 | No |

### Multiple Environments

For different environments (dev, test, prod), create separate .env files:

```bash
# Development
cp .env.example .env.dev

# Testing
cp .env.example .env.test

# Production
cp .env.example .env.prod
```

Load specific environment:
```bash
# Before running ETL
export ENV_FILE=.env.prod
python etl/l2_to_l3_party.py
```

## Troubleshooting

### Connection Errors

**MySQL Connection Failed:**
```
Error: 2003 Can't connect to MySQL server
```

Solutions:
- Verify MySQL is running: `mysql -h $DB_HOST -P $DB_PORT -u $DB_USER -p`
- Check host/port in `.env`
- Verify firewall allows connection
- Check user has sufficient privileges

**ClickHouse Connection Failed:**
```
Error: Connection refused
```

Solutions:
- Verify ClickHouse is running: `curl http://$CH_HOST:$CH_PORT/ping`
- Check host/port in `.env`
- Verify HTTP interface is enabled
- Check network connectivity

### Import Errors

**Module Not Found:**
```
ModuleNotFoundError: No module named 'mysql'
```

Solutions:
- Activate virtual environment: `source venv/bin/activate`
- Reinstall dependencies: `pip install -r requirements.txt`
- Verify Python version: `python --version` (3.9+)

### Permission Errors

**MySQL Access Denied:**
```
Error: 1045 Access denied for user 'user'@'host'
```

Solutions:
- Verify username/password in `.env`
- Grant privileges: `GRANT ALL ON *.* TO 'user'@'%'`
- Check user can access required schemas

## Uninstallation

```bash
# Deactivate virtual environment
deactivate

# Remove installation directory
cd ..
rm -rf ta-rdm-etl
```

## Next Steps

- Read [ETL-EXECUTION.md](ETL-EXECUTION.md) for execution guide
- Review [ETL-Validation-Guide.md](ETL-Validation-Guide.md) for validation
- Check [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for common issues
- See [SCHEDULING.md](SCHEDULING.md) for automation

## Support

For issues or questions, refer to documentation in `docs/` directory.
