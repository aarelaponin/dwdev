# TA-RDM ETL Troubleshooting Guide

Comprehensive troubleshooting reference for common issues in the TA-RDM ETL pipeline.

## Quick Diagnostic Steps

When encountering issues, follow these steps in order:

1. **Test database connections**: `python scripts/test_connections.py`
2. **Check .env configuration**: Verify all required variables are set
3. **Review recent logs**: Check for error messages
4. **Verify source data**: Ensure L2 MySQL has data
5. **Check disk space**: Ensure sufficient disk space available
6. **Test network connectivity**: Verify databases are reachable

## Connection Issues

### MySQL Connection Failed

#### Symptom 1: Can't connect to MySQL server

```
ERROR - MySQL connection failed
Error: 2003 (HY000): Can't connect to MySQL server on 'host' (111)
```

**Causes and Solutions:**

| Cause | Solution |
|-------|----------|
| MySQL not running | Start MySQL: `systemctl start mysql` or `brew services start mysql` |
| Wrong hostname/IP | Verify `DB_HOST` in `.env`, try `localhost`, `127.0.0.1`, or actual hostname |
| Wrong port | Verify `DB_PORT` in `.env`, default is `3306`, check your MySQL config |
| Firewall blocking | Open port: `sudo ufw allow 3306/tcp` or configure firewall |
| Network unreachable | Test with `ping $DB_HOST`, check VPN connection |

**Diagnostic Commands:**
```bash
# Test MySQL is running
systemctl status mysql  # Linux
brew services list | grep mysql  # macOS

# Test port is open
telnet $DB_HOST $DB_PORT
nc -zv $DB_HOST $DB_PORT

# Test MySQL connection manually
mysql -h $DB_HOST -P $DB_PORT -u $DB_USER -p
```

#### Symptom 2: Access denied for user

```
ERROR - 1045 (28000): Access denied for user 'ta_user'@'host'
```

**Causes and Solutions:**

| Cause | Solution |
|-------|----------|
| Wrong username | Verify `DB_USER` in `.env` |
| Wrong password | Verify `DB_PASSWORD` in `.env` (check for special characters) |
| User doesn't exist | Create user: `CREATE USER 'ta_user'@'%' IDENTIFIED BY 'password';` |
| User lacks privileges | Grant privileges: `GRANT SELECT ON *.* TO 'ta_user'@'%';` |
| Wrong host | Create user for specific host: `CREATE USER 'ta_user'@'specific_host'` |

**Diagnostic Commands:**
```bash
# Test credentials manually
mysql -h $DB_HOST -P $DB_PORT -u $DB_USER -p

# Check user exists (from MySQL)
SELECT User, Host FROM mysql.user WHERE User = 'ta_user';

# Check user privileges
SHOW GRANTS FOR 'ta_user'@'%';
```

#### Symptom 3: Unknown database

```
ERROR - 1049 (42000): Unknown database 'reference'
```

**Solution:**
The MySQL L2 schema is not created. You need to:

1. Create schemas using DDL script
2. Or verify schema names match your MySQL database
3. Check `docs/ta-rdm-mysql-ddl.sql` for required schemas

**Required MySQL Schemas:**
- reference
- party
- registration
- tax_framework
- filing_assessment
- payment_refund
- accounting
- compliance_control

### ClickHouse Connection Failed

#### Symptom 1: Connection refused

```
ERROR - ClickHouse connection failed
Error: HTTPConnectionPool: Max retries exceeded with url
```

**Causes and Solutions:**

| Cause | Solution |
|-------|----------|
| ClickHouse not running | Start ClickHouse: `systemctl start clickhouse-server` |
| Wrong hostname/IP | Verify `CH_HOST` in `.env` |
| Wrong port | Verify `CH_PORT` in `.env`, default HTTP port is `8123` |
| HTTP interface disabled | Enable in config: `<http_port>8123</http_port>` |
| Firewall blocking | Open port: `sudo ufw allow 8123/tcp` |

**Diagnostic Commands:**
```bash
# Test ClickHouse is running
systemctl status clickhouse-server  # Linux
ps aux | grep clickhouse

# Test HTTP interface
curl http://$CH_HOST:$CH_PORT/ping
# Expected output: Ok.

# Test with query
curl "http://$CH_HOST:$CH_PORT/?query=SELECT%20version()"
```

#### Symptom 2: Authentication failed

```
ERROR - Code: 516. DB::Exception: Authentication failed
```

**Causes and Solutions:**

| Cause | Solution |
|-------|----------|
| Wrong username | Verify `CH_USER` in `.env`, default is `default` |
| Wrong password | Verify `CH_PASSWORD` in `.env`, default user may have no password |
| User doesn't exist | Create user in ClickHouse: `CREATE USER etl_user IDENTIFIED BY 'password'` |
| User lacks privileges | Grant privileges: `GRANT SELECT, INSERT ON ta_dw.* TO etl_user` |

**Diagnostic Commands:**
```bash
# Test with curl (no password)
curl "http://$CH_HOST:$CH_PORT/?query=SELECT%201"

# Test with curl (with password)
curl "http://$CH_USER:$CH_PASSWORD@$CH_HOST:$CH_PORT/?query=SELECT%201"
```

#### Symptom 3: Database doesn't exist

```
ERROR - Code: 81. DB::Exception: Database ta_dw doesn't exist
```

**Solution:**
Create the ClickHouse database:

```sql
CREATE DATABASE IF NOT EXISTS ta_dw;
```

Then create dimension and fact tables using `docs/ta_dw_clickhouse_schema.sql`

## Installation Issues

### ModuleNotFoundError

#### Symptom: Module not found

```
ModuleNotFoundError: No module named 'mysql'
ModuleNotFoundError: No module named 'clickhouse_connect'
```

**Causes and Solutions:**

| Cause | Solution |
|-------|----------|
| Dependencies not installed | Run: `pip install -r requirements.txt` |
| Wrong Python environment | Activate venv: `source venv/bin/activate` |
| Wrong Python version | Verify Python 3.9+: `python --version` |
| Pip cache issue | Clear cache: `pip cache purge && pip install -r requirements.txt` |

**Diagnostic Commands:**
```bash
# Check which Python is active
which python
python --version

# Check if venv is activated
echo $VIRTUAL_ENV  # Should show path to venv

# List installed packages
pip list

# Verify required packages
pip list | grep mysql-connector-python
pip list | grep clickhouse-connect
```

### ImportError

#### Symptom: Cannot import module

```
ImportError: cannot import name 'get_db_connection' from 'utils.db_utils'
```

**Causes and Solutions:**

| Cause | Solution |
|-------|----------|
| Wrong working directory | Run from ta-rdm-etl/: `cd ta-rdm-etl && python etl/...` |
| Missing utils/ directory | Verify ta-rdm-etl/utils/ exists with db_utils.py |
| Python path issue | Set PYTHONPATH: `export PYTHONPATH=/path/to/ta-rdm-etl` |

**Diagnostic Commands:**
```bash
# Check current directory
pwd  # Should be: .../ta-rdm-etl

# Verify directory structure
ls -la utils/
ls -la etl/
ls -la config/

# Check Python path
python -c "import sys; print('\n'.join(sys.path))"
```

## ETL Execution Issues

### No Data Loaded

#### Symptom: ETL completes but no rows loaded

```
INFO - Loaded 0 records to L3
INFO - ETL Completed Successfully
```

**Causes and Solutions:**

| Cause | Solution |
|-------|----------|
| No data in L2 source | Verify MySQL L2 has data: `SELECT COUNT(*) FROM party.party;` |
| ETL filters exclude all data | Check ETL script for WHERE clauses filtering out data |
| Date range issues | Verify date filters in ETL match data date ranges |
| Dimensions not loaded | Load dimensions first before fact tables |

**Diagnostic Commands:**
```bash
# Check L2 source data counts
mysql -h $DB_HOST -u $DB_USER -p -e "
SELECT
    'party' as table_name, COUNT(*) as count FROM party.party
    UNION
    SELECT 'filing_return', COUNT(*) FROM filing_assessment.filing_return
    UNION
    SELECT 'payment', COUNT(*) FROM payment_refund.payment;
"

# Check L3 target data counts
curl "http://$CH_HOST:$CH_PORT/?query=SELECT%20COUNT(*)%20FROM%20ta_dw.dim_party"
curl "http://$CH_HOST:$CH_PORT/?query=SELECT%20COUNT(*)%20FROM%20ta_dw.fact_filing"
```

### Validation Failures

#### Symptom 1: Row count mismatch

```
✗ Row Count Validation Failed: fact_filing
Expected: 69 rows (L2 source)
Actual: 65 rows (L3 target)
Ratio: 0.94 (expected: 1.00)
```

**Causes and Solutions:**

| Cause | Solution |
|-------|----------|
| Dimension lookup failed | Check for -1 keys in fact table (lookup failures) |
| ETL filters | Review ETL script for WHERE clauses |
| Duplicate handling | Check if ETL deduplicates records |
| Data changed between runs | Re-run both source count and ETL |

**Diagnostic Steps:**
```bash
# Check for lookup failures (dim keys = -1)
curl "http://$CH_HOST:$CH_PORT/?query=
SELECT COUNT(*) as failed_lookups
FROM ta_dw.fact_filing
WHERE dim_party_key = -1 OR dim_tax_type_key = -1
"

# Compare L2 vs L3 by key
mysql -e "SELECT filing_return_id FROM filing_assessment.filing_return ORDER BY filing_return_id"
curl "http://$CH_HOST:$CH_PORT/?query=SELECT filing_id FROM ta_dw.fact_filing ORDER BY filing_id"
```

#### Symptom 2: Referential integrity failure

```
✗ Referential Integrity Failed: fact_filing.dim_party_key
Found 5 orphaned records (FK references non-existent dimension record)
```

**Causes and Solutions:**

| Cause | Solution |
|-------|----------|
| Dimensions not loaded | Load dimensions first: `python etl/l2_to_l3_party.py` |
| Dimension data incomplete | Verify all parties exist in dim_party |
| Lookup logic error | Check ETL script dimension lookup queries |

**Fix:**
```bash
# Re-load dimension
python etl/l2_to_l3_party.py

# Re-load fact table
python etl/l2_to_l3_fact_filing.py

# Re-validate
python etl/validate_etl.py
```

#### Symptom 3: Mandatory field NULL

```
✗ Mandatory Field Failed: fact_payment.payment_amount
Found 3 NULL values in mandatory field
```

**Causes and Solutions:**

| Cause | Solution |
|-------|----------|
| NULL data in L2 source | Check source: `SELECT * FROM payment WHERE amount IS NULL` |
| ETL transformation error | Review ETL script transformation logic |
| Data quality issue | Fix L2 data quality, add validation before ETL |

### Performance Issues

#### Symptom: ETL takes too long

```
INFO - Duration: 1847.23 seconds
```

**Causes and Solutions:**

| Cause | Solution |
|-------|----------|
| Large dataset | Reduce ETL_BATCH_SIZE in `.env` |
| Missing indexes | Add indexes on join columns in MySQL L2 |
| Network latency | Move ETL closer to databases, check network speed |
| Resource constraints | Check CPU, memory, disk I/O on ETL server |
| Slow queries | Review ETL query performance with EXPLAIN |

**Optimization Tips:**
```env
# Increase batch size for better throughput
ETL_BATCH_SIZE=5000

# Parallel execution (if independent)
python etl/l2_to_l3_fact_registration.py &
python etl/l2_to_l3_fact_filing.py &
wait
```

**Monitor Resources:**
```bash
# Check CPU and memory
htop

# Check disk I/O
iostat -x 1

# Check network
iftop

# Check MySQL slow queries
mysql -e "SHOW PROCESSLIST;"

# Check ClickHouse queries
curl "http://$CH_HOST:$CH_PORT/?query=SELECT%20*%20FROM%20system.processes"
```

### Duplicate Key Errors

#### Symptom: Primary key violation

```
ERROR - Code: 119. DB::Exception: Duplicate entry for primary key
```

**Causes and Solutions:**

| Cause | Solution |
|-------|----------|
| ETL ran multiple times | TRUNCATE table and re-run: `TRUNCATE TABLE ta_dw.fact_filing` |
| Source data duplicates | Check for duplicates in L2 source data |
| ETL not truncating | Verify `ETL_TRUNCATE_BEFORE_LOAD=true` in `.env` |

**Fix:**
```sql
-- Manually truncate and reload
TRUNCATE TABLE ta_dw.fact_filing;
```

```bash
# Re-run ETL
python etl/l2_to_l3_fact_filing.py
```

## Schema Issues

### Table Doesn't Exist

#### Symptom: Table not found

```
ERROR - Code: 60. DB::Exception: Table ta_dw.fact_filing doesn't exist
```

**Solution:**
Create ClickHouse L3 schema:

```bash
# Apply ClickHouse DDL
clickhouse-client --host $CH_HOST --port 9000 < docs/ta_dw_clickhouse_schema.sql

# Or create tables manually
```

### Column Doesn't Exist

#### Symptom: Unknown column

```
ERROR - Code: 47. DB::Exception: Unknown identifier 'column_name'
```

**Causes and Solutions:**

| Cause | Solution |
|-------|----------|
| Schema version mismatch | Verify ClickHouse schema matches ETL expectations |
| Column renamed | Update ETL script with correct column name |
| Wrong table | Check table name in ETL script |

**Diagnostic:**
```sql
-- Check table schema
DESCRIBE ta_dw.fact_filing;

-- Show CREATE statement
SHOW CREATE TABLE ta_dw.fact_filing;
```

## Environment Issues

### .env File Not Found

#### Symptom: Configuration not loaded

```
ERROR - .env file not found
KeyError: 'DB_HOST'
```

**Solution:**
```bash
# Verify .env exists
ls -la .env

# Create from template if missing
cp .env.example .env

# Edit with your values
nano .env
```

### Wrong Directory

#### Symptom: Files not found

```
FileNotFoundError: [Errno 2] No such file or directory: 'etl/l2_to_l3_party.py'
```

**Solution:**
```bash
# Always run from ta-rdm-etl/ directory
cd /path/to/ta-rdm-etl
python etl/l2_to_l3_party.py
```

## Logging Issues

### No Log Output

#### Symptom: No logs printed

**Solution:**
```bash
# Set log level to INFO or DEBUG
export LOG_LEVEL=DEBUG
python etl/l2_to_l3_party.py

# Or in .env
LOG_LEVEL=DEBUG
```

### Too Much Log Output

#### Symptom: Excessive logging

**Solution:**
```bash
# Set log level to WARNING or ERROR
export LOG_LEVEL=WARNING
python etl/l2_to_l3_party.py
```

## Data Quality Issues

### Unexpected NULL Values

**Check source data:**
```sql
-- Find NULLs in source
SELECT * FROM party.party WHERE party_name IS NULL;
SELECT * FROM filing_assessment.filing_return WHERE filing_date IS NULL;
```

**Add validation:**
```sql
-- Count NULLs
SELECT
    COUNT(*) as total_rows,
    SUM(CASE WHEN party_name IS NULL THEN 1 ELSE 0 END) as null_names
FROM party.party;
```

### Data Type Mismatches

**Check data types:**
```sql
-- MySQL L2
DESCRIBE party.party;

-- ClickHouse L3
DESCRIBE ta_dw.dim_party;
```

## Getting Help

### Diagnostic Information to Collect

When reporting issues, include:

1. **Error message** (full stack trace)
2. **ETL script name** (e.g., l2_to_l3_party.py)
3. **Environment** (dev/test/prod)
4. **Database versions**:
   ```bash
   mysql --version
   clickhouse-server --version
   ```
5. **Python version**: `python --version`
6. **Package versions**: `pip list`
7. **Recent logs** (last 50 lines)
8. **Configuration** (sanitized .env, remove passwords!)

### Quick Diagnostic Script

```bash
#!/bin/bash
# diagnostic.sh - Collect diagnostic information

echo "=== TA-RDM ETL Diagnostics ==="
echo ""
echo "Date: $(date)"
echo "Hostname: $(hostname)"
echo ""

echo "=== Python Environment ==="
python --version
which python
echo "VIRTUAL_ENV: $VIRTUAL_ENV"
echo ""

echo "=== Installed Packages ==="
pip list | grep -E "(mysql|clickhouse|dotenv)"
echo ""

echo "=== Database Connectivity ==="
python scripts/test_connections.py
echo ""

echo "=== Directory Structure ==="
ls -la
echo ""

echo "=== Environment Variables (sanitized) ==="
cat .env | grep -v PASSWORD | grep -v '^#' | grep -v '^$'
echo ""
```

Run: `bash diagnostic.sh > diagnostic_output.txt`

## See Also

- [INSTALLATION.md](INSTALLATION.md) - Setup and installation
- [CONFIGURATION.md](CONFIGURATION.md) - Configuration options
- [ETL-EXECUTION.md](ETL-EXECUTION.md) - Running ETL scripts
- [ETL-Validation-Guide.md](ETL-Validation-Guide.md) - Validation details
