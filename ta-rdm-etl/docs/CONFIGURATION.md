# TA-RDM ETL Configuration Guide

Complete reference for configuring the TA-RDM ETL pipeline.

## Overview

The ETL pipeline is configured primarily through environment variables stored in a `.env` file. This guide covers all configuration options, best practices, and environment-specific setups.

## Environment File (.env)

### Location

The `.env` file must be located in the root of the ta-rdm-etl directory:

```
ta-rdm-etl/
├── .env              ← Environment configuration file
├── .env.example      ← Template with all variables
├── etl/
├── utils/
└── ...
```

### Creating Your Configuration

```bash
# Copy template
cp .env.example .env

# Edit with your values
nano .env  # or vim, emacs, VS Code, etc.
```

## Configuration Variables

### MySQL L2 Configuration

Configuration for the source MySQL operational database (Layer 2).

| Variable | Description | Example | Required |
|----------|-------------|---------|----------|
| `DB_HOST` | MySQL hostname or IP address | `localhost`, `mysql.example.com` | Yes |
| `DB_PORT` | MySQL port number | `3308`, `3306` | Yes |
| `DB_USER` | MySQL username | `ta_user`, `etl_reader` | Yes |
| `DB_PASSWORD` | MySQL password | `secure_password_123` | Yes |
| `DB_CHARSET` | Character encoding | `utf8mb4` | No (default: utf8mb4) |
| `DB_CONNECT_TIMEOUT` | Connection timeout in seconds | `30` | No (default: 10) |

**Example:**
```env
DB_HOST=mysql-server.example.com
DB_PORT=3308
DB_USER=ta_user
DB_PASSWORD=MySecurePassword123!
DB_CHARSET=utf8mb4
DB_CONNECT_TIMEOUT=30
```

**MySQL User Privileges Required:**
```sql
-- Minimum required privileges for ETL user
GRANT SELECT ON reference.* TO 'ta_user'@'%';
GRANT SELECT ON party.* TO 'ta_user'@'%';
GRANT SELECT ON registration.* TO 'ta_user'@'%';
GRANT SELECT ON tax_framework.* TO 'ta_user'@'%';
GRANT SELECT ON filing_assessment.* TO 'ta_user'@'%';
GRANT SELECT ON payment_refund.* TO 'ta_user'@'%';
GRANT SELECT ON accounting.* TO 'ta_user'@'%';
GRANT SELECT ON compliance_control.* TO 'ta_user'@'%';
FLUSH PRIVILEGES;
```

### ClickHouse L3 Configuration

Configuration for the target ClickHouse data warehouse (Layer 3).

| Variable | Description | Example | Required |
|----------|-------------|---------|----------|
| `CH_HOST` | ClickHouse hostname or IP | `localhost`, `clickhouse.example.com` | Yes |
| `CH_PORT` | ClickHouse HTTP port | `8123` | Yes |
| `CH_DATABASE` | Target database name | `ta_dw` | Yes |
| `CH_USER` | ClickHouse username | `default`, `etl_writer` | Yes |
| `CH_PASSWORD` | ClickHouse password | `clickhouse_password` | No* |
| `CH_SECURE` | Use HTTPS connection | `true`, `false` | No (default: false) |
| `CH_VERIFY_SSL` | Verify SSL certificates | `true`, `false` | No (default: true) |
| `CH_CONNECT_TIMEOUT` | Connection timeout in seconds | `30` | No (default: 10) |

*Default ClickHouse user has no password by default. Set password in production.

**Example:**
```env
CH_HOST=clickhouse-server.example.com
CH_PORT=8123
CH_DATABASE=ta_dw
CH_USER=default
CH_PASSWORD=ClickHousePassword456!
CH_SECURE=false
CH_VERIFY_SSL=true
CH_CONNECT_TIMEOUT=30
```

**ClickHouse User Privileges Required:**
```sql
-- Minimum required privileges for ETL user
GRANT SELECT, INSERT, TRUNCATE, DROP TABLE ON ta_dw.* TO etl_writer;
```

### Logging Configuration

Control logging behavior and verbosity.

| Variable | Description | Values | Default |
|----------|-------------|--------|---------|
| `LOG_LEVEL` | Logging verbosity | `DEBUG`, `INFO`, `WARNING`, `ERROR` | `INFO` |
| `LOG_FORMAT` | Log message format | `simple`, `detailed` | `simple` |
| `LOG_TO_FILE` | Write logs to file | `true`, `false` | `false` |
| `LOG_FILE_PATH` | Log file location | `/var/log/ta-etl.log` | `./etl.log` |

**Example:**
```env
LOG_LEVEL=INFO
LOG_FORMAT=simple
LOG_TO_FILE=false
LOG_FILE_PATH=./logs/etl.log
```

**Log Levels:**
- `DEBUG`: Detailed diagnostic information (verbose)
- `INFO`: General informational messages (recommended for production)
- `WARNING`: Warning messages for unexpected events
- `ERROR`: Error messages only

**Log Format Examples:**

*Simple format:*
```
2025-11-06 10:30:15 - INFO - Loaded 5 records to L3
```

*Detailed format:*
```
2025-11-06 10:30:15 - etl.l2_to_l3_party - INFO - Loaded 5 records to L3 (dim_party)
```

### ETL Behavior Configuration

Control ETL execution behavior.

| Variable | Description | Values | Default |
|----------|-------------|--------|---------|
| `ETL_BATCH_SIZE` | Records per batch insert | `100`-`10000` | `1000` |
| `ETL_AUTO_VALIDATE` | Run validation after ETL | `true`, `false` | `false` |
| `ETL_STOP_ON_ERROR` | Stop pipeline on first error | `true`, `false` | `true` |
| `ETL_DRY_RUN` | Validate without loading data | `true`, `false` | `false` |
| `ETL_TRUNCATE_BEFORE_LOAD` | Truncate before loading | `true`, `false` | `true` |

**Example:**
```env
ETL_BATCH_SIZE=1000
ETL_AUTO_VALIDATE=false
ETL_STOP_ON_ERROR=true
ETL_DRY_RUN=false
ETL_TRUNCATE_BEFORE_LOAD=true
```

**Batch Size Tuning:**
- **Small batches (100-500)**: Safer, uses less memory, slower
- **Medium batches (1000-2000)**: Balanced, recommended for most cases
- **Large batches (5000-10000)**: Faster, requires more memory

### Performance Configuration

Optimize ETL performance for your environment.

| Variable | Description | Values | Default |
|----------|-------------|--------|---------|
| `ETL_PARALLEL_WORKERS` | Parallel ETL workers | `1`-`10` | `1` |
| `ETL_QUERY_TIMEOUT` | Query timeout in seconds | `60`-`3600` | `300` |
| `ETL_MEMORY_LIMIT_MB` | Memory limit per ETL | `512`-`8192` | `2048` |

**Example:**
```env
ETL_PARALLEL_WORKERS=4
ETL_QUERY_TIMEOUT=600
ETL_MEMORY_LIMIT_MB=4096
```

## Multiple Environments

### Environment-Specific Configuration Files

Maintain separate configurations for different environments:

```bash
# Development environment
.env.dev

# Testing environment
.env.test

# Production environment
.env.prod
```

### Creating Environment Files

```bash
# Create from template
cp .env.example .env.dev
cp .env.example .env.test
cp .env.example .env.prod

# Edit each with environment-specific values
nano .env.dev
nano .env.test
nano .env.prod
```

### Switching Environments

**Method 1: Symlink (Recommended)**

```bash
# Point .env to desired environment
ln -sf .env.prod .env

# Verify
ls -l .env
# Output: .env -> .env.prod
```

**Method 2: Copy**

```bash
# Copy environment-specific file to .env
cp .env.prod .env
```

**Method 3: Environment Variable**

```bash
# Set environment file path
export ETL_ENV_FILE=.env.prod

# Run ETL
python etl/l2_to_l3_party.py
```

### Example Configurations

**Development (.env.dev):**
```env
# Development MySQL (local)
DB_HOST=localhost
DB_PORT=3308
DB_USER=dev_user
DB_PASSWORD=dev_password

# Development ClickHouse (local)
CH_HOST=localhost
CH_PORT=8123
CH_DATABASE=ta_dw_dev

# Development logging (verbose)
LOG_LEVEL=DEBUG
ETL_AUTO_VALIDATE=true
```

**Testing (.env.test):**
```env
# Test MySQL (shared server)
DB_HOST=test-mysql.example.com
DB_PORT=3306
DB_USER=test_user
DB_PASSWORD=test_password

# Test ClickHouse (shared server)
CH_HOST=test-clickhouse.example.com
CH_PORT=8123
CH_DATABASE=ta_dw_test

# Test logging
LOG_LEVEL=INFO
ETL_AUTO_VALIDATE=true
ETL_DRY_RUN=false
```

**Production (.env.prod):**
```env
# Production MySQL (primary)
DB_HOST=prod-mysql.example.com
DB_PORT=3306
DB_USER=etl_reader
DB_PASSWORD=SecureProductionPassword123!

# Production ClickHouse (cluster)
CH_HOST=prod-clickhouse.example.com
CH_PORT=8123
CH_DATABASE=ta_dw
CH_SECURE=true
CH_VERIFY_SSL=true

# Production logging (minimal)
LOG_LEVEL=INFO
LOG_TO_FILE=true
LOG_FILE_PATH=/var/log/ta-etl/etl.log
ETL_STOP_ON_ERROR=true
```

## Security Best Practices

### Password Management

**Never commit .env files to version control:**
```bash
# Add to .gitignore
echo ".env" >> .gitignore
echo ".env.*" >> .gitignore
```

**Use strong passwords:**
- Minimum 12 characters
- Mix of uppercase, lowercase, numbers, symbols
- Different passwords for each environment
- Rotate passwords regularly

**Use secrets management tools (recommended for production):**
```bash
# Example: HashiCorp Vault
vault kv get secret/ta-etl/prod

# Example: AWS Secrets Manager
aws secretsmanager get-secret-value --secret-id ta-etl-prod

# Example: Environment variables from orchestration
# (set in Airflow, Kubernetes, etc.)
```

### File Permissions

Restrict access to configuration files:

```bash
# Secure .env files
chmod 600 .env
chmod 600 .env.*

# Verify
ls -l .env*
# Output: -rw------- 1 user group ... .env
```

### Network Security

**Use encrypted connections in production:**
```env
# MySQL SSL
DB_SSL_ENABLED=true
DB_SSL_CA=/path/to/ca-cert.pem

# ClickHouse HTTPS
CH_SECURE=true
CH_VERIFY_SSL=true
```

**Firewall rules:**
- Restrict database access to ETL server IPs only
- Use VPN or bastion hosts for remote access
- Block public internet access to databases

## Validation

### Test Your Configuration

```bash
# Test database connections
python scripts/test_connections.py

# Expected output:
# ✓ MySQL L2 connection: OK
# ✓ ClickHouse L3 connection: OK
```

### Verify Environment Variables

```bash
# Check if .env is loaded
python -c "
from dotenv import load_dotenv
import os
load_dotenv()
print('DB_HOST:', os.getenv('DB_HOST'))
print('CH_HOST:', os.getenv('CH_HOST'))
"
```

### Configuration Checklist

- [ ] `.env` file exists in ta-rdm-etl/ directory
- [ ] All required variables are set (MySQL and ClickHouse)
- [ ] Database connections tested successfully
- [ ] File permissions secured (chmod 600)
- [ ] `.env` added to .gitignore
- [ ] Separate configs created for dev/test/prod (if needed)
- [ ] Passwords are strong and unique
- [ ] SSL/TLS enabled for production connections

## Troubleshooting

### Configuration Not Loading

**Problem:** ETL scripts can't read .env file

**Solutions:**
```bash
# Verify .env exists
ls -la .env

# Verify .env is in correct directory
pwd  # Should be ta-rdm-etl/
ls -la .env

# Check python-dotenv is installed
pip list | grep python-dotenv
```

### Connection Failures

**Problem:** Database connections fail despite correct credentials

**Solutions:**
```bash
# Test MySQL manually
mysql -h $DB_HOST -P $DB_PORT -u $DB_USER -p

# Test ClickHouse manually
curl http://$CH_HOST:$CH_PORT/ping

# Check environment variables
echo $DB_HOST
echo $CH_HOST
```

### Variable Not Found

**Problem:** ETL script reports missing environment variable

**Solutions:**
```bash
# List all variables in .env
cat .env | grep -v '^#' | grep -v '^$'

# Verify variable name matches exactly
# Variable names are case-sensitive!
```

## See Also

- [INSTALLATION.md](INSTALLATION.md) - Installation and setup
- [ETL-EXECUTION.md](ETL-EXECUTION.md) - Running ETL scripts
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) - Common issues
- [SCHEDULING.md](SCHEDULING.md) - Automation and scheduling
