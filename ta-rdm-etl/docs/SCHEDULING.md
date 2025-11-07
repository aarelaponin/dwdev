# TA-RDM ETL Scheduling Guide

Guide for automating and scheduling the TA-RDM ETL pipeline.

## Overview

The TA-RDM ETL pipeline is designed to be run on a regular schedule to keep the ClickHouse L3 data warehouse synchronized with MySQL L2 operational data.

**Common Schedules:**
- **Nightly**: Run once per day during off-peak hours (recommended for most cases)
- **Hourly**: Run every hour for near real-time analytics
- **Weekly**: Run weekly for low-change data or development environments

## Scheduling Methods

### Method 1: Cron Jobs (Linux/macOS)

The simplest scheduling method for Unix-based systems.

#### Basic Cron Setup

```bash
# Edit crontab
crontab -e

# Add ETL job (runs daily at 2:00 AM)
0 2 * * * cd /path/to/ta-rdm-etl && source venv/bin/activate && ./scripts/run_full_etl.sh >> /var/log/ta-etl.log 2>&1
```

#### Cron Schedule Examples

```bash
# Every day at 2:00 AM
0 2 * * * /path/to/ta-rdm-etl/scripts/run_full_etl.sh

# Every 6 hours
0 */6 * * * /path/to/ta-rdm-etl/scripts/run_full_etl.sh

# Every Monday at 3:00 AM
0 3 * * 1 /path/to/ta-rdm-etl/scripts/run_full_etl.sh

# Every weekday (Mon-Fri) at 2:00 AM
0 2 * * 1-5 /path/to/ta-rdm-etl/scripts/run_full_etl.sh

# Every hour
0 * * * * /path/to/ta-rdm-etl/scripts/run_full_etl.sh

# Every 15 minutes
*/15 * * * * /path/to/ta-rdm-etl/scripts/run_full_etl.sh
```

#### Complete Cron Script

Create a wrapper script for better logging and error handling:

**`/path/to/ta-rdm-etl/scripts/run_scheduled_etl.sh`:**
```bash
#!/bin/bash
#
# Scheduled ETL Wrapper Script
# Add to crontab: 0 2 * * * /path/to/ta-rdm-etl/scripts/run_scheduled_etl.sh
#

# Configuration
ETL_HOME="/path/to/ta-rdm-etl"
LOG_DIR="$ETL_HOME/logs"
LOG_FILE="$LOG_DIR/etl-$(date +%Y%m%d-%H%M%S).log"
VENV_PATH="$ETL_HOME/venv"
MAX_LOG_FILES=30  # Keep last 30 log files

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Log start
echo "========================================" | tee -a "$LOG_FILE"
echo "ETL Started: $(date)" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# Change to ETL directory
cd "$ETL_HOME" || {
    echo "ERROR: Cannot change to ETL directory: $ETL_HOME" | tee -a "$LOG_FILE"
    exit 1
}

# Activate virtual environment
source "$VENV_PATH/bin/activate" || {
    echo "ERROR: Cannot activate virtual environment: $VENV_PATH" | tee -a "$LOG_FILE"
    exit 1
}

# Run ETL pipeline
./scripts/run_full_etl.sh >> "$LOG_FILE" 2>&1
ETL_EXIT_CODE=$?

# Log completion
echo "" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"
echo "ETL Finished: $(date)" | tee -a "$LOG_FILE"
echo "Exit Code: $ETL_EXIT_CODE" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"

# Cleanup old logs
find "$LOG_DIR" -name "etl-*.log" -type f -mtime +$MAX_LOG_FILES -delete

# Send notification on failure
if [ $ETL_EXIT_CODE -ne 0 ]; then
    # Email notification (requires mail command)
    echo "ETL pipeline failed. See log: $LOG_FILE" | \
        mail -s "TA-RDM ETL Failed" admin@example.com

    # Or Slack notification (requires curl and webhook)
    # curl -X POST -H 'Content-type: application/json' \
    #     --data "{\"text\":\"TA-RDM ETL failed. Check logs.\"}" \
    #     https://hooks.slack.com/services/YOUR/WEBHOOK/URL
fi

exit $ETL_EXIT_CODE
```

Make it executable:
```bash
chmod +x /path/to/ta-rdm-etl/scripts/run_scheduled_etl.sh
```

Add to crontab:
```bash
crontab -e

# Add:
0 2 * * * /path/to/ta-rdm-etl/scripts/run_scheduled_etl.sh
```

### Method 2: Apache Airflow (Recommended for Enterprise)

Apache Airflow provides advanced scheduling, monitoring, and dependency management.

#### Airflow DAG Definition

Create an Airflow DAG for the ETL pipeline:

**`dags/ta_rdm_etl_dag.py`:**
```python
"""
TA-RDM ETL Pipeline DAG

Schedules and monitors the complete L2 to L3 ETL pipeline.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 1),
    'email': ['data-team@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'ta_rdm_etl_pipeline',
    default_args=default_args,
    description='TA-RDM L2 to L3 ETL Pipeline',
    schedule_interval='0 2 * * *',  # Daily at 2:00 AM
    catchup=False,
    tags=['tax', 'etl', 'data-warehouse'],
)

# ETL base path
ETL_PATH = '/path/to/ta-rdm-etl'
PYTHON_BIN = f'{ETL_PATH}/venv/bin/python'

# Task: Test connections
test_connections = BashOperator(
    task_id='test_connections',
    bash_command=f'cd {ETL_PATH} && {PYTHON_BIN} scripts/test_connections.py',
    dag=dag,
)

# Task Group: Dimensions
with TaskGroup('dimensions', tooltip='Load dimension tables', dag=dag) as dimensions:
    dim_time = BashOperator(
        task_id='dim_time',
        bash_command=f'cd {ETL_PATH} && {PYTHON_BIN} etl/generate_dim_time.py',
    )

    dim_party = BashOperator(
        task_id='dim_party',
        bash_command=f'cd {ETL_PATH} && {PYTHON_BIN} etl/l2_to_l3_party.py',
    )

    dim_tax_type = BashOperator(
        task_id='dim_tax_type',
        bash_command=f'cd {ETL_PATH} && {PYTHON_BIN} etl/l2_to_l3_tax_type.py',
    )

    dim_tax_period = BashOperator(
        task_id='dim_tax_period',
        bash_command=f'cd {ETL_PATH} && {PYTHON_BIN} etl/l2_to_l3_tax_period.py',
    )

    # Dimensions can run in parallel
    [dim_time, dim_party, dim_tax_type, dim_tax_period]

# Task Group: Base Facts
with TaskGroup('base_facts', tooltip='Load base fact tables', dag=dag) as base_facts:
    fact_registration = BashOperator(
        task_id='fact_registration',
        bash_command=f'cd {ETL_PATH} && {PYTHON_BIN} etl/l2_to_l3_fact_registration.py',
    )

    fact_filing = BashOperator(
        task_id='fact_filing',
        bash_command=f'cd {ETL_PATH} && {PYTHON_BIN} etl/l2_to_l3_fact_filing.py',
    )

    fact_assessment = BashOperator(
        task_id='fact_assessment',
        bash_command=f'cd {ETL_PATH} && {PYTHON_BIN} etl/l2_to_l3_fact_assessment.py',
    )

    fact_payment = BashOperator(
        task_id='fact_payment',
        bash_command=f'cd {ETL_PATH} && {PYTHON_BIN} etl/l2_to_l3_fact_payment.py',
    )

    fact_account_balance = BashOperator(
        task_id='fact_account_balance',
        bash_command=f'cd {ETL_PATH} && {PYTHON_BIN} etl/l2_to_l3_fact_account_balance.py',
    )

    # Base facts can run in parallel
    [fact_registration, fact_filing, fact_assessment, fact_payment, fact_account_balance]

# Task Group: Compliance Facts
with TaskGroup('compliance_facts', tooltip='Load compliance fact tables', dag=dag) as compliance_facts:
    fact_collection = BashOperator(
        task_id='fact_collection',
        bash_command=f'cd {ETL_PATH} && {PYTHON_BIN} etl/l2_to_l3_fact_collection.py',
    )

    fact_refund = BashOperator(
        task_id='fact_refund',
        bash_command=f'cd {ETL_PATH} && {PYTHON_BIN} etl/l2_to_l3_fact_refund.py',
    )

    fact_audit = BashOperator(
        task_id='fact_audit',
        bash_command=f'cd {ETL_PATH} && {PYTHON_BIN} etl/l2_to_l3_fact_audit.py',
    )

    fact_objection = BashOperator(
        task_id='fact_objection',
        bash_command=f'cd {ETL_PATH} && {PYTHON_BIN} etl/l2_to_l3_fact_objection.py',
    )

    # Compliance facts can run in parallel
    [fact_collection, fact_refund, fact_audit, fact_objection]

# Task Group: Analytical Facts
with TaskGroup('analytical_facts', tooltip='Load analytical fact tables', dag=dag) as analytical_facts:
    fact_risk_assessment = BashOperator(
        task_id='fact_risk_assessment',
        bash_command=f'cd {ETL_PATH} && {PYTHON_BIN} etl/l2_to_l3_fact_risk_assessment.py',
    )

    fact_taxpayer_activity = BashOperator(
        task_id='fact_taxpayer_activity',
        bash_command=f'cd {ETL_PATH} && {PYTHON_BIN} etl/l2_to_l3_fact_taxpayer_activity.py',
    )

    # Analytical facts run sequentially (taxpayer_activity depends on other facts)
    fact_risk_assessment >> fact_taxpayer_activity

# Task: Validation
validate_etl = BashOperator(
    task_id='validate_etl',
    bash_command=f'cd {ETL_PATH} && {PYTHON_BIN} etl/validate_etl.py',
    dag=dag,
)

# Task dependencies
test_connections >> dimensions >> base_facts >> compliance_facts >> analytical_facts >> validate_etl
```

#### Deploy to Airflow

```bash
# Copy DAG to Airflow DAGs folder
cp dags/ta_rdm_etl_dag.py $AIRFLOW_HOME/dags/

# Test DAG
airflow dags test ta_rdm_etl_pipeline

# Trigger DAG manually
airflow dags trigger ta_rdm_etl_pipeline

# View DAG status
airflow dags list
airflow tasks list ta_rdm_etl_pipeline
```

### Method 3: Windows Task Scheduler

For Windows environments, use Task Scheduler.

#### PowerShell Script

**`scripts/run_scheduled_etl.ps1`:**
```powershell
# TA-RDM ETL Scheduled Task Script
# For Windows Task Scheduler

$ETL_HOME = "C:\path\to\ta-rdm-etl"
$LOG_DIR = "$ETL_HOME\logs"
$LOG_FILE = "$LOG_DIR\etl-$(Get-Date -Format 'yyyyMMdd-HHmmss').log"
$VENV_PATH = "$ETL_HOME\venv\Scripts\activate.ps1"

# Create log directory
New-Item -ItemType Directory -Force -Path $LOG_DIR | Out-Null

# Log start
"========================================" | Tee-Object -FilePath $LOG_FILE -Append
"ETL Started: $(Get-Date)" | Tee-Object -FilePath $LOG_FILE -Append
"========================================" | Tee-Object -FilePath $LOG_FILE -Append

# Change to ETL directory
Set-Location $ETL_HOME

# Activate virtual environment
& $VENV_PATH

# Run ETL pipeline
& "$ETL_HOME\scripts\run_full_etl.sh" 2>&1 | Tee-Object -FilePath $LOG_FILE -Append
$EXIT_CODE = $LASTEXITCODE

# Log completion
"========================================" | Tee-Object -FilePath $LOG_FILE -Append
"ETL Finished: $(Get-Date)" | Tee-Object -FilePath $LOG_FILE -Append
"Exit Code: $EXIT_CODE" | Tee-Object -FilePath $LOG_FILE -Append
"========================================" | Tee-Object -FilePath $LOG_FILE -Append

# Cleanup old logs (keep last 30 days)
Get-ChildItem -Path $LOG_DIR -Filter "etl-*.log" |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-30) } |
    Remove-Item

exit $EXIT_CODE
```

#### Create Scheduled Task

```powershell
# Create scheduled task (run as Administrator)
$Action = New-ScheduledTaskAction -Execute "PowerShell.exe" `
    -Argument "-ExecutionPolicy Bypass -File C:\path\to\ta-rdm-etl\scripts\run_scheduled_etl.ps1"

$Trigger = New-ScheduledTaskTrigger -Daily -At 2:00AM

$Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -RunOnlyIfNetworkAvailable

Register-ScheduledTask -TaskName "TA-RDM ETL Pipeline" `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Description "Daily TA-RDM ETL pipeline execution"
```

### Method 4: Kubernetes CronJob

For containerized deployments in Kubernetes.

#### Dockerfile

**`Dockerfile`:**
```dockerfile
FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy ETL code
COPY etl/ ./etl/
COPY utils/ ./utils/
COPY config/ ./config/
COPY scripts/ ./scripts/

# Make scripts executable
RUN chmod +x scripts/*.sh

# Set environment
ENV PYTHONUNBUFFERED=1

# Run ETL
CMD ["./scripts/run_full_etl.sh"]
```

#### Kubernetes CronJob

**`k8s/cronjob.yaml`:**
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: ta-rdm-etl
  namespace: data-platform
spec:
  schedule: "0 2 * * *"  # Daily at 2:00 AM
  concurrencyPolicy: Forbid  # Don't run if previous job still running
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 3
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: OnFailure
          containers:
          - name: etl
            image: your-registry/ta-rdm-etl:latest
            env:
            - name: DB_HOST
              valueFrom:
                secretKeyRef:
                  name: ta-rdm-secrets
                  key: db-host
            - name: DB_USER
              valueFrom:
                secretKeyRef:
                  name: ta-rdm-secrets
                  key: db-user
            - name: DB_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: ta-rdm-secrets
                  key: db-password
            - name: CH_HOST
              valueFrom:
                secretKeyRef:
                  name: ta-rdm-secrets
                  key: ch-host
            - name: CH_USER
              valueFrom:
                secretKeyRef:
                  name: ta-rdm-secrets
                  key: ch-user
            - name: CH_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: ta-rdm-secrets
                  key: ch-password
            resources:
              requests:
                memory: "1Gi"
                cpu: "500m"
              limits:
                memory: "2Gi"
                cpu: "1000m"
```

Deploy:
```bash
kubectl apply -f k8s/cronjob.yaml
```

## Monitoring and Alerting

### Log Monitoring

**Centralized Logging:**
```bash
# Send logs to syslog
logger -t ta-rdm-etl "ETL pipeline completed successfully"

# Or use journalctl (systemd)
systemd-cat -t ta-rdm-etl ./scripts/run_full_etl.sh
```

**Log Aggregation (ELK Stack):**
```bash
# Configure filebeat to ship logs
filebeat.inputs:
- type: log
  enabled: true
  paths:
    - /path/to/ta-rdm-etl/logs/*.log
  tags: ["ta-rdm", "etl"]
```

### Email Notifications

**On Success:**
```bash
# In run_scheduled_etl.sh
if [ $ETL_EXIT_CODE -eq 0 ]; then
    echo "ETL completed successfully at $(date)" | \
        mail -s "✓ TA-RDM ETL Success" admin@example.com
fi
```

**On Failure:**
```bash
if [ $ETL_EXIT_CODE -ne 0 ]; then
    echo "ETL failed. See log: $LOG_FILE" | \
        mail -s "✗ TA-RDM ETL Failed" admin@example.com
fi
```

### Slack Notifications

```bash
# Slack webhook notification
SLACK_WEBHOOK="https://hooks.slack.com/services/YOUR/WEBHOOK/URL"

if [ $ETL_EXIT_CODE -eq 0 ]; then
    MESSAGE="✓ TA-RDM ETL completed successfully"
else
    MESSAGE="✗ TA-RDM ETL failed. Check logs."
fi

curl -X POST -H 'Content-type: application/json' \
    --data "{\"text\":\"$MESSAGE\"}" \
    $SLACK_WEBHOOK
```

### Monitoring Dashboards

**Grafana Dashboard:**
- Track ETL execution time
- Monitor validation pass/fail rates
- Alert on row count anomalies
- Track data freshness

**Prometheus Metrics:**
```python
# Add to ETL scripts for Prometheus scraping
from prometheus_client import Counter, Gauge, Summary

etl_runs_total = Counter('etl_runs_total', 'Total ETL runs')
etl_duration_seconds = Summary('etl_duration_seconds', 'ETL duration')
etl_rows_loaded = Gauge('etl_rows_loaded', 'Rows loaded', ['table'])
```

## Best Practices

### Scheduling Recommendations

1. **Run during off-peak hours** (e.g., 2:00 AM - 4:00 AM)
2. **Allow sufficient time** between runs (don't overlap)
3. **Stagger schedules** if running multiple environments
4. **Monitor execution time** trends
5. **Set realistic timeouts** (e.g., 1-2 hours max)

### Error Handling

1. **Fail fast** on errors (don't continue if dimensions fail)
2. **Retry transient failures** (network issues)
3. **Alert on failures** immediately
4. **Keep detailed logs** for troubleshooting
5. **Track validation failures** separately

### Performance Optimization

1. **Run dimensions in parallel** (they're independent)
2. **Run independent facts in parallel**
3. **Schedule during low-load periods**
4. **Monitor resource usage** (CPU, memory, disk I/O)
5. **Tune batch sizes** for performance

### Data Quality

1. **Run validation after every ETL**
2. **Alert on validation failures**
3. **Track row counts over time**
4. **Monitor for anomalies** (sudden spikes/drops)
5. **Implement data quality checks** in L2 source

## Troubleshooting

### Issue: ETL doesn't run on schedule

**Check:**
- Cron service is running: `systemctl status cron`
- Crontab is correct: `crontab -l`
- Script has execute permissions: `ls -l scripts/`
- Paths are absolute (not relative)

### Issue: ETL fails in cron but works manually

**Common causes:**
- Different environment (PATH, PYTHONPATH)
- Missing .env file or wrong path
- Virtual environment not activated
- Wrong working directory

**Solution:**
Use absolute paths and explicit environment in cron script.

### Issue: Multiple ETL jobs running simultaneously

**Solution:**
Add lock file to prevent concurrent runs:

```bash
LOCKFILE="/tmp/ta-rdm-etl.lock"

# Check if already running
if [ -f "$LOCKFILE" ]; then
    echo "ETL is already running. Exiting."
    exit 1
fi

# Create lock
touch "$LOCKFILE"

# Ensure lock is removed on exit
trap "rm -f $LOCKFILE" EXIT

# Run ETL
./scripts/run_full_etl.sh
```

## See Also

- [INSTALLATION.md](INSTALLATION.md) - Setup and installation
- [CONFIGURATION.md](CONFIGURATION.md) - Configuration options
- [ETL-EXECUTION.md](ETL-EXECUTION.md) - Running ETL scripts
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) - Common issues
