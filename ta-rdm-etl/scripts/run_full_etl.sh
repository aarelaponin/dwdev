#!/bin/bash
#
# TA-RDM Full ETL Pipeline
# Runs all ETL scripts in correct dependency order
#

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================"
echo "TA-RDM Full ETL Pipeline"
echo "Started: $(date)"
echo "========================================"
echo ""

# Change to parent directory (ta-rdm-etl/)
cd "$(dirname "$0")/.."

# Set PYTHONPATH to include current directory
export PYTHONPATH="$(pwd):$PYTHONPATH"

# Check if venv exists and is activated
if [ -z "$VIRTUAL_ENV" ]; then
    echo -e "${YELLOW}Warning: Virtual environment not activated${NC}"
    echo "Consider running: source venv/bin/activate"
    echo ""
fi

# Check if .env exists
if [ ! -f ".env" ]; then
    echo -e "${RED}Error: .env file not found${NC}"
    echo "Please copy .env.example to .env and configure"
    exit 1
fi

# Function to run ETL script
run_etl() {
    script=$1
    description=$2

    echo "----------------------------------------"
    echo "Running: $description"
    echo "Script: $script"
    echo "----------------------------------------"

    if python "$script"; then
        echo -e "${GREEN}✓ Success: $description${NC}"
    else
        echo -e "${RED}✗ Failed: $description${NC}"
        echo "ETL pipeline stopped due to error"
        exit 1
    fi
    echo ""
}

# Start ETL pipeline
echo "Starting ETL pipeline..."
echo ""

# Phase 1: Dimensions
echo "=== Phase 1: Loading Dimensions ==="
run_etl "etl/generate_dim_time.py" "Time Dimension"
run_etl "etl/l2_to_l3_party.py" "Party Dimension"
run_etl "etl/l2_to_l3_tax_type.py" "Tax Type Dimension"
run_etl "etl/l2_to_l3_tax_period.py" "Tax Period Dimension"

# Phase 2: Base Facts
echo "=== Phase 2: Loading Base Facts ==="
run_etl "etl/l2_to_l3_fact_registration.py" "Registration Facts"
run_etl "etl/l2_to_l3_fact_filing.py" "Filing Facts"
run_etl "etl/l2_to_l3_fact_assessment.py" "Assessment Facts"
run_etl "etl/l2_to_l3_fact_payment.py" "Payment Facts"
run_etl "etl/l2_to_l3_fact_account_balance.py" "Account Balance Facts"

# Phase 3: Compliance Facts
echo "=== Phase 3: Loading Compliance Facts ==="
run_etl "etl/l2_to_l3_fact_collection.py" "Collection Facts"
run_etl "etl/l2_to_l3_fact_refund.py" "Refund Facts"
run_etl "etl/l2_to_l3_fact_audit.py" "Audit Facts"
run_etl "etl/l2_to_l3_fact_objection.py" "Objection Facts"

# Phase 4: Analytical Facts
echo "=== Phase 4: Loading Analytical Facts ==="
run_etl "etl/l2_to_l3_fact_risk_assessment.py" "Risk Assessment Facts"
run_etl "etl/l2_to_l3_fact_taxpayer_activity.py" "Taxpayer Activity Facts"

# Phase 5: Validation
echo "=== Phase 5: Running Validation ==="
echo "----------------------------------------"
echo "Running comprehensive ETL validation"
echo "----------------------------------------"

if python etl/validate_etl.py; then
    echo -e "${GREEN}✓ Validation passed - All checks successful${NC}"
else
    echo -e "${RED}✗ Validation failed - Check output above${NC}"
    exit 1
fi

# Summary
echo ""
echo "========================================"
echo "ETL Pipeline Completed Successfully"
echo "Finished: $(date)"
echo "========================================"
echo ""
echo "Next steps:"
echo "  - Review validation report above"
echo "  - Check data in ClickHouse L3"
echo "  - Run queries and reports"
