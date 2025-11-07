#!/bin/bash
#
# TA-RDM ETL Validation
# Runs comprehensive validation checks on loaded data
#

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================"
echo "TA-RDM ETL Validation"
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

# Run validation
echo "Running comprehensive ETL validation..."
echo "----------------------------------------"
echo ""

if python etl/validate_etl.py; then
    echo ""
    echo "========================================"
    echo -e "${GREEN}✓ VALIDATION PASSED${NC}"
    echo "Finished: $(date)"
    echo "========================================"
    exit 0
else
    echo ""
    echo "========================================"
    echo -e "${RED}✗ VALIDATION FAILED${NC}"
    echo "Finished: $(date)"
    echo "========================================"
    echo ""
    echo "Troubleshooting:"
    echo "  - Check validation output above for specific failures"
    echo "  - Review docs/TROUBLESHOOTING.md for common issues"
    echo "  - Re-run failed ETL scripts"
    echo "  - Verify L2 source data quality"
    exit 1
fi
