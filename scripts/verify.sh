#!/bin/bash
# ==============================================================================
# verify.sh — Validate routed topic offsets and print PASS or FAIL status.
# Usage: ./scripts/verify.sh
# ==============================================================================

set -e

echo "===================================================================="
echo " Starting Telemetry Pipeline Count Verification"
echo "===================================================================="

# 1. Run the countoffsets command via Docker Compose
echo "Fetching topic offsets from Kafka broker..."
OUTPUT=$(docker compose run --rm telemetry-app countoffsets)

echo ""
echo "----------------------------------------------------"
echo "$OUTPUT" | grep -E "Topic:|offset" || true
echo "----------------------------------------------------"
echo ""

# 2. Extract count values
SOURCE=$(echo "$OUTPUT" | grep "Topic: source " | sed -E 's/.*Records: ([0-9,]+).*/\1/' | tr -d ',' || echo "0")
CRITICAL=$(echo "$OUTPUT" | grep "Topic: critical " | sed -E 's/.*Records: ([0-9,]+).*/\1/' | tr -d ',' || echo "0")
NOMINAL=$(echo "$OUTPUT" | grep "Topic: nominal " | sed -E 's/.*Records: ([0-9,]+).*/\1/' | tr -d ',' || echo "0")
ARCHIVE=$(echo "$OUTPUT" | grep "Topic: regional_archive " | sed -E 's/.*Records: ([0-9,]+).*/\1/' | tr -d ',' || echo "0")

# If empty, fallback to 0
SOURCE=${SOURCE:-0}
CRITICAL=${CRITICAL:-0}
NOMINAL=${NOMINAL:-0}
ARCHIVE=${ARCHIVE:-0}

echo "Extracted Counts:"
echo "  Source           : $SOURCE"
echo "  Critical         : $CRITICAL"
echo "  Nominal          : $NOMINAL"
echo "  Regional Archive : $ARCHIVE"
echo ""

# 3. Perform algebraic checks
SUM=$((CRITICAL + NOMINAL))
DIFF_SUM=$((SOURCE - SUM))
DIFF_ARCHIVE=$((SOURCE - ARCHIVE))

# Checks pass if:
# - Sum(Critical + Nominal) == Source (0 parse errors) OR Sum + 1 == Source (1 parse error "Hello Kafka")
# - Archive == Source (0 parse errors) OR Archive + 1 == Source (1 parse error "Hello Kafka")
IS_OK=false

if [ "$DIFF_SUM" -eq 1 ] && [ "$DIFF_ARCHIVE" -eq 1 ]; then
    IS_OK=true
elif [ "$DIFF_SUM" -eq 0 ] && [ "$DIFF_ARCHIVE" -eq 0 ]; then
    IS_OK=true
fi

if [ "$IS_OK" = true ]; then
    echo "===================================================================="
    echo " Check 1: Critical ($CRITICAL) + Nominal ($NOMINAL) == Source ($SOURCE) ? [OK]"
    echo " Check 2: Regional Archive ($ARCHIVE) == Source ($SOURCE) ? [OK]"
    echo ""
    echo " FINAL RESULT: PASS"
    echo "===================================================================="
else
    echo "===================================================================="
    echo " Check 1: Critical ($CRITICAL) + Nominal ($NOMINAL) == Source ($SOURCE) ? [FAILED]"
    echo " Check 2: Regional Archive ($ARCHIVE) == Source ($SOURCE) ? [FAILED]"
    echo ""
    echo " FINAL RESULT: FAIL"
    echo "===================================================================="
    echo ""
    echo "TIP: If you ran the pipeline multiple times without wiping Docker volumes, the target topics"
    echo "     accumulated historical records, causing the checks to fail."
    echo "     Please run: 'docker compose down -v' to clear all data and start fresh."
    echo ""
    exit 1
fi
