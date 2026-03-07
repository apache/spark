#!/usr/bin/env bash

# A demo "controller" that listens to Spark logs
# and responds to the SCHEDULING_SKEW_DETECTED event.

while read line; do
  if echo "$line" | grep -q "SCHEDULING_SKEW_DETECTED"; then
    echo
    echo ">>> [SELF-HEALING DEMO] Detected scheduling skew event!"
    echo ">>> Here we could automatically trigger remediation logic (e.g. resubmit job with different settings)."
    echo
  fi
done
