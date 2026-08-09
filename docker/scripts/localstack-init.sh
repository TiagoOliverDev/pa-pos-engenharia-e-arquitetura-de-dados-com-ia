#!/bin/bash
set -euo pipefail

until awslocal s3 ls >/dev/null 2>&1; do
  sleep 2
done

awslocal s3 mb "s3://${S3_BUCKET_NAME}" >/dev/null 2>&1 || true

