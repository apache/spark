#!/bin/sh

set -e

MC_ALIAS=minio

# Setup alias
mc alias set $MC_ALIAS http://localhost:9000 minioadmin minioadmin

# Create user
mc admin user add $MC_ALIAS spark-user spark-password || true

# Create policy
cat > /tmp/spark-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
      "Effect": "Allow",
      "Resource": ["arn:aws:s3:::spark"]
    },
    {
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Effect": "Allow",
      "Resource": ["arn:aws:s3:::spark/*"]
    }
  ]
}
EOF

mc admin policy create $MC_ALIAS spark-policy /tmp/spark-policy.json || true
mc admin policy attach $MC_ALIAS spark-policy --user spark-user || true

# Check/create bucket
if mc ls $MC_ALIAS/spark >/dev/null 2>&1; then
  echo "Bucket 'spark' already exists"
else
  mc mb $MC_ALIAS/spark
fi

# Check/create folder placeholder
if mc stat $MC_ALIAS/spark/tmp-spark-offload/.keep >/dev/null 2>&1; then
  echo "Folder placeholder '.keep' exists, skipping creation"
else
  echo "" | mc pipe $MC_ALIAS/spark/tmp-spark-offload/.keep
fi