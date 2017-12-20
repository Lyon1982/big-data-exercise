#!/bin/sh

export AWS_ACCESS_KEY_ID=$1
export AWS_SECRET_ACCESS_KEY=$2
aws s3 cp --recursive s3://exa-tech-exercise .
