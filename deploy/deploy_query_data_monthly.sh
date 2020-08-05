#!/bin/bash
#$1表示当前的环境, 值可为dev, stg或者prd
cp ../dist/autoQuery/SewlsAutoQueryMonthly.js ./autoQuery/monthly/

cd ./autoQuery/monthly/

sls deploy -s $1 -v