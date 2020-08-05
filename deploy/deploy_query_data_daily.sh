#!/bin/bash
#$1表示当前的环境, 值可为dev, stg或者prd
cp ../dist/autoQuery/SewlsAutoQueryDaily.js ./autoQuery/daily/

cd ./autoQuery/daily/

sls deploy -s $1 -v