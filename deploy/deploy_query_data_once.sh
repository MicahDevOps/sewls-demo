#!/bin/bash
#$1表示当前的环境, 值可为dev, stg或者prd
cp ../dist/query/SewlsQueryAthena.js ./query/

cd ./query/

sls deploy -s $1 -v