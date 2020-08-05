#!/bin/bash
# $1表示当前的环境, 值可为dev, stg或者prd
cp ../dist/save/SewlsSaveData.js ./save/

cd ./save/

sls deploy -s $1 -v
