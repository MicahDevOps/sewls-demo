#!/bin/bash
# $1为所需要部署的js文件
# $2为需要部署的serverless.config文件所在的目录
# $3为部署的阶段
cp $1 $2

cd $2

sls deploy -s $3 -v