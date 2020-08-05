# 1. 下载代码:
- GitHub地址: https://github.sec.samsung.net/SRCG-Internet/si-sewls-lambda.git

# 2. 编译代码:
```
  npm install
  npm run build
```

# 3. 开始部署

### 3.1 部署保存数据的lambda
#### 3.1.1 创建S3储存桶, 用于存储部署相关内容.
- S3存储桶名字: si-sewls-save-data-lambda-deploy-${opt:stage}
#### 3.1.2 按如下样式配置firehose(仅需第一次部署后配置)
  - ${opt:stage}表示当前的环境, 值可为dev, stg或者prd
  - 传输流名字: si-sewls-save-data-firehose-${opt:stage}
  - 源：Direct PUT 或其他源 
  - 数据转换：已禁用 
  - IAM权限: S3读写权限, cloudwatch读写权限()
  - 目标：Amazon S3
  - S3 存储桶: s3://si-sewls-kinesisfirehose-${opt:stage} (新建）
  - S3文件前缀: save-data/dt=!{timestamp:yyyyMMdd/}
  - 错误前缀： save-data-error/result=!{firehose:error-output-type}/dt=!{timestamp:yyyyMMdd/}
  - S3 缓冲条件： 缓冲区大小 128M  缓冲时间间隔: 300s
  - 压缩: GZIP
  - 错误日志记录: 已启用
  - Role: firehose_delivery_role_${opt:stage} (新建）
  - 其他默认或者根据自己业务决定
#### 3.1.3 运行部署脚本
  - cd ./deploy
  - ./deploy_save_data_lambda.sh ${opt:stage}
#### 3.1.4 配置alb转发路径
-  转发路径: /si-sewls-api/v1/whitelabel/statistic
-  域名: 
    - dev: "http://si-dev-sewls.samsung.com.cn"
    - stg: "https://si-stg-sewls.samsung.com.cn"
    - prd: "https://si-sewls.samsung.com.cn"        
#### 3.1.5 在lambda里添加层:
  - 层名: aws-sdk
#### 3.1.6 lambda函数所需权限(仅需第一次部署后配置)
- kinesis写权限
- cloudwatch读写权限
#### 3.1.7 在Athena中创建数据库
  - 数据库名: sewls_statisticdb (可在Glue 内创建 ， 此处移动到glue)
#### 3.1.8 配置Glue爬网程序分类器
  - 分类器名: si-sewls-custom-classifier-${opt:stage}
  - 分类器类型: Grok
  - Grok 模式: %{USERNAME:oaid:string},%{NUMBER:record_date:int},%{NUMBER:event_type:short},%{NUMBER:sub_event_type:int},%{NUMBER:times:int}
#### 3.1.9 配置Glue爬网程序为S3数据建表.
  - 名称: si-sewls-glue-${opt:stage}
  - (标签、描述、安全配置和分类器（可选）) 添加分类器: si-sewls-custom-classifier-${opt:stage}
  - 数据存储: S3
  - 包含路径: s3://si-sewls-kinesisfirehose-${opt:stage}/save-data
  - 添加另一个数据存储 否
  - 创建 IAM 角色： AWSGlueServiceRole-si-sewls-${opt:stage} (- 权限: 读写S3, 读写Athena ，glue)
  - 频率: 每日1:00(UTC时间)
  - 数据库: sewls_statisticdb
  - 表前缀: si_sewls_        		
  - 配置选项: 
	1) 忽略更改，不更新数据目录中的表 
	2) 勾选: 使用表中的元数据更新所有新的和现有的分区。其他默认.
--
### 3.2 部署查询接口的lambda
#### 3.2.1 创建S3储存桶, 用于存储部署相关内容.
- S3存储桶名字: si-sewls-query-once-lambda-deploy-${opt:stage}
#### 3.2.2 运行部署脚本:
  - cd ./deploy
  - ./deploy_query_data_once.sh ${opt:stage}
#### 3.2.3 配置alb转发路径
-  转发路径: /si-sewls-api/v1/sewls/query
-  域名: 
    - dev: http://si-dev-sewls.samsung.com.cn
    - stg: https://si-stg-sewls.samsung.com.cn
    - prd: https://si-sewls.samsung.com.cn     
#### 3.2.4 在lambda里添加层:
  - 层名: aws-sdk
#### 3.2.5 lambda函数所需权限:
- Athena读取权限
- glue
- S3的读写权限
- cloudwatch读写权限

---
### 3.3 用于每月2号自动更新上个月统计数据的lambda
#### 3.3.1 创建S3储存桶, 用于存储部署相关内容.
- S3存储桶名字: si-sewls-auto-query-monthly-lambda-deploy-${opt:stage}
#### 3.3.2 运行部署脚本:
  - cd ./deploy
  - ./deploy_query_data_monthly.sh ${opt:stage}
#### 3.3.3 在lambda里添加层:
  - 层名: aws-sdk  
#### 3.3.4 lambda函数所需权限: 
- Athena读取权限
- glue
- S3的读取权限
- cloudwatch读写权限

---
### 3.4 用于每天自动更新两天前的统计数据的daily任务lambda
#### 3.3.1 创建S3储存桶, 用于存储部署相关内容.
- S3存储桶名字: si-sewls-auto-query-daily-lambda-deploy-${opt:stage}
#### 3.4.2 运行部署脚本:
  - cd ./deploy
  - ./deploy_query_data_daily.sh ${opt:stage}
#### 3.4.3 在lambda里添加层:
  - 层名: aws-sdk  
#### 3.4.4 lambda函数所需权限: 
- Athena读取权限
- glue
- S3的读取权限
- cloudwatch读写权限