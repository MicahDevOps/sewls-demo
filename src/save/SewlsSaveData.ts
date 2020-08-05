/**
 * use for write statistic data log to S3 and then it can be used by athena.
 */

import { Firehose } from "aws-sdk";

const SI_ERROR_CODE_SUCCESS = "SI_0000";
const SI_ERROR_CODE_ERROR = "SI_0001";
const SI_ERROR_CODE_BAD_REQUEST = "SI_5443";

const success_response = {
  headers: {
    "Content-Type": "application/json; charset=UTF-8",
    "si-sewls-version": process.env.npm_package_version
  },
  statusCode: 200,
  body: JSON.stringify({
    "code": SI_ERROR_CODE_SUCCESS,
    "message": "Upload data successfully"
  })
};


const failed_response = {
  headers: {
    "Content-Type": "application/json; charset=UTF-8",
    "si-sewls-version": process.env.npm_package_version
  },
  statusCode: 400,
  body: JSON.stringify({
    "code": SI_ERROR_CODE_BAD_REQUEST,
    "message": "Upload data failed, please check your data"
  })
};

exports.saveStatisticDataHandler = function (event: any, context: any, callback: any) {

  console.log(event.body);

  const lineSplitor = ",";
  const lineEnd = "\n";
  const statisticReqData = JSON.parse(event.body);

  const oaid: string = statisticReqData.oaid;
  const staticsticDataRecord = statisticReqData.staticsticData;

  if (oaid == undefined || oaid.trim().length == 0) {
    console.log("saveStatisticDataHandler oaid is null");
    callback(undefined, failed_response);
    return;
  }

  const firehose = new Firehose();
  firehose.config.region = process.env.region;

  const statisticDataLogs: Firehose.PutRecordBatchRequestEntryList = [];
  let statisticDataLog: string;
  let index = 0;
  for (let i = 0; i < staticsticDataRecord.length; i++) {
    const recordDate = parseInt(staticsticDataRecord[i].recordDate);
    const eventType = parseInt(staticsticDataRecord[i].eventType);
    const subEventType = parseInt(staticsticDataRecord[i].subEventType);
    if (isNaN(recordDate) || isNaN(eventType) || isNaN(subEventType)) {
      console.log("warning: recordDate = ", staticsticDataRecord[i].recordDate);
      console.log("warning: eventType = ", staticsticDataRecord[i].eventType);
      console.log("warning: subEventType = ", staticsticDataRecord[i].subEventType);
      continue;
    }
    statisticDataLog = oaid + lineSplitor + recordDate + lineSplitor + eventType + lineSplitor + subEventType
      + lineSplitor + staticsticDataRecord[i].times + lineEnd;
    statisticDataLogs[index++] = {
      Data: statisticDataLog
    };
  }

  // 如果没有有效数据
  if (index == 0) {
    console.log("error: no valid data uploaed");
    callback(Error(SI_ERROR_CODE_BAD_REQUEST), failed_response);
    return;
  }

  const putRecordBatchInput: Firehose.Types.PutRecordBatchInput = {
    DeliveryStreamName: process.env.DELIVERY_STREAM_NAME,
    Records: statisticDataLogs
  };

  firehose.putRecordBatch(putRecordBatchInput, function (err, data) {
    if (err) {
      console.log("putRecordBatch met error");
      console.log(err, err.stack);
      callback(Error(SI_ERROR_CODE_ERROR), failed_response);
    }
    callback(undefined, success_response);
  });

};



