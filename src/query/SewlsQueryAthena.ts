/**
 * use for write statistic data log to S3 and then it can be used by athena.
 */

import { Athena, AWSError } from "aws-sdk";

class SubData {
    startDate: number = 0;
    endDate: number = 0;
    eventTimes: number = 0;
    uniquePeopleCount: number = 0;
}

class ResultData {
    eventType: number = 0;
    subEventType: number = 0;
    subDataSize: number = 0;
    subData: SubData[] = [];
}

class SewlsQueryResult {
    dataSize: number = 0;
    data: ResultData[] = [];
}


class SewlsQueryResBody {
    code: string = "";
    message: string = "";
    result: SewlsQueryResult = undefined;
}

const SI_ERROR_CODE_SUCCESS = "SI_0000";
const SI_ERROR_CODE_ERROR = "SI_0001";
const SI_ERROR_CODE_BAD_REQUEST = "SI_5443";

const MS_A_DAY = 1000 * 24 * 3600;

const QUERY_DAILY = 1;
const QUERY_MONTHLY = 30;

const successfulResponse = {
    headers: {
        "Content-Type": "application/json; charset=UTF-8",
        "Access-Control-Allow-Origin": "*",
        "si-sewls-version": process.env.npm_package_version
    },
    statusCode: 200,
    body: JSON.stringify({
        "code": SI_ERROR_CODE_SUCCESS,
        "message": "Get data successfully."
    })
};

const failResponse = {
    headers: {
        "Content-Type": "application/json; charset=UTF-8",
        "Access-Control-Allow-Origin": "*",
        "si-sewls-version": process.env.npm_package_version
    },
    statusCode: 200,
    body: JSON.stringify({
        "code": SI_ERROR_CODE_ERROR,
        "message": "Get data failed, please try again."
    })
};


const badRequestParamResponse = {
    headers: {
        "Content-Type": "application/json; charset=UTF-8",
        "Access-Control-Allow-Origin": "*",
        "si-sewls-version": process.env.npm_package_version
    },
    statusCode: 400,
    body: JSON.stringify({
        "code": SI_ERROR_CODE_BAD_REQUEST,
        "message": "Bad Request param!"
    })
};


const SPERATOR = ",";
const athena = new Athena();
athena.config.region = process.env.region;

const sewlsQueryResult = new SewlsQueryResult();


exports.queryStatisticDataHandler = function (event: any, context: any, callback: any) {

    console.log("event", event.queryStringParameters);

    const queryStringParameters: any = event.queryStringParameters;

    const searchPeriod = parseInt(queryStringParameters.searchPeriod);
    const eventType = queryStringParameters.eventType;
    const subEventTypes: string = queryStringParameters.subEventTypes;
    const subEventTypeArray = subEventTypes.split(SPERATOR);

    if ((searchPeriod != QUERY_DAILY) && (searchPeriod != QUERY_MONTHLY)) {
        callback(Error(SI_ERROR_CODE_BAD_REQUEST), badRequestParamResponse);
        return;
    }

    const currentMs = new Date().getTime();

    sewlsQueryResult.dataSize = subEventTypeArray.length;

    const resultData: ResultData[] = [];
    for (let i = 0; i < sewlsQueryResult.dataSize; i++) {
        resultData[i] = new ResultData();
    }
    sewlsQueryResult.data = resultData;
    let queryString = "";
    for (let i: number = 0; i < sewlsQueryResult.dataSize; i++) {
        const subEventType = subEventTypeArray[i];
        resultData[i].eventType = eventType;
        resultData[i].subEventType = parseInt(subEventTypeArray[i]);

        // 如果按天查询
        let queryDayNum = 30;
        let queryMonthNum = 12;
        if (searchPeriod == 1) {
            // 统计多少天之前的数据
            queryDayNum = parseInt(process.env.queryDayNum);
            resultData[i].subDataSize = queryDayNum;
        } else {
            // 统计多少月之前的数据
            queryMonthNum = parseInt(process.env.queryMonthNum);
            resultData[i].subDataSize = queryMonthNum;
        }
        const subData: SubData[] = [];
        for (let j: number = 0; j < resultData[i].subDataSize; j++) {
            subData[j] = new SubData();
        }
        resultData[i].subData = subData;

        // 如果按天查询
        if (searchPeriod == 1) {
            const startMs: number = currentMs - (queryDayNum + 1) * MS_A_DAY;
            const startDate: number = getYyyyMMdd(new Date(startMs));
            const endDate: number = getYyyyMMdd(new Date(currentMs - MS_A_DAY));
            let index = 0;
            for (let innerStartDate: number = startDate, innerStartMs = startMs; innerStartDate < endDate; innerStartMs += MS_A_DAY) {
                const innerEndDate = getYyyyMMdd(new Date(startMs + MS_A_DAY * (index + 1)));

                // 如果结束时间大于endDate, 则不应该继续查询.
                if (innerEndDate > endDate) {
                    console.log("innerEndDate >= endDate", innerEndDate, endDate);
                    break;
                }
                subData[index].startDate = innerStartDate;
                subData[index].endDate = innerEndDate;
                // 初始化数目为0
                subData[index].eventTimes = 0;
                subData[index].uniquePeopleCount = 0;

                // console.log("innerStartDate is", innerStartDate);
                // console.log("endDate is", endDate);
                // console.log("innerEndDate is", innerEndDate);
                const tableName: string = "si_sewls_auto_query_athena_daily_" + innerStartDate;

                const tempQueryString = "SELECT event_type as eventType, sub_event_type as subEventType, record_date as recordDate, COALESCE(searchTimes, 0)  AS searchTimes, "
                    + "COALESCE(uniquePeopleCount, 0) AS uniquePeopleCount  FROM sewls_statisticdb."
                    + tableName
                    + " WHERE event_type = " + eventType
                    + " AND sub_event_type = " + subEventType
                    + " AND record_date = " + innerStartDate;

                if (queryString === "") {
                    queryString = tempQueryString;
                } else {
                    queryString += " \n UNION \n ";
                    queryString += tempQueryString;
                }
                index++;
                innerStartDate = innerEndDate;
            }
        }
        else {
            // 按月查询
            let queryYear = new Date().getFullYear();
            let queryMonth = new Date().getMonth() + 1;
            for (let i = 0; i < queryMonthNum; i++) {
                if (queryMonth == 1) {
                    queryYear = queryYear - 1;
                    queryMonth = 12;
                } else {
                    queryMonth = queryMonth - 1;
                }
                let yyyyMM = "";
                if (queryMonth < 10) {
                    yyyyMM = "" + queryYear + "0" + queryMonth;
                } else {
                    yyyyMM = "" + queryYear + queryMonth;
                }
                subData[i].startDate = parseInt(yyyyMM);
                subData[i].endDate = parseInt(yyyyMM);
                const tableName: string = "si_sewls_auto_query_athena_monthly_" + yyyyMM;
                const tempQueryString = "SELECT event_type as eventType, sub_event_type as subEventType, "
                    + yyyyMM + " as recordDate,"
                    + " COALESCE(searchTimes, 0)  AS searchTimes, COALESCE(uniquePeopleCount, 0) AS uniquePeopleCount  FROM sewls_statisticdb."
                    + tableName
                    + " WHERE event_type = " + eventType
                    + " AND sub_event_type = " + subEventType;
                if (queryString === "") {
                    queryString = tempQueryString;
                } else {
                    queryString += " \n UNION \n ";
                    queryString += tempQueryString;
                }
            }
        }
    }
    console.log("queryString is ", queryString);
    queryString += ";";
    queryOneRecord(queryString, sewlsQueryResult, function (res: any) {
        if (res === SI_ERROR_CODE_SUCCESS) {
            const sewlsQueryResBody: SewlsQueryResBody = new SewlsQueryResBody();
            sewlsQueryResBody.code = SI_ERROR_CODE_SUCCESS;
            sewlsQueryResBody.message = "Query statistic data successfully!";
            sewlsQueryResBody.result = sewlsQueryResult;
            successfulResponse.body = JSON.stringify(sewlsQueryResBody);
            console.log("sewlsQueryResBody is", successfulResponse.body);
            callback(undefined, successfulResponse);
        } else {
            callback(Error(SI_ERROR_CODE_ERROR), failResponse);
        }
    });
};
class QueryAthenaResultVO {
    eventType: number;
    subEventType: number;
    recordDate: number;
    searchTimes: number;
    uniquePeopleCount: number;
}

function queryOneRecord(queryString: string, sewlsQueryResult: SewlsQueryResult, callback: any) {
    const params: Athena.Types.StartQueryExecutionInput = {
        QueryString: queryString,
        ClientRequestToken: generateUuid(),
        ResultConfiguration: {
            EncryptionConfiguration: {
                EncryptionOption: "SSE_S3", /* required */
            },
            OutputLocation: process.env.outputLocation
        },
    };

    let queryExecutionId: string;
    athena.startQueryExecution(params, function (err, data) {
        if (err) {
            callback(SI_ERROR_CODE_ERROR, "Query failed");
            // an error occurred
            console.log("startQueryExecution.callback.err = ", err);
            console.log("startQueryExecution.callback.err.stack = ", err.stack);
            console.log("startQueryExecution.callback.data = ", data);
        }
        else {
            // successful response
            queryExecutionId = data.QueryExecutionId;
            checkQueryCreateStatus(queryExecutionId, function (statusCode: string, resDesc: string) {
                if (statusCode === SI_ERROR_CODE_SUCCESS) {
                    getQueryResultByExecutionId(queryExecutionId, function (resultCode: string, resultSet: QueryAthenaResultVO[]) {
                        if (resultCode === SI_ERROR_CODE_SUCCESS) {
                            // console.log('resultSet = ', resultSet);
                            for (let i = 0; i < resultSet.length; i++) {
                                const recordDate = resultSet[i].recordDate;
                                const eventType = resultSet[i].eventType;
                                const subEventType = resultSet[i].subEventType;
                                const searchTimes = resultSet[i].searchTimes;
                                const uniquePeopleCount = resultSet[i].uniquePeopleCount;
                                // 更新查询结果
                                for (let j = 0; j < sewlsQueryResult.dataSize; j++) {
                                    const resultData: ResultData = sewlsQueryResult.data[j];
                                    if (eventType == resultData.eventType && subEventType == resultData.subEventType) {
                                        for (let k = 0; k < resultData.subDataSize; k++) {
                                            const subData: SubData = resultData.subData[k];
                                            if (subData.startDate == recordDate) {
                                                subData.eventTimes = searchTimes;
                                                subData.uniquePeopleCount = uniquePeopleCount;
                                                break;
                                            }
                                        }
                                        break;
                                    }
                                }
                            }
                            callback(SI_ERROR_CODE_SUCCESS, resultSet);
                        } else {
                            callback(SI_ERROR_CODE_ERROR, "Query failed");
                        }
                    });
                } else {
                    callback(SI_ERROR_CODE_ERROR, "Query failed");
                }
            });
        }
    });
}

/**
 * @description checkQueryCreateStatus, check query status till it is not active.
 */
function checkQueryCreateStatus(queryExecutionId: string, callback: any) {
    const params: Athena.Types.GetQueryExecutionInput = {
        /* required */
        QueryExecutionId: queryExecutionId,
    };
    athena.getQueryExecution(params, function (err, data) {
        if (err) {
            // an error occurred
            console.log("getQueryExecution.callback.err = ", err);
            console.log("getQueryExecution.callback.err.stack = ", err.stack);
            console.log("getQueryExecution.callback.data = ", data);
            callback(SI_ERROR_CODE_ERROR, "Query failed");
        }
        else {
            // console.log("Status.State is ", data.QueryExecution.Status.State);
            if (!(data && data.QueryExecution && data.QueryExecution.Status && data.QueryExecution.Status.State)) {
                console.log("Query failed");
                callback(SI_ERROR_CODE_ERROR, "Query failed");
            } else if (data.QueryExecution.Status.State === "SUCCEEDED") {
                // console.log("Athena Query status is SUCCEEDED");
                callback(SI_ERROR_CODE_SUCCESS, "Query SUCCEEDED");
            }
            else if ((data.QueryExecution.Status.State === "FAILED" || data.QueryExecution.Status.State === "CANCELLED")) {
                console.log("Query status is ", data.QueryExecution.Status.State);
                console.log("Status.StateChangeReason is ", data.QueryExecution.Status.StateChangeReason);
                callback(SI_ERROR_CODE_ERROR, "Query failed");
            } else {
                setTimeout(() => {
                    checkQueryCreateStatus(queryExecutionId, callback);
                }, 200);
            }
        }
    });
}

/**
 * @description getQueryResultByExecutionId, execute for generating result based on queryExecutionId
 * @param {String} queryExecutionId
 * @param {Function} callback
 */


function getQueryResultByExecutionId(queryExecutionId: any, callback: any) {
    const params = {
        QueryExecutionId: queryExecutionId
    };
    athena.getQueryResults(params, function (err: AWSError, data: Athena.Types.GetQueryResultsOutput) {
        if (err) {
            console.log("AWSError erro is ", err);
            callback(SI_ERROR_CODE_ERROR, "Query failed");
        } else {
            const resultSet: QueryAthenaResultVO[] = [];
            for (let i = 1; i < data.ResultSet.Rows.length; i++) {

                if (data.ResultSet.Rows[i] != undefined) {
                    const index = i - 1;
                    resultSet[index] = new QueryAthenaResultVO();
                    resultSet[index].eventType = parseInt(data.ResultSet.Rows[i].Data[0].VarCharValue);
                    resultSet[index].subEventType = parseInt(data.ResultSet.Rows[i].Data[1].VarCharValue);
                    resultSet[index].recordDate = parseInt(data.ResultSet.Rows[i].Data[2].VarCharValue);
                    resultSet[index].searchTimes = parseInt(data.ResultSet.Rows[i].Data[3].VarCharValue);
                    resultSet[index].uniquePeopleCount = parseInt(data.ResultSet.Rows[i].Data[4].VarCharValue);
                } else {
                    console.log("data.ResultSet.Rows[i] != undefined, i =", i);
                }
            }
            callback(SI_ERROR_CODE_SUCCESS, resultSet);

        }
    });
}

function getYyyyMMdd(date: Date) {
    const yyyy = date.getFullYear();
    const MM = date.getMonth() + 1;
    const dd = date.getDate();

    const yyyyMMdd = yyyy * 10000 + MM * 100 + dd;
    // console.log("yyyyMMdd is ", yyyyMMdd);
    return yyyyMMdd;

}

function generateUuid() {
    // return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) { const r = Math.random() * 16 | 0, v = c == 'x' ? r : r & 0x3 | 0x8; return v.toString(16); });
    return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, function (c) {
        const r = (Math.random() * 16) | 0;
        const v = (c == "x") ? r : ((r & 0x3) | 0x8);
        return v.toString(16);
    });
}