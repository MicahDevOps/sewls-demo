/**
 * use for write statistic data log to S3 and then it can be used by athena.
 */

import { Athena } from "aws-sdk";

const MS_A_DAY = 1000 * 24 * 3600;

const SI_ERROR_CODE_SUCCESS = "SI_0000";
const SI_ERROR_CODE_ERROR = "SI_0001";

const athena = new Athena();
athena.config.region = process.env.region;

const outputPath = process.env.outputLocation + new Date().getFullYear() + "_" + new Date().getMonth() + "/";

// 此lambda定时执行, 每个月第二天早上8:00, 将以一个月的数据的结果缓存, 以便PM要查询时, 能够快速得到结果.
exports.autoQueryHandlerMonthly = function (event: any, context: any, callback: any) {

    const guleCrawelTableName = process.env.guleCrawelTableName;

    let queryMonth = new Date().getMonth() + 1;
    let queryYear = new Date().getFullYear();
    if (queryMonth == 1) {
        queryYear = queryYear - 1;
        queryMonth = 12;
    } else {
        queryMonth = queryMonth - 1;
    }
    let yyyyMM = "";
    if (queryMonth < 10) {
        yyyyMM += queryYear + "0" + queryMonth;
    } else {
        yyyyMM += queryYear + queryMonth;
    }
    // 每个月第一天
    const queryStartDate = yyyyMM + "01";
    // 每个月都以31号结束, 不存在的分区不影响查询结果.
    const queryendDate = yyyyMM + "31";
    let dtSet = "'" + queryStartDate + "'";
    for (let j: number = 2; j <= 31; j++) {
        let queryDate = "";
        if (j < 10) {
            queryDate += yyyyMM + "0" + j;
        } else {
            queryDate += yyyyMM + j;
        }
        dtSet = dtSet + ", '" + queryDate + "'";
    }


    const tableName = "si_sewls_auto_query_athena_monthly_" + yyyyMM;

    const queryString = "CREATE TABLE sewls_statisticdb."
        + tableName
        + " WITH ( format='PARQUET', external_location='"
        + outputPath
        + tableName
        + "' ) AS "
        + "  SELECT event_type, sub_event_type, COALESCE(SUM(times),   0) AS searchTimes, COUNT(DISTINCT oaid) AS uniquePeopleCount "
        + " FROM sewls_statisticdb." + guleCrawelTableName
        + " WHERE record_date  >= " + queryStartDate
        + " AND record_date  <= " + queryendDate
        + "  AND dt IN ( "
        + dtSet
        + ") "
        + " GROUP BY event_type, sub_event_type;";
    console.log("queryString is ", queryString);
    queryOneRecord(queryString, function (res: any) {
        let callbackResult = "Create monthly query table" + guleCrawelTableName;
        if (res === SI_ERROR_CODE_SUCCESS) {
            callbackResult += " successfuly";
            console.log(callbackResult);
            callback(undefined, callbackResult);
        } else {
            callbackResult += " failed";
            console.log(callbackResult);
            callback(Error(SI_ERROR_CODE_ERROR), callbackResult);
        }
    });
};

function queryOneRecord(queryString: string, callback: any) {
    const params: Athena.Types.StartQueryExecutionInput = {
        QueryString: queryString,
        ClientRequestToken: generateUuid(),
        ResultConfiguration: {
            EncryptionConfiguration: {
                EncryptionOption: "SSE_S3", /* required */
            },
            OutputLocation: outputPath
        },
    };
    let queryExecutionId: string;
    athena.startQueryExecution(params, function (err, data) {
        if (err) {
            // an error occurred
            console.log("err, err.stack=", err, err.stack);
        }
        else {
            // successful response
            queryExecutionId = data.QueryExecutionId;
            console.log("queryExecutionId=", queryExecutionId);
            checkQueryCreateStatus(queryExecutionId, function (statusCode: string, resDesc: string) {
                if (statusCode === SI_ERROR_CODE_SUCCESS) {
                    callback(SI_ERROR_CODE_SUCCESS, "Query Successfully!");
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

function generateUuid() {
    // return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) { const r = Math.random() * 16 | 0, v = c == 'x' ? r : r & 0x3 | 0x8; return v.toString(16); });
    return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, function (c) {
        const r = (Math.random() * 16) | 0;
        const v = (c == "x") ? r : ((r & 0x3) | 0x8);
        return v.toString(16);
    });
}