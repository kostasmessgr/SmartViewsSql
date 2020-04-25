let config = require('../config_private');
let mysqlConfig = {};
let connection = null;
const mysql = require('mysql');
const transformations = require('../helpers/transformations');
const jsonSql = require('json-sql')({ separatedValues: false });
let createTable = '';
let tableName = '';
let latestId=0;
const helper = require('../helpers/helper');

function setCreateTable (newCreateTable) {
    createTable = newCreateTable;
}

function setTableName (newTableName) {
    tableName = newTableName;
}

function insertIntoSQL(rows) {
    return new Promise((resolve, reject) => {
        var sql = jsonSql.build({
                        type: 'insert',
                        table: 'dims',
                        values: rows
                        });
                        
        var sanitizedQuery = helper.sanitizeSQLQuery(sql);                
        latestId=Number(rows[rows.length-1]['pk']);                      
         connection.query(sanitizedQuery, function(error, result){
             if (error) {
                 reject(error);
             }
             resolve(result);
         });
     });
}

function insertGbStructSQL(groupby){
    //console.log('insertgbStruvtsql')
    return new Promise((resolve,reject)=>{
        var struct = helper.getGroupByStruct(groupby);
        var sql = jsonSql.build({
                        type:'insert',
                        table:'gbStruct',
                        values:struct
        })
        var sanitizedQuery = helper.sanitizeSQLQuery(sql);
        connection.query(sanitizedQuery,function(error,result){
            if(error){
                reject(error);
            }
            resolve(result,result.id);
        })
    })
}

function getStructsSQL(){
    console.log('getStructs sql method')
    return new Promise((resolve,reject)=>{
        var sql = jsonSql.build({
            type:'select',
            table:'gbStruct',
        })
        var sanitizedQuery = helper.sanitizeSQLQuery(sql);
        //console.log(connection);
        connection.query(sanitizedQuery,function(error,result){
            if(error){
                reject(error)
            }
            //console.log('result getStructsSql: \n'+JSON.stringify(result));
            resolve(result);
        })
    })
}

 async function deleteMultipleStructsSQL(ids){
    //helper.cacheNow(ids,"Evict",'in deleteMultipleStructs');
    //return new Promise((resolve,reject)=>{
        await Promise.all(ids.map(async(id)=>{
            await deleteStructSQL(id);
        })); 
};

function deleteStructSQL(idIn){
    //console.log('delete structs sql method');
    return new Promise((resolve,reject)=>{
        var sql=jsonSql.build({
            type:'remove',
            table:'gbStruct',
            condition:{id:idIn}
        })

    var sanitizedQuery = helper.sanitizeSQLQuery(sql);
    //console.log(sanitizedQuery);
    connection.query(sanitizedQuery,function(error,result){
        if(error){
            reject(error)
        }
        //console.log('deleted from cache: '+String(result))
        //helper.cacheNow(result,'deleteStructSql',sanitizedQuery);
        resolve(result);
    })
    });
}

function getLatestFactSQL(){
    return new Promise((resolve,reject)=>{
    var sql = jsonSql.build({
        type:'select',
        table:'dims',
        condition:{
            id:latestId
        }
    })
    var sanitizedQuery = helper.sanitizeSQLQuery(sql);
    //console.log(sanitizedQuery);
    connection.query(sanitizedQuery,function(error,result){
    if(error){
        reject(error)
    }
    //console.log('latest Fact in getLatestIdSQL: '+String(result))
    resolve(result);
    })
    });
}


function getLatestIdSQL() {
    return new Promise((resolve,reject)=>{
    var sql = jsonSql.build({
        type:'select',
        fields:['id'],
        table:'dims',
        sort:{id:-1},
        limit:1
    })
    var sanitizedQuery = helper.sanitizeSQLQuery(sql);
    //console.log(sanitizedQuery);
    connection.query(sanitizedQuery,function(error,result){
        if(error){
            reject(error)
        }
        //console.log('latest id in getLatestIdSQL: '+JSON.stringify(result[0].id));
        latestId=Number(result[0].id);
        resolve(latestId);
    })
    });    
}

function getFactsFromToSql(from,to){
    
    return new Promise((resolve,reject)=>{
        var sql = jsonSql.build({
                        type:'select',
                        table:'dims',
                        condition:[
                            {id: {$gt: Number(from)}},
                            {id: {$lt: Number(to)}}
                        ]
                        });
                        //console.log(sql.query);
      var sanitizedQuery = helper.sanitizeSQLQuery(sql);                  
     connection.query(sanitizedQuery, function(error,result){
         if(error){
             reject(error);
         }
         //console.log('deltas returned from getFacts: '+result.length);
         resolve(result);
     });                   
    });
}

function getFactsHeavySql(){
    return new Promise((resolve,reject)=>{
        var sql = jsonSql.build({
                        type:'select',
                        table:'dims'
                        });
                        //console.log(sql.query)
        var sanitizedQuery = helper.sanitizeSQLQuery(sql);                
     connection.query(sanitizedQuery, function(error,result){
         if(error){
             reject(error);
         }
         resolve(result);
     });                   
    });
}


function connectToSQL () {
    return new Promise((resolve, reject) => {
        config = helper.requireUncached('../config_private');
        mysqlConfig = config.sql;
        connection = mysql.createConnection(mysqlConfig);
        connection.connect(function (err) {
            if (err) {
                /* istanbul ignore next */
                //console.error('error connecting to mySQL: ' + err.stack);
                reject(err);
            }
            resolve(true);
        });
    });
}

// It is a very common pattern to run a query to make a view materialization
// and then drop the temporary table we used to do it.
function queryAndDropTable (query, tableName) {
    return new Promise((resolve, reject) => {
        connection.query(query, function (error, results) {
            if (error) {
                /* istanbul ignore next */
                helper.log(error);
                reject(error);
            }
            connection.query('DROP TABLE ' + tableName, function (err) {
                if (err) {
                    /* istanbul ignore next */
                    helper.log(err);
                    reject(err);
                }
                resolve(results);
            });
        });
    });
}

function calculateNewGroupBy (facts, operation, gbFields, aggregationField) {
    return new Promise((resolve, reject) => {
        connection.query('DROP TABLE IF EXISTS ' + tableName, function (err) {
            if (err) {
                /* istanbul ignore next */
                helper.log(err);
                reject(err);
            }
            connection.query(createTable, function (error) { // creating the SQL table for 'Fact Table'
                if (error) {
                    /* istanbul ignore next */
                    helper.log(error);
                    reject(err);
                }
                if (facts.length === 0) {
                    reject(new Error('No facts'));
                }
                let sql = jsonSql.build({
                    type: 'insert',
                    table: tableName,
                    values: facts
                });

                let editedQuery = helper.sanitizeSQLQuery(sql);
                //console.log('inside calculate new groupBy\n'+createTable);
                //console.log(editedQuery)
                connection.query(editedQuery, function (error) { // insert facts
                    if (error) {
                        /* istanbul ignore next */
                        helper.log(error);
                        reject(error);
                    }

                    let gbQuery;
                    if (operation === 'AVERAGE') {
                        gbQuery = jsonSql.build({
                            type: 'select',
                            table: tableName,
                            group: gbFields,
                            fields: [gbFields,
                                {
                                    func: { name: 'SUM', args: [{ field: aggregationField }] }
                                },
                                {
                                    func: { name: 'COUNT', args: [{ field: aggregationField }] }
                                }]
                        });
                    } else {
                        gbQuery = jsonSql.build({
                            type: 'select',
                            table: tableName,
                            group: gbFields,
                            fields: [gbFields,
                                {
                                    func: {
                                        name: operation,
                                        args: [{ field: aggregationField }]
                                    }
                                }]
                        });
                    }

                    let editedGB = helper.sanitizeSQLQuery(gbQuery);
                    queryAndDropTable(editedGB, tableName).then(results => {
                        let groupBySqlResult = transformations.transformGBFromSQL(results, operation, aggregationField, gbFields);
                        //console.log('inside calculate new groupby: '+JSON.stringify(groupBySqlResult));
                        resolve(groupBySqlResult);
                    }).catch(err => {
                        /* istanbul ignore next */
                        helper.log(err);
                        reject(err);
                    });
                });
            });
        });
    });
}

function calculateReducedGroupBy (cachedGroupBy, view, gbFields) {
    // this means we want to calculate a different group by than the stored one
    // but however it can be calculated just from redis cache
    // calculating the reduced Group By in SQL
    //console.log('cached groupby---');
    //console.log(JSON.stringify(cachedGroupBy));
    return new Promise((resolve, reject) => {
        // console.log(cachedGroupBy.gbCreateTable)
         console.log(JSON.stringify(view));
        // console.log(gbFields)
        let tableName = cachedGroupBy.gbCreateTable.split(' ');
        tableName = tableName[3];
        tableName = tableName.split('(')[0];
        //console.log(cachedGroupBy.gbCreateTable);
        connection.query(cachedGroupBy.gbCreateTable, async function (error) {
            if (error) {
                /* istanbul ignore next */
                reject(error);
            }
            let lastCol = '';
            let prelastCol = '';
            // need this for AVERAGE calculation where we have 2 derivative columns, first is SUM, second one is COUNT
            lastCol = cachedGroupBy.gbCreateTable.split(' ');
            prelastCol = lastCol[lastCol.length - 4];
            lastCol = lastCol[lastCol.length - 2];

            let rows = helper.extractGBValues(cachedGroupBy, view);

            let sqlInsert = jsonSql.build({
                type: 'insert',
                table: tableName,
                values: rows
            });
            let editedQuery = helper.sanitizeSQLQuery(sqlInsert);
            //console.log('edited1: '+editedQuery);
            connection.query(editedQuery, function (error) {
                if (error) {
                    /* istanbul ignore next */
                    helper.log(error);
                    reject(error);
                }
                let op = helper.extractOperation(view.operation);

                let gbQuery = jsonSql.build({
                    type: 'select',
                    table: tableName,
                    group: gbFields,
                    fields: [gbFields,
                        {
                            func: {
                                name: op,
                                args: [{ field: lastCol }]
                            }
                        }]
                });
                if (view.operation === 'AVERAGE') {
                    gbQuery = jsonSql.build({
                        type: 'select',
                        table: tableName,
                        group: gbFields,
                        fields: [gbFields,
                            {
                                func: {
                                    name: 'SUM',
                                    args: [{ field: prelastCol }]
                                }
                            },
                            {
                                func: {
                                    name: 'SUM',
                                    args: [{ field: lastCol }]
                                }
                            }]
                    });
                }

                let editedGBQuery = helper.sanitizeSQLQuery(gbQuery);
                //console.log('edited query 2: '+editedGBQuery);
                queryAndDropTable(editedGBQuery, tableName).then(results => {
                    resolve(results);
                }).catch(err => {
                    /* istanbul ignore next */
                    helper.log(err);
                    /* istanbul ignore next */
                    reject(err);
                });
            });
        });
    });
}

function mergeGroupBys (groupByA, groupByB, view, viewMeta) {
    return new Promise((resolve, reject) => {
        let lastCol = viewMeta.lastCol;
        let prelastCol = viewMeta.prelastCol;
        let tableName = viewMeta.viewNameSQL;
        let gbCreateTable = view.SQLTable;
        //console.log('mergeGroupBysMethod');
        //console.log('table name: '+gbCreateTable);
        //console.log('gbA:'+JSON.stringify(groupByA));
        //console.log('gbB:'+JSON.stringify(groupByB));
        connection.query(gbCreateTable, function (error) {
            if (error) {
                /* istanbul ignore next */
                helper.log(error);
                reject(error);
            }

            let sqlInsertA = jsonSql.build({
                type: 'insert',
                table: tableName,
                values: groupByA
            });

            let sqlInsertB = jsonSql.build({
                type: 'insert',
                table: tableName,
                values: groupByB
            });

            let editedQueryA = helper.sanitizeSQLQuery(sqlInsertA);
            let editedQueryB = helper.sanitizeSQLQuery(sqlInsertB);
           // console.log('edited query1: '+editedQueryA);
            //console.log('edited query2: '+editedQueryB);

            connection.query(editedQueryA, function (err) {
                if (err) {
                    /* istanbul ignore next */
                    console.log('shit happened A');
                    helper.log(err);
                    /* istanbul ignore next */
                    reject(err);
                }
                connection.query(editedQueryB, function (err) {
                    if (err) {
                        /* istanbul ignore next */
                        helper.log(err);
                        console.log('shit happened B');
                        /* istanbul ignore next */
                        reject(err);
                    }
                    let op = helper.extractOperation(view.operation);
                    let gbQuery = jsonSql.build({
                        type: 'select',
                        table: tableName,
                        group: view.fields,
                        fields: [view.fields,
                            {
                                func: {
                                    name: op,
                                    args: [{ field: lastCol }]
                                }
                            }]
                    });
                    if (view.operation === 'AVERAGE') {
                        gbQuery = jsonSql.build({
                            type: 'select',
                            table: tableName,
                            group: view.fields,
                            fields: [view.fields,
                                {
                                    func: {
                                        name: 'SUM',
                                        args: [{ field: prelastCol }]
                                    }
                                },
                                {
                                    func: {
                                        name: 'SUM',
                                        args: [{ field: lastCol }]
                                    }
                                }]
                        });
                    }

                    let editedGBQuery = helper.sanitizeSQLQuery(gbQuery);
                    queryAndDropTable(editedGBQuery, tableName).then(results => {
                        let groupBySqlResult;
                        if (view.operation === 'AVERAGE') {
                            groupBySqlResult = transformations.transformAverage(results, view.fields, view.aggregationField);
                        } else {
                            groupBySqlResult = transformations.transformGBFromSQL(results, op, lastCol, view.fields);
                        }
                        //console.log('time to return: '+JSON.stringify(groupBySqlResult));
                        resolve(groupBySqlResult);
                    }).catch(err => {
                        /* istanbul ignore next */
                        helper.log(err);
                        /* istanbul ignore next */
                        reject(err);
                    });
                });
            });
        });
    });
}

function executeQuery (queryString) {
    return new Promise((resolve, reject) => {
        connection.query(queryString, async function (error, results) {
            if (error) {
                /* istanbul ignore next */
                reject(error)
            } else {
                resolve(results);
            }
        });
    });
}
    
module.exports = {
    setCreateTable: setCreateTable,
    insertIntoSQL: insertIntoSQL,
    insertGbStructSQL:insertGbStructSQL,
    getFactsFromToSql:getFactsFromToSql,
    deleteMultipleStructsSQL:deleteMultipleStructsSQL,
    getFactsHeavySql:getFactsHeavySql,
    getStructsSQL:getStructsSQL,
    getLatestIdSQL:getLatestIdSQL,
    setTableName: setTableName,
    connectToSQL: connectToSQL,
    calculateNewGroupBy: calculateNewGroupBy,
    calculateReducedGroupBy: calculateReducedGroupBy,
    mergeGroupBys: mergeGroupBys,
    executeQuery: executeQuery,
    getLatestFactSQL:getLatestFactSQL
};
