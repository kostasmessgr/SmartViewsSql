const config = require('../config_private');
const microtime = require('microtime');
const fs = require('fs');
var k=1;

function removeTimestamps (records) {
    for (let i = 0; i < records.length; i++) {
        delete records[i].timestamp;
    }
    return records;
}

function configFileValidations () {
    let missingFields = [];
    if (!config.hasOwnProperty('recordsSlice')) {
        missingFields.push('recordsSlice');
    }
    if (!config.hasOwnProperty('cacheEvictionPolicy')) {
        missingFields.push('cacheEvictionPolicy');
    }
    if (!config.hasOwnProperty('maxCacheSize')) {
        missingFields.push('maxCacheSize');
    }
    if (!config.hasOwnProperty('cacheSlice')) {
        missingFields.push('cacheSlice');
    }
    if (!config.hasOwnProperty('redisPort')) {
        missingFields.push('redisPort');
    }
    if (!config.hasOwnProperty('redisIP')) {
        missingFields.push('redisIP');
    }
    if (!config.hasOwnProperty('blockchainIP')) {
        missingFields.push('blockchainIP');
    }
    if (missingFields.length > 0) {
        return { passed: false, missingFields: missingFields };
    }
    let formatErrors = [];
    if (!Number.isInteger(config.recordsSlice)) {
        formatErrors.push({ field: 'recordsSlice', error: 'Should be integer' });
    }
    if (!Number.isInteger(config.cacheSlice)) {
        formatErrors.push({ field: 'cacheSlice', error: 'Should be integer' });
    }
    if (!Number.isInteger(config.maxCacheSize)) {
        formatErrors.push({ field: 'maxCacheSize', error: 'Should be integer' });
    }
    if (!Number.isInteger(config.redisPort)) {
        formatErrors.push({ field: 'redisPort', error: 'Should be integer' });
    }
    if (config.cacheEvictionPolicy !== 'FIFO' &&
        config.cacheEvictionPolicy !== 'costFunction' &&
        config.cacheEvictionPolicy !== 'cubeDistance') {
        formatErrors.push({ field: 'cacheEvictionPolicy',
            error: 'Should be either \'FIFO\' or \'costFunction\' or \'cubeDistance\'' });
    }
    if ((typeof config.blockchainIP) !== 'string') {
        formatErrors.push({ field: 'blockchainIP', error: 'Should be string' });
    }
    if ((typeof config.redisIP) !== 'string') {
        formatErrors.push({ field: 'redisIP', error: 'Should be string' });
    }

    if (formatErrors.length > 0) {
        return { passed: false, formatErrors: formatErrors };
    }
    return { passed: true };
}

function printTimes (resultObject) {
    log('sql time = ' + resultObject.sqlTime);
    log('bc time = ' + resultObject.bcTime);
    log('cache save time = ' + resultObject.cacheSaveTime);
    if (resultObject.cacheRetrieveTime) {
        log('cache retrieve time = ' + resultObject.cacheRetrieveTime);
    }
    log('total time = ' + resultObject.totalTime);
    log('all total time = ' + resultObject.allTotal);
}

function containsAllFields (transformedArray, view) {
    for (let i = 0; i < transformedArray.length; i++) {
        let containsAllFields = true;
        const crnView = transformedArray[i];

        let cachedGBFields = JSON.parse(crnView.columns);
        for (let index in cachedGBFields.fields) {
            cachedGBFields.fields[index] = cachedGBFields.fields[index].trim();
        }
        for (let j = 0; j < view.fields.length; j++) {
            if (!cachedGBFields.fields.includes(view.fields[j])) {
                containsAllFields = false
            }
        }
        transformedArray[i].containsAllFields = containsAllFields;
    }
    return transformedArray;
}

function getRandomInt (min, max) {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min + 1)) + min;
}
function getRandomFloat (min, max) {
    return (Math.random() * (max - min + 1) + min).toFixed(2);
}

function time () {
    return microtime.nowDouble();
}

function log (logString) {
    if (config.logging) {
        console.log(logString);
    }
}

function requireUncached (module) {
    delete require.cache[require.resolve(module)];
    return require(module);
}

function mergeSlicedCachedResult (allCached) {
    let mergedArray = [];
    for (const index in allCached) {
        let crnSub = allCached[index];
        let crnSubArray = JSON.parse(crnSub);
        for (const kv in crnSubArray) {
            if (kv !== 'operation' && kv !== 'groupByFields' && kv !== 'field' && kv !== 'viewName') {
                mergedArray.push(crnSubArray[kv]);
            } else {
                for (const meta in crnSubArray) {
                    mergedArray.push({ [meta]: crnSubArray[meta] });
                }
                break;
            }
        }
    }
    let gbFinal = {};
    for (const i in mergedArray) {
        const crnKey = Object.keys(mergedArray[i])[0];
        gbFinal[crnKey] = Object.values(mergedArray[i])[0];
    }
    return gbFinal;
}

function extractGBValues (reducedResult, view) {
    let rows = [];
    const gbValsReduced = Object.values(reducedResult);
    //console.log('inside extractGB '+gbValsReduced);
    //console.log('reduced resu;t: '+JSON.stringify(reducedResult));
    //console.log('view '+JSON.stringify(view));
    //console.log(view==undefined);
    let lastCol = view.SQLTable.split(' ');
    let prelastCol = lastCol[lastCol.length - 4];
    lastCol = lastCol[lastCol.length - 2];
    for (let i = 0, keys = Object.keys(reducedResult); i < keys.length; i++) {
        const key = keys[i];
        //console.log(key)
        if (key !== 'operation' && key !== 'groupByFields' && key !== 'field' && key !== 'gbCreateTable' && key !== 'viewName') {
            let crnRow = JSON.parse(key);
            if (view.operation === 'AVERAGE') {
                crnRow[prelastCol] = gbValsReduced[i]['sum'];
                crnRow[lastCol] = gbValsReduced[i]['count'];
            } else {
                crnRow[lastCol] = gbValsReduced[i];
            }
            rows.push(crnRow);
        }
    }
    //console.log('extractGB returns '+JSON.stringify(rows));
    return rows;
}

function getJSONFiles (items) {
    const suffix = '.json';
    return items.filter(file => {
        return file.indexOf(suffix) !== -1; // filtering out non-json files
    });
}

function transformGBMetadataFromBlockchain (resultGB) {
    const len = Object.keys(resultGB).length;
    for (let j = 0; j < len / 2; j++) {
        delete resultGB[j];
    }
    let transformedArray = [];
    for (let j = 0; j < resultGB.hashes.length; j++) {
        transformedArray[j] = {
            hash: resultGB.hashes[j],
            latestFact: resultGB.latFacts[j],
            columnSize: resultGB.columnSize[j],
            columns: resultGB.columns[j],
            gbTimestamp: resultGB.gbTimestamp[j],
            size: resultGB.size[j],
            id: j
        };
    }
    // then we filter out the empty objects (the ones that are deleted from blockchain, however left with zeroes)
    // it is enough to check if the hash exists
    transformedArray = transformedArray.filter(gb => {
        return gb.hash.length > 0;
    });
    return transformedArray;
}

function updateViewFrequency (factTbl, contract, crnView) {
    return new Promise(function (resolve, reject) {
        factTbl.views[crnView].frequency = factTbl.views[crnView].frequency + 1;
        delete factTbl.views[crnView].id;
        fs.writeFile('./templates/' + contract + '.json', JSON.stringify(factTbl, null, 2), function (err) {
            if (err) {
                log(err);
                reject(err);
            } else {
                log('updated view frequency');
                resolve();
            }
        });
    });
}

function extractOperation (op) {
    let operation = '';
    if (op === 'SUM' || op === 'COUNT') {
        operation = 'SUM'; // operation is set to 'SUM' both for COUNT and SUM operation
    } else {
        operation = op;
    }
    return operation;
}

function extractViewMeta (view) {
    let viewNameSQL = view.SQLTable.split(' ');
    viewNameSQL = viewNameSQL[3];
    viewNameSQL = viewNameSQL.split('(')[0];

    let lastCol = view.SQLTable.split(' ');
    let prelastCol = lastCol[lastCol.length - 4]; // need this for AVERAGE calculation where we have 2 derivative columns, first is SUM, second one is COUNT
    lastCol = lastCol[lastCol.length - 2];

    let op = extractOperation(view.operation);

    return { viewNameSQL: viewNameSQL, lastCol: lastCol, prelastCol: prelastCol, op: op };
}

function checkViewExists (viewsDefined, viewName) {
    let view = {};
    if(viewsDefined.has(viewName)){
        view = viewsDefined.get(viewName);
    }
    return view;
}

function sanitizeSQLQuery (gbQuery) {
    let query = gbQuery.query.replace(/"/g, '');
    query = query.replace(/''/g, 'null');
    return query;
}

function filterGBs (resultGB, view) {
    let transformedArray = transformGBMetadataFromBlockchain(resultGB);
    transformedArray = containsAllFields(transformedArray, view); // assigns the containsAllFields value
    let filteredGBs = [];
    for (let i = 0; i < transformedArray.length; i++) { // filter out the group bys that DO NOT CONTAIN all the fields we need -> aka containsAllFields = false
        if (transformedArray[i].containsAllFields && JSON.parse(transformedArray[i].columns).aggrFunc === view.operation) {
            filteredGBs.push(transformedArray[i]);
        }
    }
    return filteredGBs;
}

function filterGBsSQL(resultGB,view){
    //console.log('FilterdGbSql method');
    let filtered=[];
    let i=0;
    for(i;i<resultGB.length;i++){
        if(materializes(resultGB[i],view)){
            filtered.push(resultGB[i]);
        }
    }
    return filtered;
}

async function sortByEvictionCost (resultGB, latestId, view, factTbl) {
    //let transformedArray = transformGBMetadataFromBlockchain(resultGB);
    //transformedArray = containsAllFields(transformedArray, view); // assigns the containsAllFields value
    let sortedByEvictionCost = JSON.parse(JSON.stringify(resultGB));
    //cacheNow(sortedByEvictionCost,"Input sortedByEvictionCost",JSON.stringify(view.name));
    //console.log('input sorted by eviction cost'+ JSON.stringify(resultGB));
    //cacheNow(resultGB,"Input sortedByEvictionCost",JSON.stringify(view.name));
    if (config.cacheEvictionPolicy === 'cubeDistance') {
        sortedByEvictionCost = costFunctions.dataCubeDistanceBatch(sortedByEvictionCost, view);
    } else if (config.cacheEvictionPolicy === 'word2vec') {
        sortedByEvictionCost = costFunctions.word2vec(resultGB, view);
    } else if (config.cacheEvictionPolicy === 'costFunction'){
        sortedByEvictionCost = await costFunctions.dispCost(sortedByEvictionCost, latestId, factTbl);
    }
    sortedByEvictionCost.sort(function (a, b) {
        switch (config.cacheEvictionPolicy) {
            case 'FIFO':
                return parseInt(a.timestamp) - parseInt(b.timestamp);
            case 'costFunction':
                return parseFloat(a.cacheEvictionCost) - parseFloat(b.cacheEvictionCost);
            case 'word2vec':
                return parseInt(a.word2vecScore) - parseInt(b.word2vecScore);
            case 'cubeDistance':
                return parseFloat(b.dataCubeDistance) - parseFloat(a.dataCubeDistance);
        }
    });
    //cacheNow(sortedByEvictionCost,"Output sortedByEvictionCost",JSON.stringify(view.name));
    return sortedByEvictionCost;
}

function sortByCalculationCost (resultGBs, latestId, view) {
    if (config.calculationCostFunction === 'costFunction') {
        resultGBs = costFunctions.calculationCostOfficial(resultGBs, latestId); // the cost to materialize the view from each view cached
        resultGBs.sort((a, b) => parseFloat(a.calculationCost) - parseFloat(b.calculationCost)); // order ascending
    } else if (config.calculationCostFunction === 'dataCubeDistance'){
        resultGBs = costFunctions.dataCubeDistanceBatch(resultGBs, view);
        resultGBs.sort((a, b) => parseFloat(a.dataCubeDistance) - parseFloat(b.dataCubeDistance)); // order ascending
    }
    return resultGBs;
}

async function sortByWord2Vec (resultGBs, view) {
    if (resultGBs.length > 1) {
        resultGBs = await costFunctions.word2vec(resultGBs, view);
        resultGBs.sort((a, b) => parseFloat(b.word2vecScore) - parseFloat(a.word2vecScore));
    }
    return resultGBs;
}

function reconstructSlicedCachedResult (cachedGB) {
    const hashId = cachedGB.hash.split('_')[1];
    const hashBody = cachedGB.hash.split('_')[0];
    let allHashes = [];
    for (let i = 0; i <= hashId; i++) {
        allHashes.push(hashBody + '_' + i);
    }
    return allHashes;
}

function getMainTransactionObject (account) {
    return {
        from: account,
        gas: 1500000000000,
        gasPrice: '30000000000000'
    };
}

function assignTimes (result, times) {
    if (times.bcTime && times.sqlTime && times.cacheRetrieveTime && times.cacheSaveTime && times.totalTime) {
        // means we have already calculated times in previous step
        result.bcTime = times.bcTime;
        result.sqlTime = times.sqlTime;
        result.cacheRetrieveTime = times.cacheRetrieveTime;
        result.cacheSaveTime = times.cacheSaveTime;
        result.totalTime = times.totalTime;
        result.allTotal = times.totalEnd - times.totalStart;
        return result;
    }
    if (times.bcTime && times.sqlTime && times.cacheRetrieveTime && times.totalTime) {
        // means we have already calculated times in previous step
        result.bcTime = times.bcTime;
        result.sqlTime = times.sqlTime;
        result.cacheRetrieveTime = times.cacheRetrieveTime;
        result.totalTime = times.totalTime;
        result.allTotal = times.totalEnd - times.totalStart;
        return result;
    }
    if (times.bcTime && times.sqlTime && times.cacheSaveTime && times.totalTime) {
        // means we have already calculated times in previous step
        result.bcTime = times.bcTime;
        result.sqlTime = times.sqlTime;
        result.cacheSaveTime = times.cacheSaveTime;
        result.totalTime = times.totalTime;
        result.allTotal = times.totalEnd - times.totalStart;
        return result;
    }
    result.sqlTime = times.sqlTimeEnd - times.sqlTimeStart;
    result.totalTime = result.sqlTime;
    if (times.bcTimeEnd && times.bcTimeStart && times.getGroupIdTime !== null && times.getGroupIdTime !== undefined) {
        result.bcTime = (times.bcTimeEnd - times.bcTimeStart) + times.getGroupIdTime;
        result.totalTime += result.bcTime;
    }
    if (times.cacheSaveTimeStart && times.cacheSaveTimeEnd) {
        result.cacheSaveTime = times.cacheSaveTimeEnd - times.cacheSaveTimeStart;
        result.totalTime += result.cacheSaveTime;
    }
    if (times.cacheRetrieveTimeStart && times.cacheRetrieveTimeEnd) {
        result.cacheRetrieveTime = times.cacheRetrieveTimeEnd - times.cacheRetrieveTimeStart;
        result.totalTime += result.cacheRetrieveTime;
    }
    result.allTotal = times.totalEnd - times.totalStart;
    return result;
}

function findSameOldestResults (sortedByEvictionCost, view) {
    // if(sortedByEvictionCost.length>0){
    //     fileGbs(sortedByEvictionCost,view,'Check for older views');
    // }
    let sameOldestResults = [];
    var name = JSON.stringify(view.name);
    var f2 =name.substring(1,name.indexOf('('));
    for (let i = 0; i < sortedByEvictionCost.length; i++) {
        const crnRes = sortedByEvictionCost[i];
        var fields = JSON.stringify(crnRes.columns);
        f1=fields.substring(fields.indexOf('[')+1,fields.indexOf(']'));
        f1 = f1.replace(/,/g,'');
        var aggr = JSON.stringify(crnRes.aggrFunc);
        aggr=aggr.replace(/"/g,'');
        //checks(JSON.stringify(f1),JSON.stringify(f2),aggr,view.operation)
        if (JSON.stringify(f1) == JSON.stringify(f2)&& aggr==view.operation) {
            sameOldestResults.push(crnRes);
            //console.log(true);
        }
        // if(sameOldestResults.length>0){
        //     fileGbs(sameOldestResults,view,'sameOldestResults found')
        // }
        
    }
    return sameOldestResults;
}

function welcomeMessage () {
    console.log('     _____                          _ __      __ _                      ');
    console.log('    / ____|                        | |\\ \\    / /(_)                     ');
    console.log('   | (___   _ __ ___    __ _  _ __ | |_\\ \\  / /  _   ___ __      __ ___ ');
    console.log('    \\___ \\ | \'_ ` _ \\  / _` || \'__|| __| \\ / /  | | / _ \\ \\  /\\ / // __|');
    console.log('    ____) || | | | | || (_| || |   | |_  \\  /   | ||  __/ \\ V  V / \\__ \\');
    console.log('   |_____/ |_| |_| |_| \\__,_||_|    \\__|  \\/    |_| \\___|  \\_/\\_/  |___/');
    console.log('*******************************************************************************');
    console.log('                  A blockchain enabled OLAP Data Warehouse                    ');
    console.log('*******************************************************************************');
}

function errorToJson (error) {
    return {status: 'ERROR', message: error.message};
}

function getGroupByStruct(groupBy){
    var struct = {
        "hash":groupBy.hash,
        "latestFact":groupBy.latestFact,
        "size":groupBy.size,
        "colSize":groupBy.colSize,
        "columns":groupBy.columns,
        "aggrFunc":groupBy.aggrFunc,
        "timestamp":groupBy.timestamp
    }
    return struct;
}

function createStruct(crnHash, latestId, colSize, resultSize, columns,aggrFunc,timestamp){
    //console.log("createt struck columns: "+JSON.parse(columns).fields);
    var struct = {
        "hash":crnHash,
        "latestFact":latestId,
        "size":resultSize,
        "colSize":colSize,
        "columns":JSON.stringify(JSON.parse(columns).fields),
        "aggrFunc":aggrFunc,
        "timestamp":timestamp
    }
    return struct;
}

function cacheNow(groupBys,action){
    
    var data=" "+action+'\n';
    //console.log('cache now \n');
    k++;
    //console.log(JSON.stringify(groupBys));
    let i=0;
    let size=0;
    // for(i;i<parseInt(JSON.stringify(count));i++){
    //     size+= groupBys[i].size;
    // }
    console.log(groupBys.length);
    for(i;i<groupBys.length;i++){
        console.log('str:'+JSON.stringify(groupBys[i]));
        //const meta = JSON.parse(groupBys[i]);
        //const hash = JSON.parse(groupBys[i]);
     //var data='cache size '+(count/1024)+'\n';
        // //const meta2 = JSON.parse(groupBys[i].cacheEvictionCost);
        if(config.cacheEvictionPolicy=='cubeDistance'){
            data+=i+' '+JSON.stringify(groupBys[i])+" "+groupBys[i].dataCubeDistance+"\n";    
        }else if(config.cacheEvictionPolicy=='costFunction'){
            data+=i+' '+JSON.stringify(meta.fields)+" "+groupBys[i].cacheEvictionCost+"\n";
        }else{
            data+=i+' '+JSON.stringify(groupBys[i])+" ";//+groupBys[i].gbTimestamp+"\n";
        }
    }
    
    fs.appendFile(__dirname+'/../cacheRecords/W1q1000CD3022p.json', data+'\n', function (err) {
        if (err) throw err;
        //console.log('Saved!');
      });
      //console.log('json appended');
}

function fileGbs(groupBys,view,action){
    
    
    let i=0;
    let size=0;
    var data='action: '+action+'\n current view: '+JSON.stringify(view)+'\n';
    for(i;i<groupBys.length;i++){
      data+=JSON.stringify(groupBys[i])+'\n';
    }
    
    fs.appendFile(__dirname+'/../cacheRecords/q22testsameOldest.json', data+'\n', function (err) {
        if (err) throw err;
        //console.log('Saved!');
      });
      //console.log('json appended');
}
function checks(str1,str2,str3,str4,val){
    
    
    let i=0;
    let size=0;
    var data='V1 '+str1+'  v2: '+str2+'\n';
    data+='aggr1 '+str3+'  aggr2: '+str4+'\n';
    data+='result: '+val+'\n';
    
    fs.appendFile(__dirname+'/../cacheRecords/q52checkOldest.json', data+'\n', function (err) {
        if (err) throw err;
        //console.log('Saved!');
      });
      //console.log('json appended');
}


function materializes(gby,view){
    var columnsGby=JSON.stringify(gby.columns);
    var fieldsGby=(columnsGby.substring(columnsGby.indexOf('[')+1,columnsGby.indexOf(']'))).split(',')
    var columnsView=JSON.stringify(view.name);
    var fieldsView = (columnsView.substring(1,columnsView.indexOf('('))).split('');
    let i=0;
    for(i;i<fieldsView.length;i++){
        if(!fieldsGby.includes(fieldsView[i])){
            return false;
        }
    }
    return true;;
}

// function filterGBsSQL(resultGB,view){
//     console.log('FilterdGbSql method');
//     let filtered=[];
//     var name = JSON.stringify(view.name);
//     var view2Name =name.substring(1,name.indexOf('('));
//     //console.log(resultGB.length);
//     for(i=0;i<resultGB.length;i++){
//         //console.log('current: '+JSON.stringify(resultGB[i]));
//         var fields = JSON.stringify(resultGB[i].columns);
//         view1Name=fields.substring(fields.indexOf('[')+1,fields.indexOf(']'));
//         view1Name = view1Name.replace(/,/g,'');
//         if(materializes(view1Name,view2Name)){
//             filtered.push(resultGB[i]);
//             console.log("Filtered gb added : "+ view1Name);
//         }
//     }
//     return filtered;
// }

module.exports = {
    containsAllFields: containsAllFields,
    configFileValidations: configFileValidations,
    removeTimestamps: removeTimestamps,
    printTimes: printTimes,
    getRandomInt: getRandomInt,
    getRandomFloat: getRandomFloat,
    time: time,
    log: log,
    cacheNow:cacheNow,
    fileGbs:fileGbs,
    requireUncached: requireUncached,
    mergeSlicedCachedResult: mergeSlicedCachedResult,
    extractGBValues: extractGBValues,
    getJSONFiles: getJSONFiles,
    getGroupByStruct:getGroupByStruct,
    materializes:materializes,
    filterGBsSQL:filterGBsSQL,
    createStruct:createStruct,
    updateViewFrequency: updateViewFrequency,
    extractViewMeta: extractViewMeta,
    checkViewExists: checkViewExists,
    sanitizeSQLQuery: sanitizeSQLQuery,
    filterGBs: filterGBs,
    sortByEvictionCost: sortByEvictionCost,
    sortByCalculationCost: sortByCalculationCost,
    reconstructSlicedCachedResult: reconstructSlicedCachedResult,
    getMainTransactionObject: getMainTransactionObject,
    assignTimes: assignTimes,
    findSameOldestResults: findSameOldestResults,
    sortByWord2Vec: sortByWord2Vec,
    extractOperation: extractOperation,
    welcomeMessage: welcomeMessage,
    errorToJson: errorToJson
};
const costFunctions = require('./costFunctions');

