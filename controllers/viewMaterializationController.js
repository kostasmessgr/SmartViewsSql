const helper = require('../helpers/helper');
const cacheController = require('./cacheController');
const contractController = require('./contractController');
const computationsController = require('./computationsController');
const transformations = require('../helpers/transformations');
const stringify = require('fast-stringify');
let config = require('../config_private');
const fs = require('fs');
var prevCached='';
let prefetchedViews = [];


function setContract (contractObject, account) {
    cacheController.setContract(contractObject, account);
}

function calculateForDeltasAndMergeWithCached (mostEfficient, latestId, createTable,
                                               view, gbFields, sortedByEvictionCost,
                                               globalAllGroupBysTime, getLatestFactIdTime, totalStart) {
    return new Promise((resolve, reject) => {
        let matSteps = [];
        const bcTimeStart = helper.time();
        //console.log('current 23: '+view.gbfields+'\n');
        //console.log('most efficient: '+stringify(mostEfficient)+'\n');
        
        //console.log(mostEfficient.latestFact);
        //console.log('from: '+Number(mostEfficient.latestFact)+' to: '+Number(latestId)+' '+(Number(latestId)-Number(mostEfficient.latestFact)));
        computationsController.getFactsFromToSql(Number(mostEfficient.latestFact), Number(latestId)).then(async deltas => {
            console.log('***Merge with cached ***');
            console.log('from: '+(Number(mostEfficient.latestFact))+' to: '+Number(latestId))
            d_length=deltas.length;
            console.log('length: '+d_length);

            const bcTimeEnd = helper.time();
            matSteps.push({ type: 'bcFetchDeltas', numOfFacts: deltas.length });
            await computationsController.executeQuery(createTable).then(async results => {
                deltas = helper.removeTimestamps(deltas);
                //helper.cacheNow(sortedByEvictionCost,"calculateForDeltasAndMergeWithCached",JSON.stringify(view.name));
                helper.log('CALCULATING GROUP-BY FOR DELTAS:');
                const sqlTimeStart = helper.time();
                await computationsController.calculateNewGroupBy(deltas, view.operation, view.fields, view.aggregationField).then(async groupBySqlResult => {
                    const sqlTimeEnd = helper.time();
                    const allHashes = helper.reconstructSlicedCachedResult(mostEfficient);
                    matSteps.push({ type: 'sqlCalculationDeltas' });
                    const cacheRetrieveTimeStart = helper.time();
                    await cacheController.getManyCachedResults(allHashes).then(async allCached => {
                        //helper.cacheNow(allCached,'Currently in cache');
                        const cacheRetrieveTimeEnd = helper.time();
                        matSteps.push({ type: 'cacheFetch','cached':allCached.length });
                        const cachedGroupBy = cacheController.preprocessCachedGroupBy(allCached);
                        // console.log('allcached******\n'+stringify(cachedGroupBy)+'\n');
                        // //console.log(Object.keys(cachedGroupBy).length);
                        // console.log('47');
                        // console.log('cachedgrouby.field '+JSON.stringify(cachedGroupBy.field))
                        // console.log('view.aggregationfield '+JSON.stringify(view.aggregationField))
                        // console.log('view.operation '+JSON.stringify(view.operation))
                        // console.log('cachedGroupBy.operation '+JSON.stringify(cachedGroupBy.operation))

                        if (cachedGroupBy.field === view.aggregationField &&
                            view.operation === cachedGroupBy.operation) {
                                //console.log('51');
                            if (cachedGroupBy.groupByFields.length !== view.fields.length) {
                                const reductionTimeStart = helper.time();
                                computationsController.calculateReducedGroupBy(cachedGroupBy, view, gbFields).then(async reducedResult => {
                                    const reductionTimeEnd = helper.time();
                                    matSteps.push({ type: 'sqlReduction', from: cachedGroupBy.groupByFields, to: gbFields });
                                    const viewMeta = helper.extractViewMeta(view);
                                    // MERGE reducedResult with groupBySQLResult
                                    reducedResult = transformations.transformGBFromSQL(reducedResult, viewMeta.op, viewMeta.lastCol, gbFields);
                                    reducedResult.field = view.aggregationField;
                                    reducedResult.viewName = view.name;
                                    const rows = helper.extractGBValues(reducedResult, view);
                                    const rowsDeltas = helper.extractGBValues(groupBySqlResult,view);
                                    const mergeTimeStart = helper.time();
                                    computationsController.mergeGroupBys(rows, rowsDeltas,
                                        view, viewMeta).then(mergeResult => {
                                        const mergeTimeEnd = helper.time();
                                        matSteps.push({ type: 'sqlMergeReducedCachedWithDeltas', 'cached':rows.length, 'bc':rows.length,'res':mergeResult.length });
                                        mergeResult.operation = view.operation;
                                        mergeResult.field = view.aggregationField;
                                        mergeResult.gbCreateTable = view.SQLTable;
                                        mergeResult.viewName = view.name;
                                        // save on cache before return
                                        const gbSize = stringify(mergeResult).length;
                                        const rowsRes = helper.extractGBValues(mergeResult,view);
                                        const type='MergeReducedCachedWithDeltas'
                                        //helper.cacheNow(mergeResult,'Added new GB Beggining',stringify(view.name))
                                        create_recordJSON(cachedGroupBy,d_length,rowsRes,stringify(view.name),type);
                                        //console.log('after json record');
                                        if (gbSize / 1024 <= config.maxCacheSizeInKB) {
                                    
                                            const cacheSaveTimeStart = helper.time();
                                            cacheController.saveOnCache(mergeResult, view.operation, latestId - 1).then((receipt)=>{
       
                                                const cacheSaveTimeEnd = helper.time();
                                                matSteps.push({ type: 'cacheSave' });
                                                delete mergeResult.gbCreateTable;

                                                let times = { bcTime: (bcTimeEnd - bcTimeStart) + getLatestFactIdTime + globalAllGroupBysTime.getGroupIdTime + globalAllGroupBysTime.getAllGBsTime,
                                                    sqlTime: (mergeTimeEnd - mergeTimeStart) + (reductionTimeEnd - reductionTimeStart),
                                                    cacheRetrieveTime: cacheRetrieveTimeEnd - cacheRetrieveTimeStart,
                                                    cacheSaveTime: cacheSaveTimeEnd - cacheSaveTimeStart,
                                                    totalStart: totalStart };
                                                times.totalTime = times.bcTime + times.sqlTime + times.cacheRetrieveTime + times.cacheSaveTime;
                                                const sameOldestResults = helper.findSameOldestResults(sortedByEvictionCost, view);
                                                helper.log('receipt:' + JSON.stringify(receipt));
                                                times.totalEnd = helper.time();
                                                mergeResult = helper.assignTimes(mergeResult, times);
                                                mergeResult.matSteps = matSteps;
                                                helper.printTimes(mergeResult);
                                                //console.log('before resolve 101');
                                                resolve(mergeResult);
                                                return clearCacheIfNeeded(sortedByEvictionCost, mergeResult, sameOldestResults, times).catch(err => {
                                                    reject(err);
                                                });
                                            }).catch((err)=>{
                                                    reject(err);
                                            });
                                        } else {
                                            const totalEnd = helper.time();
                                            //console.log('inside else');
                                            const sqlTime = (sqlTimeEnd - sqlTimeStart);
                                            const reductionTime = (reductionTimeEnd - reductionTimeStart);
                                            const mergeTime = (mergeTimeEnd - mergeTimeStart);
                                            const bcTime = (bcTimeEnd - bcTimeStart);
                                            mergeResult.sqlTime = sqlTime + reductionTime + mergeTime;
                                            mergeResult.bcTime = bcTime + getLatestFactIdTime + globalAllGroupBysTime.getGroupIdTime + globalAllGroupBysTime.getAllGBsTime;
                                            mergeResult.cacheRetrieveTime = cacheRetrieveTimeEnd - cacheRetrieveTimeStart;
                                            mergeResult.totalTime = mergeResult.sqlTime + mergeResult.bcTime + mergeResult.cacheSaveTime + mergeResult.cacheRetrieveTime;
                                            mergeResult.allTotal = totalEnd - totalStart;
                                            mergeResult.matSteps = matSteps;
                                            helper.printTimes(mergeResult);
                                            //  prefetchedViews = prefetchNearset(10, sortedByEvictionCost, view);
                                            resolve(mergeResult);
                                        }
                                    }).catch(err => {
                                        /* istanbul ignore next */
                                        helper.log(err);
                                        /* istanbul ignore next */
                                        reject(err);
                                    });
                                }).catch(err => {
                                    /* istanbul ignore next */
                                    helper.log(err);
                                    /* istanbul ignore next */
                                    reject(err);
                                });
                            } else {
                                helper.log('GROUP-BY FIELDS OF DELTAS AND CACHED ARE THE SAME');
                                // group by fields of deltas and cached are the same so
                                // MERGE cached and groupBySqlResults
                                const times = { bcTimeEnd: bcTimeEnd,
                                    bcTimeStart: bcTimeStart,
                                    getGroupIdTime: globalAllGroupBysTime.getGroupIdTime,
                                    getAllGBsTime: globalAllGroupBysTime.getAllGBsTime,
                                    getLatestFactIdTime: getLatestFactIdTime,
                                    sqlTimeEnd: sqlTimeEnd,
                                    sqlTimeStart: sqlTimeStart,
                                    cacheRetrieveTimeEnd: cacheRetrieveTimeEnd,
                                    cacheRetrieveTimeStart: cacheRetrieveTimeStart,
                                    totalStart: totalStart };

                                await mergeCachedWithDeltasResultsSameFields(view, cachedGroupBy,
                                    groupBySqlResult, latestId, sortedByEvictionCost,d_length, times).then(result => {
                                    matSteps.push({ type: 'sqlMergeCachedWithDeltas','cached':cachedGroupBy.length });
                                    result.matSteps = matSteps;
                                    //console.log('aaaaaaa');
                                    //  prefetchedViews = prefetchNearset(10, sortedByEvictionCost, view);
                                    //console.log('before result resolve: '+JSON.stringify(result));
                                    resolve(result);
                                }).catch(err => {
                                    /* istanbul ignore next */
                                    helper.log(err);
                                    /* istanbul ignore next */
                                    reject(err);
                                });
                            }
                        }
                        //should add a fallback case
                    }).catch(err => {
                        /* istanbul ignore next */
                        helper.log(err);
                        /* istanbul ignore next */
                        reject(err);
                    });
                }).catch(err => {
                    /* istanbul ignore next */
                    helper.log(err);
                    /* istanbul ignore next */
                    reject(err);
                });
            }).catch(err => {
                /* istanbul ignore next */
                helper.log(err);
                /* istanbul ignore next */
                reject(err);
            });
        });
    });
}

function reduceGroupByFromCache (cachedGroupBy, view, gbFields, sortedByEvictionCost, times, latestId) {
    return new Promise((resolve, reject) => {
        const reductionTimeStart = helper.time();
        computationsController.calculateReducedGroupBy(cachedGroupBy, view, gbFields).then(async reducedResult => {
            const reductionTimeEnd = helper.time();
            const viewMeta = helper.extractViewMeta(view);
            if (view.operation === 'AVERAGE') {
                reducedResult = transformations.transformAverage(reducedResult, view.fields, view.aggregationField);
            } else {
                reducedResult = transformations.transformGBFromSQL(reducedResult, viewMeta.op, viewMeta.lastCol, gbFields);
            }
            reducedResult.field = view.aggregationField;
            reducedResult.viewName = view.name;
            reducedResult.operation = view.operation;
            const gbSize = stringify(reducedResult).length;
            if (gbSize / 1024 <= config.maxCacheSizeInKB) {
                let cacheSaveTimeStart = helper.time();
                cacheController.saveOnCache(reducedResult, view.operation, latestId - 1).then((receipt) => {
                    helper.log('receipt:' + JSON.stringify(receipt));
                    const cacheSaveTimeEnd = helper.time();
                    const times2 = {
                        sqlTimeEnd: reductionTimeEnd,
                        sqlTimeStart: reductionTimeStart,
                        totalStart: times.totalStart,
                        cacheSaveTimeStart: cacheSaveTimeStart,
                        cacheSaveTimeEnd: cacheSaveTimeEnd,
                        cacheRetrieveTimeStart: times.cacheRetrieveTimeStart,
                        cacheRetrieveTimeEnd: times.cacheRetrieveTimeEnd
                    };
                    times2.totalEnd = helper.time();
                    reducedResult = helper.assignTimes(reducedResult, times2);
                    helper.printTimes(reducedResult);
                    resolve(reducedResult);
                    const sameOldestResults = helper.findSameOldestResults(sortedByEvictionCost, view);
                    return clearCacheIfNeeded(sortedByEvictionCost, reducedResult, sameOldestResults, times2).catch(err => {
                        /* istanbul ignore next */
                        console.log(err);
                        /* istanbul ignore next */
                        reject(err);
                    });
                }).catch((err)=>{
                    reject(err);
                });
            } else {
                const times2 = {
                    sqlTimeEnd: reductionTimeEnd,
                    sqlTimeStart: reductionTimeStart,
                    totalStart: times.totalStart,
                    cacheRetrieveTimeStart: times.cacheRetrieveTimeStart,
                    cacheRetrieveTimeEnd: times.cacheRetrieveTimeEnd
                };
                reducedResult = helper.assignTimes(reducedResult, times2);
                helper.printTimes(reducedResult);
                // prefetchedViews = prefetchNearset(10, sortedByEvictionCost, view);
                resolve(reducedResult);
            }
        }).catch(err => {
            /* istanbul ignore next */
            console.log(err);
            /* istanbul ignore next */
            reject(err);
        });
    });
}

function mergeCachedWithDeltasResultsSameFields (view, cachedGroupBy, groupBySqlResult, latestId, sortedByEvictionCost,d_length ,times) {
    return new Promise((resolve, reject) => {
        const viewMeta = helper.extractViewMeta(view);
        const rows = helper.extractGBValues(cachedGroupBy, view);
        const rowsDelta = helper.extractGBValues(groupBySqlResult, view);
        const mergeTimeStart = helper.time();
        //helper.cacheNow(sortedByEvictionCost,"mergeCachedWithDeltasResultsSameFields",JSON.stringify(view.name));
        computationsController.mergeGroupBys(rows, rowsDelta,
            view, viewMeta).then(mergeResult => {
            let mergeTimeEnd = helper.time();
            // SAVE ON CACHE BEFORE RETURN
            helper.log('SAVE ON CACHE BEFORE RETURN');
            //console.log('mergeresult 279: '+JSON.stringify(mergeResult));
            mergeResult.operation = view.operation;
            mergeResult.field = view.aggregationField;
            mergeResult.gbCreateTable = view.SQLTable;
            mergeResult.viewName = view.name;
            var str="mergeresultsamefields";
            //helper.cacheNow(mergeResult,'Added new GB Beggining',stringify(view.name))
            const rowsRes = helper.extractGBValues(mergeResult,view);
            //console.log('mergee 269: \n '+stringify(mergeResult));
            //console.log('mergeresult '+mergeResult.length);
            //console.log('rowsres '+rowsRes.length);
            const type='mergeCachedWithDeltasResultsSameFields'
            
            create_recordJSON(cachedGroupBy,d_length,rowsRes,stringify(view.name),type);
            const gbSize = stringify(mergeResult).length;
            //console.log('gbSize '+gbSize);
            if (gbSize / 1024 <= config.maxCacheSizeInKB) {
                //console.log('insde if');
                const cacheSaveTimeStart = helper.time();
                cacheController.saveOnCache(mergeResult, view.operation, latestId - 1).then((receipt) => {
                    const cacheSaveTimeEnd = helper.time();
                    delete mergeResult.gbCreateTable;
                    let timesReady = {};
                    helper.log('receipt:' + JSON.stringify(receipt));
                    timesReady.bcTime = (times.bcTimeEnd - times.bcTimeStart) + times.getGroupIdTime + times.getAllGBsTime + times.getLatestFactIdTime;
                    timesReady.sqlTime = (mergeTimeEnd - mergeTimeStart) + (times.sqlTimeEnd - times.sqlTimeStart);
                    timesReady.cacheSaveTime = cacheSaveTimeEnd - cacheSaveTimeStart;
                    timesReady.cacheRetrieveTime = times.cacheRetrieveTimeEnd - times.cacheRetrieveTimeStart;
                    timesReady.totalTime = timesReady.bcTime + timesReady.sqlTime + timesReady.cacheSaveTime + timesReady.cacheRetrieveTime;
                    timesReady.totalStart = times.totalStart;
                    timesReady.totalEnd = helper.time();
                    mergeResult = helper.assignTimes(mergeResult, timesReady);
                    helper.printTimes(mergeResult);
                   // console.log('mergeresult before resolve: '+JSON.stringify(mergeResult));
                    resolve(mergeResult);
                    //find from sortedByEvictionCost any cached result that is exactly the same with the one requested
                    //then add it to a separate array and delete it anyway independently to if they are already in sortedByEvictionCost
                    const sameOldestResults = helper.findSameOldestResults(sortedByEvictionCost, view);
                    return clearCacheIfNeeded(sortedByEvictionCost, mergeResult, sameOldestResults, timesReady).catch(err => {
                        /* istanbul ignore next */
                        reject(err);
                    });
                }).catch((err=>{
                    reject(err);
                }));
            } else {
                //console.log('else');
                delete mergeResult.gbCreateTable;
                let timesReady = {};
                timesReady.bcTime = (times.bcTimeEnd - times.bcTimeStart) + times.getGroupIdTime + times.getAllGBsTime + times.getLatestFactIdTime;
                timesReady.sqlTime = (mergeTimeEnd - mergeTimeStart) + (times.sqlTimeEnd - times.sqlTimeStart);
                timesReady.cacheRetrieveTime = times.cacheRetrieveTimeEnd - times.cacheRetrieveTimeStart;
                timesReady.totalTime = timesReady.bcTime + timesReady.sqlTime + timesReady.cacheSaveTime + timesReady.cacheRetrieveTime;
                timesReady.totalStart = times.totalStart;
                timesReady.totalEnd = helper.time();
                mergeResult = helper.assignTimes(mergeResult, timesReady);
                helper.printTimes(mergeResult);
                // prefetchedViews = prefetchNearset(10, sortedByEvictionCost, view);
                resolve(mergeResult);
            }
        }).catch(err => {
            /* istanbul ignore next */
            reject(err);
        });
    });
}

function calculateNewGroupByFromBeginning (view, totalStart, getGroupIdTime, sortedByEvictionCost) {
    return new Promise((resolve, reject) => {
        let matSteps = [];
        let bcTimeStart = helper.time();
        //console.log('calculate from beginning');
        
        
        computationsController.getLatestIdSQL().then(latestId => {
            computationsController.getFactsHeavySql().then(retval => {
                const bcTimeEnd = helper.time();
                //console.log('latest id cgbfb '+latestId);
                if (retval.length === 0) {
                    return reject(new Error('No facts exist in blockchain'));
                }
                matSteps.push({ type: 'bcFetch', numOfFacts: retval.length });
                const facts = helper.removeTimestamps(retval);
                helper.log('CALCULATING NEW GROUP-BY FROM BEGINING');
                const sqlTimeStart = helper.time();
                computationsController.calculateNewGroupBy(facts, view.operation, view.fields, view.aggregationField).then(groupBySqlResult => {
                    matSteps.push({ type: 'sqlCalculationInitial' });
                    const sqlTimeEnd = helper.time();
                    const type = 'fromBeginning'
                    //console.log(type);
                    const rows = helper.extractGBValues(groupBySqlResult,view);
                   // console.log('after helper: '+JSON.stringify(rows))
                    create_recordJSON({},facts,rows,stringify(view.name),type);
                    groupBySqlResult.gbCreateTable = view.SQLTable;
                    groupBySqlResult.field = view.aggregationField;
                    groupBySqlResult.viewName = view.name;
                    const gbSize = stringify(groupBySqlResult).length;
                    //console.log("res: "+gbSize);
                    //helper.cacheNow(groupBySqlResult,'Added new GB Beggining',stringify(view.name))
                    if (config.cacheEnabled && ((gbSize / 1024) <= config.maxCacheSizeInKB)) {
                        const cacheSaveTimeStart = helper.time();
                        cacheController.saveOnCache(groupBySqlResult, view.operation, latestId - 1).then((receipt) => {
                            //console.log('after error! All good');
                            matSteps.push({ type: 'cacheSave' });
                            const cacheSaveTimeEnd = helper.time();
                            delete groupBySqlResult.gbCreateTable;
                            //console.log('receopt: '+JSON.stringify(receipt));
                            helper.log('receipt:' + JSON.stringify(receipt));
                            //console.log('before times');
                            let times = { sqlTime: sqlTimeEnd - sqlTimeStart,
                                bcTime: (bcTimeEnd - bcTimeStart) + getGroupIdTime,
                                cacheSaveTime: cacheSaveTimeEnd - cacheSaveTimeStart,
                                totalStart: totalStart };
                                //console.log('after times');
                            times.totalTime = times.bcTime + times.sqlTime + times.cacheSaveTime;
                            times.totalEnd = helper.time();
                            groupBySqlResult = helper.assignTimes(groupBySqlResult, times);
                            helper.printTimes(groupBySqlResult);
                            groupBySqlResult.matSteps = matSteps;
                            //console.log('resolve 401  '+JSON.stringify(groupBySqlResult));
                            resolve(groupBySqlResult);
                            let sameOldestResults = helper.findSameOldestResults(sortedByEvictionCost, view);
                            //console.log("before clear cache if needed");
                            return clearCacheIfNeeded(sortedByEvictionCost, groupBySqlResult, sameOldestResults, times).catch(err => {
                                /* istanbul ignore next */
                                reject(err);
                            }).catch((err)=>{
                                reject(err);
                            });
                        });
                    } else {
                        let totalEnd = helper.time();
                        groupBySqlResult.sqlTime = sqlTimeEnd - sqlTimeStart;
                        groupBySqlResult.bcTime = (bcTimeEnd - bcTimeStart) + getGroupIdTime;
                        groupBySqlResult.totalTime = groupBySqlResult.sqlTime + groupBySqlResult.bcTime;
                        groupBySqlResult.allTotal = totalEnd - totalStart;
                        groupBySqlResult.matSteps = matSteps;
                        helper.printTimes(groupBySqlResult);
                        // prefetchedViews = prefetchNearset(10, sortedByEvictionCost, view);
                        resolve(groupBySqlResult);
                    }
                }).catch(err => {
                    /* istanbul ignore next */
                    console.log(err);
                    /* istanbul ignore next */
                    reject(err);
                });
            });
        }).catch(err => {
            /* istanbul ignore next */
            console.log(err);
            /* istanbul ignore next */
            reject(err);
        });
    });
}

function clearCacheIfNeeded (sortedByEvictionCost, groupBySqlResult, sameOldestResults, times) {
    return new Promise((resolve, reject) => {
        let totalCurrentCacheLoad = 0; // in Bytes
        helper.log('current view ' + stringify(groupBySqlResult.viewName));
        helper.log('***CACHED OBJECTS***');
        helper.log(sortedByEvictionCost);
        
        // console.log("clear cache if needed")
        // console.log('0. '+sortedByEvictionCost.length);
        //helper.fileGbs(sortedByEvictionCost,stringify(groupBySqlResult.viewName),'Currently');
        for (let i = 0; i < sortedByEvictionCost.length; i++) {
            totalCurrentCacheLoad += parseInt(sortedByEvictionCost[i].size);
        }
        helper.cacheNow(sortedByEvictionCost,"Entire");
        //helper.cacheNow(sortedByEvictionCost,'clear cache if needed',String(totalCurrentCacheLoad/1024));
        //console.log('current cache load '+totalCurrentCacheLoad/1024);
        
            //helper.fileGbs(sortedByEvictionCostFiltered,view,'Evict');
        
        helper.log('CURRENT CACHE LOAD = ' + totalCurrentCacheLoad + ' Bytes OR ' + (totalCurrentCacheLoad / 1024) + ' KB');
        // console.log('before if');
        if (totalCurrentCacheLoad > 0 && (totalCurrentCacheLoad / 1024) >= config.maxCacheSizeInKB) {
            // delete as many cached results as to free up cache size equal to the size of the latest result we computed
            // we can easily multiply it by a factor to see how it performs
            //console.log('inside if');
            helper.log('-->CLEARING CACHE');
            let sortedByEvictionCostFiltered = [];
            const gbSize = stringify(groupBySqlResult).length;
            //helper.cacheNow(sortedByEvictionCost,"Entire",stringify(totalCurrentCacheLoad+gbSize));
            let totalSize = 0;
            for (let k = 0; k < sameOldestResults.length; k++) {
                let indexInSortedByEviction = sortedByEvictionCost.indexOf(sameOldestResults[k]);
                if (indexInSortedByEviction > -1) {
                    totalSize += parseInt(sortedByEvictionCost[indexInSortedByEviction].size);
                    sortedByEvictionCost = sortedByEvictionCost.splice(indexInSortedByEviction, 1);
                }
            }
            helper.log("SAME OLDEST TO DELETE:");
            //console.log('before sameoldest to delete');
            helper.log(sameOldestResults);
            //console.log('after same oldest to delete');
            let i = 0;
            const copy_array = sortedByEvictionCost.reverse();
            let crnSize = parseInt(copy_array[0].size);
            while ((totalSize + crnSize) < (config.maxCacheSizeInKB * 1024 - gbSize)) {
                helper.log((totalSize+crnSize) + ' < '+((config.maxCacheSizeInKB*1024) - gbSize));
                if (copy_array[i]) {
                    crnSize = parseInt(copy_array[i].size);
                } else {
                    break;
                }
                totalSize += crnSize;
                i++;
            }

            
            //console.log('need to evict')
            
            for (let k = i; k < copy_array.length; k++) {
                sortedByEvictionCostFiltered.push(copy_array[k]);  //possible error because this element already in sortedByEvictionCostFiltered
                helper.log('Evicted view ' + copy_array[k].columns+' with size: ' +(parseInt(copy_array[k].size)/1024)+' and cost: '+copy_array[k].cacheEvictionCost)
            }
            helper.cacheNow(sortedByEvictionCostFiltered,'Evict')
                        //console.log('time to evict');
            helper.log('TOTAL SIZE = ' + totalSize);
            helper.log('GB SIZE = ' + gbSize);
            const tot = (totalSize + gbSize);
            let res = tot.toString() + '\n';
            helper.log('result to txt: ' + res);
            fs.appendFile('cache_sizeCFV2412.txt', res, function (err) {
                if (err) {
                    /* istanbul ignore next */
                    return console.error(err);
                }
            });
            //console.log('0. '+sortedByEvictionCostFiltered.length);
            sortedByEvictionCostFiltered = sortedByEvictionCostFiltered.concat(sameOldestResults);
            
            contractController.deleteCachedResults(sortedByEvictionCostFiltered).then(deleteReceipt => {
                times.totalEnd = helper.time();
                if (times) {
                    groupBySqlResult = helper.assignTimes(groupBySqlResult, times);
                }
                resolve(groupBySqlResult);
            }).catch(err => {
                /* istanbul ignore next */
                reject(err);
            });
        } else {
            helper.log('-->NOT CLEARING CACHE');
            let gbSize = stringify(groupBySqlResult).length;
            helper.log('GB SIZE = ' + gbSize);
            let tot = (totalCurrentCacheLoad + gbSize);
            let res = tot.toString() + '\n';
            helper.log('result to txt: ' + res);
            //helper.cacheNow(sortedByEvictionCost,'Not clearing',stringify(groupBySqlResult.viewName));
            fs.appendFile('cache_sizeCFV2823.txt', res, function (err) {
                if (err) {
                    /* istanbul ignore next */
                    console.error(err);
                }
            });
            times.totalEnd = helper.time();
            if (times) {
                groupBySqlResult = helper.assignTimes(groupBySqlResult, times);
            }

            if (sameOldestResults.length > 0) {
                contractController.deleteCachedResults(sameOldestResults).then(deleteReceipt => {
                    times.totalEnd = helper.time();
                    if (times) {
                        groupBySqlResult = helper.assignTimes(groupBySqlResult, times);
                    }
                    helper.log('DELETED CACHED RESULTS');
                    resolve(groupBySqlResult);
                }).catch(err => {
                    /* istanbul ignore next */
                    reject(err);
                });
            } else {
                times.totalEnd = helper.time();
                if (times) {
                    groupBySqlResult = helper.assignTimes(groupBySqlResult, times);
                }
                resolve(groupBySqlResult);
            }
        }
    });
}

function calculateFromCache (cachedGroupBy, sortedByEvictionCost, view, gbFields, latestId, times, matSteps) {
    return new Promise(async (resolve, reject) => {
        if (cachedGroupBy.groupByFields.length !== view.fields.length) {
            // this means we want to calculate a different group by than the stored one
            // but however it can be calculated just from redis cache
            if (cachedGroupBy.field === view.aggregationField &&
                view.operation === cachedGroupBy.operation) {
                await reduceGroupByFromCache(cachedGroupBy, view, gbFields, sortedByEvictionCost,
                    times, latestId).then(results => {
                    matSteps.push({ type: 'sqlReduction', from: cachedGroupBy.groupByFields, to: gbFields });
                    results.matSteps = matSteps;
                    // prefetchedViews = prefetchNearset(10, sortedByEvictionCost, view);
                    return resolve(results);
                }).catch(err => {
                    /* istanbul ignore next */
                    return reject(err);
                });
            }
        } else {
            if (cachedGroupBy.field === view.aggregationField &&
                view.operation === cachedGroupBy.operation) {
                const totalEnd = helper.time();
                // this means we just have to return the group by stored in cache
                // field, operation are same and no new records written
                cachedGroupBy.cacheRetrieveTime = times.cacheRetrieveTimeEnd - times.cacheRetrieveTimeStart;
                cachedGroupBy.totalTime = cachedGroupBy.cacheRetrieveTime;
                cachedGroupBy.allTotal = totalEnd - times.totalStart;
                cachedGroupBy.matSteps = matSteps;
                // prefetchedViews = prefetchNearset(10, sortedByEvictionCost, view);
                return resolve(cachedGroupBy);
            }
        }
        // console.log('calculate from cache')
        // console.log('******line 577******');
        calculateNewGroupByFromBeginning(view, times.totalStart, times.getGroupIdTime, sortedByEvictionCost).then(result => {
            // prefetchedViews = prefetchNearset(10, sortedByEvictionCost, view);
            //console.log('result resolve 582 '+JSON.stringify(result));
            return resolve(result);
        }).catch(err => {
            /* istanbul ignore next */
            return reject(err);
        });
    });
}

async function prefetchNearset (n, cachedResults, view) {
    const resultGBs = await helper.sortByWord2Vec(cachedResults, view);
    let viewNames = [];
    for (let i = 0; i < resultGBs.length; i++) {
        const meta = JSON.parse(resultGBs[i].columns);
        viewNames.push(meta.fields.join('') + '(' + meta.aggrFunc + ')');
    }
    viewNames = viewNames.filter(function (item, pos) {
        return viewNames.indexOf(item) === pos;
    });
    if (viewNames.length > n) {
        viewNames = viewNames.slice(0, n - 1)
    }
    // prefetch nearest viewnames there
    return viewNames;
}

async function materializeView (view, contract, totalStart, createTable) {
    return new Promise(async (resolve, reject) => {
        console.log('inside materialize view');
        let materializationDone = false;
        const factTbl = require('../templates/' + contract);
        const gbFields = view.fields;
        view.fields = gbFields;
        let globalAllGroupBysTime = { getAllGBsTime: 0, getGroupIdTime: 0 };
        console.log(config.cacheEnabled);
        if (config.cacheEnabled) {
            helper.log('cache enabled = TRUE');
            console.log('get all group bys bef');
            await contractController.getAllGroupbys().then(async resultGB => {
                //console.log("outside"+JSON.stringify(resultGB));
                //helper.cacheNow(resultGB,"result getALL",view.name);
                
                if (resultGB.times.getGroupIdTime !== null && resultGB.times.getGroupIdTime !== undefined) {
                    globalAllGroupBysTime.getGroupIdTime = resultGB.times.getGroupIdTime;
                }

                if (resultGB.times.getAllGBsTime !== null && resultGB.times.getAllGBsTime !== undefined) {
                    globalAllGroupBysTime.getAllGBsTime = resultGB.times.getAllGBsTime;
                }
                delete resultGB.times;

                if (Object.keys(resultGB).length > 0) {
                
                    const filteredGBs = helper.filterGBsSQL(resultGB, view);
                
                    if (filteredGBs.length > 0) {
                        const getLatestFactIdTimeStart = helper.time();
                        console.log(603);
                        await computationsController.getLatestIdSQL().then(async latestId => {
                            
                            const sortedByCalculationCost =  helper.sortByCalculationCost(filteredGBs, latestId, view);
                            const sortedByEvictionCost =  await helper.sortByEvictionCost(resultGB, latestId, view, factTbl);
                            //helper.cacheNow(sortedByEvictionCost,"Sorted by eviction cost",view.name);
                            helper.log(sortedByEvictionCost);
                            const mostEfficient = sortedByCalculationCost[0];
                            const getLatestFactIdTime = helper.time() - getLatestFactIdTimeStart;
                            var res=JSON.stringify(mostEfficient);
                            res = mostEfficient.latestFact+' latest id: '+(latestId-1)+'\n';
                            fs.appendFile('message.txt', res, function (err) {
                                if (err) {
                                    /* istanbul ignore next */
                                    return console.error(err);
                                }
                            });
                            if (mostEfficient.latestFact >= (latestId - 1)) {
                                helper.log('NO NEW FACTS');
                                // NO NEW FACTS after the latest group by
                                // -> incrementally calculate the groupby requested by summing the one in redis cache
                                const allHashes = helper.reconstructSlicedCachedResult(mostEfficient);
                                const cacheRetrieveTimeStart = helper.time();
                                let matSteps = [];
                                await cacheController.getManyCachedResults(allHashes).then(async allCached => {
                                    //helper.cacheNow(allCached,'Currently in cache');
                                    let cacheRetrieveTimeEnd = helper.time();
                                    matSteps.push({type: 'cacheFetch'});
                                    let cachedGroupBy = cacheController.preprocessCachedGroupBy(allCached);
                                    if (cachedGroupBy) {
                                        let times = {
                                            cacheRetrieveTimeEnd: cacheRetrieveTimeEnd,
                                            cacheRetrieveTimeStart: cacheRetrieveTimeStart,
                                            totalStart: totalStart,
                                            getGroupIdTime: globalAllGroupBysTime.getGroupIdTime
                                        };
                                        await calculateFromCache(cachedGroupBy,
                                            sortedByEvictionCost, view, gbFields, latestId, times, matSteps).then(result => {
                                            materializationDone = true;
                                            resolve(result);
                                        }).catch(err => {
                                            /* istanbul ignore next */
                                            helper.log(err);
                                            /* istanbul ignore next */
                                            reject(err);
                                        });
                                    }
                                }).catch(err => {
                                    /* istanbul ignore next */
                                    helper.log(err);
                                    /* istanbul ignore next */
                                    reject(err);
                                });
                            } else {
                                helper.log('DELTAS DETECTED');
                                // we have deltas -> we fetch them
                                // CALCULATING THE VIEW JUST FOR THE DELTAS
                                // THEN MERGE IT WITH THE ONES IN CACHE
                                // THEN SAVE BACK IN CACHE
                                await calculateForDeltasAndMergeWithCached(mostEfficient,
                                    latestId, createTable, view, gbFields, sortedByEvictionCost, globalAllGroupBysTime,
                                    getLatestFactIdTime, totalStart).then(results => {
                                    materializationDone = true;
                                    resolve(results);
                                }).catch(err => {
                                    /* istanbul ignore next */
                                    helper.log(err);
                                    /* istanbul ignore next */
                                    reject(err);
                                });
                            }
                        }).catch(err => {
                            /* istanbul ignore next */
                            helper.log(err);
                            /* istanbul ignore next */
                            reject(err);
                        });
                    } else {
                        // No filtered group-bys found, proceed to group-by from the beginning
                        helper.log('NO FILTERED GROUP BYS FOUND');
                        //console.log('NO FILTERED GROUP BYS FOUND');
                        await computationsController.getLatestIdSQL(async latestId =>  {
                            
                            const sortedByEvictionCost = await helper.sortByEvictionCost(resultGB, latestId, view, factTbl);
                            helper.log("#######");
                            helper.log(sortedByEvictionCost);
                            
                            calculateNewGroupByFromBeginning(view, totalStart,
                                globalAllGroupBysTime.getGroupIdTime, sortedByEvictionCost).then(result => {
                                materializationDone = true;
                                //console.log("before resolve 715 "+JSON.stringify(result))
                                resolve(result);
                            }).catch(err => {
                                
                                /* istanbul ignore next */
                                reject(err);
                            });
                        }).catch(err => {
                            /* istanbul ignore next */
                            reject(err);
                        });
                    }
                }
            }).catch(err => {
                /* istanbul ignore next */
                console.log("error in mat error 735");
                helper.log(err);
                /* istanbul ignore next */
                reject(err);
            });
        }
        if (!materializationDone) {
            // this is the default fallback where the view requested is materialized from the beginning
            //console.log('!materializationDone 738')
            calculateNewGroupByFromBeginning(view, totalStart,
                globalAllGroupBysTime.getGroupIdTime + globalAllGroupBysTime.getAllGBsTime,
                []).then(result => {
                    //console.log('result resoved 744 '+JSON.stringify(result));
                    resolve(result);
            }).catch(err => {
                /* istanbul ignore next */
                reject(err);
            });
        }
    });
}

function create_recordJSON(rowsCache,rowsBC,rowsResult,viewName,type){
    //console.log('inside json');
    var cache_recs=0;
    var bc_recs=0;
    if(rowsCache!=undefined){
        if(Object.keys(rowsCache).length>0){
            cache_recs=Object.keys(rowsCache).length-5;
        }
    }
    if(type=='MergeReducedCachedWithDeltas' || type=="mergeCachedWithDeltasResultsSameFields"){
        bc_recs=rowsBC;
    }else{
        bc_recs=rowsBC.length;
    }
    var record = {
        "cacheRecords":cache_recs,
        "bcRecords":bc_recs,
        "resultRecords":rowsResult.length,
        "viewName":viewName,
        "type":type
    }
    
    fs.appendFile(__dirname+'/../resRecords/W3q1000CF40an600p.json', JSON.stringify(record)+', \n', function (err) {
        if (err) throw err;
        //console.log('Saved!');
      });
      //console.log('json appended');

}

module.exports = {
    setContract: setContract,
    materializeView: materializeView
};