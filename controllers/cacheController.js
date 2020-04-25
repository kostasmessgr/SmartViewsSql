const crypto = require('crypto');
let md5sum = crypto.createHash('md5');
const stringify = require('fast-stringify');
const config = require('../config_private');
const maxGbSize = config.maxGbSize;
const mb512InBytes = 512 * 1024 * 1024;
const redis = require('redis');
const client = redis.createClient(config.redisPort, config.redisIP);
let contract = null;
let mainTransactionObject = {};
let redisConnected = false;
const helper = require('../helpers/helper');
const computationsController = require('./computationsController');

function setContract (contractObject, account) {
    contract = contractObject;
    mainTransactionObject = helper.getMainTransactionObject(account);
}

client.on('connect', function () {
    redisConnected = true;
    helper.log('Redis connected');
});

client.on('error', function (err) {
    redisConnected = false;
    helper.log('Something went wrong ' + err);
});

function extractMetaKeys (gbResult) {
    return {
        operation: gbResult.operation,
        groupByFields: gbResult.groupByFields,
        field: gbResult.field,
        viewName: gbResult.viewName
    };
}

function manualSlicing (gbResult) {
    let slicedGbResult = [];
    let crnSlice = [];
    const metaKeys = extractMetaKeys(gbResult);
    for (const key of Object.keys(gbResult)) {
        if (key !== 'operation' && key !== 'groupByFields' && key !== 'field' && key !== 'viewName') {
            crnSlice.push({ [key]: gbResult[key] });
            if (crnSlice.length >= config.cacheSlice) {
                slicedGbResult.push(crnSlice);
                crnSlice = [];
            }
        }
    }
    if (crnSlice.length > 0) {
        slicedGbResult.push(crnSlice); // we have a modulo, the last slice contains less than all the previous ones
    }
    slicedGbResult.push(metaKeys);
    return slicedGbResult
}

function autoSlicing (gbResult) {
    let slicedGbResult = [];
    let crnSlice = [];
    const metaKeys = extractMetaKeys(gbResult);
    let rowsAddedInslice = 0;
    let crnSliceLengthInBytes = 0;
    for (const key of Object.keys(gbResult)) {
        if (key !== 'operation' && key !== 'groupByFields' && key !== 'field') {
            crnSlice.push({ [key]: gbResult[key] });
            rowsAddedInslice++;
            crnSliceLengthInBytes = rowsAddedInslice * maxGbSize;
            helper.log('Rows added in slice:');
            helper.log(rowsAddedInslice);
            if (crnSliceLengthInBytes === (mb512InBytes - 40)) { // for hidden character like backslashes etc
                slicedGbResult.push(crnSlice);
                crnSlice = [];
            }
        }
    }
    if (crnSlice.length > 0) {
        slicedGbResult.push(crnSlice); // we have a modulo, the last slice contains less than all the previous ones
    }
    slicedGbResult.push(metaKeys);
    return slicedGbResult
}

function saveOnCache (gbResult, operation, latestId) {
    md5sum = crypto.createHash('md5');
    let resultString = stringify(gbResult);
    md5sum.update(resultString);
    let hash = md5sum.digest('hex');
    let gbResultSize = Object.keys(gbResult).length;
    let slicedGbResult = [];
    if (config.autoCacheSlice === 'manual' && gbResultSize > config.cacheSlice) {
        slicedGbResult = manualSlicing(gbResult);
    } else {
        // redis allows 512MB per stored string, so we divide the result of our gb with 512MB to find cache slice
        // maxGbSize is the max number of bytes in a row of the result
        helper.log('Group-By result size in bytes = ' + gbResultSize * maxGbSize);
        helper.log('size a cache position can hold in bytes: ' + mb512InBytes);
        if ((gbResultSize * maxGbSize) > mb512InBytes) {
            slicedGbResult = autoSlicing(gbResult);
        } else {
            helper.log('NO SLICING NEEDED');
        }
    }
    let resultSize = resultString.length;
    helper.log('RESULT SIZE = ' + resultSize + ' bytes');
    let colSize = gbResult.groupByFields.length;
    let columns = stringify({ fields: gbResult.groupByFields, aggrFunc: gbResult.operation });
    let num = 0;
    let crnHash = '';
    if (slicedGbResult.length > 0) {
        for (const slice in slicedGbResult) {
            crnHash = hash + '_' + num;
            helper.log(crnHash);
            client.set(crnHash, stringify(slicedGbResult[slice]));
            num++;
        }
    } else {
        crnHash = hash + '_0';
        client.set(crnHash, stringify(gbResult));
    }
    var struct = helper.createStruct(crnHash, latestId, colSize, resultSize, columns,gbResult.operation,latestId);
    //console.log('before add cache '+JSON.stringify(struct));
    return computationsController.insertGbStructSQL(struct);
    //return contract.methods.addGroupBy(crnHash, latestId, colSize, resultSize, columns).send(mainTransactionObject);
}

function deleteFromCache (evicted) {
    //console.log('delete from cache cacheController');
    return new Promise((resolve) => {
        let keysToDelete = [];
        let gbIdsToDelete = [];
        // helper.cacheNow(keysToDelete,"Evict in cache controller",JSON.stringify(evicted.length));
        // console.log('1. '+evicted.length);
        // console.log('2. '+Object.keys(evicted).length);
        // console.log('3. '+Object.keys(evicted));
        let i=0;
        for (i = 0; i < evicted.length; i++) {
            keysToDelete.push(evicted[i].hash);
            //console.log('Evicted cache controller '+evicted);
            const crnHash = evicted[i].hash;
            const cachedGBSplited = crnHash.split('_');
            const cachedGBLength = parseInt(cachedGBSplited[1]);
            if (cachedGBLength > 0) { // reconstructing all the hashes in cache if it is sliced
                for (let j = 0; j < cachedGBLength; j++) {
                    keysToDelete.push(cachedGBSplited[0] + '_' + j);
                }
            }
            gbIdsToDelete[i] = evicted[i].id;
            //console.log(gbIdsToDelete[i]);
        }
        helper.log('keys to remove from cache are:');
        helper.log(keysToDelete);
        // console.log('4. '+JSON.stringify(keysToDelete));
        // console.log('5. '+Object.keys(keysToDelete).length);
        // console.log('6. '+Object.keys(gbIdsToDelete).length);
       
        // client.del(keysToDelete), (err,reply)=>{
        //     if(!err){
        //         rep=reply.toString();
        //         console.log(reply.toString());
        //     }else{
        //         console.log(error)
        //     }
        // });
        //helper.cacheNow(evicted,'Evicted');
        // helper.cacheNow(keysToDelete,"Evict--> reply",rep);
        resolve(gbIdsToDelete);
    });
}

function getManyCachedResults (allHashes) {
    //console.log('allhashes '+JSON.stringify(allHashes));
    return new Promise((resolve, reject) => {
        client.mget(allHashes, function (error, allCached) {
            if (error) {
                /* istanbul ignore next */
                reject(error);
            } else {
                //console.log('getManyCachedResults '+JSON.stringify(allCached));
                resolve(allCached);
            }
        });
    });
}

function preprocessCachedGroupBy (allCached) {
    let cachedGroupBy;
    if (allCached.length === 1) { // it is <= of slice size, so it is not sliced
        cachedGroupBy = JSON.parse(allCached[0]);
    } else { // it is sliced
        cachedGroupBy = helper.mergeSlicedCachedResult(allCached);
    }
    return cachedGroupBy;
}

function getRedisStatus () {
    return redisConnected;
}

module.exports = {
    setContract: setContract,
    saveOnCache: saveOnCache,
    deleteFromCache: deleteFromCache,
    getManyCachedResults: getManyCachedResults,
    getRedisStatus: getRedisStatus,
    preprocessCachedGroupBy: preprocessCachedGroupBy
};
