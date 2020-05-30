const cacheController = require('../controllers/cacheController');
const exec = require('child_process').execSync;
const _ = require('underscore');
const helper = require('./helper');

function costMat (V, Vc, latestFact) {
    // The cost of materializing view V using the set of cached views Vc
   // console.log('costMat Function');
  //  console.log('cost of materializing view V'+V.columns+' using VC, size: '+Vc.length);
    let costs = [];
    for (let i = 0; i < Vc.length; i++) {
        let Vi = Vc[i];
        if (isMaterializableFrom(V, Vi)) {
            const sizeDeltas = latestFact - Number.parseInt(Vi.latestFact); // latestFact is the latest fact written in bc
            const sizeCached = Number.parseInt(Vi.size);
            V.calculationCost = 600 * sizeDeltas + sizeCached;
            costs.push(V);
        }
    }
    costs.sort((a, b) => parseFloat(a.calculationCost) - parseFloat(b.calculationCost));
   // console.log('costMat result: ' + costs[0].calculationCost);
    return costs[0].calculationCost;
}

function remove (array, element) {
    return array.filter(el => el !== element);
}

async function dispCost (Vc, latestFact, factTbl) {
    return new Promise((resolve, reject) => {
        let allHashes = [];
        let toBeEvicted = [];
        for (let i = 0; i < Vc.length; i++) {
            const crnGroupBy = Vc[i];
            allHashes.push(crnGroupBy.hash);
        }
        cacheController.getManyCachedResults(allHashes).then(async allCached => {
            let freq = 0;
            allCached = allCached.filter(function (el) { // remove null objects in case they have been deleted
                return el != null;
            });

            if (allHashes.length > 1) {
                for (let j = 0; j < allCached.length; j++) {
                    const crnGb = JSON.parse(allCached[j]);
                    const viewsDefined = factTbl.views;
                    for (let crnView in viewsDefined) {
                        if (factTbl.views[crnView].name === crnGb.viewName) {
                            freq = factTbl.views[crnView].frequency;
                            break;
                        }
                    }
                }
                
                Vc=JSON.parse(JSON.stringify(Vc));
                for (let i = 0; i < Vc.length; i++) {
                    let Vi = Vc[i]; //Vi in paper
                    let VcMinusVi = remove(Vc, Vi); //set of cached views without Vi

                    
                    let viewsMaterialisableFromVi = getViewsMaterialisableFromVi(Vc, Vi, i);
                    //console.log('views materializable from vi: '+JSON.stringify(viewsMaterialisableFromVi));
                    viewsMaterialisableFromVi = remove(viewsMaterialisableFromVi, Vi);
                    let dispCostVi = 0;
                    for (let j = 0; j < viewsMaterialisableFromVi.length; j++) {
                        let V = viewsMaterialisableFromVi[j];
                        let costMatVVC = costMat(V, Vc, latestFact);
                        let costMatVVcMinusVi = costMat(V, VcMinusVi, latestFact);
                        dispCostVi += (costMatVVC - costMatVVcMinusVi);
                      //  console.log('current Vi: '+ Vi.columns + 'costMatWC: ' + costMatVVC + ' CostMatWcMinusVi: ' + costMatVVcMinusVi + ' result: ' + dispCostVi + ' frequency: ' + freq)
                    }
                    dispCostVi = dispCostVi * freq;
                    Vi.cacheEvictionCost = dispCostVi / Number.parseInt(Vi.size);
                    toBeEvicted.push(Vi);
                }
            }
            resolve(toBeEvicted);
        }).catch(err => {
            reject(err)
        });
    });
}

function getViewsMaterialisableFromVi (Vc, Vi) {
    let viewsMaterialisableFromVi = [];
    for (let j = 0; j < Vc.length; j++) { // finding all the Vs < Vi 
        if (isMaterializableFrom(Vc[j],Vi)) {
            viewsMaterialisableFromVi.push(Vc[j]);
        }
    }
    return viewsMaterialisableFromVi;
}

function calculationCostOfficial (groupBys, latestFact) { // the function we write on paper
    // where cost(Vi, V) = a * sizeDeltas(i) + sizeCached(i)
    // which is the cost to materialize view V from view Vi (where V < Vi)
    const a = 450; // factor of deltas
    let sizeDeltas = 0;
    let sizeCached = 0;
    for (let i = 0; i < groupBys.length; i++) {
        let crnGroupBy = groupBys[i];
        sizeDeltas = latestFact - Number.parseInt(crnGroupBy.latestFact); // latestFact is the latest fact written in bc
        sizeCached = Number.parseInt(crnGroupBy.size);
        crnGroupBy.calculationCost = a * sizeDeltas + sizeCached;
        groupBys[i] = crnGroupBy;
    }
    return groupBys;
}

async function word2vec (groupBys, view) {
    let victims = [];
    const viewForW2V = view.fields.toString().replace(/,/g, '');
    for (let i = 0; i < groupBys.length; i++) {
        let currentFields = JSON.parse(groupBys[i].columns);
        let newVictim = currentFields.fields.toString()
            .replace(/,/g, '')
            .replace('""', '');
        victims.push(newVictim);
    }
    let process = exec('python word2vec.py ' + victims.toString() + ' ' + viewForW2V);
    let sims = process.toString('utf8');
    sims = sims.replace('[', '').replace(']', '')
        .replace(/\n/g, '').trim().split(',');
    sims = sims.map(sim => {
        return sim.trim();
    });
    for (let i = 0; i < groupBys.length; i++) {
        let crnGroupBy = groupBys[i];
        crnGroupBy.word2vecScore = sims[i];
        groupBys[i] = crnGroupBy;
    }
    return groupBys;
}

//definition of cube distance
//For two views V1,V2 we can define their distance in data cube lattice
//as the number of roll-ups, drill-downs needed to go from V1 to V2.

//DataCubeDistance('ABC','AB'))=1 { ΑBC --rollup--> AB }
//DataCubeDistance('ABC','ACD')=2   {ABC ->AC->ACD }
//DataCubeDistance('ABC','DE) = 5 {ABC->AB->A->()->D->DE}

//dim(V)= set of dimensions in view V
//DataCubeDistance(x,y) = |dim(x) UNION dim(y) - dim(x) INTERSECTION dim(y)|

function dataCubeDistance (view1, view2) {
    var columns1=JSON.stringify(view1.columns);
    var columns2=JSON.stringify(view2.name);
    // console.log('View1: '+JSON.stringify(view1));
    // console.log('View2: '+JSON.stringify(view2));
    var fields1=(columns1.substring(columns1.indexOf('[')+1,columns1.indexOf(']'))).split(',')
    var fields2 = (columns2.substring(1,columns2.indexOf('('))).split('');
    // console.log('View1: '+JSON.stringify(fields1));
    // console.log('View2: '+JSON.stringify(fields2));
    const union = _.union(fields1, fields2).sort();
    const intersection = _.intersection(fields1, fields2).sort();
    var str=('union '+fields1+' intersection '+fields2+' --> '+(union.length - intersection.length+'\n'));
    helper.cacheNow(view1,'!',str);
    
    return union.length - intersection.length;
}

function dataCubeDistanceBatch (cachedViews, view) {
    for (let i = 0; i < cachedViews.length; i++) {
        cachedViews[i].dataCubeDistance = dataCubeDistance(cachedViews[i], view);
    }
    return cachedViews;
}

function isMaterializableFrom (view1, view2) {
    //check if view2 can materialize view1 ex. isMaterializableFrom('AB','ABC')=true
    //console.log('is materializable from');
    var columns1 = JSON.stringify(view1.columns);
    var columns2 = JSON.stringify(view2.columns);
    //console.log(JSON.stringify(view1));
    //console.log(JSON.stringify(view2));
    var fields1=(columns1.substring(columns1.indexOf('[')+1,columns1.indexOf(']'))).split(',')
    var fields2=(columns2.substring(columns2.indexOf('[')+1,columns2.indexOf(']'))).split(',')
    for (let i=0;i<fields1.length;i++) {
        fields1[i] = fields1[i].trim();
    }
    let containsAllFields = true;
    for (let k = 0; k < fields1.length; k++) {
        //console.log(fields2+' '+fields1[k]);
        if (!fields2.includes(fields1[k])) {
            containsAllFields = false
        }
    }
    return containsAllFields;
}

// function getViewsMaterialisableFromVi (Vc, Vi) {
//     let viewsMaterialisableFromVi = [];
//     for (let j = 0; j < Vc.length; j++) { // finding all the Vs < Vi
//         let crnView = Vc[j];
//         var fields = JSON.stringify(crnView.columns).replace('[','').replace(']','');
//         var fields2 = JSON.stringify(Vi.columns).replace('[','').replace(']','');
//         let crnViewFields = JSON.parse(fields);
//         let ViFields = JSON.parse(fields2);
//         for (let index in crnViewFields.fields) {
//             crnViewFields[index] = crnViewFields[index].trim();
//             //console.log('Diff fields: '+crnViewFields.fields[index])
//         }
//         let containsAllFields = true;
//         for (let k = 0; k < crnViewFields.length; k++) {
//             if (!ViFields.includes(crnViewFields[k])) {
//                 containsAllFields = false
//             }
//         }
//         if (containsAllFields) {
//             viewsMaterialisableFromVi.push(crnView);
//         }
//     }
//     return viewsMaterialisableFromVi;
// }

module.exports = {
    dispCost: dispCost,
    calculationCostOfficial: calculationCostOfficial,
    word2vec: word2vec,
    dataCubeDistanceBatch: dataCubeDistanceBatch,
    isMaterializableFrom:isMaterializableFrom
};