const fs = require('fs');
const helper = require('../helpers/helper');

const generate = async function (a, b) {
    console.log('called');
    let p3 = '.json';
    let low = String(a);
    let all = [];
    a=100000+a;
    b=100000+b;
    console.log(a+' '+b);
    for (let i = a; i < b; i++) {
        for (let j = 0; j < 1; j++) {
            let A = helper.getRandomInt(1001, 2000);
            let B = helper.getRandomInt(1001, 2000);
            let C = helper.getRandomInt(1001, 2000);
            let D = helper.getRandomInt(1001, 2000);
            let E = helper.getRandomInt(1001, 2000);
            let newObj = { pk: i, A: A, B: B, C: C, D: D, E:E};
            all.push(newObj);
            console.log(i);
        }
    }
    // await fs.writeFile('./test_data/benchmarks100000/benchmarks500/' + low + p3, JSON.stringify(all), function (err) {
    //     if (err) {
    //         return console.log(err);
    //     }
    // });
    return low + p3;
};

const main = async()=>{
    generate(0,100000);
}


module.exports = { generate };
