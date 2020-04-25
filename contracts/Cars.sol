pragma solidity ^0.4.0;

contract Cars {
	uint public dataId;
	uint public groupId;

	uint public lastCount;
	uint public lastSUM;
	uint public lastMin;
	uint public lastMax;
	uint public lastAverage;
	bytes constant MIN_LITERAL = "MIN";
	bytes constant MAX_LITERAL = "MAX";
	bytes constant AVERAGE_LITERAL = "AVERAGE";
	bytes constant COUNT_LITERAL = "COUNT";
	bytes constant SUM_LITERAL = "SUM";
	constructor() {
		dataId = 0;
		groupId = 0;
		lastCount = 0;
		lastSUM = 0;
		lastMin = 0;
		lastMax = 0;
		lastAverage = 0;
	}
	struct Car{
		string brand;
		string model;
		string year;
		string category;
		uint cylinders;
		uint timestamp;
	}
	struct groupBy{
  		string hash;
        uint timestamp;
	}
	mapping(uint => groupBy) public groupBys;

	mapping(uint =>Car) public facts;

	function addFact(string brand,string model,string year,string category,uint cylinders) public returns (string ,string ,string ,string ,uint , uint ID){
		facts[dataId].brand= brand;
		facts[dataId].model= model;
		facts[dataId].year= year;
		facts[dataId].category= category;
		facts[dataId].cylinders= cylinders;
		facts[dataId].timestamp = now;
 		dataId += 1;
		return (facts[dataId-1].brand,facts[dataId-1].model,facts[dataId-1].year,facts[dataId-1].category,facts[dataId-1].cylinders,dataId -1);
	}

	function getFact(uint id) public constant returns (string brand,string model,string year,string category,uint cylinders, uint timestamp){
		return (facts[id].brand,facts[id].model,facts[id].year,facts[id].category,facts[id].cylinders, facts[id].timestamp);
	}

	function addGroupBy(string hash, bytes category) public returns(string groupAdded, uint groupID){
    		groupBys[groupId].hash = hash;
    		groupBys[groupId].timestamp = now;
			if(keccak256(category) == keccak256(COUNT_LITERAL)){
				lastCount  = groupID;
			} else if(keccak256(category) == keccak256(SUM_LITERAL)){
				lastSUM = groupID;
			} else if(keccak256(category) == keccak256(MIN_LITERAL)){
				lastMin = groupID;
			} else if(keccak256(category) == keccak256(MAX_LITERAL)){
				lastMax = groupID;
			} else if(keccak256(category) == keccak256(AVERAGE_LITERAL)){
				lastAverage = groupID;
			}
    		groupId += 1;
    		return (groupBys[groupId-1].hash, groupId-1);
    	}
	function getGroupBy(uint idGroup) public constant returns (string groupByID, uint timeStamp){
    		return(groupBys[idGroup].hash, groupBys[idGroup].timestamp);
    	}

function getLatestGroupBy(bytes operation) public constant returns(string latestGroupBy, uint ts){
		if(groupId > 0){
			if(keccak256(operation) == keccak256(COUNT_LITERAL)){
				if(lastCount > 0){
					return (groupBys[lastCount].hash, groupBys[lastCount].timestamp);
				}
			} else if (keccak256(operation) == keccak256(SUM_LITERAL)){
				if(lastSUM > 0){
					return (groupBys[lastSUM].hash, groupBys[lastSUM].timestamp);
				}
			} else if (keccak256(operation) == keccak256(MIN_LITERAL)){
				if(lastMin > 0){
					return (groupBys[lastMin].hash, groupBys[lastMin].timestamp);
				}
			} else if (keccak256(operation) == keccak256(MAX_LITERAL)){
				if(lastMax > 0){
					return (groupBys[lastMax].hash, groupBys[lastMax].timestamp);
				}
			} else if (keccak256(operation) == keccak256(AVERAGE_LITERAL)){
				if(lastAverage > 0){
					return (groupBys[lastAverage].hash, groupBys[lastAverage].timestamp);
				}
			}
		}
			return ("",0);
	}
}