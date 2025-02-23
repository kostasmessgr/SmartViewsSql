pragma solidity ^0.6.1;
pragma experimental ABIEncoderV2;
contract ABCDE{
	uint public dataId;
	uint public groupId;

	event gbArray(string hash);
	event dataAdded(string dat);
	event groupBysDeleted(uint[] deletedIds);
	uint public viewId;

	constructor() public{
		dataId = 0;
		groupId = 0;
	}

	struct Abcde{
		string payload;
		uint timestamp;
	}
	struct groupBy{
  		string hash;
  		uint latestFact;
 		uint size;
 		uint colSize;
  		string columns;
 		uint timestamp;
	}
	mapping(uint => groupBy) public groupBys;

	mapping(uint => Abcde) public facts;

	struct gbView{
  		string viewDef;
	}
	mapping(uint => gbView) public gbViews;

	function addFact(string memory payload) public returns (string memory, uint ID){
		facts[dataId].payload= payload;
		facts[dataId].timestamp = now;
 		dataId += 1;
		return (facts[dataId-1].payload,dataId -1);
	}

	function getFact(uint id) public view returns (string memory payload, uint timestamp){
		return (facts[id].payload, facts[id].timestamp);
	}

	function addView(string memory definition) public returns(string memory viewAdded, uint viewID) {
		gbViews[viewId].viewDef = definition;
		viewId += 1;
		return (gbViews[viewId-1].viewDef, viewId-1);
	}

	function addGroupBy(string memory hash, uint latestFact, uint colSize, uint size, string memory columns) public returns(string memory groupAdded, uint groupID){
		groupBys[groupId].hash = hash;
		groupBys[groupId].timestamp = now;
		groupBys[groupId].latestFact = latestFact;
		groupBys[groupId].colSize = colSize;
		groupBys[groupId].size = size;
		groupBys[groupId].columns = columns;
		groupId += 1;
		return (groupBys[groupId-1].hash, groupId-1);
	}

	function getGroupBy(uint idGroup) public view returns (string memory groupByID, uint timeStamp, uint latFact, string memory cols, uint sz){
		return(groupBys[idGroup].hash, groupBys[idGroup].timestamp, groupBys[idGroup].latestFact, groupBys[idGroup].columns, groupBys[idGroup].size);
	}

	function getAllViews(uint viewID) public view returns (string[] memory viewDefinitions){
		string[] memory allViews = new string[](viewID);
		for(uint i =0; i < viewID; i++){
		 gbView storage crnView = gbViews[i];
		 allViews[i] = crnView.viewDef;
		}
		return(allViews);
	}

	function getAllGroupBys(uint groupById) public view returns (string[] memory hashes, uint[] memory latFacts, uint[] memory columnSize, uint[] memory size,  string[] memory columns, uint[] memory gbTimestamp) {
		string[] memory allHashes = new string[](groupById);
		uint[] memory allLatFact = new uint[](groupById);
		uint[] memory allColSize = new uint[](groupById);
		uint[] memory allSize = new uint[](groupById);
		uint[] memory allTs = new uint[](groupById);
		string[] memory allColumns = new string[](groupById);
		for(uint i =0; i < groupById; i++){
		 groupBy storage crnGb = groupBys[i];
		 allHashes[i] = crnGb.hash;
		 allLatFact[i] = crnGb.latestFact;
		 allColSize[i] = crnGb.colSize;
		 allSize[i] = crnGb.size;
		 allColumns[i] = crnGb.columns;
		 allTs[i] = crnGb.timestamp;
		}
		return(allHashes, allLatFact, allColSize, allSize, allColumns, allTs);
	}

	function getAllFacts(uint id) public view returns (string[] memory payloads, uint[] memory timestamps){
		string[] memory payloadss = new string[](id);
		uint[] memory timestampss = new uint[](id);
		for(uint i =0; i < id; i++){
			Abcde storage fact = facts[i];
			payloadss[i] = fact.payload;
			timestampss[i] = fact.timestamp;
		}
		return (payloadss,timestampss);
	}

	function getFactsFromTo(uint from, uint to) public view returns (string[] memory payloadsFromTo, uint[] memory timestampsFromTo){
		string[] memory payloadss = new string[](to - from);
		uint[] memory timestampss = new uint[](to - from);
		uint j = 0;
		for(uint i = from; i < to; i++){
			Abcde storage fact = facts[j];
			payloadss[j] = fact.payload;
			timestampss[j] = fact.timestamp;
			j++;
		}
		return (payloadss,timestampss);
	}

	function addFacts(string[] memory payloadsss) public returns (string memory, uint IDMany){
		for(uint i =0; i < payloadsss.length; i++){
			facts[dataId].payload= payloadsss[i];
			facts[dataId].timestamp = now;
			dataId += 1;
		}
		 emit dataAdded(facts[dataId-1].payload);
		return (facts[dataId-1].payload,dataId -1);
	}

	function deleteGBsById(uint[] memory gbIds) public returns (uint[] memory deletedIds){
		uint[] memory deletedIdss = new uint[](gbIds.length);
		for(uint i=0; i < gbIds.length; i++){
			uint crnDelId = gbIds[i];
			deletedIdss[i] = crnDelId;
			delete groupBys[crnDelId];
			emit dataAdded("deleted");
		}
		emit groupBysDeleted(deletedIdss);
		return (deletedIdss);
	}

}