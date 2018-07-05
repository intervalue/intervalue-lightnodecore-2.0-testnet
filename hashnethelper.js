'use strict';

// const Ice = require("ice").Ice;
// const rpc = require("./Hashnet").one.inve.rpc;
const webHelper = require("./webhelper.js");
const device = require("./device.js");
const secrethelper = require("./secrethelper.js");
var localfullnodes = [];
class HashnetHelper {
    static async buildSingleLocalfullnode() {
        if (localfullnodes.length === 0) {
            let { pubKey } = await device.getInfo();
            localfullnodes = await HashnetHelper.getLocalfullnodeList(pubKey);
        }
        if (localfullnodes.length > 0) {
            let localfullnode = localfullnodes[secrethelper.random(0, localfullnodes.length - 1)];
            console.log("get localfullnode:" + localfullnode);
            return localfullnode;
        }
        else {
            console.log("localfullnode is null.");
            return null;
        }
    }

    static async getLocalfullnodeList(pubKey) {
        let localfullnodeList = await webHelper.httpPost('http://192.168.0.88:20002/getLocalfullnodeListInShard/', null, buildData({ pubKey }));
        // let localfullnodeList = await webHelper.httpPost('http://132.124.218.43:20002/getLocalfullnodeListInShard/', null, buildData({ pubKey }));
        if (localfullnodeList) {
            return JSON.parse(localfullnodeList);
        }
        else {
            console.log("got no localfullnodeList");
            return [];
        }
    }

    static async sendMessage(unit) {
        let localfullnode = await HashnetHelper.buildSingleLocalfullnode();
        console.log("sending unit:");
        unit = JSON.stringify(unit);
        console.log(unit);
        let result = await webHelper.httpPost(getUrl(localfullnode, '/sendMessage/'), null, buildData({ unit }));
        return result;
    }

    static async getTransactionHistory(address) {
        let localfullnode = await HashnetHelper.buildSingleLocalfullnode();
        let result = await webHelper.httpPost(getUrl(localfullnode, '/getTransactionHistory/'), null, buildData({ address }));
        return result ? JSON.parse(result) : [];
    }

    static async getUnitInfo(unitId) {
        let localfullnode = await HashnetHelper.buildSingleLocalfullnode();
        let result = await webHelper.httpPost(getUrl(localfullnode, '/getUnitInfo/'), null, buildData({ unitId }));
        return result ? JSON.parse(result) : null;
    }
}

let getUrl = (localfullnode, suburl) => {
    return 'http://' + localfullnode.ip + ':' + localfullnode.httpPort + suburl;
}

let buildData = (data) => {
    return { data: JSON.stringify(data) };
}

module.exports = HashnetHelper;