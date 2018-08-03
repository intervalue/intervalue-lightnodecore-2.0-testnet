'use strict';

// const Ice = require("ice").Ice;
// const rpc = require("./Hashnet").one.inve.rpc;
const webHelper = require("./webhelper.js");
const device = require("./device.js");
const secrethelper = require("./secrethelper.js");
var db = require('./db.js');
var mutex = require('./mutex.js');
var _ = require('lodash');
var localfullnodes = [];
class HashnetHelper {
    static async buildSingleLocalfullnode() {
        if (localfullnodes.length === 0) {
            let list = await db.toList('select * from my_witnesses');
            if (list.length > 0) {
                for (var l of list) {
                    let ip = l.address.split(':')[0];
                    let httpPort = l.address.split(':')[1];
                    localfullnodes.push({ ip, httpPort });
                }
            }
            else {
                let { pubKey } = await device.getInfo();
                localfullnodes = await HashnetHelper.getLocalfullnodeList(pubKey);
            }
        }
        if (localfullnodes.length > 0) {
            let localfullnode = localfullnodes[secrethelper.random(0, localfullnodes.length - 1)];
            console.log("get localfullnode:" + JSON.stringify(localfullnode));
            return localfullnode;
        }
        else {
            console.log("localfullnode is null.");
            return null;
        }
    }

    static initialLocalfullnodeList() {
        localfullnodes = [];
    }
    static async getLocalfullnodeList(pubKey) {
        try {
            let localfullnodeList = await webHelper.httpPost(device.my_device_hashnetseed_url + '/getLocalfullnodeListInShard/', null, buildData({ pubKey }));
            // let localfullnodeList = await webHelper.httpPost('http://132.124.218.43:20002/getLocalfullnodeListInShard/', null, buildData({ pubKey }));
            if (localfullnodeList) {
                localfullnodeList = JSON.parse(localfullnodeList);
                let cmds = [];
                db.addCmd(cmds, "delete from my_witnesses");
                for (var i = 0; i < localfullnodeList.length; i++) {
                    db.addCmd(cmds, "INSERT " + db.getIgnore() + " INTO my_witnesses ( address ) values (?)",
                        localfullnodeList[i].ip + ':' + localfullnodeList[i].httpPort);
                }
                await mutex.lock(["write"], async function (unlock) {
                    try {
                        let b_result = await db.executeTrans(cmds);
                    }
                    catch (e) {
                        console.log(e);
                    }
                    finally {
                        await unlock();
                    }
                });
                return localfullnodeList;
            }
            else {
                console.log("got no localfullnodeList");
                return [];
            }
        }
        catch (e) {
            console.log(e);
            return [];
        }
    }

    static async reloadLocalfullnode(localfullnode) {
        if (localfullnode) {
            _.pull(localfullnodes, localfullnode);
            await mutex.lock(["write"], async function (unlock) {
                try {
                    await db.execute("delete from my_witnesses where address = ?", localfullnode.ip + ':' + localfullnode.httpPort);
                }
                catch (e) {
                    console.log(e);
                }
                finally {
                    await unlock();
                }
            });
        }
        if (localfullnodes.length < 5) {
            let { pubKey } = await device.getInfo();
            let localfullnodeList = await HashnetHelper.getLocalfullnodeList(pubKey);
            if (localfullnodeList.length > 0) {
                localfullnodes = localfullnodeList;
            }
        }
    }

    static async sendMessage(unit, retry) {
        let result = '';
        retry = retry || 3;
        if (retry > 1) {
            for (var i = 0; i < retry; i++) {
                result = await HashnetHelper.sendMessageTry(unit);
                if (!result) {
                    break;
                }
            }
            return result;
        }
        return await HashnetHelper.sendMessageTry(unit);
    }

    static async sendMessageTry(unit) {
        let localfullnode = await HashnetHelper.buildSingleLocalfullnode();
        try {
            if (!localfullnode) {
                throw new Error('network error, please try again.');
            }
            console.log("sending unit:");
            unit = JSON.stringify(unit);
            console.log(unit);

            let result = await webHelper.httpPost(getUrl(localfullnode, '/sendMessage/'), null, buildData({ unit }));
            return result;
        }
        catch (e) {
            if (localfullnode) {
                await HashnetHelper.reloadLocalfullnode(localfullnode);
            }
            return 'network error,please try again.';
        }
    }

    static async getTransactionHistory(address) {

        let localfullnode = await HashnetHelper.buildSingleLocalfullnode();
        try {
            if (!localfullnode) {
                throw new Error('network error, please try again.');
            }
            let result = await webHelper.httpPost(getUrl(localfullnode, '/getTransactionHistory/'), null, buildData({ address }));
            return result ? JSON.parse(result) : [];
        }
        catch (e) {
            if (localfullnode) {
                await HashnetHelper.reloadLocalfullnode(localfullnode);
            }
            return null;
        }
    }

    static async getUnitInfo(unitId) {

        let localfullnode = await HashnetHelper.buildSingleLocalfullnode();
        try {
            if (!localfullnode) {
                throw new Error('network error, please try again.');
            }
            let result = await webHelper.httpPost(getUrl(localfullnode, '/getUnitInfo/'), null, buildData({ unitId }));
            return result ? JSON.parse(result) : null;
        }
        catch (e) {
            if (localfullnode) {
                await HashnetHelper.reloadLocalfullnode(localfullnode);
            }
            return null;
        }
    }
}

let getUrl = (localfullnode, suburl) => {
    return 'http://' + localfullnode.ip + ':' + localfullnode.httpPort + suburl;
}

let buildData = (data) => {
    return { data: JSON.stringify(data) };
}

module.exports = HashnetHelper;