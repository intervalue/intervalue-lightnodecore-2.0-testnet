'use strict';

const ice = require("ice").Ice;
const rpc = require("./Hashnet").one.inve.rpc;

let localfullnodes = [];

class HashnetHelper {
    static async testConn() {
        let ic;
        try {
            ic = ice.initialize();
            const base = ic.stringToProxy("Light2local:default -h 192.168.0.120 -p 20000");
            const light2local = await rpc.localfullnode.Light2localPrx.checkedCast(base);
            if (light2local) {
                let balance = await light2local.getBalance();
                console.log(balance);
            }
            else {
                console.log("Invalid proxy");
            }
        }
        catch (ex) {
            console.log(ex.toString());
        }
        finally {
            if (ic) {
                await ic.destroy();
            }
        }
    };

    static get LocalFullNodes() {
        return localfullnodes;
    }

    static async iniGlobalLocalfullNodes(pubkey) {
        if (localfullnodes.length === 0) {
            localfullnodes = await HashnetHelper.getLocalfullnodeList(pubkey);
        }
        console.log(localfullnodes);
        return localfullnodes;
    }

    static async testFunc() {
        return await this.getBalance();
    }

    static getSingleLocalfullnode(localfullnodes) {
        if (localfullnodes.length > 0) {
            let localfullnode = localfullnodes[parseInt(Math.random() * ((localfullnodes.length - 1) - 0 + 1) + 0, 10)];
            console.log("get localfullnode:" + localfullnode);
            return localfullnode;
        }
        else {
            console.log("localfullnode is null.");
            return null;
        }
    }

    static async getLocalfullnodeList(pubkey) {
        let { ic, regist } = await buildRegist();
        let localfullnodeList = await handleRegist(ic, regist, regist.getLocalfullnodeListInShard(pubkey));
        if (localfullnodeList.err) {
            return [];
        }
        else {
            console.log("got localfullnodeList:" + localfullnodeList);
            return localfullnodeList.result;
        }
    }

    static async sendMessageDirect(pubkey, unit) {
        let localfullnode = await buildSingleLocalfullnode(pubkey);
        console.log("sending unit:");
        console.log(JSON.stringify(unit));
        let result = await HashnetHelper.sendMessage(localfullnode, unit);
        return result;
    }

    static async sendMessage(localfullnode, unit) {
        let { ic, light2local } = await buildLight2Local(localfullnode);
        console.log("sendMessage result: ");
        let result = await handleLight2Local(ic, light2local, light2local.sendMessage(unit));
        console.log(result);
        return result;
    }

    static async getBalanceDirect(pubkey, walletId) {
        let localfullnode = await buildSingleLocalfullnode(pubkey);
        let result = await HashnetHelper.getBalance(localfullnode, walletId);
        return result;
    }

    static async getBalance(localfullnode, walletId) {
        let { ic, light2local } = await buildLight2Local(localfullnode);
        console.log("getBalance result: ");
        let result = await handleLight2Local(ic, light2local, light2local.getBalance(walletId));
        console.log(result);
        return result;
    }

    static async getTransactionHistoryDirect(pubkey, walletId) {
        let localfullnode = await buildSingleLocalfullnode(pubkey);
        let result = await HashnetHelper.getTransactionHistory(localfullnode, walletId);
        return result;
    }

    static async getTransactionHistory(localfullnode, walletId) {
        let { ic, light2local } = await buildLight2Local(localfullnode);
        console.log("getTransactionHistory result: ");
        let result = await handleLight2Local(ic, light2local, light2local.getTransactionHistory(walletId));
        console.log(result);
        return result;
    }

    static async getTransactionInfoDirect(pubkey, walletId) {
        let localfullnode = await buildSingleLocalfullnode(pubkey);
        let result = await HashnetHelper.getTransactionInfo(localfullnode, walletId);
        return result;
    }

    static async getTransactionInfo(localfullnode, walletId) {
        let { ic, light2local } = await buildLight2Local(localfullnode);
        console.log("getTransactionInfo result: ");
        let result = await handleLight2Local(ic, light2local, light2local.getTransactionInfo(walletId));
        console.log(result);
        return result;
    }

    static async getUnitInfoListDirect(pubkey, walletId) {
        let localfullnode = await buildSingleLocalfullnode(pubkey);
        let result = await HashnetHelper.getUnitInfoList(localfullnode, walletId);
        return result;
    }

    static async getUnitInfoList(localfullnode, walletId) {
        let { ic, light2local } = await buildLight2Local(localfullnode);
        console.log("getUnitInfoList result: ");
        let result = await handleLight2Local(ic, light2local, light2local.getUnitInfoList(walletId));
        console.log(result);
        return result;
    }

    static async getUnitInfoDirect(pubkey, unitId) {
        let localfullnode = await buildSingleLocalfullnode(pubkey);
        let result = await HashnetHelper.getUnitInfo(localfullnode, unitId);
        return result;
    }

    static async getUnitInfo(localfullnode, unitId) {
        let { ic, light2local } = await buildLight2Local(localfullnode);
        console.log("getUnitInfo result: ");
        let result = await handleLight2Local(ic, light2local, light2local.getUnitInfo(unitId));
        console.log(result);
        return result;
    }
}

module.exports = HashnetHelper;

let buildSingleLocalfullnode = async (pubkey) => {
    await HashnetHelper.iniGlobalLocalfullNodes(pubkey);
    let localfullnode = HashnetHelper.getSingleLocalfullnode(HashnetHelper.LocalFullNodes);
    return localfullnode;
}

let buildLight2Local = async (localfullnode) => {
    if (localfullnode) {
        let ic = ice.initialize();
        let base = ic.stringToProxy("Light2local:default -h " + localfullnode.ip + " -p " + localfullnode.rpcPort);
        let light2local = await rpc.localfullnode.Light2localPrx.checkedCast(base);
        return { ic, light2local };
    }
    else {
        console.log("localfullnode is null.");
        return null;
    }
}

let buildRegist = async () => {
    let ic = ice.initialize();
    console.log("try to get getLocalfullnodeList");
    console.log("try to get RegistPrx");
    let base = ic.stringToProxy("Regist:default -h 192.168.0.120 -p 20000");
    let regist = await rpc.seed.RegistPrx.checkedCast(base);
    return { ic, regist };
}

let handleRegist = async (ic, regist, func) => {
    try {
        if (regist) {
            let result = await func;
            return { result };
        }
        else {
            return { err: "Invalid proxy" };
        }
    }
    catch (ex) {
        console.log(ex.toString());
        return { err: ex.toString() };
    }
    finally {
        if (ic) {
            await ic.destroy();
        }
    }
}

let handleLight2Local = async (ic, light2local, func) => {
    try {
        if (light2local) {
            let result = await func;
            return { result };
        }
        else {
            return { err: "Invalid proxy" };
        }
    }
    catch (ex) {
        console.log(ex.toString());
        return { err: ex.toString() };
    }
    finally {
        if (ic) {
            await ic.destroy();
        }
    }
}