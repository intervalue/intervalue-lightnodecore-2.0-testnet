const Ice = require("ice").Ice;
const rpc = require("./Hashnet").one.inve.rpc;
(async function () {
	var localfullnodes = [];
	let ic;

	try {
		ic = Ice.initialize();
		const base = ic.stringToProxy("Regist:default -h 192.168.0.120 -p 20000");
		const regist = await rpc.seed.RegistPrx.checkedCast(base);
		if (regist) {
			await regist.getLocalfullnodeListInShard(pubkey).then(localfullnodes => {
				console.log(localfullnodes);
				if (localfullnodes.length > 0) {
					this.localfullnodes = localfullnodes.slice(0);
					console.log("localfullnodes is not null.");
				}
				console.log(this.localfullnodes);
			});
		}
		else {
			console.log("Invalid proxy");
		}
	}
	catch (ex) {
		console.log("Error: ")
		console.log(ex.toString());
		process.exitCode = 1;
	}
	finally {
		if (ic) {
			await ic.destroy();
		}
	}



	try {
		ic = Ice.initialize();
		if (this.localfullnodes) {
			console.log(this.localfullnodes);
			var localfullnode;
			if (this.localfullnodes.length < 2) {
				localfullnode = this.localfullnodes[0];
			} else {
				localfullnode = this.localfullnodes[Math.floor(Math.random() * 2)];
			}
			console.log("try to connect local full node : " + localfullnode.ip + ":" + localfullnode.rpcPort);

			//console.log(localfullnode);
			const base = ic.stringToProxy("Light2local:default -h " + localfullnode.ip + " -p " + localfullnode.rpcPort);
			const light2local = await rpc.localfullnode.Light2localPrx.checkedCast(base);
			if (light2local) {
				await light2local.getBalance(walletId).then(balance => {
					console.log("balance: ");
					console.log(balance);
				});
				await light2local.getTransactionInfo(walletId).then(transaction => {
					console.log("transaction detail: ");
					console.log(transaction);
				});
				await light2local.getTransactionHistory(walletId).then(transactions => {
					console.log("transaction history: ");
					console.log(transactions);
				});
				var message = new rpc.localfullnode.Message('unit');
				await light2local.sendMessage("123412", message).then(
					result => {
						console.log("send messege result: ");
						console.log(result);
					}
				).catch(
					ex => {
						console.log("err: ");
						console.log(ex);
					}
				);
			}
			else {
				console.log("Invalid proxy");
			}
		} else {
			console.log("localfullnode is null.");
		}
	}
	catch (ex) {
		console.log("Error: ")
		console.log(ex.toString());
		process.exitCode = 1;
	}
	finally {
		if (ic) {
			await ic.destroy();
		}
	}

}());


//::ice::unmarshaloutofboundsexception reason:""  原因：两边的**.ice文件不一样