var Proxy = require('hessian-proxy').Proxy;

class hessianhelper {
    static send() {
        var proxy = new Proxy('http://127.0.0.1:9098/test-provider/provider/com.yuanxin.paas.ssb.TestService', '', '', proxy);
        return new Promise(function (resolve, reject) {
            proxy.invoke('test', null, function (err, reply) {
                console.log('test: ' + reply);
                if (err) {
                    reject(err);
                }
                else {
                    resolve(reply);
                }
            });
        });
    }
}

module.exports = hessianhelper;