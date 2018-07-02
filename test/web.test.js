const webhelper = require('../webhelper.js');
const secrethelper = require('../secrethelper.js');
//const hashnethelper = require('../hashnethelper.js');
// hashnethelper.getLocalfullnodeList('xxx').then(function (res) {
//     console.log(JSON.stringify(res));
// });
webhelper.httpPost('http://192.168.0.120:20002/getLocalfullnodeListInShard/', null,
    { data: JSON.stringify({ pubKey: 'success' }) }).then(function (res) {
        console.log(JSON.stringify(res));
    });
// var d1 = new Date();
// for (var i = 0; i < 1000; i++) {
//     webhelper.httpPost('http://192.168.0.139:30002/getTransactionHistory/', null, { data: 'success' }).then(function (res) {
//         // var d = new Date();
//         // console.log('此次调用运行时间：' + parseInt(d - d1) / 1000 + '秒.');//两个时间相差的秒数
//         console.log(JSON.stringify(res));
//     });
// }
// var d2 = new Date();
// console.log('运行时间：' + parseInt(d2 - d1) / 1000 + '秒.');//两个时间相差的秒数
// for (var i = 0; i < 100; i++) {
//     socketclient.send('192.168.0.137', 6068, 'helloworld').then(function (msg) {
//         console.log(msg);
//     })
// }

// const hessianhelper = require('../hessianhelper.js');

// hessianhelper.send().then(function (res) {

// });

// var Proxy = require('hessian-proxy').Proxy;

// var proxy = new Proxy('http://127.0.0.1:9098/test-provider/provider/com.yuanxin.paas.ssb.TestService', '', '', proxy);

// proxy.invoke('test', null, function (err, reply) {
//     console.log('test: ' + reply);
// });

// proxy.invoke('test0', [25], function (err, reply) {
//     console.log('test0: ' + JSON.stringify(reply));
// })

// proxy.invoke('test1', null, function (err, reply) {
//     if (err) {
//         console.log('test1: ' + err);
//     }

//     console.log('test1: ' + JSON.stringify(reply));
// })

// var argForTest2 = {
//     i: 2
// };

// argForTest2.__type__ = 'com.yuanxin.paas.ssb.Arg';

// proxy.invoke('test2', [argForTest2], function (err, reply) {
//     if (err) {
//         console.log('test2: ' + err);
//     }

//     console.log('test2: ' + JSON.stringify(reply));
// })


// var argForTest3 = {
//     i: 3
// };

// argForTest3.__type__ = 'com.yuanxin.paas.ssb.Arg';

// proxy.invoke('test3', [argForTest3], function (err, reply) {
//     if (err) {
//         console.log('test3: ' + err);
//     }

//     console.log('test3: ' + JSON.stringify(reply));
// })
