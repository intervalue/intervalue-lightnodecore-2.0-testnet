"use strict";
var gNet = require('net');

class socketclient {
    static send(url, port, request) {
        return new Promise(function (resolve, reject) {
            var _client = new gNet.Socket();
            _client.connect(port, url, function (err) {
                if (err) {
                    console.log('connect socket server error');
                    reject(err);
                    return;
                }
                console.log('connect socket server succussfully!');
                _client.write(request + '\n');
                _client.on('data', function (msg) {
                    console.log('response msg:');
                    console.log(msg.toString());
                    _client.end();
                    resolve(msg.toString());
                })
                _client.on('error', function (err) {
                    console.log('error:');
                    console.log(err);
                    reject(msg);
                    return;
                })
                _client.on('end', function () {
                    console.log('break up');
                })
            })
        })
    }
}

module.exports = socketclient;
