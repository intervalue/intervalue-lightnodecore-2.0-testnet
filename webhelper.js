"use strict";

let sa = require("superagent");
// let request = require("request");
let timeout = 2 * 1000;
class WebHelper {
    static httpGet(url, headers) {
        return new Promise(function (resolve, reject) {
            sa
                .get(url)
                .set(headers == null ? {} : headers)
                .end(function (err, res) {
                    if (err) {
                        reject(err);
                        return;
                    }
                    console.log(JSON.stringify(res.text));
                    resolve(res.text);
                });
        });
    }

    // static httpGet2(url) {
    //     return new Promise(function (resolve, reject) {
    //         req.get(url, function (err, response, body) {
    //             if (err) {
    //                 reject(err);
    //                 return;
    //             }
    //             resolve(body);
    //         });
    //     });

    // }

    static httpPost(url, headers, data) {
        return new Promise(function (resolve, reject) {
            sa
                .post(url)
                .type('form')
                .set(headers == null ? {} : headers)
                .send(data)
                .timeout(timeout)
                .end(function (err, res) {
                    if (err) {
                        reject(err);
                        return;
                    }
                    resolve(res.text);
                });
        });
    }

    // static httpPost2(url, headers, data) {
    //     let option = {
    //         url: url,
    //         method: "POST",
    //         json: true,
    //         headers: headers,
    //         form: data
    //     };
    //     return new Promise((resolve, reject) => {
    //         request(option, function (error, response, body) {
    //             console.log(JSON.stringify(response));
    //             resolve(body);
    //         });
    //     });
    // }
}

module.exports = WebHelper;