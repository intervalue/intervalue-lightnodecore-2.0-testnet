"use strict";

class SecretHelper {
    static FromBase64StringToBytes(base64str) {
        return new Buffer(base64str, "base64");
    }

    static FromBase64String(base64str) {
        let bytes = SecretHelper.FromBase64StringToBytes(base64str);
        return bytes.toString();
    }

    static FromUrlBase64String(urlBase64Str) {
        return decodeURI(SecretHelper.FromBase64String(urlBase64Str));
    }

    static ToBase64String(source) {
        let bytes = new Buffer(source);
        return bytes.toString("base64");
    }

    static ToBase64StringFromBytes(source) {
        return source.toString("base64");
    }

    static ToUrlBase64String(source) {
        return SecretHelper.ToBase64String(encodeURI(source));
    }

    static ToMD5(source) {
        var crypto = require("crypto");
        return crypto.createHash("md5").update(source).digest("hex").toUpperCase();
    }

    static ToSHA1(source) {
        var crypto = require("crypto");
        return crypto.createHash("sha1").update(source).digest("hex").toUpperCase();
    }

    static NewPassword(len) {
        let char = "AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789!?#@*&.,;:+-=()[]_";
        let pwd = "";
        if (len == null) {
            len = 10;
        }
        while (len--) {
            pwd += char[SecretHelper.random(0, char.length - 1)];
        }
        return pwd;
    }

    static NewID(len) {
        let char = "AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789_";
        let pwd = "";
        if (len == null) {
            len = 10;
        }
        while (len--) {
            pwd += char[SecretHelper.random(0, char.length - 1)];
        }
        return pwd;
    }

    static random(min, max) {
        return min + parseInt(Math.random() * (max - min + 1), 10);
    }
}



module.exports = SecretHelper;