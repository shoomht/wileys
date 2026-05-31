"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.LTHashAntiTampering = exports.expandAppStateKeys = exports.hkdf = exports.md5 = void 0;
let runtime;
try {
    runtime = require("whatsapp-rust-bridge");
}
catch (error) {
    runtime = require("./rust-bridge-shim.js");
}
exports.md5 = runtime.md5;
exports.hkdf = runtime.hkdf;
exports.expandAppStateKeys = runtime.expandAppStateKeys;
exports.LTHashAntiTampering = runtime.LTHashAntiTampering;



