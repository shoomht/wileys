"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.LTHashAntiTampering = void 0;
exports.md5 = md5;
exports.hkdf = hkdf;
exports.expandAppStateKeys = expandAppStateKeys;
const crypto_1 = require("crypto");
function toBuffer(input) {
    if (Buffer.isBuffer(input)) return input;
    if (input instanceof Uint8Array) return Buffer.from(input);
    if (ArrayBuffer.isView(input)) return Buffer.from(input.buffer, input.byteOffset, input.byteLength);
    if (input instanceof ArrayBuffer) return Buffer.from(input);
    if (typeof input === "string") return Buffer.from(input);
    return Buffer.from(input || []);
}
function normalizeHkdfOptions(options = {}) {
    const salt = options.salt == null ? Buffer.alloc(0) : toBuffer(options.salt);
    const info = options.info == null ? Buffer.alloc(0) : toBuffer(options.info);
    return { salt, info };
}
function md5(data) {
    return (0, crypto_1.createHash)("md5").update(toBuffer(data)).digest();
}
function hkdf(key, length, options = {}) {
    const { salt, info } = normalizeHkdfOptions(options);
    const out = crypto_1.hkdfSync("sha256", toBuffer(key), salt, info, length);
    return Buffer.from(out);
}
function expandAppStateKeys(keyData) {
    const expanded = hkdf(toBuffer(keyData), 160, { info: "WhatsApp Mutation Keys" });
    return {
        indexKey: expanded.subarray(0, 32),
        valueEncryptionKey: expanded.subarray(32, 64),
        valueMacKey: expanded.subarray(64, 96),
        snapshotMacKey: expanded.subarray(96, 128),
        patchMacKey: expanded.subarray(128, 160)
    };
}
class LTHashAntiTampering {
    constructor(info = "WhatsApp Patch Integrity", size = 128) {
        this.info = toBuffer(info);
        this.size = size;
    }
    subtractThenAdd(base, subtract = [], add = []) {
        const output = Buffer.from(toBuffer(base));
        this.subtractThenAddInPlace(output, subtract, add);
        return output;
    }
    subtractThenAddInPlace(base, subtract = [], add = []) {
        this.multipleOp(base, subtract, true);
        this.multipleOp(base, add, false);
        return base;
    }
    multipleOp(base, items, subtract) {
        for (const item of items || []) {
            const expanded = hkdf(item, this.size, { info: this.info });
            this.performPointwiseWithOverflow(base, expanded, subtract);
        }
    }
    performPointwiseWithOverflow(base, input, subtract) {
        for (let i = 0; i < base.length; i += 2) {
            const x = base.readUInt16LE(i);
            const y = input.readUInt16LE(i);
            const result = subtract ? (x - y + 0x10000) & 0xFFFF : (x + y) & 0xFFFF;
            base.writeUInt16LE(result, i);
        }
        return base;
    }
}
exports.LTHashAntiTampering = LTHashAntiTampering;



