"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.audioToBuffer = audioToBuffer;
const buffer_1 = require("buffer");
const promises_1 = __importDefault(require("fs/promises"));
const stream_1 = require("stream");
const streamToBuffer_js_1 = require("./streamToBuffer.js");
/**
 * Mengonversi berbagai input audio (Buffer, URL, path file, atau Readable stream) menjadi Buffer.
 * @param {Buffer|string|Readable} audio Input audio.
 * @returns {Promise<Buffer>} Buffer hasil konversi.
 */
async function audioToBuffer(audio) {
    if (buffer_1.Buffer.isBuffer(audio)) {
        return audio;
    }
    if (typeof audio === 'string') {
        if (/^https?:\/\//.test(audio)) {
            const res = await fetch(audio); // native fetch bawaan Node 20
            return buffer_1.Buffer.from(await res.arrayBuffer());
        }
        return promises_1.default.readFile(audio);
    }
    if (audio instanceof stream_1.Readable) {
        return (0, streamToBuffer_js_1.streamToBuffer)(audio);
    }
    throw new TypeError('Unsupported audio input type');
}



