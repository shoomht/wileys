"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.prepareStream = exports.getStatusCodeForMediaRetry = exports.decryptMediaRetryData = exports.decodeMediaRetryNode = exports.encryptMediaRetryRequest = exports.getWAUploadToServer = exports.uploadWithNodeHttp = exports.downloadEncryptedContent = exports.downloadContentFromMessage = exports.getUrlFromDirectPath = exports.encryptedStream = exports.getHttpStream = exports.getStream = exports.toBuffer = exports.toReadable = exports.mediaMessageSHA256B64 = exports.generateProfilePicture = exports.encodeBase64EncodedStringForUpload = exports.extractImageThumb = exports.getRawMediaUploadData = exports.hkdfInfoKey = void 0;
exports.getMediaKeys = getMediaKeys;
exports.getAudioDuration = getAudioDuration;
exports.getAudioWaveform = getAudioWaveform;
exports.generateThumbnail = generateThumbnail;
exports.extensionForMediaMessage = extensionForMediaMessage;
const events_1 = require("events");
const boom_1 = require("@hapi/boom");
const Crypto = __importStar(require("crypto"));
const fs_1 = require("fs");
const os_1 = require("os");
const path_1 = require("path");
const stream_1 = require("stream");
const url_1 = require("url");
const index_js_1 = require("../../WAProto/index.js");
const index_js_2 = require("../Defaults/index.js");
const index_js_3 = require("../WABinary/index.js");
const crypto_js_1 = require("./crypto.js");
const generics_js_1 = require("./generics.js");
const getTmpFilesDirectory = () => (0, os_1.tmpdir)();
const getImageProcessingLibrary = async () => {
    //@ts-ignore
    const [jimp, sharp] = await Promise.all([Promise.resolve().then(() => __importStar(require('jimp'))).catch(() => { }), Promise.resolve().then(() => __importStar(require('sharp'))).catch(() => { })]);
    if (sharp) {
        return { sharp };
    }
    if (jimp) {
        return { jimp };
    }
    throw new boom_1.Boom('No image processing library available');
};
const hkdfInfoKey = (type) => {
    const hkdfInfo = index_js_2.MEDIA_HKDF_KEY_MAPPING[type];
    return `WhatsApp ${hkdfInfo} Keys`;
};
exports.hkdfInfoKey = hkdfInfoKey;
const getRawMediaUploadData = async (media, mediaType, logger) => {
    const { stream } = await (0, exports.getStream)(media);
    const hasher = Crypto.createHash('sha256');
    const filePath = (0, path_1.join)((0, os_1.tmpdir)(), mediaType + (0, generics_js_1.generateMessageIDV2)());
    const fileWriteStream = (0, fs_1.createWriteStream)(filePath);
    let fileLength = 0;
    try {
        for await (const data of stream) {
            fileLength += data.length;
            hasher.update(data);
            if (!fileWriteStream.write(data)) {
                await (0, events_1.once)(fileWriteStream, 'drain');
            }
        }
        fileWriteStream.end();
        await (0, events_1.once)(fileWriteStream, 'finish');
        stream.destroy();
        const fileSha256 = hasher.digest();
        return {
            filePath: filePath,
            fileSha256,
            fileLength
        };
    }
    catch (error) {
        fileWriteStream.destroy();
        stream.destroy();
        try {
            await fs_1.promises.unlink(filePath);
        }
        catch {
            //
        }
        throw error;
    }
};
exports.getRawMediaUploadData = getRawMediaUploadData;
/** generates all the keys required to encrypt/decrypt & sign a media message */
async function getMediaKeys(buffer, mediaType) {
    if (!buffer) {
        throw new boom_1.Boom('Cannot derive from empty media key');
    }
    if (typeof buffer === 'string') {
        buffer = Buffer.from(buffer.replace('data:;base64,', ''), 'base64');
    }
    // expand using HKDF to 112 bytes, also pass in the relevant app info
    const expandedMediaKey = (0, crypto_js_1.hkdf)(buffer, 112, { info: (0, exports.hkdfInfoKey)(mediaType) });
    return {
        iv: expandedMediaKey.slice(0, 16),
        cipherKey: expandedMediaKey.slice(16, 48),
        macKey: expandedMediaKey.slice(48, 80)
    };
}
const extractImageThumb = async (bufferOrFilePath, width = 32) => {
    // TODO: Move entirely to sharp, removing jimp as it supports readable streams
    // This will have positive speed and performance impacts as well as minimizing RAM usage.
    if (bufferOrFilePath instanceof stream_1.Readable) {
        bufferOrFilePath = await (0, exports.toBuffer)(bufferOrFilePath);
    }
    const lib = await getImageProcessingLibrary();
    if ('sharp' in lib && typeof lib.sharp?.default === 'function') {
        const img = lib.sharp.default(bufferOrFilePath);
        const dimensions = await img.metadata();
        const buffer = await img.resize(width).jpeg({ quality: 50 }).toBuffer();
        return {
            buffer,
            original: {
                width: dimensions.width,
                height: dimensions.height
            }
        };
    }
    else if ('jimp' in lib && typeof lib.jimp?.Jimp === 'object') {
        const jimp = await lib.jimp.Jimp.read(bufferOrFilePath);
        const dimensions = {
            width: jimp.width,
            height: jimp.height
        };
        const buffer = await jimp
            .resize({ w: width, mode: lib.jimp.ResizeStrategy.BILINEAR })
            .getBuffer('image/jpeg', { quality: 50 });
        return {
            buffer,
            original: dimensions
        };
    }
    else {
        throw new boom_1.Boom('No image processing library available');
    }
};
exports.extractImageThumb = extractImageThumb;
const encodeBase64EncodedStringForUpload = (b64) => encodeURIComponent(b64.replace(/\+/g, '-').replace(/\//g, '_').replace(/\=+$/, ''));
exports.encodeBase64EncodedStringForUpload = encodeBase64EncodedStringForUpload;
const generateProfilePicture = async (mediaUpload, dimensions) => {
    let buffer;
    const { width: w = 640, height: h = 640 } = dimensions || {};
    if (Buffer.isBuffer(mediaUpload)) {
        buffer = mediaUpload;
    }
    else {
        // Use getStream to handle all WAMediaUpload types (Buffer, Stream, URL)
        const { stream } = await (0, exports.getStream)(mediaUpload);
        // Convert the resulting stream to a buffer
        buffer = await (0, exports.toBuffer)(stream);
    }
    const lib = await getImageProcessingLibrary();
    let img;
    if ('sharp' in lib && typeof lib.sharp?.default === 'function') {
        img = lib.sharp
            .default(buffer)
            .resize(w, h)
            .jpeg({
            quality: 50
        })
            .toBuffer();
    }
    else if ('jimp' in lib && typeof lib.jimp?.Jimp === 'function') {
        const jimp = await lib.jimp.Jimp.read(buffer);
        const min = Math.min(jimp.width, jimp.height);
        const cropped = jimp.crop({ x: 0, y: 0, w: min, h: min });
        img = cropped.resize({ w, h, mode: lib.jimp.ResizeStrategy.BILINEAR }).getBuffer('image/jpeg', { quality: 50 });
    }
    else {
        throw new boom_1.Boom('No image processing library available');
    }
    return {
        img: await img
    };
};
exports.generateProfilePicture = generateProfilePicture;
/** gets the SHA256 of the given media message */
const mediaMessageSHA256B64 = (message) => {
    const media = Object.values(message)[0];
    return media?.fileSha256 && Buffer.from(media.fileSha256).toString('base64');
};
exports.mediaMessageSHA256B64 = mediaMessageSHA256B64;
async function getAudioDuration(buffer, mimeType) {
    const musicMetadata = await Promise.resolve().then(() => __importStar(require('music-metadata')));
    let metadata;
    const options = {
        duration: true,
        ...(mimeType ? { mimeType } : {})
    };
    if (Buffer.isBuffer(buffer)) {
        metadata = await musicMetadata.parseBuffer(buffer, mimeType || undefined, options);
    }
    else if (typeof buffer === 'string') {
        // parseFile tidak support mimeType langsung, tapi kita bisa baca sebagai buffer
        // supaya mimeType hint bisa dipass — penting untuk m4a/aac yang nama filenya tanpa ekstensi
        try {
            metadata = await musicMetadata.parseFile(buffer, options);
        }
        catch (_) {
            // fallback: baca file sebagai buffer lalu parse dengan mimeType
            const { readFileSync } = await Promise.resolve().then(() => __importStar(require('fs')));
            const buf = readFileSync(buffer);
            metadata = await musicMetadata.parseBuffer(buf, mimeType || undefined, options);
        }
    }
    else {
        metadata = await musicMetadata.parseStream(buffer, mimeType || undefined, options);
    }
    const dur = metadata.format.duration;
    // Jangan return NaN/undefined — WhatsApp akan tampilkan "Loading..." kalau seconds invalid
    return (typeof dur === 'number' && !isNaN(dur) && isFinite(dur)) ? Math.round(dur) : 0;
}
/**
  referenced from and modifying https://github.com/wppconnect-team/wa-js/blob/main/src/chat/functions/prepareAudioWaveform.ts
 */
async function getAudioWaveform(buffer, logger) {
    try {
        // @ts-ignore
        const { default: decoder } = await Promise.resolve().then(() => __importStar(require('audio-decode')));
        let audioData;
        if (Buffer.isBuffer(buffer)) {
            audioData = buffer;
        }
        else if (typeof buffer === 'string') {
            const rStream = (0, fs_1.createReadStream)(buffer);
            audioData = await (0, exports.toBuffer)(rStream);
        }
        else {
            audioData = await (0, exports.toBuffer)(buffer);
        }
        // Skip audio-decode for large buffers (> 3MB)
        if (audioData.length > 3 * 1024 * 1024) {
            return undefined;
        }
        const audioBuffer = await decoder(audioData);
        const rawData = audioBuffer.getChannelData(0);
        const samples = 64;
        const blockSize = Math.floor(rawData.length / samples);
        const filteredData = [];
        for (let i = 0; i < samples; i++) {
            const blockStart = blockSize * i;
            let sum = 0;
            for (let j = 0; j < blockSize; j++) {
                sum = sum + Math.abs(rawData[blockStart + j]);
            }
            filteredData.push(sum / blockSize);
        }
        const multiplier = Math.pow(Math.max(...filteredData), -1);
        const normalizedData = filteredData.map(n => n * multiplier);
        const waveform = new Uint8Array(normalizedData.map(n => Math.floor(100 * n)));
        return waveform;
    }
    catch (e) {
    }
}
const toReadable = (buffer) => {
    const readable = new stream_1.Readable({ read: () => { }, highWaterMark: 64 * 1024 });
    readable.push(buffer);
    readable.push(null);
    return readable;
};
exports.toReadable = toReadable;
const toBuffer = async (stream) => {
    const chunks = [];
    for await (const chunk of stream) {
        chunks.push(chunk);
    }
    stream.destroy();
    return Buffer.concat(chunks);
};
exports.toBuffer = toBuffer;
const getStream = async (item, opts) => {
    if (item === undefined || item === null) {
        throw new Error('Media input is undefined');
    }
    if (Buffer.isBuffer(item)) {
        return { stream: (0, exports.toReadable)(item), type: 'buffer' };
    }
    if ('stream' in item) {
        return { stream: item.stream, type: 'readable' };
    }
    const urlStr = item.url.toString();
    if (urlStr.startsWith('data:')) {
        const buffer = Buffer.from(urlStr.split(',')[1], 'base64');
        return { stream: (0, exports.toReadable)(buffer), type: 'buffer' };
    }
    if (urlStr.startsWith('http://') || urlStr.startsWith('https://')) {
        return { stream: await (0, exports.getHttpStream)(item.url, opts), type: 'remote' };
    }
    return { stream: (0, fs_1.createReadStream)(item.url), type: 'file' };
};
exports.getStream = getStream;
/** generates a thumbnail for a given media, if required */
async function generateThumbnail(file, mediaType, options) {
    let thumbnail;
    let originalImageDimensions;
    if (mediaType === 'image') {
        const { buffer, original } = await (0, exports.extractImageThumb)(file);
        thumbnail = buffer.toString('base64');
        if (original.width && original.height) {
            originalImageDimensions = {
                width: original.width,
                height: original.height
            };
        }
    }
    else if (mediaType === 'video') {
        // Video thumbnail generation skipped (ffmpeg removed)
    }
    return {
        thumbnail,
        originalImageDimensions
    };
}
const getHttpStream = async (url, options = {}) => {
    const response = await fetch(url.toString(), {
        dispatcher: options.dispatcher,
        method: 'GET',
        headers: options.headers
    });
    if (!response.ok) {
        throw new boom_1.Boom(`Failed to fetch stream from ${url}`, { statusCode: response.status, data: { url } });
    }
    // @ts-ignore Node18+ Readable.fromWeb exists
    return response.body instanceof stream_1.Readable ? response.body : stream_1.Readable.fromWeb(response.body);
};
exports.getHttpStream = getHttpStream;
const encryptedStream = async (media, mediaType, { logger, saveOriginalFileIfRequired, opts, isPtt, forceOpus } = {}) => {
    const { stream, type } = await (0, exports.getStream)(media, opts);
    let finalStream = stream;
    let opusConverted = false;
    const mediaKey = Crypto.randomBytes(32);
    const { cipherKey, iv, macKey } = await getMediaKeys(mediaKey, mediaType);
    const encWriteStream = new stream_1.Readable({ read: () => { }, highWaterMark: 64 * 1024 });
    let bodyPath;
    let writeStream;
    let didSaveToTmpPath = false;
    if (type === 'file') {
        bodyPath = media.url?.toString?.() || media.url;
    }
    else if (saveOriginalFileIfRequired) {
        bodyPath = (0, path_1.join)(getTmpFilesDirectory(), mediaType + (0, generics_js_1.generateMessageIDV2)());
        writeStream = (0, fs_1.createWriteStream)(bodyPath);
        didSaveToTmpPath = true;
    }
    let fileLength = 0;
    const aes = Crypto.createCipheriv('aes-256-cbc', cipherKey, iv);
    let hmac = Crypto.createHmac('sha256', macKey).update(iv);
    let sha256Plain = Crypto.createHash('sha256');
    let sha256Enc = Crypto.createHash('sha256');
    const onChunk = (buff) => {
        sha256Enc = sha256Enc.update(buff);
        hmac = hmac.update(buff);
        encWriteStream.push(buff);
    };
    try {
        for await (const data of finalStream) {
            fileLength += data.length;
            if (type === 'remote' &&
                opts?.maxContentLength &&
                fileLength + data.length > opts.maxContentLength) {
                throw new boom_1.Boom(`content length exceeded when encrypting "${type}"`, {
                    data: { media, type }
                });
            }
            sha256Plain = sha256Plain.update(data);
            if (writeStream) {
                if (!writeStream.write(data)) {
                    await (0, events_1.once)(writeStream, 'drain');
                }
            }
            onChunk(aes.update(data));
        }
        onChunk(aes.final());
        const mac = hmac.digest().slice(0, 10);
        sha256Enc = sha256Enc.update(mac);
        const fileSha256 = sha256Plain.digest();
        const fileEncSha256 = sha256Enc.digest();
        encWriteStream.push(mac);
        encWriteStream.push(null);
        writeStream?.end();
        if (writeStream)
            await (0, events_1.once)(writeStream, 'finish');
        finalStream.destroy();
        return {
            mediaKey,
            encWriteStream,
            bodyPath,
            mac,
            fileEncSha256,
            fileSha256,
            fileLength,
            didSaveToTmpPath,
            opusConverted
        };
    }
    catch (error) {
        encWriteStream.destroy();
        writeStream?.destroy();
        aes.destroy();
        hmac.destroy();
        sha256Plain.destroy();
        sha256Enc.destroy();
        stream.destroy();
        if (didSaveToTmpPath) {
            try {
                await fs_1.promises.unlink(bodyPath);
            }
            catch (_) { }
        }
        throw error;
    }
};
exports.encryptedStream = encryptedStream;
const DEF_HOST = 'mmg.whatsapp.net';
const AES_CHUNK_SIZE = 16;
const toSmallestChunkSize = (num) => {
    return Math.floor(num / AES_CHUNK_SIZE) * AES_CHUNK_SIZE;
};
const getUrlFromDirectPath = (directPath) => `https://${DEF_HOST}${directPath}`;
exports.getUrlFromDirectPath = getUrlFromDirectPath;
const downloadContentFromMessage = async ({ mediaKey, directPath, url }, type, opts = {}) => {
    const isValidMediaUrl = url?.startsWith('https://mmg.whatsapp.net/');
    const downloadUrl = isValidMediaUrl ? url : (0, exports.getUrlFromDirectPath)(directPath);
    if (!downloadUrl) {
        throw new boom_1.Boom('No valid media URL or directPath present in message', { statusCode: 400 });
    }
    const keys = await getMediaKeys(mediaKey, type);
    return (0, exports.downloadEncryptedContent)(downloadUrl, keys, opts);
};
exports.downloadContentFromMessage = downloadContentFromMessage;
/**
 * Decrypts and downloads an AES256-CBC encrypted file given the keys.
 * Assumes the SHA256 of the plaintext is appended to the end of the ciphertext
 * */
const downloadEncryptedContent = async (downloadUrl, { cipherKey, iv }, { startByte, endByte, options } = {}) => {
    let bytesFetched = 0;
    let startChunk = 0;
    let firstBlockIsIV = false;
    // if a start byte is specified -- then we need to fetch the previous chunk as that will form the IV
    if (startByte) {
        const chunk = toSmallestChunkSize(startByte || 0);
        if (chunk) {
            startChunk = chunk - AES_CHUNK_SIZE;
            bytesFetched = chunk;
            firstBlockIsIV = true;
        }
    }
    const endChunk = endByte ? toSmallestChunkSize(endByte || 0) + AES_CHUNK_SIZE : undefined;
    const headersInit = options?.headers ? options.headers : undefined;
    const headers = {
        ...(headersInit
            ? Array.isArray(headersInit)
                ? Object.fromEntries(headersInit)
                : headersInit
            : {}),
        Origin: index_js_2.DEFAULT_ORIGIN
    };
    if (startChunk || endChunk) {
        headers.Range = `bytes=${startChunk}-`;
        if (endChunk) {
            headers.Range += endChunk;
        }
    }
    // download the message
    const fetched = await (0, exports.getHttpStream)(downloadUrl, {
        ...(options || {}),
        headers
    });
    let remainingBytes = Buffer.from([]);
    let aes;
    const pushBytes = (bytes, push) => {
        if (startByte || endByte) {
            const start = bytesFetched >= startByte ? undefined : Math.max(startByte - bytesFetched, 0);
            const end = bytesFetched + bytes.length < endByte ? undefined : Math.max(endByte - bytesFetched, 0);
            push(bytes.slice(start, end));
            bytesFetched += bytes.length;
        }
        else {
            push(bytes);
        }
    };
    const output = new stream_1.Transform({
        transform(chunk, _, callback) {
            let data = Buffer.concat([remainingBytes, chunk]);
            const decryptLength = toSmallestChunkSize(data.length);
            remainingBytes = data.slice(decryptLength);
            data = data.slice(0, decryptLength);
            if (!aes) {
                let ivValue = iv;
                if (firstBlockIsIV) {
                    ivValue = data.slice(0, AES_CHUNK_SIZE);
                    data = data.slice(AES_CHUNK_SIZE);
                }
                aes = Crypto.createDecipheriv('aes-256-cbc', cipherKey, ivValue);
                // if an end byte that is not EOF is specified
                // stop auto padding (PKCS7) -- otherwise throws an error for decryption
                if (endByte) {
                    aes.setAutoPadding(false);
                }
            }
            try {
                pushBytes(aes.update(data), b => this.push(b));
                callback();
            }
            catch (error) {
                callback(error);
            }
        },
        final(callback) {
            try {
                pushBytes(aes.final(), b => this.push(b));
                callback();
            }
            catch (error) {
                callback(error);
            }
        }
    });
    return fetched.pipe(output, { end: true });
};
exports.downloadEncryptedContent = downloadEncryptedContent;
function extensionForMediaMessage(message) {
    const getExtension = (mimetype) => mimetype.split(';')[0]?.split('/')[1];
    const type = Object.keys(message)[0];
    let extension;
    if (type === 'locationMessage' || type === 'liveLocationMessage' || type === 'productMessage') {
        extension = '.jpeg';
    }
    else {
        const messageContent = message[type];
        extension = getExtension(messageContent.mimetype);
    }
    return extension;
}
const isNodeRuntime = () => {
    return (typeof process !== 'undefined' &&
        process.versions?.node !== null &&
        typeof process.versions.bun === 'undefined' &&
        typeof globalThis.Deno === 'undefined');
};
const uploadWithNodeHttp = async ({ url, filePath, headers, timeoutMs, agent }, redirectCount = 0) => {
    if (redirectCount > 5) {
        throw new Error('Too many redirects');
    }
    const parsedUrl = new url_1.URL(url);
    const httpModule = parsedUrl.protocol === 'https:' ? await Promise.resolve().then(() => __importStar(require('https'))) : await Promise.resolve().then(() => __importStar(require('http')));
    // Get file size for Content-Length header (required for Node.js streaming)
    const fileStats = await fs_1.promises.stat(filePath);
    const fileSize = fileStats.size;
    return new Promise((resolve, reject) => {
        const req = httpModule.request({
            hostname: parsedUrl.hostname,
            port: parsedUrl.port || (parsedUrl.protocol === 'https:' ? 443 : 80),
            path: parsedUrl.pathname + parsedUrl.search,
            method: 'POST',
            headers: {
                ...headers,
                'Content-Length': fileSize
            },
            agent,
            timeout: timeoutMs
        }, res => {
            // Handle redirects (3xx)
            if (res.statusCode && res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
                res.resume(); // Consume response to free resources
                const newUrl = new url_1.URL(res.headers.location, url).toString();
                resolve((0, exports.uploadWithNodeHttp)({
                    url: newUrl,
                    filePath,
                    headers,
                    timeoutMs,
                    agent
                }, redirectCount + 1));
                return;
            }
            let body = '';
            res.on('data', chunk => (body += chunk));
            res.on('end', () => {
                try {
                    resolve(JSON.parse(body));
                }
                catch {
                    resolve(undefined);
                }
            });
        });
        req.on('error', reject);
        req.on('timeout', () => {
            req.destroy();
            reject(new Error('Upload timeout'));
        });
        const stream = (0, fs_1.createReadStream)(filePath);
        stream.pipe(req);
        stream.on('error', err => {
            req.destroy();
            reject(err);
        });
    });
};
exports.uploadWithNodeHttp = uploadWithNodeHttp;
const uploadWithFetch = async ({ url, filePath, headers, timeoutMs, agent }) => {
    // Convert Node.js Readable to Web ReadableStream
    const nodeStream = (0, fs_1.createReadStream)(filePath);
    const webStream = stream_1.Readable.toWeb(nodeStream);
    const response = await fetch(url, {
        dispatcher: agent,
        method: 'POST',
        body: webStream,
        headers,
        duplex: 'half',
        signal: timeoutMs ? AbortSignal.timeout(timeoutMs) : undefined
    });
    try {
        return (await response.json());
    }
    catch {
        return undefined;
    }
};
/**
 * Uploads media to WhatsApp servers.
 *
 * ## Why we have two upload implementations:
 *
 * Node.js's native `fetch` (powered by undici) has a known bug where it buffers
 * the entire request body in memory before sending, even when using streams.
 * This causes memory issues with large files (e.g., 1GB file = 1GB+ memory usage).
 * See: https://github.com/nodejs/undici/issues/4058
 *
 * Other runtimes (Bun, Deno, browsers) correctly stream the request body without
 * buffering, so we can use the web-standard Fetch API there.
 *
 * ## Future considerations:
 * Once the undici bug is fixed, we can simplify this to use only the Fetch API
 * across all runtimes. Monitor the GitHub issue for updates.
 */
const uploadMedia = async (params, logger) => {
    if (isNodeRuntime()) {
        return (0, exports.uploadWithNodeHttp)(params);
    }
    else {
        return uploadWithFetch(params);
    }
};
const getWAUploadToServer = ({ customUploadHosts, fetchAgent, logger, options }, refreshMediaConn) => {
    return async (streamOrPath, { mediaType, fileEncSha256B64, timeoutMs, newsletter }) => {
        // send a query JSON to obtain the url & auth token to upload our media
        let uploadInfo = await refreshMediaConn(false);
        let urls;
        const hosts = [...customUploadHosts, ...uploadInfo.hosts];
        fileEncSha256B64 = (0, exports.encodeBase64EncodedStringForUpload)(fileEncSha256B64);
        // Prepare common headers
        const customHeaders = (() => {
            const hdrs = options?.headers;
            if (!hdrs)
                return {};
            return Array.isArray(hdrs) ? Object.fromEntries(hdrs) : hdrs;
        })();
        const headers = {
            ...customHeaders,
            'Content-Type': 'application/octet-stream',
            Origin: index_js_2.DEFAULT_ORIGIN
        };
        // Collect buffer from Readable stream or read from file path
        let reqBuffer;
        if (Buffer.isBuffer(streamOrPath)) {
            reqBuffer = streamOrPath;
        }
        else if (typeof streamOrPath === 'string') {
            reqBuffer = await fs_1.promises.readFile(streamOrPath);
        }
        else {
            // Readable stream
            const chunks = [];
            for await (const chunk of streamOrPath)
                chunks.push(chunk);
            reqBuffer = Buffer.concat(chunks);
        }
        // Newsletter uses different upload path
        let mediaPath = index_js_2.MEDIA_PATH_MAP[mediaType];
        if (newsletter) {
            mediaPath = mediaPath?.replace('/mms/', '/newsletter/newsletter-');
        }
        for (const { hostname, maxContentLengthBytes } of hosts) {
            const auth = encodeURIComponent(uploadInfo.auth);
            const url = `https://${hostname}${mediaPath}/${fileEncSha256B64}?auth=${auth}&token=${fileEncSha256B64}`;
            let result;
            try {
                // Upload buffer directly (AstraBail optimization avoids file I/O issues)
                const axios = (await Promise.resolve().then(() => __importStar(require('axios')))).default;
                const body = await axios.post(url, reqBuffer, {
                    ...options,
                    headers: {
                        ...headers,
                    },
                    httpsAgent: fetchAgent,
                    timeout: timeoutMs,
                    responseType: 'json',
                    maxBodyLength: Infinity,
                    maxContentLength: Infinity,
                });
                result = body.data;
                if (result?.url || result?.direct_path) {
                    urls = {
                        mediaUrl: result.url,
                        directPath: result.direct_path,
                        handle: result.handle,
                        meta_hmac: result.meta_hmac,
                        fbid: result.fbid,
                        ts: result.ts
                    };
                    break;
                }
                else {
                    uploadInfo = await refreshMediaConn(true);
                    throw new Error(`upload failed, reason: ${JSON.stringify(result)}`);
                }
            }
            catch (error) {
                const isLast = hostname === hosts[uploadInfo.hosts.length - 1]?.hostname;
                logger.warn({ trace: error?.stack, uploadResult: result }, `Error in uploading to ${hostname} ${isLast ? '' : ', retrying...'}`);
            }
        }
        if (!urls) {
            throw new boom_1.Boom('Media upload failed on all hosts', { statusCode: 500 });
        }
        return urls;
    };
};
exports.getWAUploadToServer = getWAUploadToServer;
const getMediaRetryKey = (mediaKey) => {
    return (0, crypto_js_1.hkdf)(mediaKey, 32, { info: 'WhatsApp Media Retry Notification' });
};
/**
 * Generate a binary node that will request the phone to re-upload the media & return the newly uploaded URL
 */
const encryptMediaRetryRequest = (key, mediaKey, meId) => {
    const recp = { stanzaId: key.id };
    const recpBuffer = index_js_1.proto.ServerErrorReceipt.encode(recp).finish();
    const iv = Crypto.randomBytes(12);
    const retryKey = getMediaRetryKey(mediaKey);
    const ciphertext = (0, crypto_js_1.aesEncryptGCM)(recpBuffer, retryKey, iv, Buffer.from(key.id));
    const req = {
        tag: 'receipt',
        attrs: {
            id: key.id,
            to: (0, index_js_3.jidNormalizedUser)(meId),
            type: 'server-error'
        },
        content: [
            // this encrypt node is actually pretty useless
            // the media is returned even without this node
            // keeping it here to maintain parity with WA Web
            {
                tag: 'encrypt',
                attrs: {},
                content: [
                    { tag: 'enc_p', attrs: {}, content: ciphertext },
                    { tag: 'enc_iv', attrs: {}, content: iv }
                ]
            },
            {
                tag: 'rmr',
                attrs: {
                    jid: key.remoteJid,
                    from_me: (!!key.fromMe).toString(),
                    // @ts-ignore
                    participant: key.participant || undefined
                }
            }
        ]
    };
    return req;
};
exports.encryptMediaRetryRequest = encryptMediaRetryRequest;
const decodeMediaRetryNode = (node) => {
    const rmrNode = (0, index_js_3.getBinaryNodeChild)(node, 'rmr');
    const event = {
        key: {
            id: node.attrs.id,
            remoteJid: rmrNode.attrs.jid,
            fromMe: rmrNode.attrs.from_me === 'true',
            participant: rmrNode.attrs.participant
        }
    };
    const errorNode = (0, index_js_3.getBinaryNodeChild)(node, 'error');
    if (errorNode) {
        const errorCode = +errorNode.attrs.code;
        event.error = new boom_1.Boom(`Failed to re-upload media (${errorCode})`, {
            data: errorNode.attrs,
            statusCode: (0, exports.getStatusCodeForMediaRetry)(errorCode)
        });
    }
    else {
        const encryptedInfoNode = (0, index_js_3.getBinaryNodeChild)(node, 'encrypt');
        const ciphertext = (0, index_js_3.getBinaryNodeChildBuffer)(encryptedInfoNode, 'enc_p');
        const iv = (0, index_js_3.getBinaryNodeChildBuffer)(encryptedInfoNode, 'enc_iv');
        if (ciphertext && iv) {
            event.media = { ciphertext, iv };
        }
        else {
            event.error = new boom_1.Boom('Failed to re-upload media (missing ciphertext)', { statusCode: 404 });
        }
    }
    return event;
};
exports.decodeMediaRetryNode = decodeMediaRetryNode;
const decryptMediaRetryData = ({ ciphertext, iv }, mediaKey, msgId) => {
    const retryKey = getMediaRetryKey(mediaKey);
    const plaintext = (0, crypto_js_1.aesDecryptGCM)(ciphertext, retryKey, iv, Buffer.from(msgId));
    return index_js_1.proto.MediaRetryNotification.decode(plaintext);
};
exports.decryptMediaRetryData = decryptMediaRetryData;
const getStatusCodeForMediaRetry = (code) => MEDIA_RETRY_STATUS_MAP[code];
exports.getStatusCodeForMediaRetry = getStatusCodeForMediaRetry;
const MEDIA_RETRY_STATUS_MAP = {
    [index_js_1.proto.MediaRetryNotification.ResultType.SUCCESS]: 200,
    [index_js_1.proto.MediaRetryNotification.ResultType.DECRYPTION_ERROR]: 412,
    [index_js_1.proto.MediaRetryNotification.ResultType.NOT_FOUND]: 404,
    [index_js_1.proto.MediaRetryNotification.ResultType.GENERAL_ERROR]: 418
};
const prepareStream = async (media, mediaType, { logger, saveOriginalFileIfRequired, opts, isPtt, forceOpus } = {}) => {
    const { stream, type } = await (0, exports.getStream)(media, opts);
    let buffer = await (0, exports.toBuffer)(stream);
    let opusConverted = false;
    let bodyPath;
    let didSaveToTmpPath = false;
    try {
        if (type === 'file') {
            bodyPath = media.url?.toString?.() || media.url;
        }
        else if (saveOriginalFileIfRequired) {
            bodyPath = (0, path_1.join)(getTmpFilesDirectory(), mediaType + (0, generics_js_1.generateMessageIDV2)());
            const { writeFileSync } = await Promise.resolve().then(() => __importStar(require('fs')));
            writeFileSync(bodyPath, buffer);
            didSaveToTmpPath = true;
        }
        const fileLength = buffer.length;
        const fileSha256 = Crypto.createHash('sha256').update(buffer).digest();
        return {
            mediaKey: undefined,
            encWriteStream: buffer,
            fileLength,
            fileSha256,
            fileEncSha256: undefined,
            bodyPath,
            didSaveToTmpPath,
            opusConverted
        };
    }
    catch (error) {
        if (didSaveToTmpPath && bodyPath) {
            try {
                await fs_1.promises.unlink(bodyPath);
            }
            catch (_) { }
        }
        throw error;
    }
};
exports.prepareStream = prepareStream;



