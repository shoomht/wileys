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
exports.getQuotedMsg = exports.prepareAlbumMessageContent = exports.patchMessageForMdIfRequired = exports.assertMediaContent = exports.downloadMediaMessage = exports.aggregateMessageKeysNotFromMe = exports.updateMessageWithEventResponse = exports.updateMessageWithPollUpdate = exports.updateMessageWithReaction = exports.updateMessageWithReceipt = exports.getDevice = exports.extractMessageContent = exports.normalizeMessageContent = exports.getMediaTypeFromContentType = exports.getContentType = exports.generateWAMessage = exports.generateWAMessageFromContent = exports.generateWAMessageContent = exports.hasNonNullishProperty = exports.generateForwardMessageContent = exports.prepareDisappearingMessageSettingContent = exports.prepareWAMessageMedia = exports.generateLinkPreviewIfRequired = exports.extractUrlFromText = void 0;
exports.getAggregateVotesInPollMessage = getAggregateVotesInPollMessage;
exports.getAggregateResponsesInEventMessage = getAggregateResponsesInEventMessage;
const boom_1 = require("@hapi/boom");
const crypto_1 = require("crypto");
const fs_1 = require("fs");
const index_js_1 = require("../../WAProto/index.js");
const index_js_2 = require("../Defaults/index.js");
const index_js_3 = require("../Types/index.js");
const index_js_4 = require("../WABinary/index.js");
const crypto_js_1 = require("./crypto.js");
const generics_js_1 = require("./generics.js");
const messages_media_js_1 = require("./messages-media.js");
const _require = require;
const reporting_utils_js_1 = require("./reporting-utils.js");
const MIMETYPE_MAP = {
    image: 'image/jpeg',
    video: 'video/mp4',
    document: 'application/pdf',
    audio: 'audio/ogg; codecs=opus',
    sticker: 'image/webp',
    'product-catalog-image': 'image/jpeg'
};
/** Map ekstensi audio ke mimetype */
const AUDIO_MIMETYPE_MAP = {
    ogg: 'audio/ogg; codecs=opus',
    oga: 'audio/ogg; codecs=opus',
    opus: 'audio/ogg; codecs=opus',
    mp3: 'audio/mpeg',
    mpeg: 'audio/mpeg',
    mp4: 'audio/mp4',
    m4a: 'audio/mp4',
    aac: 'audio/aac',
    wav: 'audio/wav',
    wave: 'audio/wav',
    flac: 'audio/flac',
    webm: 'audio/webm',
    amr: 'audio/amr',
    '3gp': 'audio/3gpp',
    '3gpp': 'audio/3gpp',
    wma: 'audio/x-ms-wma',
    caf: 'audio/x-caf',
    aiff: 'audio/aiff',
    aif: 'audio/aiff',
};
/**
 * Deteksi mimetype audio dari magic bytes buffer.
 * Return null jika tidak dikenali.
 */
const detectAudioMimetypeFromBuffer = (buf) => {
    if (!buf || buf.length < 12)
        return null;
    // OGG
    if (buf[0] === 0x4F && buf[1] === 0x67 && buf[2] === 0x67 && buf[3] === 0x53)
        return 'audio/ogg; codecs=opus';
    // MP3 (ID3 tag atau sync bits)
    if ((buf[0] === 0x49 && buf[1] === 0x44 && buf[2] === 0x33) ||
        (buf[0] === 0xFF && (buf[1] & 0xE0) === 0xE0))
        return 'audio/mpeg';
    // MP4/M4A (ftyp box)
    if (buf[4] === 0x66 && buf[5] === 0x74 && buf[6] === 0x79 && buf[7] === 0x70)
        return 'audio/mp4';
    // RIFF/WAV
    if (buf[0] === 0x52 && buf[1] === 0x49 && buf[2] === 0x46 && buf[3] === 0x46 &&
        buf[8] === 0x57 && buf[9] === 0x41 && buf[10] === 0x56 && buf[11] === 0x45)
        return 'audio/wav';
    // FLAC
    if (buf[0] === 0x66 && buf[1] === 0x4C && buf[2] === 0x61 && buf[3] === 0x43)
        return 'audio/flac';
    // WEBM/MKV
    if (buf[0] === 0x1A && buf[1] === 0x45 && buf[2] === 0xDF && buf[3] === 0xA3)
        return 'audio/webm';
    // AMR
    if (buf[0] === 0x23 && buf[1] === 0x21 && buf[2] === 0x41 && buf[3] === 0x4D &&
        buf[4] === 0x52)
        return 'audio/amr';
    return null;
};
/**
 * Deteksi mimetype audio secara otomatis dari media input.
 * Cek: 1) ekstensi URL/path, 2) magic bytes buffer, 3) fallback ke ogg/opus.
 */
const detectAudioMimetype = async (media) => {
    // Cek ekstensi dari URL atau path string
    if (typeof media === 'string' || (media && typeof media === 'object' && 'url' in media)) {
        const urlStr = typeof media === 'string' ? media : media.url?.toString?.() ?? '';
        // Ambil path tanpa query string, lalu cari semua ekstensi
        const pathOnly = urlStr.split('?')[0];
        // Cek ekstensi terakhir (.m4a, .mp3, dst)
        const extMatch = pathOnly.match(/\.([a-zA-Z0-9]{2,5})(?:[^/]*)?$/);
        if (extMatch) {
            const ext = extMatch[1].toLowerCase();
            if (AUDIO_MIMETYPE_MAP[ext])
                return AUDIO_MIMETYPE_MAP[ext];
        }
        // Fallback: scan semua segmen path untuk ekstensi audio yang dikenal
        // Contoh: ".plus.aac.ep.m4a" → cek tiap segment dari belakang
        const segments = pathOnly.split('.');
        for (let i = segments.length - 1; i >= 0; i--) {
            const seg = segments[i].toLowerCase().split('/')[0].split('?')[0];
            if (AUDIO_MIMETYPE_MAP[seg])
                return AUDIO_MIMETYPE_MAP[seg];
        }
    }
    // Cek magic bytes jika Buffer
    if (Buffer.isBuffer(media)) {
        const detected = detectAudioMimetypeFromBuffer(media);
        if (detected)
            return detected;
    }
    // Fallback: default ogg/opus
    return MIMETYPE_MAP.audio;
};
const MessageTypeProto = {
    image: index_js_3.WAProto.Message.ImageMessage,
    video: index_js_3.WAProto.Message.VideoMessage,
    audio: index_js_3.WAProto.Message.AudioMessage,
    sticker: index_js_3.WAProto.Message.StickerMessage,
    document: index_js_3.WAProto.Message.DocumentMessage
};
/**
 * Uses a regex to test whether the string contains a URL, and returns the URL if it does.
 * @param text eg. hello https://google.com
 * @returns the URL, eg. https://google.com
 */
const extractUrlFromText = (text) => text.match(index_js_2.URL_REGEX)?.[0];
exports.extractUrlFromText = extractUrlFromText;
const generateLinkPreviewIfRequired = async (text, getUrlInfo, logger) => {
    const url = (0, exports.extractUrlFromText)(text);
    if (!!getUrlInfo && url) {
        try {
            const urlInfo = await getUrlInfo(url);
            return urlInfo;
        }
        catch (error) {
            // ignore if fails
            logger?.warn({ trace: error.stack }, 'url generation failed');
        }
    }
};
exports.generateLinkPreviewIfRequired = generateLinkPreviewIfRequired;
const assertColor = async (color) => {
    let assertedColor;
    if (typeof color === 'number') {
        assertedColor = color > 0 ? color : 0xffffffff + Number(color) + 1;
    }
    else {
        let hex = color.trim().replace('#', '');
        if (hex.length <= 6) {
            hex = 'FF' + hex.padStart(6, '0');
        }
        assertedColor = parseInt(hex, 16);
        return assertedColor;
    }
};
const prepareWAMessageMedia = async (message, options) => {
    const logger = options.logger;
    let mediaType;
    for (const key of index_js_2.MEDIA_KEYS) {
        if (key in message) {
            mediaType = key;
        }
    }
    if (!mediaType) {
        throw new boom_1.Boom('Invalid media type', { statusCode: 400 });
    }
    const uploadData = {
        ...message,
        media: message[mediaType]
    };
    delete uploadData[mediaType];
    // check if cacheable + generate cache key
    const cacheableKey = typeof uploadData.media === 'object' &&
        'url' in uploadData.media &&
        !!uploadData.media.url &&
        !!options.mediaCache &&
        mediaType + ':' + uploadData.media.url.toString();
    if (mediaType === 'document' && !uploadData.fileName) {
        uploadData.fileName = 'file';
    }
    if (!uploadData.mimetype) {
        uploadData.mimetype = MIMETYPE_MAP[mediaType];
    }
    if (cacheableKey) {
        const mediaBuff = await options.mediaCache.get(cacheableKey);
        if (mediaBuff) {
            logger?.debug({ cacheableKey }, 'got media cache hit');
            const obj = index_js_1.proto.Message.decode(mediaBuff);
            const key = `${mediaType}Message`;
            Object.assign(obj[key], { ...uploadData, media: undefined });
            return obj;
        }
    }
    const isNewsletter = !!options.jid && (0, index_js_4.isJidNewsletter)(options.jid);
    if (isNewsletter)
        options.newsletter = true;
    const requiresDurationComputation = mediaType === 'audio' && typeof uploadData.seconds === 'undefined';
    const requiresThumbnailComputation = (mediaType === 'image' || mediaType === 'video') && typeof uploadData['jpegThumbnail'] === 'undefined';
    const requiresWaveformProcessing = mediaType === 'audio' && (uploadData.ptt === true || !!options.backgroundColor);
    const requiresAudioBackground = options.backgroundColor && mediaType === 'audio';
    const requiresOriginalForSomeProcessing = requiresDurationComputation || requiresThumbnailComputation || requiresWaveformProcessing;
    let streamResult;
    try {
        streamResult = await (options.newsletter ? messages_media_js_1.prepareStream : messages_media_js_1.encryptedStream)(uploadData.media, options.mediaTypeOverride || mediaType, {
            logger,
            saveOriginalFileIfRequired: requiresOriginalForSomeProcessing,
            opts: options.options,
            isPtt: uploadData.ptt,
        });
    }
    catch (streamErr) {
        throw streamErr;
    }
    const { mediaKey, encWriteStream, bodyPath, fileEncSha256, fileSha256, fileLength, didSaveToTmpPath } = streamResult;
    const fileEncSha256B64 = (options.newsletter ? fileSha256 : fileEncSha256 ?? fileSha256).toString('base64');
    const [{ mediaUrl, directPath, handle: uploadHandle }] = await Promise.all([
        (async () => {
            const result = await options.upload(encWriteStream, {
                fileEncSha256B64,
                mediaType,
                timeoutMs: options.mediaUploadTimeoutMs,
                newsletter: !!options.newsletter
            });
            return result;
        })(),
        (async () => {
            try {
                if (requiresThumbnailComputation) {
                    const { thumbnail, originalImageDimensions } = await (0, messages_media_js_1.generateThumbnail)(bodyPath, mediaType, options);
                    uploadData.jpegThumbnail = thumbnail;
                    if (!uploadData.width && originalImageDimensions) {
                        uploadData.width = originalImageDimensions.width;
                        uploadData.height = originalImageDimensions.height;
                        logger?.debug('set dimensions');
                    }
                    logger?.debug('generated thumbnail');
                }
                if (requiresDurationComputation) {
                    try {
                        if (bodyPath) {
                            uploadData.seconds = await (0, messages_media_js_1.getAudioDuration)(bodyPath, uploadData.mimetype);
                        }
                    }
                    catch (err) {
                        uploadData.seconds = 0;
                    }
                    // Pastikan seconds valid — NaN/undefined bikin WhatsApp tampilkan Loading...
                    if (typeof uploadData.seconds !== 'number' || isNaN(uploadData.seconds)) {
                        uploadData.seconds = 0;
                    }
                    logger?.debug('computed audio duration');
                }
                if (requiresWaveformProcessing) {
                    try {
                        uploadData.waveform = await (0, messages_media_js_1.getAudioWaveform)(bodyPath || encWriteStream, logger);
                    }
                    catch (err) {
                    }
                    if (!uploadData.waveform) {
                        uploadData.waveform = new Uint8Array([0, 99, 0, 99, 0, 99, 0, 99, 88, 99, 0, 99, 0, 55, 0, 99, 0, 99, 0, 99, 0, 99, 0, 99, 88, 99, 0, 99, 0, 55, 0, 99]);
                    }
                }
                if (requiresAudioBackground) {
                    uploadData.backgroundArgb = await assertColor(options.backgroundColor);
                    logger?.debug('computed backgroundColor audio status');
                }
            }
            catch (error) {
                logger?.warn({ trace: error.stack }, 'failed to obtain extra info');
            }
        })()
    ]).finally(async () => {
        try {
            if (!Buffer.isBuffer(encWriteStream)) {
                encWriteStream.destroy?.();
            }
            if (didSaveToTmpPath && bodyPath) {
                await fs_1.promises.unlink(bodyPath).catch(() => { });
            }
        }
        catch (error) {
            logger?.warn('failed to remove tmp file');
        }
    });
    const obj = index_js_3.WAProto.Message.fromObject({
        [`${mediaType}Message`]: MessageTypeProto[mediaType].fromObject({
            url: uploadHandle ? undefined : mediaUrl,
            directPath,
            mediaKey: mediaKey,
            fileEncSha256: fileEncSha256,
            fileSha256,
            fileLength,
            mediaKeyTimestamp: uploadHandle ? undefined : (0, generics_js_1.unixTimestampSeconds)(),
            ...uploadData,
            media: undefined,
            ...(options?.contextInfo ? { contextInfo: options.contextInfo } : {})
        })
    });
    if (uploadData.ptv) {
        obj.ptvMessage = obj.videoMessage;
        delete obj.videoMessage;
    }
    // Attach uploadHandle so sendMessage can use it as media_id
    if (uploadHandle) {
        obj._uploadHandle = uploadHandle;
    }
    if (mediaType === 'audio') {
    }
    if (cacheableKey) {
        logger?.debug({ cacheableKey }, 'set cache');
        await options.mediaCache.set(cacheableKey, index_js_3.WAProto.Message.encode(obj).finish());
    }
    return obj;
};
exports.prepareWAMessageMedia = prepareWAMessageMedia;
const prepareDisappearingMessageSettingContent = (ephemeralExpiration) => {
    ephemeralExpiration = ephemeralExpiration || 0;
    const content = {
        ephemeralMessage: {
            message: {
                protocolMessage: {
                    type: index_js_3.WAProto.Message.ProtocolMessage.Type.EPHEMERAL_SETTING,
                    ephemeralExpiration
                }
            }
        }
    };
    return index_js_3.WAProto.Message.fromObject(content);
};
exports.prepareDisappearingMessageSettingContent = prepareDisappearingMessageSettingContent;
/**
 * Generate forwarded message content like WA does
 * @param message the message to forward
 * @param options.forceForward will show the message as forwarded even if it is from you
 */
const generateForwardMessageContent = (message, forceForward) => {
    let content = message.message;
    if (!content) {
        throw new boom_1.Boom('no content in message', { statusCode: 400 });
    }
    // hacky copy
    content = (0, exports.normalizeMessageContent)(content);
    content = index_js_1.proto.Message.decode(index_js_1.proto.Message.encode(content).finish());
    let key = Object.keys(content)[0];
    let score = content?.[key]?.contextInfo?.forwardingScore || 0;
    score += message.key.fromMe && !forceForward ? 0 : 1;
    if (key === 'conversation') {
        content.extendedTextMessage = { text: content[key] };
        delete content.conversation;
        key = 'extendedTextMessage';
    }
    const key_ = content?.[key];
    if (score > 0) {
        key_.contextInfo = { forwardingScore: score, isForwarded: true };
    }
    else {
        key_.contextInfo = {};
    }
    return content;
};
exports.generateForwardMessageContent = generateForwardMessageContent;
const hasNonNullishProperty = (message, key) => {
    return (typeof message === 'object' &&
        message !== null &&
        key in message &&
        message[key] !== null &&
        message[key] !== undefined);
};
exports.hasNonNullishProperty = hasNonNullishProperty;
function hasOptionalProperty(obj, key) {
    return typeof obj === 'object' && obj !== null && key in obj && obj[key] !== null;
}
const generateWAMessageContent = async (message, options) => {
    var _a, _b;
    let m = {};
    if ((0, exports.hasNonNullishProperty)(message, 'text')) {
        const extContent = { text: message.text };
        let urlInfo = message.linkPreview;
        if (typeof urlInfo === 'undefined') {
            urlInfo = await (0, exports.generateLinkPreviewIfRequired)(message.text, options.getUrlInfo, options.logger);
        }
        if (urlInfo) {
            extContent.matchedText = urlInfo['matched-text'];
            extContent.jpegThumbnail = urlInfo.jpegThumbnail;
            extContent.description = urlInfo.description;
            extContent.title = urlInfo.title;
            extContent.previewType = 0;
            const img = urlInfo.highQualityThumbnail;
            if (img) {
                extContent.thumbnailDirectPath = img.directPath;
                extContent.mediaKey = img.mediaKey;
                extContent.mediaKeyTimestamp = img.mediaKeyTimestamp;
                extContent.thumbnailWidth = img.width;
                extContent.thumbnailHeight = img.height;
                extContent.thumbnailSha256 = img.fileSha256;
                extContent.thumbnailEncSha256 = img.fileEncSha256;
            }
        }
        if (options.backgroundColor) {
            extContent.backgroundArgb = await assertColor(options.backgroundColor);
        }
        if (options.font) {
            extContent.font = options.font;
        }
        m.extendedTextMessage = extContent;
    }
    else if ((0, exports.hasNonNullishProperty)(message, 'contacts')) {
        const contactLen = message.contacts.contacts.length;
        if (!contactLen) {
            throw new boom_1.Boom('require atleast 1 contact', { statusCode: 400 });
        }
        if (contactLen === 1) {
            m.contactMessage = index_js_3.WAProto.Message.ContactMessage.create(message.contacts.contacts[0]);
        }
        else {
            m.contactsArrayMessage = index_js_3.WAProto.Message.ContactsArrayMessage.create(message.contacts);
        }
    }
    else if ((0, exports.hasNonNullishProperty)(message, 'location')) {
        if (message.live) {
            m.liveLocationMessage = index_js_3.WAProto.Message.LiveLocationMessage.create(message.location);
        }
        else {
            m.locationMessage = index_js_3.WAProto.Message.LocationMessage.create(message.location);
        }
        const locType = message.live ? 'liveLocationMessage' : 'locationMessage';
        if (m[locType]) {
            m[locType].contextInfo = {
                ...(message.contextInfo || {}),
                ...(message.mentions ? { mentionedJid: message.mentions } : {})
            };
        }
    }
    else if ((0, exports.hasNonNullishProperty)(message, 'react')) {
        if (!message.react.senderTimestampMs) {
            message.react.senderTimestampMs = Date.now();
        }
        m.reactionMessage = index_js_3.WAProto.Message.ReactionMessage.create(message.react);
    }
    else if ((0, exports.hasNonNullishProperty)(message, 'delete')) {
        m.protocolMessage = {
            key: message.delete,
            type: index_js_3.WAProto.Message.ProtocolMessage.Type.REVOKE
        };
    }
    else if ((0, exports.hasNonNullishProperty)(message, 'forward')) {
        m = (0, exports.generateForwardMessageContent)(message.forward, message.force);
    }
    else if ((0, exports.hasNonNullishProperty)(message, 'disappearingMessagesInChat')) {
        const exp = typeof message.disappearingMessagesInChat === 'boolean'
            ? message.disappearingMessagesInChat
                ? index_js_2.WA_DEFAULT_EPHEMERAL
                : 0
            : message.disappearingMessagesInChat;
        m = (0, exports.prepareDisappearingMessageSettingContent)(exp);
    }
    else if ((0, exports.hasNonNullishProperty)(message, 'groupInvite')) {
        m.groupInviteMessage = {};
        m.groupInviteMessage.inviteCode = message.groupInvite.inviteCode;
        m.groupInviteMessage.inviteExpiration = message.groupInvite.inviteExpiration;
        m.groupInviteMessage.caption = message.groupInvite.text;
        m.groupInviteMessage.groupJid = message.groupInvite.jid;
        m.groupInviteMessage.groupName = message.groupInvite.subject;
        //TODO: use built-in interface and get disappearing mode info etc.
        //TODO: cache / use store!?
        if (options.getProfilePicUrl) {
            const pfpUrl = await options.getProfilePicUrl(message.groupInvite.jid, 'preview');
            if (pfpUrl) {
                const resp = await fetch(pfpUrl, { method: 'GET', dispatcher: options?.options?.dispatcher });
                if (resp.ok) {
                    const buf = Buffer.from(await resp.arrayBuffer());
                    m.groupInviteMessage.jpegThumbnail = buf;
                }
            }
        }
    }
    else if ((0, exports.hasNonNullishProperty)(message, 'pin')) {
        m.pinInChatMessage = {};
        m.messageContextInfo = {};
        m.pinInChatMessage.key = message.pin.key;
        m.pinInChatMessage.type = message.pin?.type || 1;
        m.pinInChatMessage.senderTimestampMs = message.pin?.time || Date.now();
        m.messageContextInfo.messageAddOnDurationInSecs = message.pin.type === 1 ? message.pin.time || 86400 : 0;
        m.messageContextInfo.messageAddOnExpiryType = index_js_1.proto.MessageContextInfo.MessageAddonExpiryType.STATIC;
    }
    else if ((0, exports.hasNonNullishProperty)(message, 'keep')) {
        m.keepInChatMessage = {};
        m.keepInChatMessage.key = message.keep.key;
        m.keepInChatMessage.keepType = message.keep?.type || 1;
        m.keepInChatMessage.timestampMs = message.keep?.time || Date.now();
    }
    else if ((0, exports.hasNonNullishProperty)(message, 'call')) {
        m.scheduledCallCreationMessage = {};
        m.scheduledCallCreationMessage.scheduledTimestampMs = message.call?.time || Date.now();
        m.scheduledCallCreationMessage.callType = message.call?.type || 1;
        m.scheduledCallCreationMessage.title = message.call?.name || 'Call Creation';
        m.scheduledCallCreationMessage.contextInfo = {
            ...(message.contextInfo || {}),
            ...(message.mentions ? { mentionedJid: message.mentions } : {})
        };
    }
    else if ((0, exports.hasNonNullishProperty)(message, 'paymentInvite')) {
        m.messageContextInfo = {};
        m.paymentInviteMessage = {};
        m.paymentInviteMessage.expiryTimestamp = message.paymentInvite?.expiry || 0;
        m.paymentInviteMessage.serviceType = message.paymentInvite?.type || 2;
        m.paymentInviteMessage.contextInfo = {
            ...(message.contextInfo || {}),
            ...(message.mentions ? { mentionedJid: message.mentions } : {})
        };
    }
    else if ((0, exports.hasNonNullishProperty)(message, 'buttonReply')) {
        switch (message.type) {
            case 'list':
                m.listResponseMessage = {
                    title: message.buttonReply.title,
                    description: message.buttonReply.description,
                    singleSelectReply: {
                        selectedRowId: message.buttonReply.rowId
                    },
                    listType: index_js_1.proto.Message.ListResponseMessage.ListType.SINGLE_SELECT
                };
                break;
            case 'template':
                m.templateButtonReplyMessage = {
                    selectedDisplayText: message.buttonReply.displayText,
                    selectedId: message.buttonReply.id,
                    selectedIndex: message.buttonReply.index
                };
                break;
            case 'plain':
                m.buttonsResponseMessage = {
                    selectedButtonId: message.buttonReply.id,
                    selectedDisplayText: message.buttonReply.displayText,
                    type: index_js_1.proto.Message.ButtonsResponseMessage.Type.DISPLAY_TEXT
                };
                break;
            case 'interactive':
                m.interactiveResponseMessage = {
                    body: {
                        text: message.buttonReply.displayText,
                        format: index_js_1.proto.Message.InteractiveResponseMessage.Body.Format.EXTENSIONS_1
                    },
                    nativeFlowResponseMessage: {
                        name: message.buttonReply.nativeFlows.name,
                        paramsJson: message.buttonReply.nativeFlows.paramsJson,
                        version: message.buttonReply.nativeFlows.version
                    }
                };
                break;
        }
    }
    else if ((0, exports.hasNonNullishProperty)(message, 'album')) {
        const imageMessages = message.album.filter(item => 'image' in item);
        const videoMessages = message.album.filter(item => 'video' in item);
        m.albumMessage = index_js_3.WAProto.Message.AlbumMessage.fromObject({
            expectedImageCount: imageMessages.length,
            expectedVideoCount: videoMessages.length
        });
    }
    else if ((0, exports.hasNonNullishProperty)(message, 'order')) {
        m.orderMessage = index_js_3.WAProto.Message.OrderMessage.fromObject(message.order);
        m.orderMessage.contextInfo = {
            ...(message.contextInfo || {}),
            ...(message.mentions ? { mentionedJid: message.mentions } : {})
        };
    }
    else if ((0, exports.hasNonNullishProperty)(message, 'payment')) {
        const requestPaymentMessage = {
            amount: {
                currencyCode: message.payment?.currency || 'IDR',
                offset: message.payment?.offset || 0,
                value: message.payment?.amount || 999999999
            },
            expiryTimestamp: message.payment?.expiry || 0,
            amount1000: (message.payment?.amount || 999999999) * 1000,
            currencyCodeIso4217: message.payment?.currency || 'IDR',
            requestFrom: message.payment?.from || '0@s.whatsapp.net',
            noteMessage: {
                extendedTextMessage: {
                    text: message.payment?.note || 'Notes'
                }
            },
            background: {
                placeholderArgb: message.payment?.image?.placeholderArgb || 4278190080,
                textArgb: message.payment?.image?.textArgb || 4294967295,
                subtextArgb: message.payment?.image?.subtextArgb || 4294967295,
                type: 1
            }
        };
        requestPaymentMessage.noteMessage.extendedTextMessage.contextInfo = {
            ...(message.contextInfo || {}),
            ...(message.mentions ? { mentionedJid: message.mentions } : {})
        };
        m.requestPaymentMessage = requestPaymentMessage;
    }
    else if ((0, exports.hasNonNullishProperty)(message, 'pollResult')) {
        if (!Array.isArray(message.pollResult.values)) {
            throw new boom_1.Boom('Invalid pollResult values', { statusCode: 400 });
        }
        const pollResultSnapshotMessage = {
            name: message.pollResult.name,
            pollVotes: message.pollResult.values.map(([optionName, optionVoteCount]) => ({
                optionName,
                optionVoteCount
            }))
        };
        pollResultSnapshotMessage.contextInfo = {
            ...(message.contextInfo || {}),
            ...(message.mentions ? { mentionedJid: message.mentions } : {})
        };
        m.pollResultSnapshotMessage = pollResultSnapshotMessage;
    }
    else if (hasOptionalProperty(message, 'ptv') && message.ptv) {
        const { videoMessage } = await (0, exports.prepareWAMessageMedia)({ video: message.video }, options);
        m.ptvMessage = videoMessage;
    }
    else if ((0, exports.hasNonNullishProperty)(message, 'product')) {
        const { imageMessage } = await (0, exports.prepareWAMessageMedia)({ image: message.product.productImage }, options);
        m.productMessage = index_js_3.WAProto.Message.ProductMessage.create({
            ...message,
            product: {
                ...message.product,
                productImage: imageMessage
            }
        });
    }
    else if ((0, exports.hasNonNullishProperty)(message, 'listReply')) {
        m.listResponseMessage = { ...message.listReply };
    }
    else if ((0, exports.hasNonNullishProperty)(message, 'event')) {
        m.eventMessage = {};
        const startTime = Math.floor(message.event.startDate.getTime() / 1000);
        if (message.event.call && options.getCallLink) {
            const token = await options.getCallLink(message.event.call, { startTime });
            m.eventMessage.joinLink = (message.event.call === 'audio' ? index_js_2.CALL_AUDIO_PREFIX : index_js_2.CALL_VIDEO_PREFIX) + token;
        }
        m.messageContextInfo = {
            // encKey
            messageSecret: message.event.messageSecret || (0, crypto_1.randomBytes)(32)
        };
        m.eventMessage.name = message.event.name;
        m.eventMessage.description = message.event.description;
        m.eventMessage.startTime = startTime;
        m.eventMessage.endTime = message.event.endDate ? message.event.endDate.getTime() / 1000 : undefined;
        m.eventMessage.isCanceled = message.event.isCancelled ?? false;
        m.eventMessage.extraGuestsAllowed = message.event.extraGuestsAllowed;
        m.eventMessage.isScheduleCall = message.event.isScheduleCall ?? false;
        m.eventMessage.location = message.event.location;
    }
    else if ((0, exports.hasNonNullishProperty)(message, 'poll')) {
        (_a = message.poll).selectableCount || (_a.selectableCount = 0);
        (_b = message.poll).toAnnouncementGroup || (_b.toAnnouncementGroup = false);
        if (!Array.isArray(message.poll.values)) {
            throw new boom_1.Boom('Invalid poll values', { statusCode: 400 });
        }
        if (message.poll.selectableCount < 0 || message.poll.selectableCount > message.poll.values.length) {
            throw new boom_1.Boom(`poll.selectableCount in poll should be >= 0 and <= ${message.poll.values.length}`, {
                statusCode: 400
            });
        }
        m.messageContextInfo = {
            // encKey
            messageSecret: message.poll.messageSecret || (0, crypto_1.randomBytes)(32)
        };
        const pollCreationMessage = {
            name: message.poll.name,
            selectableOptionsCount: message.poll.selectableCount,
            options: message.poll.values.map(optionName => ({ optionName }))
        };
        if (message.poll.toAnnouncementGroup) {
            // poll v2 is for community announcement groups (single select and multiple)
            m.pollCreationMessageV2 = pollCreationMessage;
        }
        else {
            if (message.poll.selectableCount === 1) {
                //poll v3 is for single select polls
                m.pollCreationMessageV3 = pollCreationMessage;
            }
            else {
                // poll for multiple choice polls
                m.pollCreationMessage = pollCreationMessage;
            }
        }
    }
    else if ((0, exports.hasNonNullishProperty)(message, 'stickerPack')) {
        const { zip } = _require('fflate');
        const { stickers, cover, name, publisher, packId, description } = message.stickerPack;
        const resolvedCover = cover || stickers?.[0]?.cover || stickers?.[0]?.data || stickers?.[0]?.sticker || stickers?.[0]?.image || stickers?.[0]?.url || stickers?.[0];
        // ── Validasi jumlah sticker ───────────────────────────────────────────
        if (stickers.length > 60) {
            throw new boom_1.Boom('Sticker pack exceeds the maximum limit of 60 stickers', { statusCode: 400 });
        }
        if (stickers.length === 0) {
            throw new boom_1.Boom('Sticker pack must contain at least one sticker', { statusCode: 400 });
        }
        const stickerPackId = packId || (0, generics_js_1.generateMessageIDV2)();
        const [_sharp, _jimp] = await Promise.all([Promise.resolve().then(() => __importStar(require('sharp'))).catch(() => null), Promise.resolve().then(() => __importStar(require('jimp'))).catch(() => null)]);
        const lib = _sharp ? { sharp: _sharp } : _jimp ? { jimp: _jimp } : null;
        if (!lib)
            throw new boom_1.Boom('No image processing library available (install sharp or jimp)');
        // ── Helper: deteksi WebP dari magic bytes ─────────────────────────────
        const isWebPBuffer = (buf) => (buf.length >= 12 &&
            buf[0] === 0x52 && buf[1] === 0x49 && buf[2] === 0x46 && buf[3] === 0x46 &&
            buf[8] === 0x57 && buf[9] === 0x45 && buf[10] === 0x42 && buf[11] === 0x50);
        // ── Helper: deteksi animasi WebP (VP8X/ANIM/ANMF chunks) ─────────────
        const isAnimatedWebP = (buf) => {
            if (!isWebPBuffer(buf))
                return false;
            let offset = 12;
            while (offset < buf.length - 8) {
                const fourCC = buf.toString('ascii', offset, offset + 4);
                const chunkSize = buf.readUInt32LE(offset + 4);
                if (fourCC === 'VP8X') {
                    const flagsOffset = offset + 8;
                    if (flagsOffset < buf.length && (buf[flagsOffset] & 0x02))
                        return true;
                }
                else if (fourCC === 'ANIM' || fourCC === 'ANMF') {
                    return true;
                }
                offset += 8 + chunkSize + (chunkSize % 2);
            }
            return false;
        };
        // ── Step 1: proses & zip semua sticker ────────────────────────────────
        const stickerData = {};
        const stickerPromises = stickers.map(async (s, i) => {
            const { stream } = await (0, messages_media_js_1.getStream)(s.data || s.sticker);
            const buffer = await (0, messages_media_js_1.toBuffer)(stream);
            let webpBuffer;
            let isAnimated = false;
            if (isWebPBuffer(buffer)) {
                webpBuffer = buffer;
                isAnimated = isAnimatedWebP(buffer);
            }
            else if ('sharp' in lib && lib.sharp) {
                webpBuffer = await lib.sharp.default(buffer).webp().toBuffer();
            }
            else {
                throw new boom_1.Boom('No image processing library (sharp) available for converting sticker to WebP. ' +
                    'Either install sharp or provide stickers in WebP format.');
            }
            if (webpBuffer.length > 1024 * 1024) {
                throw new boom_1.Boom(`Sticker at index ${i} exceeds the 1MB size limit`, { statusCode: 400 });
            }
            const hash = (0, crypto_js_1.sha256)(webpBuffer).toString('base64').replace(/\//g, '-');
            const fileName = `${hash}.webp`;
            stickerData[fileName] = [new Uint8Array(webpBuffer), { level: 0 }];
            return {
                fileName,
                mimetype: 'image/webp',
                isAnimated,
                emojis: s.emojis || [],
                accessibilityLabel: s.accessibilityLabel || ''
            };
        });
        const stickerMetadata = await Promise.all(stickerPromises);
        // ── Step 2: proses cover & masukkan ke dalam ZIP ──────────────────────
        const trayIconFileName = `${stickerPackId}.webp`;
        if (!resolvedCover) {
            throw new boom_1.Boom('Sticker pack cover is missing and no fallback sticker was found', { statusCode: 400 });
        }
        const coverBuffer = await (0, messages_media_js_1.toBuffer)((await (0, messages_media_js_1.getStream)(resolvedCover)).stream);
        let coverWebpBuffer;
        if (isWebPBuffer(coverBuffer)) {
            coverWebpBuffer = coverBuffer;
        }
        else if ('sharp' in lib && lib.sharp) {
            coverWebpBuffer = await lib.sharp.default(coverBuffer).webp().toBuffer();
        }
        else {
            throw new boom_1.Boom('No image processing library (sharp) available for converting cover to WebP. ' +
                'Either install sharp or provide cover in WebP format.');
        }
        stickerData[trayIconFileName] = [new Uint8Array(coverWebpBuffer), { level: 0 }];
        // ── Step 3: buat ZIP buffer ───────────────────────────────────────────
        const zipBuffer = await new Promise((resolve, reject) => {
            zip(stickerData, (err, data) => {
                if (err)
                    reject(err);
                else
                    resolve(Buffer.from(data));
            });
        });
        // ── Step 4: encrypt ZIP (generate random mediaKey) ────────────────────
        const stickerPackUpload = await (0, messages_media_js_1.encryptedStream)(zipBuffer, 'sticker-pack', {
            logger: options.logger,
            opts: options.options
        });
        // ── Step 5: upload ZIP ────────────────────────────────────────────────
        const stickerPackUploadResult = await options.upload(stickerPackUpload.encWriteStream, {
            fileEncSha256B64: stickerPackUpload.fileEncSha256.toString('base64'),
            mediaType: 'sticker-pack',
            timeoutMs: options.mediaUploadTimeoutMs
        });
        // ── Step 6: build stickerPackMessage ──────────────────────────────────
        m.stickerPackMessage = {
            name,
            publisher,
            stickerPackId,
            packDescription: description,
            stickerPackOrigin: index_js_3.WAProto.Message.StickerPackMessage.StickerPackOrigin.THIRD_PARTY,
            stickerPackSize: zipBuffer.length,
            stickers: stickerMetadata,
            fileSha256: stickerPackUpload.fileSha256,
            fileEncSha256: stickerPackUpload.fileEncSha256,
            mediaKey: stickerPackUpload.mediaKey,
            directPath: stickerPackUploadResult.directPath,
            fileLength: stickerPackUpload.fileLength,
            mediaKeyTimestamp: (0, generics_js_1.unixTimestampSeconds)(),
            trayIconFileName
        };
        // ── Step 7: generate & upload thumbnail (pakai mediaKey yang sama) ────
        try {
            let thumbnailBuffer;
            if ('sharp' in lib && lib.sharp) {
                thumbnailBuffer = await lib.sharp.default(coverBuffer).resize(252, 252).jpeg().toBuffer();
            }
            else if ('jimp' in lib && lib.jimp) {
                const jimpImage = await (lib.jimp.Jimp || lib.jimp.default).read(coverBuffer);
                thumbnailBuffer = await jimpImage.resize({ w: 252, h: 252 }).getBuffer('image/jpeg');
            }
            else {
                throw new Error('No image processing library available for thumbnail generation');
            }
            if (!thumbnailBuffer || thumbnailBuffer.length === 0) {
                throw new Error('Failed to generate thumbnail buffer');
            }
            const thumbUpload = await (0, messages_media_js_1.encryptedStream)(thumbnailBuffer, 'thumbnail-sticker-pack', {
                logger: options.logger,
                opts: options.options,
                mediaKey: stickerPackUpload.mediaKey
            });
            const thumbUploadResult = await options.upload(thumbUpload.encWriteStream, {
                fileEncSha256B64: thumbUpload.fileEncSha256.toString('base64'),
                mediaType: 'thumbnail-sticker-pack',
                timeoutMs: options.mediaUploadTimeoutMs
            });
            Object.assign(m.stickerPackMessage, {
                thumbnailDirectPath: thumbUploadResult.directPath,
                thumbnailSha256: thumbUpload.fileSha256,
                thumbnailEncSha256: thumbUpload.fileEncSha256,
                thumbnailHeight: 252,
                thumbnailWidth: 252,
                imageDataHash: (0, crypto_js_1.sha256)(thumbnailBuffer).toString('base64')
            });
        }
        catch (e) {
            options.logger?.warn?.(`Thumbnail generation failed: ${e}`);
        }
        m.stickerPackMessage.contextInfo = {
            ...(message.contextInfo || {}),
            ...(message.mentions ? { mentionedJid: message.mentions } : {})
        };
    }
    else if ((0, exports.hasNonNullishProperty)(message, 'adminInvite')) {
        m.newsletterAdminInviteMessage = {};
        m.newsletterAdminInviteMessage.newsletterJid = message.adminInvite.jid;
        m.newsletterAdminInviteMessage.newsletterName = message.adminInvite.name;
        m.newsletterAdminInviteMessage.caption = message.adminInvite.caption;
        m.newsletterAdminInviteMessage.inviteExpiration = message.adminInvite.expiration;
        if (message.adminInvite.jpegThumbnail) {
            m.newsletterAdminInviteMessage.jpegThumbnail = message.adminInvite.jpegThumbnail;
        }
        else if (options.getProfilePicUrl) {
            try {
                const pfpUrl = await options.getProfilePicUrl(message.adminInvite.jid);
                if (pfpUrl) {
                    const { thumbnail } = await (0, messages_media_js_1.generateThumbnail)(pfpUrl, 'image');
                    m.newsletterAdminInviteMessage.jpegThumbnail = thumbnail;
                }
            }
            catch (_) { }
        }
        m.newsletterAdminInviteMessage.contextInfo = {
            ...(message.contextInfo || {}),
            ...(message.mentions ? { mentionedJid: message.mentions } : {})
        };
    }
    else if ((0, exports.hasNonNullishProperty)(message, 'sharePhoneNumber')) {
        m.protocolMessage = {
            type: index_js_1.proto.Message.ProtocolMessage.Type.SHARE_PHONE_NUMBER
        };
    }
    else if ((0, exports.hasNonNullishProperty)(message, 'requestPhoneNumber')) {
        m.requestPhoneNumberMessage = {};
    }
    else if ((0, exports.hasNonNullishProperty)(message, 'limitSharing')) {
        m.protocolMessage = {
            type: index_js_1.proto.Message.ProtocolMessage.Type.LIMIT_SHARING,
            limitSharing: {
                sharingLimited: message.limitSharing === true,
                trigger: 1,
                limitSharingSettingTimestamp: Date.now(),
                initiatedByMe: true
            }
        };
    }
    else if ('interactiveMessage' in message && !!message.interactiveMessage) {
        // ── Passthrough interactiveMessage raw object ──────────────────────
        // Must be BEFORE the else block to avoid hitting prepareWAMessageMedia
        // which throws 'Invalid media type' for interactiveMessage keys.
        m = { interactiveMessage: message.interactiveMessage };
    }
    else {
        m = await (0, exports.prepareWAMessageMedia)(message, options);
    }
    if ('sections' in message && !!message.sections) {
        const listMessage = {
            title: message.title,
            buttonText: message.buttonText,
            footerText: message.footer,
            description: message.text,
            sections: message.sections,
            listType: index_js_1.proto.Message.ListMessage.ListType.SINGLE_SELECT
        };
        listMessage.contextInfo = {
            ...(message.contextInfo || {}),
            ...(message.mentions ? { mentionedJid: message.mentions } : {})
        };
        m = { listMessage };
    }
    else if ('productList' in message && !!message.productList) {
        const thumbnail = message.thumbnail ? await (0, messages_media_js_1.generateThumbnail)(message.thumbnail, 'image') : null;
        const listMessage = {
            title: message.title,
            buttonText: message.buttonText,
            footerText: message.footer,
            description: message.text,
            productListInfo: {
                productSections: message.productList,
                headerImage: {
                    productId: message.productList[0].products[0].productId,
                    jpegThumbnail: thumbnail?.thumbnail || null
                },
                businessOwnerJid: message.businessOwnerJid
            },
            listType: index_js_1.proto.Message.ListMessage.ListType.PRODUCT_LIST
        };
        listMessage.contextInfo = {
            ...(message.contextInfo || {}),
            ...(message.mentions ? { mentionedJid: message.mentions } : {})
        };
        m = { listMessage };
    }
    else if ('buttons' in message && !!message.buttons) {
        const buttonsMessage = {
            buttons: message.buttons.map(b => ({ ...b, type: index_js_1.proto.Message.ButtonsMessage.Button.Type.RESPONSE }))
        };
        if ('text' in message) {
            buttonsMessage.contentText = message.text;
            buttonsMessage.headerType = index_js_1.proto.Message.ButtonsMessage.HeaderType.EMPTY;
        }
        else {
            if ('caption' in message) {
                buttonsMessage.contentText = message.caption;
            }
            const type = Object.keys(m)[0].replace('Message', '').toUpperCase();
            buttonsMessage.headerType = index_js_1.proto.Message.ButtonsMessage.HeaderType[type];
            Object.assign(buttonsMessage, m);
        }
        if ('footer' in message && !!message.footer) {
            buttonsMessage.footerText = message.footer;
        }
        if ('title' in message && !!message.title) {
            buttonsMessage.text = message.title;
            buttonsMessage.headerType = index_js_1.proto.Message.ButtonsMessage.HeaderType.TEXT;
        }
        buttonsMessage.contextInfo = {
            ...(message.contextInfo || {}),
            ...(message.mentions ? { mentionedJid: message.mentions } : {})
        };
        m = { buttonsMessage };
    }
    else if ('templateButtons' in message && !!message.templateButtons) {
        const hydratedTemplate = {
            hydratedButtons: message.templateButtons
        };
        if ('text' in message) {
            hydratedTemplate.hydratedContentText = message.text;
        }
        else {
            if ('caption' in message) {
                hydratedTemplate.hydratedContentText = message.caption;
            }
            Object.assign(hydratedTemplate, m);
        }
        if ('footer' in message && !!message.footer) {
            hydratedTemplate.hydratedFooterText = message.footer;
        }
        hydratedTemplate.contextInfo = {
            ...(message.contextInfo || {}),
            ...(message.mentions ? { mentionedJid: message.mentions } : {})
        };
        m = { templateMessage: { fourRowTemplate: hydratedTemplate, hydratedTemplate } };
    }
    else if ('interactiveButtons' in message && !!message.interactiveButtons) {
        const interactiveMessage = {
            nativeFlowMessage: index_js_1.proto.Message.InteractiveMessage.NativeFlowMessage.fromObject({
                buttons: message.interactiveButtons,
            })
        };
        if ('text' in message) {
            interactiveMessage.body = { text: message.text };
        }
        else if ('caption' in message) {
            interactiveMessage.body = { text: message.caption };
            interactiveMessage.header = {
                title: message.title,
                subtitle: message.subtitle,
                hasMediaAttachment: message?.media ?? false,
            };
            Object.assign(interactiveMessage.header, m);
        }
        if ('footer' in message && !!message.footer) {
            interactiveMessage.footer = { text: message.footer };
        }
        if ('title' in message && !!message.title) {
            interactiveMessage.header = {
                title: message.title,
                subtitle: message.subtitle,
                hasMediaAttachment: message?.media ?? false,
            };
            Object.assign(interactiveMessage.header, m);
        }
        if ('contextInfo' in message && !!message.contextInfo) {
            interactiveMessage.contextInfo = message.contextInfo;
        }
        if ('mentions' in message && !!message.mentions) {
            interactiveMessage.contextInfo = { mentionedJid: message.mentions };
        }
        m = { interactiveMessage };
    }
    else if ('shop' in message && !!message.shop) {
        const interactiveMessage = {
            shopStorefrontMessage: index_js_1.proto.Message.InteractiveMessage.ShopMessage.fromObject({
                surface: message.shop,
                id: message.id
            })
        };
        if ('text' in message) {
            interactiveMessage.body = { text: message.text };
        }
        else if ('caption' in message) {
            interactiveMessage.body = { text: message.caption };
            interactiveMessage.header = {
                title: message.title,
                subtitle: message.subtitle,
                hasMediaAttachment: message?.media ?? false,
            };
            Object.assign(interactiveMessage.header, m);
        }
        if ('footer' in message && !!message.footer) {
            interactiveMessage.footer = { text: message.footer };
        }
        if ('title' in message && !!message.title) {
            interactiveMessage.header = {
                title: message.title,
                subtitle: message.subtitle,
                hasMediaAttachment: message?.media ?? false,
            };
            Object.assign(interactiveMessage.header, m);
        }
        if ('contextInfo' in message && !!message.contextInfo) {
            interactiveMessage.contextInfo = message.contextInfo;
        }
        if ('mentions' in message && !!message.mentions) {
            interactiveMessage.contextInfo = { mentionedJid: message.mentions };
        }
        m = { interactiveMessage };
    }
    else if ('collection' in message && !!message.collection) {
        const interactiveMessage = {
            collectionMessage: {
                bizJid: message.collection.bizJid,
                id: message.collection.id,
                messageVersion: message?.collection?.version
            }
        };
        if ('text' in message) {
            interactiveMessage.body = { text: message.text };
            interactiveMessage.header = {
                title: message.title,
                subtitle: message.subtitle,
                hasMediaAttachment: false
            };
        }
        else {
            if ('caption' in message) {
                interactiveMessage.body = { text: message.caption };
                interactiveMessage.header = {
                    title: message.title,
                    subtitle: message.subtitle,
                    hasMediaAttachment: message.hasMediaAttachment ? message.hasMediaAttachment : false,
                    ...Object.assign(interactiveMessage, m)
                };
            }
        }
        if ('footer' in message && !!message.footer) {
            interactiveMessage.footer = { text: message.footer };
        }
        interactiveMessage.contextInfo = {
            ...(message.contextInfo || {}),
            ...(message.mentions ? { mentionedJid: message.mentions } : {})
        };
        m = { interactiveMessage };
    }
    else if ('cards' in message && !!message.cards) {
        const slides = await Promise.all(message.cards.map(async (slide) => {
            const { image, video, product, title, body, footer, buttons } = slide;
            let header;
            if (product) {
                const { imageMessage } = await (0, exports.prepareWAMessageMedia)({ image: product.productImage, ...options }, options);
                header = {
                    productMessage: {
                        product: {
                            ...product,
                            productImage: imageMessage,
                        },
                        ...slide
                    }
                };
            }
            else if (image) {
                header = await (0, exports.prepareWAMessageMedia)({ image: image, ...options }, options);
            }
            else if (video) {
                header = await (0, exports.prepareWAMessageMedia)({ video: video, ...options }, options);
            }
            return {
                header: {
                    title,
                    hasMediaAttachment: true,
                    ...header
                },
                body: { text: body },
                footer: { text: footer },
                nativeFlowMessage: { buttons }
            };
        }));
        const interactiveMessage = {
            carouselMessage: { cards: slides }
        };
        if ('text' in message) {
            interactiveMessage.body = { text: message.text };
            interactiveMessage.header = {
                title: message.title,
                subtitle: message.subtitle,
                hasMediaAttachment: false
            };
        }
        if ('footer' in message && !!message.footer) {
            interactiveMessage.footer = { text: message.footer };
        }
        interactiveMessage.contextInfo = {
            ...(message.contextInfo || {}),
            ...(message.mentions ? { mentionedJid: message.mentions } : {})
        };
        m = { interactiveMessage };
    }
    if (hasOptionalProperty(message, 'ephemeral') && !!message.ephemeral) {
        m = { ephemeralMessage: { message: m } };
    }
    if (hasOptionalProperty(message, 'viewOnce') && !!message.viewOnce) {
        m = { viewOnceMessageV2: { message: m } };
    }
    if (hasOptionalProperty(message, 'viewOnceExt') && !!message.viewOnceExt) {
        m = { viewOnceMessageV2Extension: { message: m } };
    }
    if (hasOptionalProperty(message, 'mentions') && message.mentions?.length) {
        const messageType = Object.keys(m)[0];
        const key = m[messageType];
        if ('contextInfo' in key && !!key.contextInfo) {
            key.contextInfo.mentionedJid = message.mentions;
        }
        else if (key) {
            key.contextInfo = {
                mentionedJid: message.mentions
            };
        }
    }
    if (hasOptionalProperty(message, 'edit')) {
        m = {
            protocolMessage: {
                key: message.edit,
                editedMessage: m,
                timestampMs: Date.now(),
                type: index_js_3.WAProto.Message.ProtocolMessage.Type.MESSAGE_EDIT
            }
        };
    }
    if (hasOptionalProperty(message, 'contextInfo') && !!message.contextInfo) {
        const messageType = Object.keys(m)[0];
        const key = m[messageType];
        if ('contextInfo' in key && !!key.contextInfo) {
            key.contextInfo = { ...key.contextInfo, ...message.contextInfo };
        }
        else if (key) {
            key.contextInfo = message.contextInfo;
        }
    }
    if ((0, reporting_utils_js_1.shouldIncludeReportingToken)(m) && !options.newsletter) {
        m.messageContextInfo = m.messageContextInfo || {};
        if (!m.messageContextInfo.messageSecret) {
            m.messageContextInfo.messageSecret = (0, crypto_1.randomBytes)(32);
        }
    }
    return index_js_3.WAProto.Message.create(m);
};
exports.generateWAMessageContent = generateWAMessageContent;
const generateWAMessageFromContent = (jid, message, options) => {
    // set timestamp to now
    // if not specified
    if (!options.timestamp) {
        options.timestamp = new Date();
    }
    const innerMessage = (0, exports.normalizeMessageContent)(message);
    const key = (0, exports.getContentType)(innerMessage);
    const timestamp = (0, generics_js_1.unixTimestampSeconds)(options.timestamp);
    const { quoted, userJid } = options;
    if (quoted && !(0, index_js_4.isJidNewsletter)(jid)) {
        const participant = quoted.key.fromMe
            ? userJid // TODO: Add support for LIDs
            : quoted.participant || quoted.key.participant || quoted.key.remoteJid;
        let quotedMsg = (0, exports.normalizeMessageContent)(quoted.message);
        const msgType = (0, exports.getContentType)(quotedMsg);
        // strip any redundant properties
        quotedMsg = index_js_1.proto.Message.create({ [msgType]: quotedMsg[msgType] });
        const quotedContent = quotedMsg[msgType];
        if (typeof quotedContent === 'object' && quotedContent && 'contextInfo' in quotedContent) {
            delete quotedContent.contextInfo;
        }
        const contextInfo = ('contextInfo' in innerMessage[key] && innerMessage[key]?.contextInfo) || {};
        contextInfo.participant = (0, index_js_4.jidNormalizedUser)(participant);
        contextInfo.stanzaId = quoted.key.id;
        contextInfo.quotedMessage = quotedMsg;
        // if a participant is quoted, then it must be a group
        // hence, remoteJid of group must also be entered
        if (jid !== quoted.key.remoteJid) {
            contextInfo.remoteJid = quoted.key.remoteJid;
        }
        if (contextInfo && innerMessage[key]) {
            /* @ts-ignore */
            innerMessage[key].contextInfo = contextInfo;
        }
    }
    if (
    // if we want to send a disappearing message
    !!options?.ephemeralExpiration &&
        // and it's not a protocol message -- delete, toggle disappear message
        key !== 'protocolMessage' &&
        // already not converted to disappearing message
        key !== 'ephemeralMessage' &&
        // newsletters don't support ephemeral messages
        !(0, index_js_4.isJidNewsletter)(jid)) {
        /* @ts-ignore */
        innerMessage[key].contextInfo = {
            ...(innerMessage[key].contextInfo || {}),
            expiration: options.ephemeralExpiration || index_js_2.WA_DEFAULT_EPHEMERAL
            //ephemeralSettingTimestamp: options.ephemeralOptions.eph_setting_ts?.toString()
        };
    }
    message = index_js_3.WAProto.Message.create(message);
    const messageJSON = {
        key: {
            remoteJid: jid,
            fromMe: true,
            id: options?.messageId || (0, generics_js_1.generateMessageIDV2)()
        },
        message: message,
        messageTimestamp: timestamp,
        messageStubParameters: [],
        participant: (0, index_js_4.isJidGroup)(jid) || (0, index_js_4.isJidStatusBroadcast)(jid) ? userJid : undefined, // TODO: Add support for LIDs
        status: index_js_3.WAMessageStatus.PENDING
    };
    return index_js_3.WAProto.WebMessageInfo.fromObject(messageJSON);
};
exports.generateWAMessageFromContent = generateWAMessageFromContent;
const generateWAMessage = async (jid, content, options) => {
    // ensure msg ID is with every log
    options.logger = options?.logger?.child({ msgId: options.messageId });
    // Pass jid + newsletter flag to generateWAMessageContent (astrabail patch)
    const _isNewsletter = typeof jid === 'string' && jid.endsWith('@newsletter');
    return (0, exports.generateWAMessageFromContent)(jid, await (0, exports.generateWAMessageContent)(content, { newsletter: _isNewsletter, ...options, jid }), options);
};
exports.generateWAMessage = generateWAMessage;
/** Get the key to access the true type of content */
const getContentType = (content) => {
    if (content) {
        const keys = Object.keys(content);
        const key = keys.find(k => (k === 'conversation' || k.includes('Message')) && k !== 'senderKeyDistributionMessage');
        return key;
    }
};
exports.getContentType = getContentType;
/**
 * Maps a message content type key to its MediaType string.
 * Handles ptvMessage → 'ptv', audioMessage ptt → 'ptt', etc.
 */
const getMediaTypeFromContentType = (contentType, content) => {
    if (!contentType)
        return undefined;
    if (contentType === 'ptvMessage')
        return 'ptv';
    if (contentType === 'audioMessage' && content?.[contentType]?.ptt)
        return 'ptt';
    if (contentType === 'videoMessage' && content?.[contentType]?.gifPlayback)
        return 'gif';
    return contentType.replace('Message', '');
};
exports.getMediaTypeFromContentType = getMediaTypeFromContentType;
/**
 * Normalizes ephemeral, view once messages to regular message content
 * Eg. image messages in ephemeral messages, in view once messages etc.
 * @param content
 * @returns
 */
const normalizeMessageContent = (content) => {
    if (!content) {
        return undefined;
    }
    // set max iterations to prevent an infinite loop
    for (let i = 0; i < 5; i++) {
        const inner = getFutureProofMessage(content);
        if (!inner) {
            break;
        }
        content = inner.message;
    }
    return content;
    function getFutureProofMessage(message) {
        return (message?.ephemeralMessage ||
            message?.viewOnceMessage ||
            message?.documentWithCaptionMessage ||
            message?.viewOnceMessageV2 ||
            message?.viewOnceMessageV2Extension ||
            message?.editedMessage ||
            message?.associatedChildMessage ||
            message?.groupStatusMessage ||
            message?.groupStatusMessageV2);
    }
};
exports.normalizeMessageContent = normalizeMessageContent;
/**
 * Extract the true message content from a message
 * Eg. extracts the inner message from a disappearing message/view once message
 */
const extractMessageContent = (content) => {
    const extractFromTemplateMessage = (msg) => {
        if (msg.imageMessage) {
            return { imageMessage: msg.imageMessage };
        }
        else if (msg.documentMessage) {
            return { documentMessage: msg.documentMessage };
        }
        else if (msg.videoMessage) {
            return { videoMessage: msg.videoMessage };
        }
        else if (msg.locationMessage) {
            return { locationMessage: msg.locationMessage };
        }
        else {
            return {
                conversation: 'contentText' in msg ? msg.contentText : 'hydratedContentText' in msg ? msg.hydratedContentText : ''
            };
        }
    };
    content = (0, exports.normalizeMessageContent)(content);
    if (content?.buttonsMessage) {
        return extractFromTemplateMessage(content.buttonsMessage);
    }
    if (content?.templateMessage?.hydratedFourRowTemplate) {
        return extractFromTemplateMessage(content?.templateMessage?.hydratedFourRowTemplate);
    }
    if (content?.templateMessage?.hydratedTemplate) {
        return extractFromTemplateMessage(content?.templateMessage?.hydratedTemplate);
    }
    if (content?.templateMessage?.fourRowTemplate) {
        return extractFromTemplateMessage(content?.templateMessage?.fourRowTemplate);
    }
    return content;
};
exports.extractMessageContent = extractMessageContent;
/**
 * Returns the device predicted by message ID
 */
const getDevice = (id) => /^3A.{18}$/.test(id)
    ? 'ios'
    : /^3E.{20}$/.test(id)
        ? 'web'
        : /^(.{21}|.{32})$/.test(id)
            ? 'android'
            : /^(3F|.{18}$)/.test(id)
                ? 'desktop'
                : 'unknown';
exports.getDevice = getDevice;
/** Upserts a receipt in the message */
const updateMessageWithReceipt = (msg, receipt) => {
    msg.userReceipt = msg.userReceipt || [];
    const recp = msg.userReceipt.find(m => m.userJid === receipt.userJid);
    if (recp) {
        Object.assign(recp, receipt);
    }
    else {
        msg.userReceipt.push(receipt);
    }
};
exports.updateMessageWithReceipt = updateMessageWithReceipt;
/** Update the message with a new reaction */
const updateMessageWithReaction = (msg, reaction) => {
    const authorID = (0, generics_js_1.getKeyAuthor)(reaction.key);
    const reactions = (msg.reactions || []).filter(r => (0, generics_js_1.getKeyAuthor)(r.key) !== authorID);
    reaction.text = reaction.text || '';
    reactions.push(reaction);
    msg.reactions = reactions;
};
exports.updateMessageWithReaction = updateMessageWithReaction;
/** Update the message with a new poll update */
const updateMessageWithPollUpdate = (msg, update) => {
    const authorID = (0, generics_js_1.getKeyAuthor)(update.pollUpdateMessageKey);
    const reactions = (msg.pollUpdates || []).filter(r => (0, generics_js_1.getKeyAuthor)(r.pollUpdateMessageKey) !== authorID);
    if (update.vote?.selectedOptions?.length) {
        reactions.push(update);
    }
    msg.pollUpdates = reactions;
};
exports.updateMessageWithPollUpdate = updateMessageWithPollUpdate;
/** Update the message with a new event response */
const updateMessageWithEventResponse = (msg, update) => {
    const authorID = (0, generics_js_1.getKeyAuthor)(update.eventResponseMessageKey);
    const responses = (msg.eventResponses || []).filter(r => (0, generics_js_1.getKeyAuthor)(r.eventResponseMessageKey) !== authorID);
    responses.push(update);
    msg.eventResponses = responses;
};
exports.updateMessageWithEventResponse = updateMessageWithEventResponse;
/**
 * Aggregates all poll updates in a poll.
 * @param msg the poll creation message
 * @param meId your jid
 * @returns A list of options & their voters
 */
function getAggregateVotesInPollMessage({ message, pollUpdates }, meId) {
    const opts = message?.pollCreationMessage?.options ||
        message?.pollCreationMessageV2?.options ||
        message?.pollCreationMessageV3?.options ||
        [];
    const voteHashMap = opts.reduce((acc, opt) => {
        const hash = (0, crypto_js_1.sha256)(Buffer.from(opt.optionName || '')).toString();
        acc[hash] = {
            name: opt.optionName || '',
            voters: []
        };
        return acc;
    }, {});
    for (const update of pollUpdates || []) {
        const { vote } = update;
        if (!vote) {
            continue;
        }
        for (const option of vote.selectedOptions || []) {
            const hash = option.toString();
            let data = voteHashMap[hash];
            if (!data) {
                voteHashMap[hash] = {
                    name: 'Unknown',
                    voters: []
                };
                data = voteHashMap[hash];
            }
            voteHashMap[hash].voters.push((0, generics_js_1.getKeyAuthor)(update.pollUpdateMessageKey, meId));
        }
    }
    return Object.values(voteHashMap);
}
/**
 * Aggregates all event responses in an event message.
 * @param msg the event creation message
 * @param meId your jid
 * @returns A list of response types & their responders
 */
function getAggregateResponsesInEventMessage({ eventResponses }, meId) {
    const responseTypes = ['GOING', 'NOT_GOING', 'MAYBE'];
    const responseMap = {};
    for (const type of responseTypes) {
        responseMap[type] = {
            response: type,
            responders: []
        };
    }
    for (const update of eventResponses || []) {
        const responseType = update.eventResponse || 'UNKNOWN';
        if (responseType !== 'UNKNOWN' && responseMap[responseType]) {
            responseMap[responseType].responders.push((0, generics_js_1.getKeyAuthor)(update.eventResponseMessageKey, meId));
        }
    }
    return Object.values(responseMap);
}
/** Given a list of message keys, aggregates them by chat & sender. Useful for sending read receipts in bulk */
const aggregateMessageKeysNotFromMe = (keys) => {
    const keyMap = {};
    for (const { remoteJid, id, participant, fromMe } of keys) {
        if (!fromMe) {
            const uqKey = `${remoteJid}:${participant || ''}`;
            if (!keyMap[uqKey]) {
                keyMap[uqKey] = {
                    jid: remoteJid,
                    participant: participant,
                    messageIds: []
                };
            }
            keyMap[uqKey].messageIds.push(id);
        }
    }
    return Object.values(keyMap);
};
exports.aggregateMessageKeysNotFromMe = aggregateMessageKeysNotFromMe;
const REUPLOAD_REQUIRED_STATUS = [410, 404];
/**
 * Downloads the given message. Throws an error if it's not a media message
 */
const downloadMediaMessage = async (message, type, options, ctx) => {
    const result = await downloadMsg().catch(async (error) => {
        if (ctx &&
            typeof error?.status === 'number' && // treat errors with status as HTTP failures requiring reupload
            REUPLOAD_REQUIRED_STATUS.includes(error.status)) {
            ctx.logger.info({ key: message.key }, 'sending reupload media request...');
            // request reupload
            message = await ctx.reuploadRequest(message);
            const result = await downloadMsg();
            return result;
        }
        throw error;
    });
    return result;
    async function downloadMsg() {
        const mContent = (0, exports.extractMessageContent)(message.message);
        if (!mContent) {
            throw new boom_1.Boom('No message present', { statusCode: 400, data: message });
        }
        const contentType = (0, exports.getContentType)(mContent);
        let mediaType = (0, exports.getMediaTypeFromContentType)(contentType, mContent);
        const media = mContent[contentType];
        if (!media || typeof media !== 'object' || (!('url' in media) && !('thumbnailDirectPath' in media))) {
            throw new boom_1.Boom(`"${contentType}" message is not a media message`);
        }
        let download;
        if ('thumbnailDirectPath' in media && !('url' in media)) {
            download = {
                directPath: media.thumbnailDirectPath,
                mediaKey: media.mediaKey
            };
            mediaType = 'thumbnail-link';
        }
        else {
            download = media;
        }
        const stream = await (0, messages_media_js_1.downloadContentFromMessage)(download, mediaType, options);
        if (type === 'buffer') {
            const bufferArray = [];
            for await (const chunk of stream) {
                bufferArray.push(chunk);
            }
            return Buffer.concat(bufferArray);
        }
        return stream;
    }
};
exports.downloadMediaMessage = downloadMediaMessage;
/** Checks whether the given message is a media message; if it is returns the inner content */
const assertMediaContent = (content) => {
    content = (0, exports.extractMessageContent)(content);
    const mediaContent = content?.documentMessage ||
        content?.imageMessage ||
        content?.videoMessage ||
        content?.audioMessage ||
        content?.stickerMessage;
    if (!mediaContent) {
        throw new boom_1.Boom('given message is not a media message', { statusCode: 400, data: content });
    }
    return mediaContent;
};
exports.assertMediaContent = assertMediaContent;
const patchMessageForMdIfRequired = (message) => {
    if (message?.buttonsMessage ||
        message?.templateMessage ||
        message?.listMessage ||
        message?.interactiveMessage?.nativeFlowMessage) {
        message = {
            viewOnceMessageV2Extension: {
                message: {
                    messageContextInfo: {
                        deviceListMetadataVersion: 2,
                        deviceListMetadata: {}
                    },
                    ...message
                }
            }
        };
    }
    return message;
};
exports.patchMessageForMdIfRequired = patchMessageForMdIfRequired;
const prepareAlbumMessageContent = async (jid, albums, options) => {
    let mediaHandle;
    let mediaMsg;
    const message = [];
    const albumMsg = (0, exports.generateWAMessageFromContent)(jid, {
        albumMessage: {
            expectedImageCount: albums.filter(item => 'image' in item).length,
            expectedVideoCount: albums.filter(item => 'video' in item).length
        }
    }, options);
    await options.sock.relayMessage(jid, albumMsg.message, { messageId: albumMsg.key.id });
    for (const i in albums) {
        const media = albums[i];
        if ('image' in media) {
            mediaMsg = await (0, exports.generateWAMessage)(jid, { image: media.image, ...media, ...options }, {
                userJid: options.userJid,
                upload: async (encFilePath, opts) => {
                    const up = await options.sock.waUploadToServer(encFilePath, { ...opts, newsletter: (0, index_js_4.isJidNewsletter)(jid) });
                    mediaHandle = up.handle;
                    return up;
                },
                ...options
            });
        }
        else if ('video' in media) {
            mediaMsg = await (0, exports.generateWAMessage)(jid, { video: media.video, ...media, ...options }, {
                userJid: options.userJid,
                upload: async (encFilePath, opts) => {
                    const up = await options.sock.waUploadToServer(encFilePath, { ...opts, newsletter: (0, index_js_4.isJidNewsletter)(jid) });
                    mediaHandle = up.handle;
                    return up;
                },
                ...options
            });
        }
        if (mediaMsg) {
            mediaMsg.message.messageContextInfo = {
                messageSecret: (0, crypto_1.randomBytes)(32),
                messageAssociation: {
                    associationType: 1,
                    parentMessageKey: albumMsg.key
                }
            };
        }
        message.push(mediaMsg);
    }
    return message;
};
exports.prepareAlbumMessageContent = prepareAlbumMessageContent;
/**
 * Ekstrak quoted message dari contextInfo pesan.
 * Return object yang sudah dinormalisasi, termasuk debug info LID/PN.
 *
 * @param {object} msg - WAMessage object dari event messages.upsert
 * @param {object} [options]
 * @param {boolean} [options.debug] - jika true, log info ke console
 * @returns {object|null} quoted message info atau null jika tidak ada
 */
const getQuotedMsg = (msg, options = {}) => {
    const { debug = false } = options;
    const msgContent = (0, exports.normalizeMessageContent)(msg?.message);
    if (!msgContent)
        return null;
    const msgType = (0, exports.getContentType)(msgContent);
    if (!msgType)
        return null;
    const innerMsg = msgContent[msgType];
    if (!innerMsg)
        return null;
    const contextInfo = innerMsg.contextInfo;
    if (!contextInfo?.stanzaId || !contextInfo?.quotedMessage)
        return null;
    // Normalisasi participant (sender quoted message)
    const participant = contextInfo.participant || msg.key?.participant || msg.key?.remoteJid || '';
    // Normalisasi mentionedJid di quoted message
    const quotedMsgContent = (0, exports.normalizeMessageContent)(contextInfo.quotedMessage);
    const quotedMsgType = (0, exports.getContentType)(quotedMsgContent);
    const quotedInnerMsg = quotedMsgType ? quotedMsgContent?.[quotedMsgType] : null;
    const quotedMentionedJid = quotedInnerMsg?.contextInfo?.mentionedJid || [];
    const quotedText = quotedInnerMsg?.text || quotedInnerMsg?.caption || quotedInnerMsg?.conversation || '';
    const result = {
        key: {
            id: contextInfo.stanzaId,
            remoteJid: contextInfo.remoteJid || msg.key?.remoteJid || '',
            participant: participant,
            fromMe: false
        },
        message: contextInfo.quotedMessage,
        participant,
        sender: participant,
        text: quotedText,
        type: quotedMsgType || '',
        mentionedJid: quotedMentionedJid,
        // Debug info
        _rawContextInfo: debug ? contextInfo : undefined
    };
    if (debug) {
        const hasLidInText = typeof quotedText === 'string' && /@\d{13,20}/.test(quotedText);
        const hasLidInMentioned = quotedMentionedJid.some(j => j?.endsWith('@lid'));
        const hasLidParticipant = participant?.endsWith('@lid');
        console.log('[getQuotedMsg DEBUG]', JSON.stringify({
            stanzaId: contextInfo.stanzaId,
            participant,
            participantIsLid: hasLidParticipant,
            quotedText,
            textHasLid: hasLidInText,
            mentionedJid: quotedMentionedJid,
            mentionedHasLid: hasLidInMentioned,
            quotedMsgType
        }, null, 2));
    }
    return result;
};
exports.getQuotedMsg = getQuotedMsg;



