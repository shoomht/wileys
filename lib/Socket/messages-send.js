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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.makeMessagesSocket = void 0;
const node_cache_1 = __importDefault(require("@cacheable/node-cache"));
const boom_1 = require("@hapi/boom");
const crypto_1 = require("crypto");
const index_js_1 = require("../../WAProto/index.js");
const index_js_2 = require("../Defaults/index.js");
const index_js_3 = require("../Utils/index.js");
const link_preview_js_1 = require("../Utils/link-preview.js");
const make_mutex_js_1 = require("../Utils/make-mutex.js");
const reporting_utils_js_1 = require("../Utils/reporting-utils.js");
const index_js_4 = require("../WABinary/index.js");
const generics_js_1 = require("../Utils/generics.js");
const index_js_5 = require("../WAUSync/index.js");
const newsletter_js_1 = require("./newsletter.js");
// Inline helper — no external import needed
const _isNewsletterJid = (jid) => typeof jid === 'string' && jid.endsWith('@newsletter');
const makeMessagesSocket = (config) => {
    const { logger, linkPreviewImageThumbnailWidth, generateHighQualityLinkPreview, options: httpRequestOptions, patchMessageBeforeSending, cachedGroupMetadata, enableRecentMessageCache, maxMsgRetryCount } = config;
    const sock = (0, newsletter_js_1.makeNewsletterSocket)(config);
    const { ev, authState, messageMutex, signalRepository, upsertMessage, query, fetchPrivacySettings, sendNode, groupMetadata, groupToggleEphemeral } = sock;
    const userDevicesCache = config.userDevicesCache ||
        new node_cache_1.default({
            stdTTL: index_js_2.DEFAULT_CACHE_TTLS.USER_DEVICES, // 5 minutes
            useClones: false
        });
    const peerSessionsCache = new node_cache_1.default({
        stdTTL: index_js_2.DEFAULT_CACHE_TTLS.USER_DEVICES,
        useClones: false
    });
    // Initialize message retry manager if enabled
    const messageRetryManager = enableRecentMessageCache ? new index_js_3.MessageRetryManager(logger, maxMsgRetryCount) : null;
    // Prevent race conditions in Signal session encryption by user
    const encryptionMutex = (0, make_mutex_js_1.makeKeyedMutex)();
    let mediaConn;
    const refreshMediaConn = async (forceGet = false) => {
        const media = await mediaConn;
        if (!media || forceGet || new Date().getTime() - media.fetchDate.getTime() > media.ttl * 1000) {
            mediaConn = (async () => {
                const result = await query({
                    tag: 'iq',
                    attrs: {
                        type: 'set',
                        xmlns: 'w:m',
                        to: index_js_4.S_WHATSAPP_NET
                    },
                    content: [{ tag: 'media_conn', attrs: {} }]
                });
                const mediaConnNode = (0, index_js_4.getBinaryNodeChild)(result, 'media_conn');
                // TODO: explore full length of data that whatsapp provides
                const node = {
                    hosts: (0, index_js_4.getBinaryNodeChildren)(mediaConnNode, 'host').map(({ attrs }) => ({
                        hostname: attrs.hostname,
                        maxContentLengthBytes: +attrs.maxContentLengthBytes
                    })),
                    auth: mediaConnNode.attrs.auth,
                    ttl: +mediaConnNode.attrs.ttl,
                    fetchDate: new Date()
                };
                logger.debug('fetched media conn');
                return node;
            })();
        }
        return mediaConn;
    };
    /**
     * generic send receipt function
     * used for receipts of phone call, read, delivery etc.
     * */
    const sendReceipt = async (jid, participant, messageIds, type) => {
        if (!messageIds || messageIds.length === 0) {
            throw new boom_1.Boom('missing ids in receipt');
        }
        const node = {
            tag: 'receipt',
            attrs: {
                id: messageIds[0]
            }
        };
        const isReadReceipt = type === 'read' || type === 'read-self';
        if (isReadReceipt) {
            node.attrs.t = (0, index_js_3.unixTimestampSeconds)().toString();
        }
        if (type === 'sender' && ((0, index_js_4.isPnUser)(jid) || (0, index_js_4.isLidUser)(jid))) {
            node.attrs.recipient = jid;
            node.attrs.to = participant;
        }
        else {
            node.attrs.to = jid;
            if (participant) {
                node.attrs.participant = participant;
            }
        }
        if (type) {
            node.attrs.type = type;
        }
        const remainingMessageIds = messageIds.slice(1);
        if (remainingMessageIds.length) {
            node.content = [
                {
                    tag: 'list',
                    attrs: {},
                    content: remainingMessageIds.map(id => ({
                        tag: 'item',
                        attrs: { id }
                    }))
                }
            ];
        }
        logger.debug({ attrs: node.attrs, messageIds }, 'sending receipt for messages');
        await sendNode(node);
    };
    /** Correctly bulk send receipts to multiple chats, participants */
    const sendReceipts = async (keys, type) => {
        const recps = (0, index_js_3.aggregateMessageKeysNotFromMe)(keys);
        for (const { jid, participant, messageIds } of recps) {
            await sendReceipt(jid, participant, messageIds, type);
        }
    };
    /** Bulk read messages. Keys can be from different chats & participants */
    const readMessages = async (keys) => {
        const privacySettings = await fetchPrivacySettings();
        // based on privacy settings, we have to change the read type
        const readType = privacySettings.readreceipts === 'all' ? 'read' : 'read-self';
        await sendReceipts(keys, readType);
    };
    /** Fetch all the devices we've to send a message to */
    const getUSyncDevices = async (jids, useCache, ignoreZeroDevices) => {
        const deviceResults = [];
        if (!useCache) {
            logger.debug('not using cache for devices');
        }
        const toFetch = [];
        const jidsWithUser = jids
            .map(jid => {
            const decoded = (0, index_js_4.jidDecode)(jid);
            const user = decoded?.user;
            const device = decoded?.device;
            const isExplicitDevice = typeof device === 'number' && device >= 0;
            if (isExplicitDevice && user) {
                deviceResults.push({
                    user,
                    device,
                    jid
                });
                return null;
            }
            jid = (0, index_js_4.jidNormalizedUser)(jid);
            return { jid, user };
        })
            .filter(jid => jid !== null);
        let mgetDevices;
        if (useCache && userDevicesCache.mget) {
            const usersToFetch = jidsWithUser.map(j => j?.user).filter(Boolean);
            mgetDevices = await userDevicesCache.mget(usersToFetch);
        }
        for (const { jid, user } of jidsWithUser) {
            if (useCache) {
                const devices = mgetDevices?.[user] ||
                    (userDevicesCache.mget ? undefined : (await userDevicesCache.get(user)));
                if (devices) {
                    const devicesWithJid = devices.map(d => ({
                        ...d,
                        jid: (0, index_js_4.jidEncode)(d.user, d.server, d.device)
                    }));
                    deviceResults.push(...devicesWithJid);
                    logger.trace({ user }, 'using cache for devices');
                }
                else {
                    toFetch.push(jid);
                }
            }
            else {
                toFetch.push(jid);
            }
        }
        if (!toFetch.length) {
            return deviceResults;
        }
        const requestedLidUsers = new Set();
        for (const jid of toFetch) {
            if ((0, index_js_4.isLidUser)(jid) || (0, index_js_4.isHostedLidUser)(jid)) {
                const user = (0, index_js_4.jidDecode)(jid)?.user;
                if (user)
                    requestedLidUsers.add(user);
            }
        }
        const query = new index_js_5.USyncQuery().withContext('message').withDeviceProtocol().withLIDProtocol();
        for (const jid of toFetch) {
            query.withUser(new index_js_5.USyncUser().withId(jid)); // todo: investigate - the idea here is that <user> should have an inline lid field with the lid being the pn equivalent
        }
        const result = await sock.executeUSyncQuery(query);
        if (result) {
            // TODO: LID MAP this stuff (lid protocol will now return lid with devices)
            const lidResults = result.list.filter(a => !!a.lid);
            if (lidResults.length > 0) {
                logger.trace('Storing LID maps from device call');
                await signalRepository.lidMapping.storeLIDPNMappings(lidResults.map(a => ({ lid: a.lid, pn: a.id })));
                // Force-refresh sessions for newly mapped LIDs to align identity addressing
                try {
                    const lids = lidResults.map(a => a.lid);
                    if (lids.length) {
                        await assertSessions(lids, true);
                    }
                }
                catch (e) {
                    logger.warn({ e, count: lidResults.length }, 'failed to assert sessions for newly mapped LIDs');
                }
            }
            const extracted = (0, index_js_3.extractDeviceJids)(result?.list, authState.creds.me.id, authState.creds.me.lid, ignoreZeroDevices);
            const deviceMap = {};
            for (const item of extracted) {
                deviceMap[item.user] = deviceMap[item.user] || [];
                deviceMap[item.user]?.push(item);
            }
            // Process each user's devices as a group for bulk LID migration
            for (const [user, userDevices] of Object.entries(deviceMap)) {
                const isLidUser = requestedLidUsers.has(user);
                // Process all devices for this user
                for (const item of userDevices) {
                    const finalJid = isLidUser
                        ? (0, index_js_4.jidEncode)(user, item.server, item.device)
                        : (0, index_js_4.jidEncode)(item.user, item.server, item.device);
                    deviceResults.push({
                        ...item,
                        jid: finalJid
                    });
                    logger.debug({
                        user: item.user,
                        device: item.device,
                        finalJid,
                        usedLid: isLidUser
                    }, 'Processed device with LID priority');
                }
            }
            if (userDevicesCache.mset) {
                // if the cache supports mset, we can set all devices in one go
                await userDevicesCache.mset(Object.entries(deviceMap).map(([key, value]) => ({ key, value })));
            }
            else {
                for (const key in deviceMap) {
                    if (deviceMap[key])
                        await userDevicesCache.set(key, deviceMap[key]);
                }
            }
            const userDeviceUpdates = {};
            for (const [userId, devices] of Object.entries(deviceMap)) {
                if (devices && devices.length > 0) {
                    userDeviceUpdates[userId] = devices.map(d => d.device?.toString() || '0');
                }
            }
            if (Object.keys(userDeviceUpdates).length > 0) {
                try {
                    await authState.keys.set({ 'device-list': userDeviceUpdates });
                    logger.debug({ userCount: Object.keys(userDeviceUpdates).length }, 'stored user device lists for bulk migration');
                }
                catch (error) {
                    logger.warn({ error }, 'failed to store user device lists');
                }
            }
        }
        return deviceResults;
    };
    /**
     * Update Member Label
     */
    const updateMemberLabel = (jid, memberLabel) => {
        return relayMessage(jid, {
            protocolMessage: {
                type: index_js_1.proto.Message.ProtocolMessage.Type.GROUP_MEMBER_LABEL_CHANGE,
                memberLabel: {
                    label: memberLabel?.slice(0, 30),
                    labelTimestamp: (0, index_js_3.unixTimestampSeconds)()
                }
            }
        }, {
            additionalNodes: [
                {
                    tag: 'meta',
                    attrs: {
                        tag_reason: 'user_update',
                        appdata: 'member_tag'
                    },
                    content: undefined
                }
            ]
        });
    };
    const assertSessions = async (jids, force) => {
        let didFetchNewSession = false;
        const uniqueJids = [...new Set(jids)]; // Deduplicate JIDs
        const jidsRequiringFetch = [];
        logger.debug({ jids }, 'assertSessions call with jids');
        // Check peerSessionsCache and validate sessions using libsignal loadSession
        for (const jid of uniqueJids) {
            const signalId = signalRepository.jidToSignalProtocolAddress(jid);
            const cachedSession = peerSessionsCache.get(signalId);
            if (cachedSession !== undefined) {
                if (cachedSession && !force) {
                    continue; // Session exists in cache
                }
            }
            else {
                const sessionValidation = await signalRepository.validateSession(jid);
                const hasSession = sessionValidation.exists;
                peerSessionsCache.set(signalId, hasSession);
                if (hasSession && !force) {
                    continue;
                }
            }
            jidsRequiringFetch.push(jid);
        }
        if (jidsRequiringFetch.length) {
            // LID if mapped, otherwise original
            const wireJids = [
                ...jidsRequiringFetch.filter(jid => !!(0, index_js_4.isLidUser)(jid) || !!(0, index_js_4.isHostedLidUser)(jid)),
                ...((await signalRepository.lidMapping.getLIDsForPNs(jidsRequiringFetch.filter(jid => !!(0, index_js_4.isPnUser)(jid) || !!(0, index_js_4.isHostedPnUser)(jid)))) || []).map(a => a.lid)
            ];
            logger.debug({ jidsRequiringFetch, wireJids }, 'fetching sessions');
            const result = await query({
                tag: 'iq',
                attrs: {
                    xmlns: 'encrypt',
                    type: 'get',
                    to: index_js_4.S_WHATSAPP_NET
                },
                content: [
                    {
                        tag: 'key',
                        attrs: {},
                        content: wireJids.map(jid => {
                            const attrs = { jid };
                            if (force)
                                attrs.reason = 'identity';
                            return { tag: 'user', attrs };
                        })
                    }
                ]
            });
            await (0, index_js_3.parseAndInjectE2ESessions)(result, signalRepository);
            didFetchNewSession = true;
            // Cache fetched sessions using wire JIDs
            for (const wireJid of wireJids) {
                const signalId = signalRepository.jidToSignalProtocolAddress(wireJid);
                peerSessionsCache.set(signalId, true);
            }
        }
        return didFetchNewSession;
    };
    const sendPeerDataOperationMessage = async (pdoMessage) => {
        //TODO: for later, abstract the logic to send a Peer Message instead of just PDO - useful for App State Key Resync with phone
        if (!authState.creds.me?.id) {
            throw new boom_1.Boom('Not authenticated');
        }
        const protocolMessage = {
            protocolMessage: {
                peerDataOperationRequestMessage: pdoMessage,
                type: index_js_1.proto.Message.ProtocolMessage.Type.PEER_DATA_OPERATION_REQUEST_MESSAGE
            }
        };
        const meJid = (0, index_js_4.jidNormalizedUser)(authState.creds.me.id);
        const msgId = await relayMessage(meJid, protocolMessage, {
            additionalAttributes: {
                category: 'peer',
                push_priority: 'high_force'
            },
            additionalNodes: [
                {
                    tag: 'meta',
                    attrs: { appdata: 'default' }
                }
            ]
        });
        return msgId;
    };
    const createParticipantNodes = async (recipientJids, message, extraAttrs, dsmMessage) => {
        if (!recipientJids.length) {
            return { nodes: [], shouldIncludeDeviceIdentity: false };
        }
        const patched = await patchMessageBeforeSending(message, recipientJids);
        const patchedMessages = Array.isArray(patched)
            ? patched
            : recipientJids.map(jid => ({ recipientJid: jid, message: patched }));
        let shouldIncludeDeviceIdentity = false;
        const meId = authState.creds.me.id;
        const meLid = authState.creds.me?.lid;
        const meLidUser = meLid ? (0, index_js_4.jidDecode)(meLid)?.user : null;
        const encryptionPromises = patchedMessages.map(async ({ recipientJid: jid, message: patchedMessage }) => {
            try {
                if (!jid)
                    return null;
                let msgToEncrypt = patchedMessage;
                if (dsmMessage) {
                    const { user: targetUser } = (0, index_js_4.jidDecode)(jid);
                    const { user: ownPnUser } = (0, index_js_4.jidDecode)(meId);
                    const ownLidUser = meLidUser;
                    const isOwnUser = targetUser === ownPnUser || (ownLidUser && targetUser === ownLidUser);
                    const isExactSenderDevice = jid === meId || (meLid && jid === meLid);
                    if (isOwnUser && !isExactSenderDevice) {
                        msgToEncrypt = dsmMessage;
                        logger.debug({ jid, targetUser }, 'Using DSM for own device');
                    }
                }
                const bytes = (0, index_js_3.encodeWAMessage)(msgToEncrypt);
                const mutexKey = jid;
                const node = await encryptionMutex.mutex(mutexKey, async () => {
                    const { type, ciphertext } = await signalRepository.encryptMessage({ jid, data: bytes });
                    if (type === 'pkmsg') {
                        shouldIncludeDeviceIdentity = true;
                    }
                    return {
                        tag: 'to',
                        attrs: { jid },
                        content: [
                            {
                                tag: 'enc',
                                attrs: { v: '2', type, ...(extraAttrs || {}) },
                                content: ciphertext
                            }
                        ]
                    };
                });
                return node;
            }
            catch (err) {
                logger.error({ jid, err }, 'Failed to encrypt for recipient');
                return null;
            }
        });
        const nodes = (await Promise.all(encryptionPromises)).filter(node => node !== null);
        if (recipientJids.length > 0 && nodes.length === 0) {
            throw new boom_1.Boom('All encryptions failed', { statusCode: 500 });
        }
        return { nodes, shouldIncludeDeviceIdentity };
    };
    const relayMessage = async (jid, message, { messageId: msgId, participant, additionalAttributes, additionalNodes, useUserDevicesCache, useCachedGroupMetadata, statusJidList, AI = false }) => {
        const meId = authState.creds.me.id;
        const meLid = authState.creds.me?.lid;
        const isRetryResend = Boolean(participant?.jid);
        let shouldIncludeDeviceIdentity = isRetryResend;
        const statusJid = 'status@broadcast';
        const { user, server } = (0, index_js_4.jidDecode)(jid);
        const isGroup = server === 'g.us';
        const isStatus = jid === statusJid;
        const isLid = server === 'lid';
        const isNewsletter = server === 'newsletter';
        const isGroupOrStatus = isGroup || isStatus;
        const finalJid = jid;
        msgId = msgId || (0, index_js_3.generateMessageIDV2)(meId);
        useUserDevicesCache = useUserDevicesCache !== false;
        useCachedGroupMetadata = useCachedGroupMetadata !== false && !isStatus;
        const participants = [];
        const destinationJid = !isStatus ? finalJid : statusJid;
        const binaryNodeContent = [];
        const devices = [];
        let reportingMessage;
        const meMsg = {
            deviceSentMessage: {
                destinationJid,
                message
            },
            messageContextInfo: message.messageContextInfo
        };
        const extraAttrs = {};
        if ((message?.interactiveMessage || message?.buttonsMessage || message?.listMessage || message?.templateMessage)
            && !additionalNodes?.some(node => node?.tag === 'biz')) {
            const btnNode = getButtonArgs(message);
            if (btnNode) {
                additionalNodes = [...(additionalNodes || []), btnNode];
            }
        }
        if (participant) {
            if (!isGroup && !isStatus) {
                additionalAttributes = { ...additionalAttributes, device_fanout: 'false' };
            }
            const { user, device } = (0, index_js_4.jidDecode)(participant.jid);
            devices.push({
                user,
                device,
                jid: participant.jid
            });
        }
        await authState.keys.transaction(async () => {
            const mediaType = getMediaType(message);
            if (mediaType) {
                extraAttrs['mediatype'] = mediaType;
            }
            if (isNewsletter) {
                // Handle edit
                if (message.protocolMessage?.editedMessage) {
                    msgId = message.protocolMessage.key?.id;
                    message = message.protocolMessage.editedMessage;
                }
                // Handle delete/revoke
                if (message.protocolMessage?.type === index_js_1.proto.Message.ProtocolMessage.Type.REVOKE) {
                    msgId = message.protocolMessage.key?.id;
                    message = {};
                }
                // ── Newsletter Button Compatibility Patch ──────────────────────
                // interactiveMessage (quick_reply / single_select / cta_url) bisa
                // dikirim ke newsletter langsung — WA menerima via proto encoding.
                // listMessage & buttonsMessage dikonversi ke interactiveMessage
                // supaya konsisten dengan cara bot menulis pesan.
                // ──────────────────────────────────────────────────────────────
                if (message.listMessage) {
                    const list = message.listMessage;
                    message = {
                        interactiveMessage: {
                            nativeFlowMessage: {
                                buttons: [{
                                        name: 'single_select',
                                        buttonParamsJson: JSON.stringify({
                                            title: list.buttonText || 'Select',
                                            sections: (list.sections || []).map(sec => ({
                                                title: sec.title || '',
                                                highlight_label: '',
                                                rows: (sec.rows || []).map(row => ({
                                                    header: '',
                                                    title: row.title || '',
                                                    description: row.description || '',
                                                    id: row.rowId || row.id || ''
                                                }))
                                            }))
                                        })
                                    }],
                                messageParamsJson: '',
                                messageVersion: 1
                            },
                            body: { text: list.description || '' },
                            ...(list.footerText ? { footer: { text: list.footerText } } : {}),
                            ...(list.title ? { header: { title: list.title, hasMediaAttachment: false, subtitle: '' } } : {})
                        }
                    };
                }
                else if (message.buttonsMessage) {
                    const bMsg = message.buttonsMessage;
                    message = {
                        interactiveMessage: {
                            nativeFlowMessage: {
                                buttons: (bMsg.buttons || []).map(btn => ({
                                    name: 'quick_reply',
                                    buttonParamsJson: JSON.stringify({
                                        display_text: btn.buttonText?.displayText || btn.buttonText || '',
                                        id: btn.buttonId || btn.buttonText?.displayText || ''
                                    })
                                })),
                                messageParamsJson: '',
                                messageVersion: 1
                            },
                            body: { text: bMsg.contentText || bMsg.text || '' },
                            ...(bMsg.footerText ? { footer: { text: bMsg.footerText } } : {}),
                        }
                    };
                }
                // ── End Newsletter Button Compatibility Patch ──────────────────
                const patched = patchMessageBeforeSending ? await patchMessageBeforeSending(message, []) : message;
                if (Array.isArray(patched)) {
                    throw new Error('Per-jid patching is not supported in channel');
                }
                const bytes = (0, index_js_3.encodeNewsletterMessage)(patched);
                // Set mediatype for interactive messages
                if (patched.interactiveMessage && !extraAttrs['mediatype']) {
                    extraAttrs['mediatype'] = 'interactive';
                }
                // extraAttrs already has mediatype set above if media message
                binaryNodeContent.push({
                    tag: 'plaintext',
                    attrs: extraAttrs,
                    content: bytes
                });
                logger.debug({ msgId, extraAttrs }, `sending newsletter message to ${jid}`);
                const stanza = {
                    tag: 'message',
                    attrs: {
                        to: jid,
                        id: msgId,
                        type: getMessageType(message),
                        ...(additionalAttributes || {})
                    },
                    content: binaryNodeContent
                };
                await sendNode(stanza);
                return;
            }
            if ((0, index_js_3.normalizeMessageContent)(message)?.pinInChatMessage || (0, index_js_3.normalizeMessageContent)(message)?.reactionMessage) {
                extraAttrs['decrypt-fail'] = 'hide'; // todo: expand for reactions and other types
            }
            if (isGroupOrStatus && !isRetryResend) {
                const [groupData, senderKeyMap] = await Promise.all([
                    (async () => {
                        let groupData = useCachedGroupMetadata && cachedGroupMetadata ? await cachedGroupMetadata(jid) : undefined; // todo: should we rely on the cache specially if the cache is outdated and the metadata has new fields?
                        if (groupData && Array.isArray(groupData?.participants)) {
                            logger.trace({ jid, participants: groupData.participants.length }, 'using cached group metadata');
                        }
                        else if (!isStatus) {
                            groupData = await groupMetadata(jid); // TODO: start storing group participant list + addr mode in Signal & stop relying on this
                        }
                        return groupData;
                    })(),
                    (async () => {
                        if (!participant && !isStatus) {
                            // what if sender memory is less accurate than the cached metadata
                            // on participant change in group, we should do sender memory manipulation
                            const result = await authState.keys.get('sender-key-memory', [jid]); // TODO: check out what if the sender key memory doesn't include the LID stuff now?
                            return result[jid] || {};
                        }
                        return {};
                    })()
                ]);
                const participantsList = groupData ? groupData.participants.map(p => p.id) : [];
                if (groupData?.ephemeralDuration && groupData.ephemeralDuration > 0) {
                    additionalAttributes = {
                        ...additionalAttributes,
                        expiration: groupData.ephemeralDuration.toString()
                    };
                }
                if (isStatus && statusJidList) {
                    participantsList.push(...statusJidList);
                }
                const additionalDevices = await getUSyncDevices(participantsList, !!useUserDevicesCache, false);
                devices.push(...additionalDevices);
                if (isGroup) {
                    // ── FIX: Adaptive Addressing Mode ────────────────────────────────────
                    // Hanya gunakan 'lid' jika bot BENAR-BENAR sudah terdaftar sebagai
                    // LID user. Jika tidak, fallback ke 'pn' agar pesan tidak stuck
                    // di grup yang belum sepenuhnya migrasi ke sistem LID WhatsApp.
                    const botHasLid = !!(meLid && meLid !== meId);
                    additionalAttributes = {
                        ...additionalAttributes,
                        addressing_mode: groupData?.addressingMode || (botHasLid ? 'lid' : 'pn')
                    };
                    // ── End Adaptive Addressing Mode ─────────────────────────────────────
                }
                // Ensure groupStatusMessageV2 always has messageContextInfo.messageSecret
                if (message?.groupStatusMessageV2 && !message?.messageContextInfo?.messageSecret) {
                    const { randomBytes } = await Promise.resolve().then(() => __importStar(require('node:crypto')));
                    message = {
                        ...message,
                        messageContextInfo: {
                            ...(message.messageContextInfo || {}),
                            messageSecret: randomBytes(32)
                        },
                        groupStatusMessageV2: {
                            ...message.groupStatusMessageV2,
                            message: {
                                ...(message.groupStatusMessageV2.message || {}),
                                messageContextInfo: {
                                    ...(message.groupStatusMessageV2.message?.messageContextInfo || {}),
                                    messageSecret: message.messageContextInfo?.messageSecret || randomBytes(32)
                                }
                            }
                        }
                    };
                }
                // ── Group Interactive Message Compatibility Patch ──────────────
                // WhatsApp groups do NOT render listMessage, buttonsMessage, or
                // templateMessage natively in MD protocol. All must be converted
                // to interactiveMessage with nativeFlowMessage so they display
                // and are interactive in group chats.
                // ──────────────────────────────────────────────────────────────
                // FIX: ViewOnce Guard — jika pesan sudah di-wrap viewOnce oleh
                // user (misal: relayMessage langsung), ekstrak inner message dulu
                // agar konversi bisa berjalan dengan benar.
                const _voKey = message.viewOnceMessage ? 'viewOnceMessage'
                    : message.viewOnceMessageV2 ? 'viewOnceMessageV2'
                    : message.viewOnceMessageV2Extension ? 'viewOnceMessageV2Extension'
                    : null;
                if (_voKey) {
                    const _inner = message[_voKey]?.message;
                    if (_inner && (_inner.listMessage || _inner.buttonsMessage || _inner.templateMessage)) {
                        message = _inner; // unwrap untuk dikonversi, nanti patchMessageBeforeSending re-wrap
                    }
                }
                // 1. listMessage → interactiveMessage single_select
                if (message.listMessage) {
                    const list = message.listMessage;
                    const interactiveMessage = {
                        nativeFlowMessage: {
                            buttons: [{
                                    name: 'single_select',
                                    buttonParamsJson: JSON.stringify({
                                        title: list.buttonText || 'Select',
                                        sections: (list.sections || []).map(section => ({
                                            title: section.title || '',
                                            highlight_label: '',
                                            rows: (section.rows || []).map(row => ({
                                                header: '',
                                                title: row.title || '',
                                                description: row.description || '',
                                                id: row.rowId || row.id || ''
                                            }))
                                        }))
                                    })
                                }],
                            messageParamsJson: '',
                            messageVersion: 1
                        },
                        body: { text: list.description || '' },
                        footer: list.footerText ? { text: list.footerText } : undefined,
                        header: list.title ? { title: list.title, hasMediaAttachment: false, subtitle: '' } : undefined,
                        contextInfo: list.contextInfo
                    };
                    message = { interactiveMessage };
                }
                // 2. buttonsMessage → interactiveMessage nativeFlowMessage quick_reply buttons
                else if (message.buttonsMessage) {
                    const bMsg = message.buttonsMessage;
                    const buttons = (bMsg.buttons || []).map(btn => ({
                        name: 'quick_reply',
                        buttonParamsJson: JSON.stringify({
                            display_text: btn.buttonText?.displayText || btn.buttonText || '',
                            id: btn.buttonId || btn.buttonText?.displayText || ''
                        })
                    }));
                    const interactiveMessage = {
                        nativeFlowMessage: {
                            buttons,
                            messageParamsJson: '',
                            messageVersion: 1
                        },
                        body: { text: bMsg.contentText || bMsg.text || '' },
                        footer: bMsg.footerText ? { text: bMsg.footerText } : undefined,
                        header: bMsg.text
                            ? { title: bMsg.text, hasMediaAttachment: false, subtitle: '' }
                            : (bMsg.imageMessage || bMsg.videoMessage || bMsg.documentMessage
                                ? { hasMediaAttachment: true, ...(bMsg.imageMessage ? { imageMessage: bMsg.imageMessage } : {}), ...(bMsg.videoMessage ? { videoMessage: bMsg.videoMessage } : {}) }
                                : undefined),
                        contextInfo: bMsg.contextInfo
                    };
                    message = { interactiveMessage };
                }
                // 3. templateMessage (hydratedFourRowTemplate) → interactiveMessage nativeFlowMessage
                else if (message.templateMessage) {
                    const tmpl = message.templateMessage.hydratedTemplate || message.templateMessage.fourRowTemplate;
                    if (tmpl) {
                        const hydratedButtons = tmpl.hydratedButtons || [];
                        const buttons = hydratedButtons.map(hBtn => {
                            if (hBtn.quickReplyButton) {
                                return {
                                    name: 'quick_reply',
                                    buttonParamsJson: JSON.stringify({
                                        display_text: hBtn.quickReplyButton.displayText || '',
                                        id: hBtn.quickReplyButton.id || hBtn.quickReplyButton.displayText || ''
                                    })
                                };
                            }
                            else if (hBtn.urlButton) {
                                return {
                                    name: 'cta_url',
                                    buttonParamsJson: JSON.stringify({
                                        display_text: hBtn.urlButton.displayText || '',
                                        url: hBtn.urlButton.url || '',
                                        merchant_url: hBtn.urlButton.url || ''
                                    })
                                };
                            }
                            else if (hBtn.callButton) {
                                return {
                                    name: 'cta_call',
                                    buttonParamsJson: JSON.stringify({
                                        display_text: hBtn.callButton.displayText || '',
                                        phone_number: hBtn.callButton.phoneNumber || ''
                                    })
                                };
                            }
                            return null;
                        }).filter(Boolean);
                        const interactiveMessage = {
                            nativeFlowMessage: {
                                buttons,
                                messageParamsJson: '',
                                messageVersion: 1
                            },
                            body: { text: tmpl.hydratedContentText || tmpl.contentText || '' },
                            footer: tmpl.hydratedFooterText ? { text: tmpl.hydratedFooterText } : undefined,
                            header: tmpl.hydratedTitleText
                                ? { title: tmpl.hydratedTitleText, hasMediaAttachment: false, subtitle: '' }
                                : (tmpl.imageMessage || tmpl.videoMessage || tmpl.documentMessage
                                    ? { hasMediaAttachment: true, ...(tmpl.imageMessage ? { imageMessage: tmpl.imageMessage } : {}), ...(tmpl.videoMessage ? { videoMessage: tmpl.videoMessage } : {}) }
                                    : undefined),
                            contextInfo: tmpl.contextInfo
                        };
                        message = { interactiveMessage };
                    }
                }
                // ── End Group Interactive Message Compatibility Patch ──────────
                const patched = await patchMessageBeforeSending(message);
                if (Array.isArray(patched)) {
                    throw new boom_1.Boom('Per-jid patching is not supported in groups');
                }
                const bytes = (0, index_js_3.encodeWAMessage)(patched);
                reportingMessage = patched;
                const groupAddressingMode = additionalAttributes?.['addressing_mode'] || groupData?.addressingMode || 'lid';
                const groupSenderIdentity = groupAddressingMode === 'lid' && meLid ? meLid : meId;
                const { ciphertext, senderKeyDistributionMessage } = await signalRepository.encryptGroupMessage({
                    group: destinationJid,
                    data: bytes,
                    meId: groupSenderIdentity
                });
                const senderKeyRecipients = [];
                for (const device of devices) {
                    const deviceJid = device.jid;
                    const hasKey = !!senderKeyMap[deviceJid];
                    if ((!hasKey || !!participant) &&
                        !(0, index_js_4.isHostedLidUser)(deviceJid) &&
                        !(0, index_js_4.isHostedPnUser)(deviceJid) &&
                        device.device !== 99) {
                        //todo: revamp all this logic
                        // the goal is to follow with what I said above for each group, and instead of a true false map of ids, we can set an array full of those the app has already sent pkmsgs
                        senderKeyRecipients.push(deviceJid);
                        senderKeyMap[deviceJid] = true;
                    }
                }
                if (senderKeyRecipients.length) {
                    logger.debug({ senderKeyJids: senderKeyRecipients }, 'sending new sender key');
                    const senderKeyMsg = {
                        senderKeyDistributionMessage: {
                            axolotlSenderKeyDistributionMessage: senderKeyDistributionMessage,
                            groupId: destinationJid
                        }
                    };
                    const senderKeySessionTargets = senderKeyRecipients;
                    await assertSessions(senderKeySessionTargets);
                    const result = await createParticipantNodes(senderKeyRecipients, senderKeyMsg, extraAttrs);
                    shouldIncludeDeviceIdentity = shouldIncludeDeviceIdentity || result.shouldIncludeDeviceIdentity;
                    participants.push(...result.nodes);
                }
                binaryNodeContent.push({
                    tag: 'enc',
                    attrs: { v: '2', type: 'skmsg', ...extraAttrs },
                    content: ciphertext
                });
                await authState.keys.set({ 'sender-key-memory': { [jid]: senderKeyMap } });
            }
            else {
                // ADDRESSING CONSISTENCY: Match own identity to conversation context
                // TODO: investigate if this is true
                let ownId = meId;
                if (isLid && meLid) {
                    ownId = meLid;
                    logger.debug({ to: jid, ownId }, 'Using LID identity for @lid conversation');
                }
                else {
                    logger.debug({ to: jid, ownId }, 'Using PN identity for @s.whatsapp.net conversation');
                }
                const { user: ownUser } = (0, index_js_4.jidDecode)(ownId);
                if (!participant) {
                    const patchedForReporting = await patchMessageBeforeSending(message, [jid]);
                    reportingMessage = Array.isArray(patchedForReporting)
                        ? patchedForReporting.find(item => item.recipientJid === jid) || patchedForReporting[0]
                        : patchedForReporting;
                }
                if (!isRetryResend) {
                    const targetUserServer = isLid ? 'lid' : 's.whatsapp.net';
                    devices.push({
                        user,
                        device: 0,
                        jid: (0, index_js_4.jidEncode)(user, targetUserServer, 0) // rajeh, todo: this entire logic is convoluted and weird.
                    });
                    if (user !== ownUser) {
                        const ownUserServer = isLid ? 'lid' : 's.whatsapp.net';
                        const ownUserForAddressing = isLid && meLid ? (0, index_js_4.jidDecode)(meLid).user : (0, index_js_4.jidDecode)(meId).user;
                        devices.push({
                            user: ownUserForAddressing,
                            device: 0,
                            jid: (0, index_js_4.jidEncode)(ownUserForAddressing, ownUserServer, 0)
                        });
                    }
                    if (additionalAttributes?.['category'] !== 'peer') {
                        // Clear placeholders and enumerate actual devices
                        devices.length = 0;
                        // Use conversation-appropriate sender identity
                        const senderIdentity = isLid && meLid
                            ? (0, index_js_4.jidEncode)((0, index_js_4.jidDecode)(meLid)?.user, 'lid', undefined)
                            : (0, index_js_4.jidEncode)((0, index_js_4.jidDecode)(meId)?.user, 's.whatsapp.net', undefined);
                        // Enumerate devices for sender and target with consistent addressing
                        const sessionDevices = await getUSyncDevices([senderIdentity, jid], true, false);
                        devices.push(...sessionDevices);
                        logger.debug({
                            deviceCount: devices.length,
                            devices: devices.map(d => `${d.user}:${d.device}@${(0, index_js_4.jidDecode)(d.jid)?.server}`)
                        }, 'Device enumeration complete with unified addressing');
                    }
                }
                const allRecipients = [];
                const meRecipients = [];
                const otherRecipients = [];
                const { user: mePnUser } = (0, index_js_4.jidDecode)(meId);
                const { user: meLidUser } = meLid ? (0, index_js_4.jidDecode)(meLid) : { user: null };
                for (const { user, jid } of devices) {
                    const isExactSenderDevice = jid === meId || (meLid && jid === meLid);
                    if (isExactSenderDevice) {
                        logger.debug({ jid, meId, meLid }, 'Skipping exact sender device (whatsmeow pattern)');
                        continue;
                    }
                    // Check if this is our device (could match either PN or LID user)
                    const isMe = user === mePnUser || user === meLidUser;
                    if (isMe) {
                        meRecipients.push(jid);
                    }
                    else {
                        otherRecipients.push(jid);
                    }
                    allRecipients.push(jid);
                }
                await assertSessions(allRecipients);
                const [{ nodes: meNodes, shouldIncludeDeviceIdentity: s1 }, { nodes: otherNodes, shouldIncludeDeviceIdentity: s2 }] = await Promise.all([
                    // For own devices: use DSM if available (1:1 chats only)
                    createParticipantNodes(meRecipients, meMsg || message, extraAttrs),
                    createParticipantNodes(otherRecipients, message, extraAttrs, meMsg)
                ]);
                participants.push(...meNodes);
                participants.push(...otherNodes);
                if (meRecipients.length > 0 || otherRecipients.length > 0) {
                    extraAttrs['phash'] = (0, index_js_3.generateParticipantHashV2)([...meRecipients, ...otherRecipients]);
                }
                shouldIncludeDeviceIdentity = shouldIncludeDeviceIdentity || s1 || s2;
            }
            if (isRetryResend) {
                const isParticipantLid = (0, index_js_4.isLidUser)(participant.jid);
                const isMe = (0, index_js_4.areJidsSameUser)(participant.jid, isParticipantLid ? meLid : meId);
                const encodedMessageToSend = isMe
                    ? (0, index_js_3.encodeWAMessage)({
                        deviceSentMessage: {
                            destinationJid,
                            message
                        }
                    })
                    : (0, index_js_3.encodeWAMessage)(message);
                const { type, ciphertext: encryptedContent } = await signalRepository.encryptMessage({
                    data: encodedMessageToSend,
                    jid: participant.jid
                });
                binaryNodeContent.push({
                    tag: 'enc',
                    attrs: {
                        v: '2',
                        type,
                        count: participant.count.toString()
                    },
                    content: encryptedContent
                });
            }
            if (participants.length) {
                if (additionalAttributes?.['category'] === 'peer') {
                    const peerNode = participants[0]?.content?.[0];
                    if (peerNode) {
                        binaryNodeContent.push(peerNode); // push only enc
                    }
                }
                else {
                    binaryNodeContent.push({
                        tag: 'participants',
                        attrs: {},
                        content: participants
                    });
                }
            }
            const stanza = {
                tag: 'message',
                attrs: {
                    id: msgId,
                    to: destinationJid,
                    type: getMessageType(message),
                    ...(additionalAttributes || {})
                },
                content: binaryNodeContent
            };
            // if the participant to send to is explicitly specified (generally retry recp)
            // ensure the message is only sent to that person
            // if a retry receipt is sent to everyone -- it'll fail decryption for everyone else who received the msg
            if (participant) {
                if ((0, index_js_4.isJidGroup)(destinationJid)) {
                    stanza.attrs.to = destinationJid;
                    stanza.attrs.participant = participant.jid;
                }
                else if ((0, index_js_4.areJidsSameUser)(participant.jid, meId)) {
                    stanza.attrs.to = participant.jid;
                    stanza.attrs.recipient = destinationJid;
                }
                else {
                    stanza.attrs.to = participant.jid;
                }
            }
            else {
                stanza.attrs.to = destinationJid;
            }
            if (shouldIncludeDeviceIdentity) {
                ;
                stanza.content.push({
                    tag: 'device-identity',
                    attrs: {},
                    content: (0, index_js_3.encodeSignedDeviceIdentity)(authState.creds.account, true)
                });
                logger.debug({ jid }, 'adding device identity');
            }
            if (!isNewsletter &&
                !isRetryResend &&
                reportingMessage?.messageContextInfo?.messageSecret &&
                (0, reporting_utils_js_1.shouldIncludeReportingToken)(reportingMessage)) {
                try {
                    const encoded = (0, index_js_3.encodeWAMessage)(reportingMessage);
                    const reportingKey = {
                        id: msgId,
                        fromMe: true,
                        remoteJid: destinationJid,
                        participant: participant?.jid
                    };
                    const reportingNode = await (0, reporting_utils_js_1.getMessageReportingToken)(encoded, reportingMessage, reportingKey);
                    if (reportingNode) {
                        ;
                        stanza.content.push(reportingNode);
                        logger.trace({ jid }, 'added reporting token to message');
                    }
                }
                catch (error) {
                    logger.warn({ jid, trace: error?.stack }, 'failed to attach reporting token');
                }
            }
            const contactTcTokenData = !isGroup && !isRetryResend && !isStatus ? await authState.keys.get('tctoken', [destinationJid]) : {};
            const tcTokenBuffer = contactTcTokenData[destinationJid]?.token;
            if (tcTokenBuffer) {
                ;
                stanza.content.push({
                    tag: 'tctoken',
                    attrs: {},
                    content: tcTokenBuffer
                });
            }
            if (AI && !isGroup && !isStatus && !isNewsletter) {
                const existingBizBot = (0, index_js_4.getBinaryFilteredBizBot)(additionalNodes || []);
                if (existingBizBot) {
                    if (additionalNodes && additionalNodes.length > 0) {
                        stanza.content.push(...additionalNodes);
                    }
                }
                else {
                    stanza.content.push({ tag: 'bot', attrs: { biz_bot: '1' } });
                    if (additionalNodes && additionalNodes.length > 0) {
                        stanza.content.push(...additionalNodes);
                    }
                }
            }
            else if (additionalNodes && additionalNodes.length > 0) {
                ;
                stanza.content.push(...additionalNodes);
            }
            logger.debug({ msgId }, `sending message to ${participants.length} devices`);
            await sendNode(stanza);
            // Add message to retry cache if enabled
            if (messageRetryManager && !participant) {
                messageRetryManager.addRecentMessage(destinationJid, msgId, message);
            }
        }, meId);
        return msgId;
    };
    const getMessageType = (message) => {
        // groupStatusMessageV2 must be checked BEFORE normalizeMessageContent
        // because normalizeMessageContent will unwrap it into inner message
        if (message?.groupStatusMessageV2 || message?.groupStatusMessage) {
            return 'text';
        }
        const normalizedMessage = (0, index_js_3.normalizeMessageContent)(message);
        if (!normalizedMessage)
            return 'text';
        if (normalizedMessage.reactionMessage || normalizedMessage.encReactionMessage) {
            return 'reaction';
        }
        if (normalizedMessage.pollCreationMessage ||
            normalizedMessage.pollCreationMessageV2 ||
            normalizedMessage.pollCreationMessageV3 ||
            normalizedMessage.pollUpdateMessage) {
            return 'poll';
        }
        if (normalizedMessage.eventMessage) {
            return 'event';
        }
        if (normalizedMessage.interactiveMessage) {
            return 'text';
        }
        if (getMediaType(normalizedMessage) !== '') {
            return 'media';
        }
        return 'text';
    };
    const getMediaType = (message) => {
        if (message.imageMessage) {
            return 'image';
        }
        else if (message.videoMessage) {
            return message.videoMessage.gifPlayback ? 'gif' : 'video';
        }
        else if (message.ptvMessage) {
            return 'video';
        }
        else if (message.audioMessage) {
            return message.audioMessage.ptt ? 'ptt' : 'audio';
        }
        else if (message.contactMessage) {
            return 'vcard';
        }
        else if (message.documentMessage) {
            return 'document';
        }
        else if (message.contactsArrayMessage) {
            return 'contact_array';
        }
        else if (message.liveLocationMessage) {
            return 'livelocation';
        }
        else if (message.stickerMessage) {
            return 'sticker';
        }
        else if (message.stickerPackMessage) {
            return 'sticker_pack';
        }
        else if (message.listMessage) {
            return 'list';
        }
        else if (message.listResponseMessage) {
            return 'list_response';
        }
        else if (message.buttonsResponseMessage) {
            return 'buttons_response';
        }
        else if (message.orderMessage) {
            return 'order';
        }
        else if (message.productMessage) {
            return 'product';
        }
        else if (message.interactiveResponseMessage) {
            return 'native_flow_response';
        }
        else if (message.groupInviteMessage) {
            return 'url';
        }
        return '';
    };
    const getPrivacyTokens = async (jids) => {
        const t = (0, index_js_3.unixTimestampSeconds)().toString();
        const result = await query({
            tag: 'iq',
            attrs: {
                to: index_js_4.S_WHATSAPP_NET,
                type: 'set',
                xmlns: 'privacy'
            },
            content: [
                {
                    tag: 'tokens',
                    attrs: {},
                    content: jids.map(jid => ({
                        tag: 'token',
                        attrs: {
                            jid: (0, index_js_4.jidNormalizedUser)(jid),
                            t,
                            type: 'trusted_contact'
                        }
                    }))
                }
            ]
        });
        return result;
    };
    const waUploadToServer = (0, index_js_3.getWAUploadToServer)(config, refreshMediaConn);
    const waitForMsgMediaUpdate = (0, index_js_3.bindWaitForEvent)(ev, 'messages.media-update');
    // ── Button type helpers (ported from itsukichan) ──────────────────────────
    const getButtonType = (message) => {
        if (message.listMessage)
            return 'list';
        if (message.buttonsMessage)
            return 'buttons';
        if (message.templateMessage)
            return 'template';
        if (message.interactiveMessage?.nativeFlowMessage)
            return 'native_flow';
        if (message.interactiveMessage?.shopStorefrontMessage)
            return 'shop';
        if (message.interactiveMessage?.collectionMessage)
            return 'collection';
        if (message.interactiveMessage?.carouselMessage)
            return 'carousel';
        if (message.interactiveMessage)
            return 'interactive';
        return null;
    };
    const getButtonArgs = (message) => {
        const nativeFlow = message.interactiveMessage?.nativeFlowMessage;
        const firstButtonName = nativeFlow?.buttons?.[0]?.name;
        const nativeFlowSpecials = [
            'mpm', 'cta_catalog', 'send_location',
            'call_permission_request', 'wa_payment_transaction_details',
            'automated_greeting_message_view_catalog'
        ];
        const ts = (0, index_js_3.unixTimestampSeconds)().toString();
        const bizBase = { actual_actors: '2', host_storage: '2', privacy_mode_ts: ts };
        const qualityControl = { tag: 'quality_control', attrs: { source_type: 'third_party' } };
        if (nativeFlow && (firstButtonName === 'review_and_pay' || firstButtonName === 'payment_info')) {
            return {
                tag: 'biz',
                attrs: { native_flow_name: firstButtonName === 'review_and_pay' ? 'order_details' : firstButtonName }
            };
        }
        else if (nativeFlow && nativeFlowSpecials.includes(firstButtonName)) {
            return {
                tag: 'biz', attrs: bizBase,
                content: [{
                        tag: 'interactive', attrs: { type: 'native_flow', v: '1' },
                        content: [{ tag: 'native_flow', attrs: { v: '2', name: firstButtonName } }]
                    }, qualityControl]
            };
        }
        else if (nativeFlow || message.buttonsMessage || message.templateMessage || message.interactiveMessage) {
            // All interactive types in groups use native_flow biz node
            // This covers: nativeFlowMessage, buttonsMessage→interactiveMessage,
            // templateMessage→interactiveMessage, and direct interactiveMessage
            return {
                tag: 'biz', attrs: bizBase,
                content: [{
                        tag: 'interactive', attrs: { type: 'native_flow', v: '1' },
                        content: [{ tag: 'native_flow', attrs: { v: '9', name: 'mixed' } }]
                    }, qualityControl]
            };
        }
        else if (message.listMessage) {
            // Standalone listMessage (private chat) keeps the list biz node
            return {
                tag: 'biz', attrs: bizBase,
                content: [{ tag: 'list', attrs: { v: '2', type: 'product_list' } }, qualityControl]
            };
        }
        else {
            return { tag: 'biz', attrs: bizBase };
        }
    };
    // ── End button type helpers ───────────────────────────────────────────────
    return {
        ...sock,
        getPrivacyTokens,
        assertSessions,
        relayMessage,
        sendReceipt,
        sendReceipts,
        readMessages,
        refreshMediaConn,
        waUploadToServer,
        fetchPrivacySettings,
        sendPeerDataOperationMessage,
        createParticipantNodes,
        getUSyncDevices,
        messageRetryManager,
        updateMemberLabel,
        updateMediaMessage: async (message) => {
            const content = (0, index_js_3.assertMediaContent)(message.message);
            const mediaKey = content.mediaKey;
            const meId = authState.creds.me.id;
            const node = (0, index_js_3.encryptMediaRetryRequest)(message.key, mediaKey, meId);
            let error = undefined;
            await Promise.all([
                sendNode(node),
                waitForMsgMediaUpdate(async (update) => {
                    const result = update.find(c => c.key.id === message.key.id);
                    if (result) {
                        if (result.error) {
                            error = result.error;
                        }
                        else {
                            try {
                                const media = (0, index_js_3.decryptMediaRetryData)(result.media, mediaKey, result.key.id);
                                if (media.result !== index_js_1.proto.MediaRetryNotification.ResultType.SUCCESS) {
                                    const resultStr = index_js_1.proto.MediaRetryNotification.ResultType[media.result];
                                    throw new boom_1.Boom(`Media re-upload failed by device (${resultStr})`, {
                                        data: media,
                                        statusCode: (0, index_js_3.getStatusCodeForMediaRetry)(media.result) || 404
                                    });
                                }
                                content.directPath = media.directPath;
                                content.url = (0, index_js_3.getUrlFromDirectPath)(content.directPath);
                                logger.debug({ directPath: media.directPath, key: result.key }, 'media update successful');
                            }
                            catch (err) {
                                error = err;
                            }
                        }
                        return true;
                    }
                })
            ]);
            if (error) {
                throw error;
            }
            ev.emit('messages.update', [{ key: message.key, update: { message: message.message } }]);
            return message;
        },
        resize: async (media, width, height) => {
            const { getStream, toBuffer } = await Promise.resolve().then(() => __importStar(require('../Utils/messages-media.js')));
            let sharp;
            try {
                sharp = (await Promise.resolve().then(() => __importStar(require('sharp')))).default;
            }
            catch (_) { }
            if (!sharp)
                throw new Error('sharp is required for resize');
            const { stream } = await getStream(media);
            const buf = await toBuffer(stream);
            return sharp(buf).resize(width, height, { fit: 'cover' }).toBuffer();
        },
        sendStatusMentions: async (content, jids = []) => {
            const userJid = (0, index_js_4.jidNormalizedUser)(authState.creds.me.id);
            let allUsers = new Set();
            allUsers.add(userJid);
            for (const id of jids) {
                const isGroup = (0, index_js_4.isJidGroup)(id);
                const isPrivate = (0, index_js_4.isJidUser)(id);
                if (isGroup) {
                    try {
                        const metadata = (cachedGroupMetadata ? await cachedGroupMetadata(id) : null) || await groupMetadata(id);
                        const participants = metadata.participants.map(p => (0, index_js_4.jidNormalizedUser)(p.id));
                        participants.forEach(j => allUsers.add(j));
                    }
                    catch (error) {
                        logger.error(`Error getting metadata for group ${id}: ${error}`);
                    }
                }
                else if (isPrivate) {
                    allUsers.add((0, index_js_4.jidNormalizedUser)(id));
                }
            }
            const uniqueUsers = Array.from(allUsers);
            const getRandomHexColor = () => '#' + Math.floor(Math.random() * 16777215).toString(16).padStart(6, '0');
            const isMedia = content.image || content.video || content.audio;
            const isAudio = !!content.audio;
            const messageContent = { ...content };
            if (isMedia && !isAudio) {
                if (messageContent.text) {
                    messageContent.caption = messageContent.text;
                    delete messageContent.text;
                }
                delete messageContent.ptt;
                delete messageContent.font;
                delete messageContent.backgroundColor;
                delete messageContent.textColor;
            }
            if (isAudio) {
                delete messageContent.text;
                delete messageContent.caption;
                delete messageContent.font;
                delete messageContent.textColor;
            }
            const font = !isMedia ? (content.font ?? Math.floor(Math.random() * 9)) : undefined;
            const textColor = !isMedia ? (content.textColor ?? getRandomHexColor()) : undefined;
            const backgroundColor = (!isMedia || isAudio) ? (content.backgroundColor ?? getRandomHexColor()) : undefined;
            const ptt = isAudio ? (typeof content.ptt === 'boolean' ? content.ptt : true) : undefined;
            let msg;
            let mediaHandle;
            try {
                msg = await (0, index_js_3.generateWAMessage)(index_js_4.STORIES_JID, messageContent, {
                    logger,
                    userJid,
                    getUrlInfo: text => (0, link_preview_js_1.getUrlInfo)(text, {
                        thumbnailWidth: linkPreviewImageThumbnailWidth,
                        fetchOpts: { timeout: 3000 },
                        logger,
                        uploadImage: generateHighQualityLinkPreview ? waUploadToServer : undefined
                    }),
                    upload: async (encFilePath, opts) => {
                        const up = await waUploadToServer(encFilePath, { ...opts });
                        mediaHandle = up.handle;
                        return up;
                    },
                    mediaCache: config.mediaCache,
                    options: config.options,
                    font,
                    textColor,
                    backgroundColor,
                    ptt
                });
            }
            catch (error) {
                logger.error(`Error generating status message: ${error}`);
                throw error;
            }
            await relayMessage(index_js_4.STORIES_JID, msg.message, {
                messageId: msg.key.id,
                statusJidList: uniqueUsers,
                additionalNodes: [{
                        tag: 'meta',
                        attrs: {},
                        content: [{
                                tag: 'mentioned_users',
                                attrs: {},
                                content: jids.map(jid => ({
                                    tag: 'to',
                                    attrs: { jid: (0, index_js_4.jidNormalizedUser)(jid) }
                                }))
                            }]
                    }]
            });
            for (const id of jids) {
                try {
                    const normalizedId = (0, index_js_4.jidNormalizedUser)(id);
                    const isPrivate = (0, index_js_4.isJidUser)(normalizedId);
                    const type = isPrivate ? 'statusMentionMessage' : 'groupStatusMentionMessage';
                    const protocolMessage = {
                        [type]: {
                            message: {
                                protocolMessage: {
                                    key: msg.key,
                                    type: 25
                                }
                            }
                        },
                        messageContextInfo: {
                            messageSecret: (0, crypto_1.randomBytes)(32)
                        }
                    };
                    const statusMsg = await (0, index_js_3.generateWAMessageFromContent)(normalizedId, protocolMessage, {});
                    await relayMessage(normalizedId, statusMsg.message, {
                        additionalNodes: [{
                                tag: 'meta',
                                attrs: isPrivate
                                    ? { is_status_mention: 'true' }
                                    : { is_group_status_mention: 'true' }
                            }]
                    });
                    await (0, generics_js_1.delay)(2000);
                }
                catch (error) {
                    logger.error(`Error sending status mention to ${id}: ${error}`);
                }
            }
            return msg;
        },
        sendMessage: async (jid, content, options = {}) => {
            const userJid = authState.creds.me.id;
            // ── FIX: AstraBail Button Format Compatibility Bridge ─────────────
            // Bot yang pakai format button lama (seperti di library lain):
            // { buttons: [{buttonId:'x', buttonText:{displayText:'y'}, type:1}] }
            // dikonversi ke interactiveButtons AstraBail agar bisa diproses
            // dengan benar, termasuk di dalam grup besar.
            if (typeof content === 'object' &&
                Array.isArray(content.buttons) &&
                content.buttons.length > 0 &&
                !content.buttons.some(b => b.nativeFlowInfo) &&
                content.buttons.every(b => b.type === 1 || (!b.type && b.buttonId))) {
                const interactiveButtons = content.buttons.map(b => ({
                    name: 'quick_reply',
                    buttonParamsJson: JSON.stringify({
                        display_text: b.buttonText?.displayText || b.buttonId || 'Button',
                        id: b.buttonId || b.buttonText?.displayText || 'btn'
                    })
                }));
                const { buttons: _b, headerType: _h, viewOnce: _v, ...restContent } = content;
                content = { ...restContent, interactiveButtons };
            }
            // ── End AstraBail Button Format Compatibility Bridge ──────────────
            // ── Normalize: buttons[].nativeFlowInfo -> interactiveButtons ──────
            if (typeof content === 'object' &&
                Array.isArray(content.buttons) &&
                content.buttons.length > 0 &&
                content.buttons.some(b => b.nativeFlowInfo)) {
                const interactiveButtons = content.buttons.map(b => {
                    if (b.nativeFlowInfo) {
                        return {
                            name: b.nativeFlowInfo.name,
                            buttonParamsJson: b.nativeFlowInfo.paramsJson || '{}'
                        };
                    }
                    return {
                        name: 'quick_reply',
                        buttonParamsJson: JSON.stringify({
                            display_text: b.buttonText?.displayText || b.buttonId || 'Button',
                            id: b.buttonId || b.buttonText?.displayText || 'btn'
                        })
                    };
                });
                const { buttons, headerType, viewOnce, ...rest } = content;
                content = { ...rest, interactiveButtons };
            }
            // ── Interactive Button (sendButton logic) ──────────────────────────
            if (typeof content === 'object' && Array.isArray(content.interactiveButtons) && content.interactiveButtons.length > 0) {
                const { text = '', caption = '', title = '', footer = '', interactiveButtons, hasMediaAttachment = false, image = null, video = null, document = null, mimetype = null, jpegThumbnail = null, location = null, product = null, businessOwnerJid = null, externalAdReply = null, } = content;
                // Normalize buttons
                const processedButtons = [];
                for (let i = 0; i < interactiveButtons.length; i++) {
                    const btn = interactiveButtons[i];
                    if (!btn || typeof btn !== 'object')
                        throw new Error(`interactiveButtons[${i}] must be an object`);
                    if (btn.name && btn.buttonParamsJson) {
                        processedButtons.push(btn);
                        continue;
                    }
                    if (btn.id || btn.text || btn.displayText) {
                        processedButtons.push({ name: 'quick_reply', buttonParamsJson: JSON.stringify({ display_text: btn.text || btn.displayText || `Button ${i + 1}`, id: btn.id || `quick_${i + 1}` }) });
                        continue;
                    }
                    if (btn.buttonId && btn.buttonText?.displayText) {
                        processedButtons.push({ name: 'quick_reply', buttonParamsJson: JSON.stringify({ display_text: btn.buttonText.displayText, id: btn.buttonId }) });
                        continue;
                    }
                    throw new Error(`interactiveButtons[${i}] has invalid shape`);
                }
                let messageContent = {};
                // Header
                if (image) {
                    const mi = Buffer.isBuffer(image) ? { image } : { image: { url: typeof image === 'object' ? image.url : image } };
                    const pm = await (0, index_js_3.prepareWAMessageMedia)(mi, { upload: waUploadToServer });
                    messageContent.header = { title: title || '', hasMediaAttachment: true, imageMessage: pm.imageMessage };
                }
                else if (video) {
                    const mi = Buffer.isBuffer(video) ? { video } : { video: { url: typeof video === 'object' ? video.url : video } };
                    const pm = await (0, index_js_3.prepareWAMessageMedia)(mi, { upload: waUploadToServer });
                    messageContent.header = { title: title || '', hasMediaAttachment: true, videoMessage: pm.videoMessage };
                }
                else if (document) {
                    const mi = Buffer.isBuffer(document) ? { document } : { document: { url: typeof document === 'object' ? document.url : document } };
                    if (mimetype && typeof mi.document === 'object')
                        mi.document.mimetype = mimetype;
                    if (jpegThumbnail) {
                        const thumb = Buffer.isBuffer(jpegThumbnail) ? jpegThumbnail : await (async () => { try {
                            const r = await fetch(jpegThumbnail);
                            return Buffer.from(await r.arrayBuffer());
                        }
                        catch {
                            return undefined;
                        } })();
                        if (thumb)
                            mi.document.jpegThumbnail = thumb;
                    }
                    const pm = await (0, index_js_3.prepareWAMessageMedia)(mi, { upload: waUploadToServer });
                    messageContent.header = { title: title || '', hasMediaAttachment: true, documentMessage: pm.documentMessage };
                }
                else if (location && typeof location === 'object') {
                    messageContent.header = { title: title || location.name || 'Location', hasMediaAttachment: false, locationMessage: { degreesLatitude: location.degreesLatitude || location.degressLatitude || 0, degreesLongitude: location.degreesLongitude || location.degressLongitude || 0, name: location.name || '', address: location.address || '' } };
                }
                else if (product && typeof product === 'object') {
                    let productImageMessage = null;
                    if (product.productImage) {
                        const mi = Buffer.isBuffer(product.productImage) ? { image: product.productImage } : { image: { url: typeof product.productImage === 'object' ? product.productImage.url : product.productImage } };
                        const pm = await (0, index_js_3.prepareWAMessageMedia)(mi, { upload: waUploadToServer });
                        productImageMessage = pm.imageMessage;
                    }
                    messageContent.header = { title: title || product.title || 'Product', hasMediaAttachment: false, productMessage: { product: { productImage: productImageMessage, productId: product.productId || '', title: product.title || '', description: product.description || '', currencyCode: product.currencyCode || 'USD', priceAmount1000: parseInt(product.priceAmount1000) || 0, retailerId: product.retailerId || '', url: product.url || '', productImageCount: product.productImageCount || 1 }, businessOwnerJid: businessOwnerJid || product.businessOwnerJid || userJid } };
                }
                else if (title) {
                    messageContent.header = { title, hasMediaAttachment: false };
                }
                const hasMedia = !!(image || video || document || location || product);
                const bodyText = hasMedia ? caption : text || caption;
                if (bodyText)
                    messageContent.body = { text: bodyText };
                if (footer)
                    messageContent.footer = { text: footer };
                messageContent.nativeFlowMessage = { buttons: processedButtons };
                // Context info
                if (externalAdReply && typeof externalAdReply === 'object') {
                    messageContent.contextInfo = { externalAdReply: { title: externalAdReply.title || '', body: externalAdReply.body || '', mediaType: externalAdReply.mediaType || 1, sourceUrl: externalAdReply.sourceUrl || externalAdReply.url || '', thumbnailUrl: externalAdReply.thumbnailUrl || externalAdReply.thumbnail || '', renderLargerThumbnail: externalAdReply.renderLargerThumbnail || false, showAdAttribution: externalAdReply.showAdAttribution !== false, containsAutoReply: externalAdReply.containsAutoReply || false, ...(externalAdReply.mediaUrl && { mediaUrl: externalAdReply.mediaUrl }), ...(Buffer.isBuffer(externalAdReply.thumbnail) && { thumbnail: externalAdReply.thumbnail }), ...(externalAdReply.jpegThumbnail && { jpegThumbnail: externalAdReply.jpegThumbnail }) }, ...(options.mentionedJid && { mentionedJid: options.mentionedJid }) };
                }
                else if (options.mentionedJid) {
                    messageContent.contextInfo = { mentionedJid: options.mentionedJid };
                }
                const payload = index_js_1.proto.Message.InteractiveMessage.create(messageContent);
                const msg = (0, index_js_3.generateWAMessageFromContent)(jid, { viewOnceMessage: { message: { interactiveMessage: payload } } }, { userJid, quoted: options?.quoted || null });
                const additionalNodes = [{ tag: 'biz', attrs: {}, content: [{ tag: 'interactive', attrs: { type: 'native_flow', v: '1' }, content: [{ tag: 'native_flow', attrs: { v: '9', name: 'mixed' } }] }] }];
                await relayMessage(jid, msg.message, { messageId: msg.key.id, additionalNodes });
                return msg;
            }
            // ── End Interactive Button ─────────────────────────────────────────
            // ── FIX: Album Message Handler (AstraBail-compatible + delay configurable) ──
            if (typeof content === 'object' && 'album' in content && content.album) {
                const albumDelay = config.albumMessageItemDelayMs ?? 800;
                const { album, caption } = content;
                if (caption && album.length > 0 && !album[0].caption) {
                    album[0].caption = caption;
                }
                let mediaHandle;
                const albumMsg = (0, index_js_3.generateWAMessageFromContent)(jid, {
                    albumMessage: {
                        expectedImageCount: album.filter(item => 'image' in item).length,
                        expectedVideoCount: album.filter(item => 'video' in item).length
                    }
                }, { userJid, ...options });
                await relayMessage(jid, albumMsg.message, { messageId: albumMsg.key.id });
                for (const i in album) {
                    const media = album[i];
                    let mediaMsg;
                    if ('image' in media) {
                        mediaMsg = await (0, index_js_3.generateWAMessage)(jid, {
                            image: media.image,
                            ...(media.caption ? { caption: media.caption } : {})
                        }, {
                            logger, userJid,
                            upload: async (readStream, opts) => {
                                const up = await waUploadToServer(readStream, { ...opts, newsletter: _isNewsletterJid(jid) });
                                mediaHandle = up.handle;
                                return up;
                            },
                            ...options
                        });
                    } else if ('video' in media) {
                        mediaMsg = await (0, index_js_3.generateWAMessage)(jid, {
                            video: media.video,
                            ...(media.caption ? { caption: media.caption } : {}),
                            ...(media.gifPlayback !== undefined ? { gifPlayback: media.gifPlayback } : {})
                        }, {
                            logger, userJid,
                            upload: async (readStream, opts) => {
                                const up = await waUploadToServer(readStream, { ...opts, newsletter: _isNewsletterJid(jid) });
                                mediaHandle = up.handle;
                                return up;
                            },
                            ...options
                        });
                    }
                    if (mediaMsg) {
                        const { randomBytes: _rb } = require('node:crypto');
                        mediaMsg.message.messageContextInfo = {
                            messageSecret: _rb(32),
                            messageAssociation: {
                                associationType: 1,
                                parentMessageKey: albumMsg.key
                            }
                        };
                        await relayMessage(jid, mediaMsg.message, { messageId: mediaMsg.key.id });
                        await (0, generics_js_1.delay)(albumDelay);
                    }
                }
                if (config.emitOwnEvents) {
                    process.nextTick(async () => {
                        await messageMutex.mutex(() => upsertMessage(albumMsg, 'append'));
                    });
                }
                return albumMsg;
            }
            // ── End Album Message Handler ─────────────────────────────────────
            if (typeof content === 'object' &&
                'disappearingMessagesInChat' in content &&
                typeof content['disappearingMessagesInChat'] !== 'undefined' &&
                (0, index_js_4.isJidGroup)(jid)) {
                const { disappearingMessagesInChat } = content;
                const value = typeof disappearingMessagesInChat === 'boolean'
                    ? disappearingMessagesInChat
                        ? index_js_2.WA_DEFAULT_EPHEMERAL
                        : 0
                    : disappearingMessagesInChat;
                await groupToggleEphemeral(jid, value);
            }
            else {
                let mediaHandle;
                const fullMsg = await (0, index_js_3.generateWAMessage)(jid, content, {
                    logger,
                    userJid,
                    getUrlInfo: text => (0, link_preview_js_1.getUrlInfo)(text, {
                        thumbnailWidth: linkPreviewImageThumbnailWidth,
                        fetchOpts: {
                            timeout: 3000,
                            ...(httpRequestOptions || {})
                        },
                        logger,
                        uploadImage: generateHighQualityLinkPreview ? waUploadToServer : undefined
                    }),
                    //TODO: CACHE
                    getProfilePicUrl: sock.profilePictureUrl,
                    getCallLink: sock.createCallLink,
                    newsletter: _isNewsletterJid(jid),
                    upload: async (encFilePath, opts) => {
                        const up = await waUploadToServer(encFilePath, { ...opts, newsletter: _isNewsletterJid(jid) });
                        mediaHandle = up.handle;
                        return up;
                    },
                    mediaCache: config.mediaCache,
                    options: config.options,
                    messageId: (0, index_js_3.generateMessageIDV2)(sock.user?.id),
                    ...options
                });
                if (content?.audio && options?.contextInfo) {
                    const msgContent = fullMsg.message;
                    if (msgContent?.audioMessage) {
                        msgContent.audioMessage.contextInfo = options.contextInfo;
                    }
                }
                // Extract handle from newsletter upload (set by prepareWAMessageMedia)
                if (!mediaHandle) {
                    const msgContent = fullMsg.message;
                    const msgTypes = ['audioMessage', 'imageMessage', 'videoMessage', 'documentMessage', 'stickerMessage'];
                    for (const t of msgTypes) {
                        if (msgContent?.[t]?._uploadHandle) {
                            mediaHandle = msgContent[t]._uploadHandle;
                            delete msgContent[t]._uploadHandle;
                            break;
                        }
                    }
                }
                const isEventMsg = 'event' in content && !!content.event;
                const isDeleteMsg = 'delete' in content && !!content.delete;
                const isEditMsg = 'edit' in content && !!content.edit;
                const isPinMsg = 'pin' in content && !!content.pin;
                const isPollMessage = 'poll' in content && !!content.poll;
                const additionalAttributes = {};
                const additionalNodes = [];
                // required for delete
                if (isDeleteMsg) {
                    // if the chat is a group, and I am not the author, then delete the message as an admin
                    if ((0, index_js_4.isJidGroup)(content.delete?.remoteJid) && !content.delete?.fromMe) {
                        additionalAttributes.edit = '8';
                    }
                    else {
                        additionalAttributes.edit = '7';
                    }
                }
                else if (isEditMsg) {
                    additionalAttributes.edit = '1';
                }
                else if (isPinMsg) {
                    additionalAttributes.edit = '2';
                }
                else if (isPollMessage) {
                    additionalNodes.push({
                        tag: 'meta',
                        attrs: {
                            polltype: 'creation'
                        }
                    });
                }
                else if (isEventMsg) {
                    additionalNodes.push({
                        tag: 'meta',
                        attrs: {
                            event_type: 'creation'
                        }
                    });
                }
                // Auto-attach biz node for button/list/interactive messages
                const buttonType = getButtonType(fullMsg.message);
                if (buttonType) {
                    const btnNode = getButtonArgs(fullMsg.message);
                    if (btnNode)
                        additionalNodes.push(btnNode);
                }
                if (mediaHandle) {
                    additionalAttributes['media_id'] = mediaHandle;
                }
                await relayMessage(jid, fullMsg.message, {
                    messageId: fullMsg.key.id,
                    useCachedGroupMetadata: options.useCachedGroupMetadata,
                    additionalAttributes,
                    statusJidList: options.statusJidList,
                    additionalNodes,
                    AI: options.ai || false
                });
                if (config.emitOwnEvents) {
                    process.nextTick(async () => {
                        await messageMutex.mutex(() => upsertMessage(fullMsg, 'append'));
                    });
                }
                return fullMsg;
            }
        }
    };
};
exports.makeMessagesSocket = makeMessagesSocket;



