"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.makeMessagesRecvSocket = void 0;
const node_cache_1 = __importDefault(require("@cacheable/node-cache"));
const boom_1 = require("@hapi/boom");
const crypto_1 = require("crypto");
const long_1 = __importDefault(require("long"));
const index_js_1 = require("../../WAProto/index.js");
const index_js_2 = require("../Defaults/index.js");
const index_js_3 = require("../Types/index.js");
const index_js_4 = require("../Utils/index.js");
const make_mutex_js_1 = require("../Utils/make-mutex.js");
const index_js_5 = require("../WABinary/index.js");
const groups_js_1 = require("./groups.js");
const messages_send_js_1 = require("./messages-send.js");
const index_js_6 = require("../WAUSync/index.js");
const makeMessagesRecvSocket = (config) => {
    const { logger, retryRequestDelayMs, maxMsgRetryCount, getMessage, shouldIgnoreJid, enableAutoSessionRecreation } = config;
    const sock = (0, messages_send_js_1.makeMessagesSocket)(config);
    const { ev, authState, ws, messageMutex, notificationMutex, receiptMutex, signalRepository, query, upsertMessage, resyncAppState, onUnexpectedError, assertSessions, sendNode, relayMessage, sendReceipt, uploadPreKeys, sendPeerDataOperationMessage, messageRetryManager } = sock;
    /** this mutex ensures that each retryRequest will wait for the previous one to finish */
    const retryMutex = (0, make_mutex_js_1.makeMutex)();
    const msgRetryCache = config.msgRetryCounterCache ||
        new node_cache_1.default({
            stdTTL: index_js_2.DEFAULT_CACHE_TTLS.MSG_RETRY, // 1 hour
            useClones: false
        });
    const callOfferCache = config.callOfferCache ||
        new node_cache_1.default({
            stdTTL: index_js_2.DEFAULT_CACHE_TTLS.CALL_OFFER, // 5 mins
            useClones: false
        });
    const placeholderResendCache = config.placeholderResendCache ||
        new node_cache_1.default({
            stdTTL: index_js_2.DEFAULT_CACHE_TTLS.MSG_RETRY, // 1 hour
            useClones: false
        });
    // Debounce identity-change session refreshes per JID to avoid bursts
    const identityAssertDebounce = new node_cache_1.default({ stdTTL: 5, useClones: false });
    let sendActiveReceipts = false;
    const fetchMessageHistory = async (count, oldestMsgKey, oldestMsgTimestamp) => {
        if (!authState.creds.me?.id) {
            throw new boom_1.Boom('Not authenticated');
        }
        const pdoMessage = {
            historySyncOnDemandRequest: {
                chatJid: oldestMsgKey.remoteJid,
                oldestMsgFromMe: oldestMsgKey.fromMe,
                oldestMsgId: oldestMsgKey.id,
                oldestMsgTimestampMs: oldestMsgTimestamp,
                onDemandMsgCount: count
            },
            peerDataOperationRequestType: index_js_1.proto.Message.PeerDataOperationRequestType.HISTORY_SYNC_ON_DEMAND
        };
        return sendPeerDataOperationMessage(pdoMessage);
    };
    const requestPlaceholderResend = async (messageKey, msgData) => {
        if (!authState.creds.me?.id) {
            throw new boom_1.Boom('Not authenticated');
        }
        if (await placeholderResendCache.get(messageKey?.id)) {
            logger.debug({ messageKey }, 'already requested resend');
            return;
        }
        else {
            // Store original message data so PDO response handler can preserve
            // metadata (LID details, timestamps, etc.) that the phone may omit
            await placeholderResendCache.set(messageKey?.id, msgData || true);
        }
        await (0, index_js_4.delay)(2000);
        if (!(await placeholderResendCache.get(messageKey?.id))) {
            logger.debug({ messageKey }, 'message received while resend requested');
            return 'RESOLVED';
        }
        const pdoMessage = {
            placeholderMessageResendRequest: [
                {
                    messageKey
                }
            ],
            peerDataOperationRequestType: index_js_1.proto.Message.PeerDataOperationRequestType.PLACEHOLDER_MESSAGE_RESEND
        };
        setTimeout(async () => {
            if (await placeholderResendCache.get(messageKey?.id)) {
                logger.debug({ messageKey }, 'PDO message without response after 8 seconds. Phone possibly offline');
                await placeholderResendCache.del(messageKey?.id);
            }
        }, 8000);
        return sendPeerDataOperationMessage(pdoMessage);
    };
    // Handles mex newsletter notifications
    const handleMexNewsletterNotification = async (node) => {
        const mexNode = (0, index_js_5.getBinaryNodeChild)(node, 'mex');
        if (!mexNode?.content) {
            logger.warn({ node }, 'Invalid mex newsletter notification');
            return;
        }
        let data;
        try {
            data = JSON.parse(mexNode.content.toString());
        }
        catch (error) {
            logger.error({ err: error, node }, 'Failed to parse mex newsletter notification');
            return;
        }
        const operation = data?.operation;
        const updates = data?.updates;
        if (!updates || !operation) {
            logger.warn({ data }, 'Invalid mex newsletter notification content');
            return;
        }
        logger.info({ operation, updates }, 'got mex newsletter notification');
        switch (operation) {
            case 'NotificationNewsletterUpdate':
                for (const update of updates) {
                    if (update.jid && update.settings && Object.keys(update.settings).length > 0) {
                        ev.emit('newsletter-settings.update', {
                            id: update.jid,
                            update: update.settings
                        });
                    }
                }
                break;
            case 'NotificationNewsletterAdminPromote':
                for (const update of updates) {
                    if (update.jid && update.user) {
                        ev.emit('newsletter-participants.update', {
                            id: update.jid,
                            author: node.attrs.from,
                            user: update.user,
                            new_role: 'ADMIN',
                            action: 'promote'
                        });
                    }
                }
                break;
            default:
                logger.info({ operation, data }, 'Unhandled mex newsletter notification');
                break;
        }
    };
    // Handles newsletter notifications
    const handleNewsletterNotification = async (node) => {
        const from = node.attrs.from;
        const child = (0, index_js_5.getAllBinaryNodeChildren)(node)[0];
        const author = node.attrs.participant;
        logger.info({ from, child }, 'got newsletter notification');
        switch (child.tag) {
            case 'reaction':
                const reactionUpdate = {
                    id: from,
                    server_id: child.attrs.message_id,
                    reaction: {
                        code: (0, index_js_5.getBinaryNodeChildString)(child, 'reaction'),
                        count: 1
                    }
                };
                ev.emit('newsletter.reaction', reactionUpdate);
                break;
            case 'view':
                const viewUpdate = {
                    id: from,
                    server_id: child.attrs.message_id,
                    count: parseInt(child.content?.toString() || '0', 10)
                };
                ev.emit('newsletter.view', viewUpdate);
                break;
            case 'participant':
                const participantUpdate = {
                    id: from,
                    author,
                    user: child.attrs.jid,
                    action: child.attrs.action,
                    new_role: child.attrs.role
                };
                ev.emit('newsletter-participants.update', participantUpdate);
                break;
            case 'update':
                const settingsNode = (0, index_js_5.getBinaryNodeChild)(child, 'settings');
                if (settingsNode) {
                    const update = {};
                    const nameNode = (0, index_js_5.getBinaryNodeChild)(settingsNode, 'name');
                    if (nameNode?.content)
                        update.name = nameNode.content.toString();
                    const descriptionNode = (0, index_js_5.getBinaryNodeChild)(settingsNode, 'description');
                    if (descriptionNode?.content)
                        update.description = descriptionNode.content.toString();
                    ev.emit('newsletter-settings.update', {
                        id: from,
                        update
                    });
                }
                break;
            case 'message':
                const plaintextNode = (0, index_js_5.getBinaryNodeChild)(child, 'plaintext');
                if (plaintextNode?.content) {
                    try {
                        const contentBuf = typeof plaintextNode.content === 'string'
                            ? Buffer.from(plaintextNode.content, 'binary')
                            : Buffer.from(plaintextNode.content);
                        const messageProto = index_js_1.proto.Message.decode(contentBuf).toJSON();
                        const fullMessage = index_js_1.proto.WebMessageInfo.fromObject({
                            key: {
                                remoteJid: from,
                                id: child.attrs.message_id || child.attrs.server_id,
                                fromMe: false // TODO: is this really true though
                            },
                            message: messageProto,
                            messageTimestamp: +child.attrs.t
                        }).toJSON();
                        await upsertMessage(fullMessage, 'notify');
                        logger.info('Processed plaintext newsletter message');
                    }
                    catch (error) {
                        logger.error({ error }, 'Failed to decode plaintext newsletter message');
                    }
                }
                break;
            default:
                logger.warn({ node }, 'Unknown newsletter notification');
                break;
        }
    };
    const sendMessageAck = async ({ tag, attrs, content }, errorCode) => {
        const stanza = {
            tag: 'ack',
            attrs: {
                id: attrs.id,
                to: attrs.from,
                class: tag
            }
        };
        if (!!errorCode) {
            stanza.attrs.error = errorCode.toString();
        }
        if (!!attrs.participant) {
            stanza.attrs.participant = attrs.participant;
        }
        if (!!attrs.recipient) {
            stanza.attrs.recipient = attrs.recipient;
        }
        if (!!attrs.type &&
            (tag !== 'message' || (0, index_js_5.getBinaryNodeChild)({ tag, attrs, content }, 'unavailable') || errorCode !== 0)) {
            stanza.attrs.type = attrs.type;
        }
        if (tag === 'message' && (0, index_js_5.getBinaryNodeChild)({ tag, attrs, content }, 'unavailable')) {
            stanza.attrs.from = authState.creds.me.id;
        }
        logger.debug({ recv: { tag, attrs }, sent: stanza.attrs }, 'sent ack');
        await sendNode(stanza);
    };
    const rejectCall = async (callId, callFrom) => {
        const stanza = {
            tag: 'call',
            attrs: {
                from: authState.creds.me.id,
                to: callFrom
            },
            content: [
                {
                    tag: 'reject',
                    attrs: {
                        'call-id': callId,
                        'call-creator': callFrom,
                        count: '0'
                    },
                    content: undefined
                }
            ]
        };
        await query(stanza);
    };
    const sendRetryRequest = async (node, forceIncludeKeys = false) => {
        const { fullMessage } = (0, index_js_4.decodeMessageNode)(node, authState.creds.me.id, authState.creds.me.lid || '');
        const { key: msgKey } = fullMessage;
        const msgId = msgKey.id;
        if (messageRetryManager) {
            // Check if we've exceeded max retries using the new system
            if (messageRetryManager.hasExceededMaxRetries(msgId)) {
                logger.debug({ msgId }, 'reached retry limit with new retry manager, clearing');
                messageRetryManager.markRetryFailed(msgId);
                return;
            }
            // Increment retry count using new system
            const retryCount = messageRetryManager.incrementRetryCount(msgId);
            // Use the new retry count for the rest of the logic
            const key = `${msgId}:${msgKey?.participant}`;
            await msgRetryCache.set(key, retryCount);
        }
        else {
            // Fallback to old system
            const key = `${msgId}:${msgKey?.participant}`;
            let retryCount = (await msgRetryCache.get(key)) || 0;
            if (retryCount >= maxMsgRetryCount) {
                logger.debug({ retryCount, msgId }, 'reached retry limit, clearing');
                await msgRetryCache.del(key);
                return;
            }
            retryCount += 1;
            await msgRetryCache.set(key, retryCount);
        }
        const key = `${msgId}:${msgKey?.participant}`;
        const retryCount = (await msgRetryCache.get(key)) || 1;
        const { account, signedPreKey, signedIdentityKey: identityKey } = authState.creds;
        const fromJid = node.attrs.from;
        // Check if we should recreate the session
        let shouldRecreateSession = false;
        let recreateReason = '';
        if (enableAutoSessionRecreation && messageRetryManager && retryCount > 1) {
            try {
                // Check if we have a session with this JID
                const sessionId = signalRepository.jidToSignalProtocolAddress(fromJid);
                const hasSession = await signalRepository.validateSession(fromJid);
                const result = messageRetryManager.shouldRecreateSession(fromJid, hasSession.exists);
                shouldRecreateSession = result.recreate;
                recreateReason = result.reason;
                if (shouldRecreateSession) {
                    logger.debug({ fromJid, retryCount, reason: recreateReason }, 'recreating session for retry');
                    // Delete existing session to force recreation
                    await authState.keys.set({ session: { [sessionId]: null } });
                    forceIncludeKeys = true;
                }
            }
            catch (error) {
                logger.warn({ error, fromJid }, 'failed to check session recreation');
            }
        }
        if (retryCount <= 2) {
            // Use new retry manager for phone requests if available
            if (messageRetryManager) {
                // Schedule phone request with delay (like whatsmeow)
                messageRetryManager.schedulePhoneRequest(msgId, async () => {
                    try {
                        const requestId = await requestPlaceholderResend(msgKey);
                        logger.debug(`sendRetryRequest: requested placeholder resend (${requestId}) for message ${msgId} (scheduled)`);
                    }
                    catch (error) {
                        logger.warn({ error, msgId }, 'failed to send scheduled phone request');
                    }
                });
            }
            else {
                // Fallback to immediate request
                const msgId = await requestPlaceholderResend(msgKey);
                logger.debug(`sendRetryRequest: requested placeholder resend for message ${msgId}`);
            }
        }
        const deviceIdentity = (0, index_js_4.encodeSignedDeviceIdentity)(account, true);
        await authState.keys.transaction(async () => {
            const receipt = {
                tag: 'receipt',
                attrs: {
                    id: msgId,
                    type: 'retry',
                    to: node.attrs.from
                },
                content: [
                    {
                        tag: 'retry',
                        attrs: {
                            count: retryCount.toString(),
                            id: node.attrs.id,
                            t: node.attrs.t,
                            v: '1',
                            // ADD ERROR FIELD
                            error: '0'
                        }
                    },
                    {
                        tag: 'registration',
                        attrs: {},
                        content: (0, index_js_4.encodeBigEndian)(authState.creds.registrationId)
                    }
                ]
            };
            if (node.attrs.recipient) {
                receipt.attrs.recipient = node.attrs.recipient;
            }
            if (node.attrs.participant) {
                receipt.attrs.participant = node.attrs.participant;
            }
            if (retryCount > 1 || forceIncludeKeys || shouldRecreateSession) {
                const { update, preKeys } = await (0, index_js_4.getNextPreKeys)(authState, 1);
                const [keyId] = Object.keys(preKeys);
                const key = preKeys[+keyId];
                const content = receipt.content;
                content.push({
                    tag: 'keys',
                    attrs: {},
                    content: [
                        { tag: 'type', attrs: {}, content: Buffer.from(index_js_2.KEY_BUNDLE_TYPE) },
                        { tag: 'identity', attrs: {}, content: identityKey.public },
                        (0, index_js_4.xmppPreKey)(key, +keyId),
                        (0, index_js_4.xmppSignedPreKey)(signedPreKey),
                        { tag: 'device-identity', attrs: {}, content: deviceIdentity }
                    ]
                });
                ev.emit('creds.update', update);
            }
            await sendNode(receipt);
            logger.info({ msgAttrs: node.attrs, retryCount }, 'sent retry receipt');
        }, authState?.creds?.me?.id || 'sendRetryRequest');
    };
    const handleEncryptNotification = async (node) => {
        const from = node.attrs.from;
        if (from === index_js_5.S_WHATSAPP_NET) {
            const countChild = (0, index_js_5.getBinaryNodeChild)(node, 'count');
            const count = +countChild.attrs.value;
            const shouldUploadMorePreKeys = count < index_js_2.MIN_PREKEY_COUNT;
            logger.debug({ count, shouldUploadMorePreKeys }, 'recv pre-key count');
            if (shouldUploadMorePreKeys) {
                await uploadPreKeys();
            }
        }
        else {
            const result = await (0, index_js_4.handleIdentityChange)(node, {
                meId: authState.creds.me?.id,
                meLid: authState.creds.me?.lid,
                validateSession: signalRepository.validateSession,
                assertSessions,
                debounceCache: identityAssertDebounce,
                logger
            });
            if (result.action === 'no_identity_node') {
                logger.info({ node }, 'unknown encrypt notification');
            }
        }
    };
    const handleGroupNotification = (fullNode, child, msg) => {
        // TODO: Support PN/LID (Here is only LID now)
        const actingParticipantLid = fullNode.attrs.participant;
        const actingParticipantPn = fullNode.attrs.participant_pn;
        const affectedParticipantLid = (0, index_js_5.getBinaryNodeChild)(child, 'participant')?.attrs?.jid || actingParticipantLid;
        const affectedParticipantPn = (0, index_js_5.getBinaryNodeChild)(child, 'participant')?.attrs?.phone_number || actingParticipantPn;
        switch (child?.tag) {
            case 'create':
                const metadata = (0, groups_js_1.extractGroupMetadata)(child);
                msg.messageStubType = index_js_3.WAMessageStubType.GROUP_CREATE;
                msg.messageStubParameters = [metadata.subject];
                msg.key = { participant: metadata.owner, participantAlt: metadata.ownerPn };
                ev.emit('chats.upsert', [
                    {
                        id: metadata.id,
                        name: metadata.subject,
                        conversationTimestamp: metadata.creation
                    }
                ]);
                ev.emit('groups.upsert', [
                    {
                        ...metadata,
                        author: actingParticipantLid,
                        authorPn: actingParticipantPn
                    }
                ]);
                break;
            case 'ephemeral':
            case 'not_ephemeral':
                msg.message = {
                    protocolMessage: {
                        type: index_js_1.proto.Message.ProtocolMessage.Type.EPHEMERAL_SETTING,
                        ephemeralExpiration: +(child.attrs.expiration || 0)
                    }
                };
                break;
            case 'modify':
                const oldNumber = (0, index_js_5.getBinaryNodeChildren)(child, 'participant').map(p => p.attrs.jid);
                msg.messageStubParameters = oldNumber || [];
                msg.messageStubType = index_js_3.WAMessageStubType.GROUP_PARTICIPANT_CHANGE_NUMBER;
                break;
            case 'promote':
            case 'demote':
            case 'remove':
            case 'add':
            case 'leave':
                const stubType = `GROUP_PARTICIPANT_${child.tag.toUpperCase()}`;
                msg.messageStubType = index_js_3.WAMessageStubType[stubType];
                const participants = (0, index_js_5.getBinaryNodeChildren)(child, 'participant').map(({ attrs }) => {
                    // TODO: Store LID MAPPINGS
                    return {
                        id: attrs.jid,
                        phoneNumber: (0, index_js_5.isLidUser)(attrs.jid) && (0, index_js_5.isPnUser)(attrs.phone_number) ? attrs.phone_number : undefined,
                        lid: (0, index_js_5.isPnUser)(attrs.jid) && (0, index_js_5.isLidUser)(attrs.lid) ? attrs.lid : undefined,
                        admin: (attrs.type || null)
                    };
                });
                if (participants.length === 1 &&
                    // if recv. "remove" message and sender removed themselves
                    // mark as left
                    ((0, index_js_5.areJidsSameUser)(participants[0].id, actingParticipantLid) ||
                        (0, index_js_5.areJidsSameUser)(participants[0].id, actingParticipantPn)) &&
                    child.tag === 'remove') {
                    msg.messageStubType = index_js_3.WAMessageStubType.GROUP_PARTICIPANT_LEAVE;
                }
                msg.messageStubParameters = participants.map(a => JSON.stringify(a));
                break;
            case 'subject':
                msg.messageStubType = index_js_3.WAMessageStubType.GROUP_CHANGE_SUBJECT;
                msg.messageStubParameters = [child.attrs.subject];
                break;
            case 'description':
                const description = (0, index_js_5.getBinaryNodeChild)(child, 'body')?.content?.toString();
                msg.messageStubType = index_js_3.WAMessageStubType.GROUP_CHANGE_DESCRIPTION;
                msg.messageStubParameters = description ? [description] : undefined;
                break;
            case 'announcement':
            case 'not_announcement':
                msg.messageStubType = index_js_3.WAMessageStubType.GROUP_CHANGE_ANNOUNCE;
                msg.messageStubParameters = [child.tag === 'announcement' ? 'on' : 'off'];
                break;
            case 'locked':
            case 'unlocked':
                msg.messageStubType = index_js_3.WAMessageStubType.GROUP_CHANGE_RESTRICT;
                msg.messageStubParameters = [child.tag === 'locked' ? 'on' : 'off'];
                break;
            case 'invite':
                msg.messageStubType = index_js_3.WAMessageStubType.GROUP_CHANGE_INVITE_LINK;
                msg.messageStubParameters = [child.attrs.code];
                break;
            case 'member_add_mode':
                const addMode = child.content;
                if (addMode) {
                    msg.messageStubType = index_js_3.WAMessageStubType.GROUP_MEMBER_ADD_MODE;
                    msg.messageStubParameters = [addMode.toString()];
                }
                break;
            case 'membership_approval_mode':
                const approvalMode = (0, index_js_5.getBinaryNodeChild)(child, 'group_join');
                if (approvalMode) {
                    msg.messageStubType = index_js_3.WAMessageStubType.GROUP_MEMBERSHIP_JOIN_APPROVAL_MODE;
                    msg.messageStubParameters = [approvalMode.attrs.state];
                }
                break;
            case 'created_membership_requests':
                msg.messageStubType = index_js_3.WAMessageStubType.GROUP_MEMBERSHIP_JOIN_APPROVAL_REQUEST_NON_ADMIN_ADD;
                msg.messageStubParameters = [
                    JSON.stringify({ lid: affectedParticipantLid, pn: affectedParticipantPn }),
                    'created',
                    child.attrs.request_method
                ];
                break;
            case 'revoked_membership_requests':
                const isDenied = (0, index_js_5.areJidsSameUser)(affectedParticipantLid, actingParticipantLid);
                // TODO: LIDMAPPING SUPPORT
                msg.messageStubType = index_js_3.WAMessageStubType.GROUP_MEMBERSHIP_JOIN_APPROVAL_REQUEST_NON_ADMIN_ADD;
                msg.messageStubParameters = [
                    JSON.stringify({ lid: affectedParticipantLid, pn: affectedParticipantPn }),
                    isDenied ? 'revoked' : 'rejected'
                ];
                break;
        }
    };
    const processNotification = async (node) => {
        const result = {};
        const [child] = (0, index_js_5.getAllBinaryNodeChildren)(node);
        const nodeType = node.attrs.type;
        const from = (0, index_js_5.jidNormalizedUser)(node.attrs.from);
        switch (nodeType) {
            case 'newsletter':
                await handleNewsletterNotification(node);
                break;
            case 'mex':
                await handleMexNewsletterNotification(node);
                break;
            case 'w:gp2':
                // TODO: HANDLE PARTICIPANT_PN
                handleGroupNotification(node, child, result);
                break;
            case 'mediaretry':
                const event = (0, index_js_4.decodeMediaRetryNode)(node);
                ev.emit('messages.media-update', [event]);
                break;
            case 'encrypt':
                await handleEncryptNotification(node);
                break;
            case 'devices':
                const devices = (0, index_js_5.getBinaryNodeChildren)(child, 'device');
                if ((0, index_js_5.areJidsSameUser)(child.attrs.jid, authState.creds.me.id) ||
                    (0, index_js_5.areJidsSameUser)(child.attrs.lid, authState.creds.me.lid)) {
                    const deviceData = devices.map(d => ({ id: d.attrs.jid, lid: d.attrs.lid }));
                    logger.info({ deviceData }, 'my own devices changed');
                }
                //TODO: drop a new event, add hashes
                break;
            case 'server_sync':
                const update = (0, index_js_5.getBinaryNodeChild)(node, 'collection');
                if (update) {
                    const name = update.attrs.name;
                    await resyncAppState([name], false);
                }
                break;
            case 'picture':
                const setPicture = (0, index_js_5.getBinaryNodeChild)(node, 'set');
                const delPicture = (0, index_js_5.getBinaryNodeChild)(node, 'delete');
                // TODO: WAJIDHASH stuff proper support inhouse
                ev.emit('contacts.update', [
                    {
                        id: (0, index_js_5.jidNormalizedUser)(node?.attrs?.from) || (setPicture || delPicture)?.attrs?.hash || '',
                        imgUrl: setPicture ? 'changed' : 'removed'
                    }
                ]);
                if ((0, index_js_5.isJidGroup)(from)) {
                    const node = setPicture || delPicture;
                    result.messageStubType = index_js_3.WAMessageStubType.GROUP_CHANGE_ICON;
                    if (setPicture) {
                        result.messageStubParameters = [setPicture.attrs.id];
                    }
                    result.participant = node?.attrs.author;
                    result.key = {
                        ...(result.key || {}),
                        participant: setPicture?.attrs.author
                    };
                }
                break;
            case 'account_sync':
                if (child.tag === 'disappearing_mode') {
                    const newDuration = +child.attrs.duration;
                    const timestamp = +child.attrs.t;
                    logger.info({ newDuration }, 'updated account disappearing mode');
                    ev.emit('creds.update', {
                        accountSettings: {
                            ...authState.creds.accountSettings,
                            defaultDisappearingMode: {
                                ephemeralExpiration: newDuration,
                                ephemeralSettingTimestamp: timestamp
                            }
                        }
                    });
                }
                else if (child.tag === 'blocklist') {
                    const blocklists = (0, index_js_5.getBinaryNodeChildren)(child, 'item');
                    for (const { attrs } of blocklists) {
                        const blocklist = [attrs.jid];
                        const type = attrs.action === 'block' ? 'add' : 'remove';
                        ev.emit('blocklist.update', { blocklist, type });
                    }
                }
                break;
            case 'link_code_companion_reg':
                const linkCodeCompanionReg = (0, index_js_5.getBinaryNodeChild)(node, 'link_code_companion_reg');
                const ref = toRequiredBuffer((0, index_js_5.getBinaryNodeChildBuffer)(linkCodeCompanionReg, 'link_code_pairing_ref'));
                const primaryIdentityPublicKey = toRequiredBuffer((0, index_js_5.getBinaryNodeChildBuffer)(linkCodeCompanionReg, 'primary_identity_pub'));
                const primaryEphemeralPublicKeyWrapped = toRequiredBuffer((0, index_js_5.getBinaryNodeChildBuffer)(linkCodeCompanionReg, 'link_code_pairing_wrapped_primary_ephemeral_pub'));
                const codePairingPublicKey = await decipherLinkPublicKey(primaryEphemeralPublicKeyWrapped);
                const companionSharedKey = index_js_4.Curve.sharedKey(authState.creds.pairingEphemeralKeyPair.private, codePairingPublicKey);
                const random = (0, crypto_1.randomBytes)(32);
                const linkCodeSalt = (0, crypto_1.randomBytes)(32);
                const linkCodePairingExpanded = (0, index_js_4.hkdf)(companionSharedKey, 32, {
                    salt: linkCodeSalt,
                    info: 'link_code_pairing_key_bundle_encryption_key'
                });
                const encryptPayload = Buffer.concat([
                    Buffer.from(authState.creds.signedIdentityKey.public),
                    primaryIdentityPublicKey,
                    random
                ]);
                const encryptIv = (0, crypto_1.randomBytes)(12);
                const encrypted = (0, index_js_4.aesEncryptGCM)(encryptPayload, linkCodePairingExpanded, encryptIv, Buffer.alloc(0));
                const encryptedPayload = Buffer.concat([linkCodeSalt, encryptIv, encrypted]);
                const identitySharedKey = index_js_4.Curve.sharedKey(authState.creds.signedIdentityKey.private, primaryIdentityPublicKey);
                const identityPayload = Buffer.concat([companionSharedKey, identitySharedKey, random]);
                authState.creds.advSecretKey = Buffer.from((0, index_js_4.hkdf)(identityPayload, 32, { info: 'adv_secret' })).toString('base64');
                await query({
                    tag: 'iq',
                    attrs: {
                        to: index_js_5.S_WHATSAPP_NET,
                        type: 'set',
                        id: sock.generateMessageTag(),
                        xmlns: 'md'
                    },
                    content: [
                        {
                            tag: 'link_code_companion_reg',
                            attrs: {
                                jid: authState.creds.me.id,
                                stage: 'companion_finish'
                            },
                            content: [
                                {
                                    tag: 'link_code_pairing_wrapped_key_bundle',
                                    attrs: {},
                                    content: encryptedPayload
                                },
                                {
                                    tag: 'companion_identity_public',
                                    attrs: {},
                                    content: authState.creds.signedIdentityKey.public
                                },
                                {
                                    tag: 'link_code_pairing_ref',
                                    attrs: {},
                                    content: ref
                                }
                            ]
                        }
                    ]
                });
                authState.creds.registered = true;
                ev.emit('creds.update', authState.creds);
                break;
            case 'privacy_token':
                await handlePrivacyTokenNotification(node);
                break;
        }
        if (Object.keys(result).length) {
            return result;
        }
    };
    const handlePrivacyTokenNotification = async (node) => {
        const tokensNode = (0, index_js_5.getBinaryNodeChild)(node, 'tokens');
        const from = (0, index_js_5.jidNormalizedUser)(node.attrs.from);
        if (!tokensNode)
            return;
        const tokenNodes = (0, index_js_5.getBinaryNodeChildren)(tokensNode, 'token');
        for (const tokenNode of tokenNodes) {
            const { attrs, content } = tokenNode;
            const type = attrs.type;
            const timestamp = attrs.t;
            if (type === 'trusted_contact' && content instanceof Buffer) {
                logger.debug({
                    from,
                    timestamp,
                    tcToken: content
                }, 'received trusted contact token');
                await authState.keys.set({
                    tctoken: { [from]: { token: content, timestamp } }
                });
            }
        }
    };
    async function decipherLinkPublicKey(data) {
        const buffer = toRequiredBuffer(data);
        const salt = buffer.slice(0, 32);
        const secretKey = await (0, index_js_4.derivePairingCodeKey)(authState.creds.pairingCode, salt);
        const iv = buffer.slice(32, 48);
        const payload = buffer.slice(48, 80);
        return (0, index_js_4.aesDecryptCTR)(payload, secretKey, iv);
    }
    function toRequiredBuffer(data) {
        if (data === undefined) {
            throw new boom_1.Boom('Invalid buffer', { statusCode: 400 });
        }
        return data instanceof Buffer ? data : Buffer.from(data);
    }
    const willSendMessageAgain = async (id, participant) => {
        const key = `${id}:${participant}`;
        const retryCount = (await msgRetryCache.get(key)) || 0;
        return retryCount < maxMsgRetryCount;
    };
    const updateSendMessageAgainCount = async (id, participant) => {
        const key = `${id}:${participant}`;
        const newValue = ((await msgRetryCache.get(key)) || 0) + 1;
        await msgRetryCache.set(key, newValue);
    };
    const sendMessagesAgain = async (key, ids, retryNode) => {
        const remoteJid = key.remoteJid;
        const participant = key.participant || remoteJid;
        const retryCount = +retryNode.attrs.count || 1;
        // Try to get messages from cache first, then fallback to getMessage
        const msgs = [];
        for (const id of ids) {
            let msg;
            // Try to get from retry cache first if enabled
            if (messageRetryManager) {
                const cachedMsg = messageRetryManager.getRecentMessage(remoteJid, id);
                if (cachedMsg) {
                    msg = cachedMsg.message;
                    logger.debug({ jid: remoteJid, id }, 'found message in retry cache');
                    // Mark retry as successful since we found the message
                    messageRetryManager.markRetrySuccess(id);
                }
            }
            // Fallback to getMessage if not found in cache
            if (!msg) {
                msg = await getMessage({ ...key, id });
                if (msg) {
                    logger.debug({ jid: remoteJid, id }, 'found message via getMessage');
                    // Also mark as successful if found via getMessage
                    if (messageRetryManager) {
                        messageRetryManager.markRetrySuccess(id);
                    }
                }
            }
            msgs.push(msg);
        }
        // if it's the primary jid sending the request
        // just re-send the message to everyone
        // prevents the first message decryption failure
        const sendToAll = !(0, index_js_5.jidDecode)(participant)?.device;
        // Check if we should recreate session for this retry
        let shouldRecreateSession = false;
        let recreateReason = '';
        if (enableAutoSessionRecreation && messageRetryManager && retryCount > 1) {
            try {
                const sessionId = signalRepository.jidToSignalProtocolAddress(participant);
                const hasSession = await signalRepository.validateSession(participant);
                const result = messageRetryManager.shouldRecreateSession(participant, hasSession.exists);
                shouldRecreateSession = result.recreate;
                recreateReason = result.reason;
                if (shouldRecreateSession) {
                    logger.debug({ participant, retryCount, reason: recreateReason }, 'recreating session for outgoing retry');
                    await authState.keys.set({ session: { [sessionId]: null } });
                }
            }
            catch (error) {
                logger.warn({ error, participant }, 'failed to check session recreation for outgoing retry');
            }
        }
        await assertSessions([participant], true);
        if ((0, index_js_5.isJidGroup)(remoteJid)) {
            await authState.keys.set({ 'sender-key-memory': { [remoteJid]: null } });
        }
        logger.debug({ participant, sendToAll, shouldRecreateSession, recreateReason }, 'forced new session for retry recp');
        for (const [i, msg] of msgs.entries()) {
            if (!ids[i])
                continue;
            if (msg && (await willSendMessageAgain(ids[i], participant))) {
                await updateSendMessageAgainCount(ids[i], participant);
                const msgRelayOpts = { messageId: ids[i] };
                if (sendToAll) {
                    msgRelayOpts.useUserDevicesCache = false;
                }
                else {
                    msgRelayOpts.participant = {
                        jid: participant,
                        count: +retryNode.attrs.count
                    };
                }
                await relayMessage(key.remoteJid, msg, msgRelayOpts);
            }
            else {
                logger.debug({ jid: key.remoteJid, id: ids[i] }, 'recv retry request, but message not available');
            }
        }
    };
    const handleReceipt = async (node) => {
        const { attrs, content } = node;
        const isLid = attrs.from.includes('lid');
        const isNodeFromMe = (0, index_js_5.areJidsSameUser)(attrs.participant || attrs.from, isLid ? authState.creds.me?.lid : authState.creds.me?.id);
        const remoteJid = !isNodeFromMe || (0, index_js_5.isJidGroup)(attrs.from) ? attrs.from : attrs.recipient;
        const fromMe = !attrs.recipient || ((attrs.type === 'retry' || attrs.type === 'sender') && isNodeFromMe);
        const key = {
            remoteJid,
            id: '',
            fromMe,
            participant: attrs.participant
        };
        if (shouldIgnoreJid(remoteJid) && remoteJid !== index_js_5.S_WHATSAPP_NET) {
            logger.debug({ remoteJid }, 'ignoring receipt from jid');
            await sendMessageAck(node);
            return;
        }
        const ids = [attrs.id];
        if (Array.isArray(content)) {
            const items = (0, index_js_5.getBinaryNodeChildren)(content[0], 'item');
            ids.push(...items.map(i => i.attrs.id));
        }
        try {
            await Promise.all([
                receiptMutex.mutex(async () => {
                    const status = (0, index_js_4.getStatusFromReceiptType)(attrs.type);
                    if (typeof status !== 'undefined' &&
                        // basically, we only want to know when a message from us has been delivered to/read by the other person
                        // or another device of ours has read some messages
                        (status >= index_js_1.proto.WebMessageInfo.Status.SERVER_ACK || !isNodeFromMe)) {
                        if ((0, index_js_5.isJidGroup)(remoteJid) || (0, index_js_5.isJidStatusBroadcast)(remoteJid)) {
                            if (attrs.participant) {
                                const updateKey = status === index_js_1.proto.WebMessageInfo.Status.DELIVERY_ACK ? 'receiptTimestamp' : 'readTimestamp';
                                ev.emit('message-receipt.update', ids.map(id => ({
                                    key: { ...key, id },
                                    receipt: {
                                        userJid: (0, index_js_5.jidNormalizedUser)(attrs.participant),
                                        [updateKey]: +attrs.t
                                    }
                                })));
                            }
                        }
                        else {
                            ev.emit('messages.update', ids.map(id => ({
                                key: { ...key, id },
                                update: { status, messageTimestamp: (0, index_js_4.toNumber)(+(attrs.t ?? 0)) }
                            })));
                        }
                    }
                    if (attrs.type === 'retry') {
                        // correctly set who is asking for the retry
                        key.participant = key.participant || attrs.from;
                        const retryNode = (0, index_js_5.getBinaryNodeChild)(node, 'retry');
                        if (ids[0] && key.participant && (await willSendMessageAgain(ids[0], key.participant))) {
                            if (key.fromMe) {
                                try {
                                    await updateSendMessageAgainCount(ids[0], key.participant);
                                    logger.debug({ attrs, key }, 'recv retry request');
                                    await sendMessagesAgain(key, ids, retryNode);
                                }
                                catch (error) {
                                    logger.error({ key, ids, trace: error instanceof Error ? error.stack : 'Unknown error' }, 'error in sending message again');
                                }
                            }
                            else {
                                logger.info({ attrs, key }, 'recv retry for not fromMe message');
                            }
                        }
                        else {
                            logger.info({ attrs, key }, 'will not send message again, as sent too many times');
                        }
                    }
                })
            ]);
        }
        finally {
            await sendMessageAck(node);
        }
    };
    const handleNotification = async (node) => {
        const remoteJid = node.attrs.from;
        if (shouldIgnoreJid(remoteJid) && remoteJid !== index_js_5.S_WHATSAPP_NET) {
            logger.debug({ remoteJid, id: node.attrs.id }, 'ignored notification');
            await sendMessageAck(node);
            return;
        }
        // ✅ Kirim ACK segera ke WA server agar koneksi tidak dianggap timeout
        // Proses notifikasi dilakukan di background via setImmediate — non-blocking
        sendMessageAck(node).catch(err => onUnexpectedError(err, 'sending notification ack'));
        setImmediate(async () => {
            try {
                await notificationMutex.mutex(async () => {
                    const msg = await processNotification(node);
                    if (msg) {
                        const fromMe = (0, index_js_5.areJidsSameUser)(node.attrs.participant || remoteJid, authState.creds.me.id);
                        const { senderAlt: participantAlt, addressingMode } = (0, index_js_4.extractAddressingContext)(node);
                        msg.key = {
                            remoteJid,
                            fromMe,
                            participant: node.attrs.participant,
                            participantAlt,
                            addressingMode,
                            id: node.attrs.id,
                            ...(msg.key || {})
                        };
                        msg.participant ?? (msg.participant = node.attrs.participant);
                        msg.messageTimestamp = +node.attrs.t;
                        const fullMsg = index_js_1.proto.WebMessageInfo.fromObject(msg);
                        await upsertMessage(fullMsg, 'append');
                    }
                });
            }
            catch (err) {
                onUnexpectedError(err, 'handling notification');
            }
        });
    };
    const resolveMentionedLIDs = async (msg, lidMapping) => {
        if (msg.key?.participant?.endsWith('@lid')) {
            try {
                const pn = await lidMapping.getPNForLID(msg.key.participant);
                if (pn) {
                    logger.debug({ lid: msg.key.participant, pn }, 'resolved key.participant LID → PN');
                    msg.key.participant = pn;
                }
            }
            catch { }
        }
        if (msg.key?.remoteJid?.endsWith('@lid')) {
            try {
                const pn = await lidMapping.getPNForLID(msg.key.remoteJid);
                if (pn) {
                    logger.debug({ lid: msg.key.remoteJid, pn }, 'resolved key.remoteJid LID → PN');
                    msg.key.remoteJid = pn;
                }
            }
            catch { }
        }
        const msgContent = msg.message;
        if (!msgContent)
            return;
        const getContextInfo = (content) => {
            if (!content || typeof content !== 'object')
                return null;
            if (content.contextInfo)
                return content.contextInfo;
            for (const val of Object.values(content)) {
                const found = getContextInfo(val);
                if (found)
                    return found;
            }
            return null;
        };
        const getTextField = (content) => {
            if (!content || typeof content !== 'object')
                return null;
            for (const key of ['text', 'caption', 'conversation']) {
                if (typeof content[key] === 'string')
                    return { obj: content, key };
            }
            for (const val of Object.values(content)) {
                const found = getTextField(val);
                if (found)
                    return found;
            }
            return null;
        };
        // Resolve semua contextInfo termasuk yang ada di dalam quotedMessage
        const getAllContextInfos = (content, results = []) => {
            if (!content || typeof content !== 'object')
                return results;
            if (content.contextInfo) {
                results.push(content.contextInfo);
                // Juga cari di dalam quotedMessage secara rekursif
                if (content.contextInfo.quotedMessage) {
                    getAllContextInfos(content.contextInfo.quotedMessage, results);
                }
            }
            for (const val of Object.values(content)) {
                if (val && typeof val === 'object' && !results.includes(val)) {
                    getAllContextInfos(val, results);
                }
            }
            return results;
        };
        const contextInfo = getContextInfo(msgContent);
        // Resolve participant LID di semua contextInfo (termasuk quotedMessage)
        const allContextInfos = getAllContextInfos(msgContent);
        for (const ctx of allContextInfos) {
            if (ctx?.participant?.endsWith('@lid')) {
                try {
                    const pn = await lidMapping.getPNForLID(ctx.participant);
                    if (pn) {
                        logger.debug({ lid: ctx.participant, pn }, 'resolved nested contextInfo.participant LID → PN');
                        ctx.participant = pn;
                    }
                }
                catch { }
            }
            // Resolve mentionedJid di quotedMessage contextInfo juga
            if (ctx !== contextInfo && ctx?.mentionedJid?.length) {
                const lids = ctx.mentionedJid.filter(j => j?.endsWith('@lid'));
                for (const lid of lids) {
                    try {
                        const pn = await lidMapping.getPNForLID(lid);
                        if (pn)
                            ctx.mentionedJid = ctx.mentionedJid.map(j => j === lid ? pn : j);
                    }
                    catch { }
                }
            }
        }
        // resolve contextInfo.participant (sender of quoted message) jika masih LID
        if (contextInfo?.participant?.endsWith('@lid')) {
            try {
                const pn = await lidMapping.getPNForLID(contextInfo.participant);
                if (pn) {
                    logger.debug({ lid: contextInfo.participant, pn }, 'resolved contextInfo.participant LID → PN');
                    contextInfo.participant = pn;
                }
            }
            catch { }
        }
        if (!contextInfo?.mentionedJid?.length)
            return;
        // Fix text yang masih mengandung LID number meskipun mentionedJid sudah resolved ke PN
        // Kasus: mentionedJid sudah jadi @s.whatsapp.net tapi text masih "@165159209271535" (LID num)
        const textFieldEarly = getTextField(msgContent);
        if (textFieldEarly) {
            let earlyText = textFieldEarly.obj[textFieldEarly.key] || '';
            // Cek apakah text mengandung @angka panjang (>12 digit = kemungkinan LID number bukan nomor HP)
            const lidNumPattern = /@(\d{13,20})/g;
            const lidNumMatches = [...earlyText.matchAll(lidNumPattern)];
            if (lidNumMatches.length > 0) {
                // Untuk setiap mentionedJid yang sudah resolved, cek apakah ada LID number di text
                for (const resolvedJid of contextInfo.mentionedJid) {
                    if (resolvedJid?.endsWith('@lid'))
                        continue; // belum resolved, skip dulu
                    const pnNum = resolvedJid.split('@')[0].split(':')[0];
                    if (!pnNum)
                        continue;
                    // Cari LID number di text yang paling mungkin match (berdasarkan urutan)
                    for (const match of lidNumMatches) {
                        const lidNum = match[1];
                        if (earlyText.includes(`@${lidNum}`)) {
                            earlyText = earlyText.split(`@${lidNum}`).join(`@${pnNum}`);
                            logger.debug({ lidNum, pnNum }, 'replaced LID number in text with PN number');
                            break;
                        }
                    }
                }
                textFieldEarly.obj[textFieldEarly.key] = earlyText;
            }
        }
        const hasLid = contextInfo.mentionedJid.some((j) => j?.endsWith('@lid'));
        if (!hasLid)
            return;
        const lidJids = contextInfo.mentionedJid.filter((j) => j?.endsWith('@lid'));
        const resolveMap = new Map();
        const stillUnresolved = [];
        for (const lidJid of lidJids) {
            try {
                const pn = await lidMapping.getPNForLID(lidJid);
                if (pn) {
                    resolveMap.set(lidJid, pn);
                }
                else {
                    stillUnresolved.push(lidJid);
                }
            }
            catch {
                stillUnresolved.push(lidJid);
            }
        }
        if (stillUnresolved.length > 0) {
            try {
                const usyncQ = new index_js_6.USyncQuery()
                    .withContactProtocol()
                    .withContext('background');
                for (const lidJid of stillUnresolved) {
                    usyncQ.withUser(new index_js_6.USyncUser().withId(lidJid));
                }
                const result = await sock.executeUSyncQuery(usyncQ);
                if (result?.list) {
                    const mappings = [];
                    for (const item of result.list) {
                        // item.id bisa berupa PN (@s.whatsapp.net) atau LID (@lid)
                        // stillUnresolved berisi LID. Match berdasarkan numeric prefix
                        const itemNum = (item.id ?? '').split('@')[0].split(':')[0];
                        const lidJid = stillUnresolved.find(l => {
                            if (l === item.id)
                                return true;
                            const lNum = l.split('@')[0].split(':')[0];
                            return itemNum && lNum && itemNum === lNum;
                        });
                        if (lidJid && item.id && !item.id.endsWith('@lid')) {
                            resolveMap.set(lidJid, item.id);
                            mappings.push({ lid: lidJid, pn: item.id });
                            logger.debug({ lid: lidJid, pn: item.id }, 'USync resolved LID → PN');
                        }
                    }
                    if (mappings.length > 0) {
                        lidMapping.storeLIDPNMappings(mappings).catch(() => { });
                    }
                }
            }
            catch (e) {
                logger.debug({ err: e }, 'USync LID resolve failed, using cache only');
            }
        }
        // ── Text-based PN extraction ─────────────────────────────────────────────
        // WA kadang sudah resolve nomor HP di text (@6285133801810) tapi mentionedJid
        // masih berisi LID. Kita ekstrak semua @nomor dari text lalu matching ke LID
        // yang masih unresolved berdasarkan urutan kemunculan di mentionedJid.
        const textField = getTextField(msgContent);
        const stillUnresolvedAfterUSync = lidJids.filter(l => !resolveMap.has(l));
        if (stillUnresolvedAfterUSync.length > 0 && textField) {
            const rawText = textField.obj[textField.key] || '';
            // Ekstrak semua mention @nomor dari text (hanya angka, min 7 digit)
            const mentionMatches = [...rawText.matchAll(/@(\d{7,15})/g)].map(m => m[1]);
            if (mentionMatches.length > 0) {
                // Match LID ke nomor berdasarkan urutan index di mentionedJid
                const lidOrder = contextInfo.mentionedJid
                    .map((jid, idx) => ({ jid, idx }))
                    .filter(({ jid }) => stillUnresolvedAfterUSync.includes(jid));
                for (let i = 0; i < lidOrder.length && i < mentionMatches.length; i++) {
                    const lidJid = lidOrder[i].jid;
                    const phoneNum = mentionMatches[i];
                    const pnJid = `${phoneNum}@s.whatsapp.net`;
                    resolveMap.set(lidJid, pnJid);
                    // Simpan mapping baru ini ke store untuk request berikutnya
                    lidMapping.storeLIDPNMappings([{ lid: lidJid, pn: pnJid }]).catch(() => { });
                    logger.debug({ lid: lidJid, pn: pnJid }, 'text-extracted PN for LID → PN');
                }
            }
        }
        // ── End text-based PN extraction ─────────────────────────────────────────
        contextInfo.mentionedJid = contextInfo.mentionedJid.map((jid) => {
            if (!jid?.endsWith('@lid'))
                return jid;
            const resolved = resolveMap.get(jid);
            if (resolved) {
                logger.debug({ lid: jid, pn: resolved }, 'resolved mentionedJid LID → PN');
                return resolved;
            }
            // Tetap kembalikan LID jika benar-benar tidak bisa di-resolve
            return jid;
        });
        if (textField) {
            let text = textField.obj[textField.key];
            // Replace LID number di text dengan PN number yang sudah di-resolve
            for (const [lidJid, pnJid] of resolveMap) {
                const lidNum = lidJid.split('@')[0].split(':')[0] ?? '';
                const pnNum = pnJid.replace('@s.whatsapp.net', '').split(':')[0] ?? '';
                if (lidNum && pnNum && text.includes(lidNum)) {
                    text = text.split(lidNum).join(pnNum);
                }
            }
            textField.obj[textField.key] = text;
        }
    };
    const handleMessage = async (node) => {
        if (shouldIgnoreJid(node.attrs.from) && node.attrs.from !== index_js_5.S_WHATSAPP_NET) {
            logger.debug({ key: node.attrs.key }, 'ignored message');
            await sendMessageAck(node, index_js_4.NACK_REASONS.UnhandledError);
            return;
        }
        const encNode = (0, index_js_5.getBinaryNodeChild)(node, 'enc');
        // TODO: temporary fix for crashes and issues resulting of failed msmsg decryption
        if (encNode?.attrs.type === 'msmsg') {
            logger.debug({ key: node.attrs.key }, 'ignored msmsg');
            await sendMessageAck(node, index_js_4.NACK_REASONS.MissingMessageSecret);
            return;
        }
        const { fullMessage: msg, category, author, decrypt } = (0, index_js_4.decryptMessageNode)(node, authState.creds.me.id, authState.creds.me.lid || '', signalRepository, logger);
        const alt = msg.key.participantAlt || msg.key.remoteJidAlt;
        // store new mappings we didn't have before
        if (!!alt) {
            const altServer = (0, index_js_5.jidDecode)(alt)?.server;
            const primaryJid = msg.key.participant || msg.key.remoteJid;
            if (altServer === 'lid') {
                if (!(await signalRepository.lidMapping.getPNForLID(alt))) {
                    await signalRepository.lidMapping.storeLIDPNMappings([{ lid: alt, pn: primaryJid }]);
                    await signalRepository.migrateSession(primaryJid, alt);
                }
            }
            else {
                await signalRepository.lidMapping.storeLIDPNMappings([{ lid: primaryJid, pn: alt }]);
                await signalRepository.migrateSession(alt, primaryJid);
            }
        }
        if (msg.key?.remoteJid && msg.key?.id && messageRetryManager) {
            messageRetryManager.addRecentMessage(msg.key.remoteJid, msg.key.id, msg.message);
            logger.debug({
                jid: msg.key.remoteJid,
                id: msg.key.id
            }, 'Added message to recent cache for retry receipts');
        }
        try {
            await messageMutex.mutex(async () => {
                await decrypt();
                // message failed to decrypt
                if (msg.messageStubType === index_js_1.proto.WebMessageInfo.StubType.CIPHERTEXT && msg.category !== 'peer') {
                    if (msg?.messageStubParameters?.[0] === index_js_4.MISSING_KEYS_ERROR_TEXT) {
                        return sendMessageAck(node, index_js_4.NACK_REASONS.ParsingError);
                    }
                    if (msg.messageStubParameters?.[0] === index_js_4.NO_MESSAGE_FOUND_ERROR_TEXT) {
                        // Message arrived without encryption (e.g. CTWA ads messages).
                        // Check if this is eligible for placeholder resend (matching WA Web filters).
                        const unavailableNode = (0, index_js_5.getBinaryNodeChild)(node, 'unavailable');
                        const unavailableType = unavailableNode?.attrs?.type;
                        if (unavailableType === 'bot_unavailable_fanout' ||
                            unavailableType === 'hosted_unavailable_fanout' ||
                            unavailableType === 'view_once_unavailable_fanout') {
                            logger.debug({ msgId: msg.key.id, unavailableType }, 'skipping placeholder resend for excluded unavailable type');
                            return sendMessageAck(node);
                        }
                        const messageAge = (0, index_js_4.unixTimestampSeconds)() - (0, index_js_4.toNumber)(msg.messageTimestamp);
                        if (messageAge > index_js_2.PLACEHOLDER_MAX_AGE_SECONDS) {
                            logger.debug({ msgId: msg.key.id, messageAge }, 'skipping placeholder resend for old message');
                            return sendMessageAck(node);
                        }
                        // Request the real content from the phone via placeholder resend PDO.
                        // Upsert the CIPHERTEXT stub as a placeholder (like WA Web's processPlaceholderMsg),
                        // and store the requestId in stubParameters[1] so users can correlate
                        // with the incoming PDO response event.
                        const cleanKey = {
                            remoteJid: msg.key.remoteJid,
                            fromMe: msg.key.fromMe,
                            id: msg.key.id,
                            participant: msg.key.participant
                        };
                        // Cache the original message metadata so the PDO response handler
                        // can preserve key fields (LID details etc.) that the phone may omit
                        const msgData = {
                            key: msg.key,
                            messageTimestamp: msg.messageTimestamp,
                            pushName: msg.pushName,
                            participant: msg.participant,
                            verifiedBizName: msg.verifiedBizName
                        };
                        requestPlaceholderResend(cleanKey, msgData)
                            .then(requestId => {
                            if (requestId && requestId !== 'RESOLVED') {
                                logger.debug({ msgId: msg.key.id, requestId }, 'requested placeholder resend for unavailable message');
                                ev.emit('messages.update', [
                                    {
                                        key: msg.key,
                                        update: { messageStubParameters: [index_js_4.NO_MESSAGE_FOUND_ERROR_TEXT, requestId] }
                                    }
                                ]);
                            }
                        })
                            .catch(err => {
                            logger.warn({ err, msgId: msg.key.id }, 'failed to request placeholder resend for unavailable message');
                        });
                        await sendMessageAck(node);
                        // Don't return — fall through to upsertMessage so the stub is emitted
                    }
                    else {
                        // Skip retry for expired status messages (>24h old)
                        if ((0, index_js_5.isJidStatusBroadcast)(msg.key.remoteJid)) {
                            const messageAge = (0, index_js_4.unixTimestampSeconds)() - (0, index_js_4.toNumber)(msg.messageTimestamp);
                            if (messageAge > index_js_2.STATUS_EXPIRY_SECONDS) {
                                logger.debug({ msgId: msg.key.id, messageAge, remoteJid: msg.key.remoteJid }, 'skipping retry for expired status message');
                                return sendMessageAck(node);
                            }
                        }
                        const errorMessage = msg?.messageStubParameters?.[0] || '';
                        const isPreKeyError = errorMessage.includes('PreKey');
                        logger.debug(`[handleMessage] Attempting retry request for failed decryption`);
                        // Handle both pre-key and normal retries in single mutex
                        await retryMutex.mutex(async () => {
                            try {
                                if (!ws.isOpen) {
                                    logger.debug({ node }, 'Connection closed, skipping retry');
                                    return;
                                }
                                // Handle pre-key errors with upload and delay
                                if (isPreKeyError) {
                                    logger.info({ error: errorMessage }, 'PreKey error detected, uploading and retrying');
                                    try {
                                        logger.debug('Uploading pre-keys for error recovery');
                                        await uploadPreKeys(5);
                                        logger.debug('Waiting for server to process new pre-keys');
                                        await (0, index_js_4.delay)(1000);
                                    }
                                    catch (uploadErr) {
                                        logger.error({ uploadErr }, 'Pre-key upload failed, proceeding with retry anyway');
                                    }
                                }
                                const encNode = (0, index_js_5.getBinaryNodeChild)(node, 'enc');
                                await sendRetryRequest(node, !encNode);
                                if (retryRequestDelayMs) {
                                    await (0, index_js_4.delay)(retryRequestDelayMs);
                                }
                            }
                            catch (err) {
                                logger.error({ err, isPreKeyError }, 'Failed to handle retry, attempting basic retry');
                                // Still attempt retry even if pre-key upload failed
                                try {
                                    const encNode = (0, index_js_5.getBinaryNodeChild)(node, 'enc');
                                    await sendRetryRequest(node, !encNode);
                                }
                                catch (retryErr) {
                                    logger.error({ retryErr }, 'Failed to send retry after error handling');
                                }
                            }
                            await sendMessageAck(node, index_js_4.NACK_REASONS.UnhandledError);
                        });
                    }
                }
                else {
                    if (messageRetryManager && msg.key.id) {
                        messageRetryManager.cancelPendingPhoneRequest(msg.key.id);
                    }
                    const isNewsletter = (0, index_js_5.isJidNewsletter)(msg.key.remoteJid);
                    if (!isNewsletter) {
                        // no type in the receipt => message delivered
                        let type = undefined;
                        let participant = msg.key.participant;
                        if (category === 'peer') {
                            // special peer message
                            type = 'peer_msg';
                        }
                        else if (msg.key.fromMe) {
                            // message was sent by us from a different device
                            type = 'sender';
                            // need to specially handle this case
                            if ((0, index_js_5.isLidUser)(msg.key.remoteJid) || (0, index_js_5.isLidUser)(msg.key.remoteJidAlt)) {
                                participant = author; // TODO: investigate sending receipts to LIDs and not PNs
                            }
                        }
                        else if (!sendActiveReceipts) {
                            type = 'inactive';
                        }
                        await sendReceipt(msg.key.remoteJid, participant, [msg.key.id], type);
                        // send ack for history message
                        const isAnyHistoryMsg = (0, index_js_4.getHistoryMsg)(msg.message);
                        if (isAnyHistoryMsg) {
                            const jid = (0, index_js_5.jidNormalizedUser)(msg.key.remoteJid);
                            await sendReceipt(jid, undefined, [msg.key.id], 'hist_sync'); // TODO: investigate
                        }
                    }
                    else {
                        await sendMessageAck(node);
                        logger.debug({ key: msg.key }, 'processed newsletter message without receipts');
                    }
                }
                (0, index_js_4.cleanMessage)(msg, authState.creds.me.id, authState.creds.me.lid);
                await resolveMentionedLIDs(msg, signalRepository.lidMapping);
                // ── Post-resolve: fix semua LID yang tersisa di message object ────────
                // Setelah resolveMentionedLIDs:
                //   1. Fix text yang masih "@LIDnum" → "@PNnum"
                //   2. Fix contextInfo.participant yang masih @lid (sender quoted)
                //   3. Fix mentionedJid & text di dalam quotedMessage
                try {
                    const _safeSet = (obj, key, val) => {
                        try {
                            obj[key] = val;
                        }
                        catch (_) {
                            // proto readonly fallback
                            const proto = Object.getPrototypeOf(obj);
                            const fresh = Object.assign(Object.create(proto), obj, { [key]: val });
                            // replace reference di parent jika bisa — caller handles
                            return fresh;
                        }
                        return obj;
                    };
                    const _fixTextLid = (innerMsg, mentionedJids, msgTypeLabel) => {
                        const pnNumbers = (mentionedJids || [])
                            .filter(j => j && !j.endsWith('@lid'))
                            .map(j => j.split('@')[0].split(':')[0])
                            .filter(Boolean);
                        if (!pnNumbers.length)
                            return;
                        for (const textKey of ['text', 'caption', 'conversation']) {
                            const originalText = innerMsg[textKey];
                            if (typeof originalText !== 'string')
                                continue;
                            const lidPattern = /@(\d{13,20})/g;
                            let newText = originalText;
                            let match;
                            let pnIdx = 0;
                            while ((match = lidPattern.exec(originalText)) !== null) {
                                if (pnIdx >= pnNumbers.length)
                                    break;
                                const lidNum = match[1];
                                const pnNum = pnNumbers[pnIdx++];
                                newText = newText.split(`@${lidNum}`).join(`@${pnNum}`);
                                logger.debug({ lidNum, pnNum, type: msgTypeLabel, textKey }, 'post-resolve: replaced LID num in text');
                            }
                            if (newText !== originalText)
                                _safeSet(innerMsg, textKey, newText);
                        }
                    };
                    const msgObj = msg.message;
                    if (msgObj) {
                        for (const msgType of Object.keys(msgObj)) {
                            const innerMsg = msgObj[msgType];
                            if (!innerMsg || typeof innerMsg !== 'object')
                                continue;
                            const ctxInfo = innerMsg.contextInfo;
                            if (!ctxInfo)
                                continue;
                            // 1. Fix text LID → PN (pesan utama)
                            if (ctxInfo.mentionedJid?.length) {
                                _fixTextLid(innerMsg, ctxInfo.mentionedJid, msgType);
                            }
                            // 2. Fix contextInfo.participant (sender quoted) jika masih LID
                            if (ctxInfo.participant?.endsWith('@lid')) {
                                try {
                                    const pn = await signalRepository.lidMapping.getPNForLID(ctxInfo.participant);
                                    if (pn) {
                                        _safeSet(ctxInfo, 'participant', pn);
                                        logger.debug({ lid: ctxInfo.participant, pn }, 'post-resolve: fixed contextInfo.participant LID');
                                    }
                                }
                                catch (_) { }
                            }
                            // 3. Fix mentionedJid & text di dalam quotedMessage
                            if (ctxInfo.quotedMessage) {
                                for (const qType of Object.keys(ctxInfo.quotedMessage)) {
                                    const qInner = ctxInfo.quotedMessage[qType];
                                    if (!qInner || typeof qInner !== 'object')
                                        continue;
                                    const qCtx = qInner.contextInfo;
                                    if (!qCtx?.mentionedJid?.length)
                                        continue;
                                    // Resolve LID di mentionedJid quoted
                                    const resolvedQ = await Promise.all(qCtx.mentionedJid.map(async (jid) => {
                                        if (!jid?.endsWith('@lid'))
                                            return jid;
                                        try {
                                            const pn = await signalRepository.lidMapping.getPNForLID(jid);
                                            return pn || jid;
                                        }
                                        catch (_) {
                                            return jid;
                                        }
                                    }));
                                    if (resolvedQ.some((j, i) => j !== qCtx.mentionedJid[i])) {
                                        _safeSet(qCtx, 'mentionedJid', resolvedQ);
                                        logger.debug({ resolvedQ }, 'post-resolve: fixed quotedMessage mentionedJid');
                                    }
                                    // Fix text LID → PN di quoted
                                    _fixTextLid(qInner, resolvedQ, `quoted.${qType}`);
                                }
                            }
                        }
                    }
                }
                catch (lidFixErr) {
                    logger.debug({ err: lidFixErr }, 'post-resolve LID fix failed (non-critical)');
                }
                // ── End post-resolve fix ──────────────────────────────────────────────
                await upsertMessage(msg, node.attrs.offline ? 'append' : 'notify');
            });
        }
        catch (error) {
            logger.error({ error, node: (0, index_js_5.binaryNodeToString)(node) }, 'error in handling message');
        }
    };
    const handleCall = async (node) => {
        const { attrs } = node;
        const [infoChild] = (0, index_js_5.getAllBinaryNodeChildren)(node);
        const status = (0, index_js_4.getCallStatusFromNode)(infoChild);
        if (!infoChild) {
            throw new boom_1.Boom('Missing call info in call node');
        }
        const callId = infoChild.attrs['call-id'];
        const from = infoChild.attrs.from || infoChild.attrs['call-creator'];
        const call = {
            chatId: attrs.from,
            from,
            callerPn: infoChild.attrs['caller_pn'],
            id: callId,
            date: new Date(+attrs.t * 1000),
            offline: !!attrs.offline,
            status
        };
        if (status === 'offer') {
            call.isVideo = !!(0, index_js_5.getBinaryNodeChild)(infoChild, 'video');
            call.isGroup = infoChild.attrs.type === 'group' || !!infoChild.attrs['group-jid'];
            call.groupJid = infoChild.attrs['group-jid'];
            await callOfferCache.set(call.id, call);
        }
        const existingCall = await callOfferCache.get(call.id);
        // use existing call info to populate this event
        if (existingCall) {
            call.isVideo = existingCall.isVideo;
            call.isGroup = existingCall.isGroup;
            call.callerPn = call.callerPn || existingCall.callerPn;
        }
        // delete data once call has ended
        if (status === 'reject' || status === 'accept' || status === 'timeout' || status === 'terminate') {
            await callOfferCache.del(call.id);
        }
        ev.emit('call', [call]);
        await sendMessageAck(node);
    };
    const handleBadAck = async ({ attrs }) => {
        const key = { remoteJid: attrs.from, fromMe: true, id: attrs.id };
        // WARNING: REFRAIN FROM ENABLING THIS FOR NOW. IT WILL CAUSE A LOOP
        // // current hypothesis is that if pash is sent in the ack
        // // it means -- the message hasn't reached all devices yet
        // // we'll retry sending the message here
        // if(attrs.phash) {
        // 	logger.info({ attrs }, 'received phash in ack, resending message...')
        // 	const msg = await getMessage(key)
        // 	if(msg) {
        // 		await relayMessage(key.remoteJid!, msg, { messageId: key.id!, useUserDevicesCache: false })
        // 	} else {
        // 		logger.warn({ attrs }, 'could not send message again, as it was not found')
        // 	}
        // }
        // error in acknowledgement,
        // device could not display the message
        if (attrs.error) {
            logger.warn({ attrs }, 'received error in ack');
            ev.emit('messages.update', [
                {
                    key,
                    update: {
                        status: index_js_3.WAMessageStatus.ERROR,
                        messageStubParameters: [attrs.error]
                    }
                }
            ]);
            // resend the message with device_fanout=false, use at your own risk
            // if (attrs.error === '475') {
            // 	const msg = await getMessage(key)
            // 	if (msg) {
            // 		await relayMessage(key.remoteJid!, msg, {
            // 			messageId: key.id!,
            // 			useUserDevicesCache: false,
            // 			additionalAttributes: {
            // 				device_fanout: 'false'
            // 			}
            // 		})
            // 	}
            // }
        }
    };
    /// processes a node with the given function
    /// and adds the task to the existing buffer if we're buffering events
    const processNodeWithBuffer = async (node, identifier, exec) => {
        ev.buffer();
        try {
            await execTask();
        }
        finally {
            // Gunakan setImmediate agar event loop sempat memproses I/O lain
            // sebelum buffer di-flush — ini yang mencegah bot "stuck" saat ada user join
            await new Promise(resolve => setImmediate(resolve));
            ev.flush();
        }
        function execTask() {
            return exec(node, false).catch(err => onUnexpectedError(err, identifier));
        }
    };
    /** Yields control to the event loop to prevent blocking */
    const yieldToEventLoop = () => {
        return new Promise(resolve => setImmediate(resolve));
    };
    const makeOfflineNodeProcessor = () => {
        const nodeProcessorMap = new Map([
            ['message', handleMessage],
            ['call', handleCall],
            ['receipt', handleReceipt],
            ['notification', handleNotification]
        ]);
        const nodes = [];
        let isProcessing = false;
        // Number of nodes to process before yielding to event loop
        const BATCH_SIZE = 10;
        const enqueue = (type, node) => {
            nodes.push({ type, node });
            if (isProcessing) {
                return;
            }
            isProcessing = true;
            const promise = async () => {
                let processedInBatch = 0;
                while (nodes.length && ws.isOpen) {
                    const { type, node } = nodes.shift();
                    const nodeProcessor = nodeProcessorMap.get(type);
                    if (!nodeProcessor) {
                        onUnexpectedError(new Error(`unknown offline node type: ${type}`), 'processing offline node');
                        continue;
                    }
                    await nodeProcessor(node);
                    processedInBatch++;
                    // Yield to event loop after processing a batch
                    // This prevents blocking the event loop for too long when there are many offline nodes
                    if (processedInBatch >= BATCH_SIZE) {
                        processedInBatch = 0;
                        await yieldToEventLoop();
                    }
                }
                isProcessing = false;
            };
            promise().catch(error => onUnexpectedError(error, 'processing offline nodes'));
        };
        return { enqueue };
    };
    const offlineNodeProcessor = makeOfflineNodeProcessor();
    const processNode = async (type, node, identifier, exec) => {
        const isOffline = !!node.attrs.offline;
        if (isOffline) {
            offlineNodeProcessor.enqueue(type, node);
        }
        else {
            await processNodeWithBuffer(node, identifier, exec);
        }
    };
    // recv a message
    ws.on('CB:message', async (node) => {
        await processNode('message', node, 'processing message', handleMessage);
    });
    ws.on('CB:call', async (node) => {
        await processNode('call', node, 'handling call', handleCall);
    });
    ws.on('CB:receipt', async (node) => {
        await processNode('receipt', node, 'handling receipt', handleReceipt);
    });
    ws.on('CB:notification', async (node) => {
        await processNode('notification', node, 'handling notification', handleNotification);
    });
    ws.on('CB:ack,class:message', (node) => {
        handleBadAck(node).catch(error => onUnexpectedError(error, 'handling bad ack'));
    });
    ev.on('call', async ([call]) => {
        if (!call) {
            return;
        }
        // missed call + group call notification message generation
        if (call.status === 'timeout' || (call.status === 'offer' && call.isGroup)) {
            const msg = {
                key: {
                    remoteJid: call.chatId,
                    id: call.id,
                    fromMe: false
                },
                messageTimestamp: (0, index_js_4.unixTimestampSeconds)(call.date)
            };
            if (call.status === 'timeout') {
                if (call.isGroup) {
                    msg.messageStubType = call.isVideo
                        ? index_js_3.WAMessageStubType.CALL_MISSED_GROUP_VIDEO
                        : index_js_3.WAMessageStubType.CALL_MISSED_GROUP_VOICE;
                }
                else {
                    msg.messageStubType = call.isVideo ? index_js_3.WAMessageStubType.CALL_MISSED_VIDEO : index_js_3.WAMessageStubType.CALL_MISSED_VOICE;
                }
            }
            else {
                msg.message = { call: { callKey: Buffer.from(call.id) } };
            }
            const protoMsg = index_js_1.proto.WebMessageInfo.fromObject(msg);
            await upsertMessage(protoMsg, call.offline ? 'append' : 'notify');
        }
    });
    ev.on('connection.update', ({ isOnline }) => {
        if (typeof isOnline !== 'undefined') {
            sendActiveReceipts = isOnline;
            logger.trace(`sendActiveReceipts set to "${sendActiveReceipts}"`);
        }
    });
    return {
        ...sock,
        sendMessageAck,
        sendRetryRequest,
        rejectCall,
        fetchMessageHistory,
        requestPlaceholderResend,
        messageRetryManager
    };
};
exports.makeMessagesRecvSocket = makeMessagesRecvSocket;



