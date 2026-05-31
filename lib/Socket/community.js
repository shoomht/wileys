"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.extractCommunityMetadata = exports.makeCommunitiesSocket = void 0;
const index_js_1 = require("../../WAProto/index.js");
const index_js_2 = require("../Types/index.js");
const index_js_3 = require("../Utils/index.js");
const index_js_4 = require("../WABinary/index.js");
const business_js_1 = require("./business.js");
const makeCommunitiesSocket = (config) => {
    const sock = (0, business_js_1.makeBusinessSocket)(config);
    const { authState, ev, query, groupMetadata, upsertMessage } = sock;
    const communityQuery = async (jid, type, content) => (query({
        tag: 'iq',
        attrs: {
            type,
            xmlns: 'w:g2',
            to: jid,
        },
        content
    }));
    const communityMetadata = async (jid) => {
        const result = await communityQuery(jid, 'get', [{ tag: 'query', attrs: { request: 'interactive' } }]);
        return (0, exports.extractCommunityMetadata)(result);
    };
    const communityFetchAllParticipating = async () => {
        const result = await query({
            tag: 'iq',
            attrs: {
                to: '@g.us',
                xmlns: 'w:g2',
                type: 'get',
            },
            content: [
                {
                    tag: 'participating',
                    attrs: {},
                    content: [
                        { tag: 'participants', attrs: {} },
                        { tag: 'description', attrs: {} }
                    ]
                }
            ]
        });
        const data = {};
        const communitiesChild = (0, index_js_4.getBinaryNodeChild)(result, 'communities');
        if (communitiesChild) {
            const communities = (0, index_js_4.getBinaryNodeChildren)(communitiesChild, 'community');
            for (const communityNode of communities) {
                const meta = (0, exports.extractCommunityMetadata)({
                    tag: 'result',
                    attrs: {},
                    content: [communityNode]
                });
                data[meta.id] = meta;
            }
        }
        sock.ev.emit('groups.update', Object.values(data));
        return data;
    };
    sock.ws.on('CB:ib,,dirty', async (node) => {
        const { attrs } = (0, index_js_4.getBinaryNodeChild)(node, 'dirty');
        if (attrs.type !== 'communities') {
            return;
        }
        await communityFetchAllParticipating();
        await sock.cleanDirtyBits('groups');
    });
    return {
        ...sock,
        communityQuery,
        communityMetadata,
        communityCreate: async (subject, body) => {
            const descriptionId = (0, index_js_3.generateMessageID)().substring(0, 12);
            const result = await communityQuery('@g.us', 'set', [
                {
                    tag: 'create',
                    attrs: { subject },
                    content: [{
                            tag: 'description',
                            attrs: {
                                id: descriptionId
                            },
                            content: [{
                                    tag: 'body',
                                    attrs: {},
                                    content: Buffer.from(body || '', 'utf-8')
                                }]
                        },
                        {
                            tag: 'parent',
                            attrs: {
                                default_membership_approval_mode: 'request_required'
                            }
                        },
                        {
                            tag: 'allow_non_admin_sub_group_creation',
                            attrs: {}
                        },
                        {
                            tag: 'create_general_chat',
                            attrs: {}
                        }]
                }
            ]);
            return (0, exports.extractCommunityMetadata)(result);
        },
        communityLeave: async (id) => {
            await communityQuery('@g.us', 'set', [
                {
                    tag: 'leave',
                    attrs: {},
                    content: [
                        { tag: 'community', attrs: { id } }
                    ]
                }
            ]);
        },
        communityUpdateSubject: async (jid, subject) => {
            await communityQuery(jid, 'set', [
                {
                    tag: 'subject',
                    attrs: {},
                    content: Buffer.from(subject, 'utf-8')
                }
            ]);
        },
        communityRequestParticipantsList: async (jid) => {
            const result = await communityQuery(jid, 'get', [
                {
                    tag: 'membership_approval_requests',
                    attrs: {}
                }
            ]);
            const node = (0, index_js_4.getBinaryNodeChild)(result, 'membership_approval_requests');
            const participants = (0, index_js_4.getBinaryNodeChildren)(node, 'membership_approval_request');
            return participants.map(v => v.attrs);
        },
        communityRequestParticipantsUpdate: async (jid, participants, action) => {
            const result = await communityQuery(jid, 'set', [{
                    tag: 'membership_requests_action',
                    attrs: {},
                    content: [
                        {
                            tag: action,
                            attrs: {},
                            content: participants.map(jid => ({
                                tag: 'participant',
                                attrs: { jid }
                            }))
                        }
                    ]
                }]);
            const node = (0, index_js_4.getBinaryNodeChild)(result, 'membership_requests_action');
            const nodeAction = (0, index_js_4.getBinaryNodeChild)(node, action);
            const participantsAffected = (0, index_js_4.getBinaryNodeChildren)(nodeAction, 'participant');
            return participantsAffected.map(p => {
                return { status: p.attrs.error || '200', jid: p.attrs.jid };
            });
        },
        communityParticipantsUpdate: async (jid, participants, action) => {
            const result = await communityQuery(jid, 'set', [
                {
                    tag: action,
                    attrs: {},
                    content: participants.map(jid => ({
                        tag: 'participant',
                        attrs: { jid }
                    }))
                }
            ]);
            const node = (0, index_js_4.getBinaryNodeChild)(result, action);
            const participantsAffected = (0, index_js_4.getBinaryNodeChildren)(node, 'participant');
            return participantsAffected.map(p => {
                return { status: p.attrs.error || '200', jid: p.attrs.jid, content: p };
            });
        },
        communityUpdateDescription: async (jid, description) => {
            const metadata = await communityMetadata(jid);
            const prev = metadata.descId ? metadata.descId : null;
            await communityQuery(jid, 'set', [
                {
                    tag: 'description',
                    attrs: {
                        ...(description ? { id: (0, index_js_3.generateMessageID)() } : { delete: 'true' }),
                        ...(prev ? { prev } : {})
                    },
                    content: description ? [
                        { tag: 'body', attrs: {}, content: Buffer.from(description, 'utf-8') }
                    ] : undefined
                }
            ]);
        },
        communityInviteCode: async (jid) => {
            const result = await communityQuery(jid, 'get', [{ tag: 'invite', attrs: {} }]);
            const inviteNode = (0, index_js_4.getBinaryNodeChild)(result, 'invite');
            return inviteNode?.attrs?.code;
        },
        communityRevokeInvite: async (jid) => {
            const result = await communityQuery(jid, 'set', [{ tag: 'invite', attrs: {} }]);
            const inviteNode = (0, index_js_4.getBinaryNodeChild)(result, 'invite');
            return inviteNode?.attrs?.code;
        },
        communityAcceptInvite: async (code) => {
            const results = await communityQuery('@g.us', 'set', [{ tag: 'invite', attrs: { code } }]);
            const result = (0, index_js_4.getBinaryNodeChild)(results, 'community');
            return result?.attrs?.jid;
        },
        /**
         * revoke a v4 invite for someone
         * @param communityJid community jid
         * @param invitedJid jid of person you invited
         * @returns true if successful
         */
        communityRevokeInviteV4: async (communityJid, invitedJid) => {
            const result = await communityQuery(communityJid, 'set', [{ tag: 'revoke', attrs: {}, content: [{ tag: 'participant', attrs: { jid: invitedJid } }] }]);
            return !!result;
        },
        /**
         * accept a GroupInviteMessage
         * @param key the key of the invite message, or optionally only provide the jid of the person who sent the invite
         * @param inviteMessage the message to accept
         */
        communityAcceptInviteV4: ev.createBufferedFunction(async (key, inviteMessage) => {
            key = typeof key === 'string' ? { remoteJid: key } : key;
            const results = await communityQuery(inviteMessage.groupJid, 'set', [{
                    tag: 'accept',
                    attrs: {
                        code: inviteMessage.inviteCode,
                        expiration: inviteMessage.inviteExpiration.toString(),
                        admin: key.remoteJid
                    }
                }]);
            // if we have the full message key
            // update the invite message to be expired
            if (key.id) {
                // create new invite message that is expired
                inviteMessage = index_js_1.proto.Message.GroupInviteMessage.fromObject(inviteMessage);
                inviteMessage.inviteExpiration = 0;
                inviteMessage.inviteCode = '';
                ev.emit('messages.update', [
                    {
                        key,
                        update: {
                            message: {
                                groupInviteMessage: inviteMessage
                            }
                        }
                    }
                ]);
            }
            // generate the group add message
            await upsertMessage({
                key: {
                    remoteJid: inviteMessage.groupJid,
                    id: (0, index_js_3.generateMessageID)(authState.creds.me?.id),
                    fromMe: false,
                    participant: key.remoteJid,
                },
                messageStubType: index_js_2.WAMessageStubType.GROUP_PARTICIPANT_ADD,
                messageStubParameters: [
                    authState.creds.me.id
                ],
                participant: key.remoteJid,
                messageTimestamp: (0, index_js_3.unixTimestampSeconds)()
            }, 'notify');
            return results.attrs.from;
        }),
        communityGetInviteInfo: async (code) => {
            const results = await communityQuery('@g.us', 'get', [{ tag: 'invite', attrs: { code } }]);
            return (0, exports.extractCommunityMetadata)(results);
        },
        communityToggleEphemeral: async (jid, ephemeralExpiration) => {
            const content = ephemeralExpiration ?
                { tag: 'ephemeral', attrs: { expiration: ephemeralExpiration.toString() } } :
                { tag: 'not_ephemeral', attrs: {} };
            await communityQuery(jid, 'set', [content]);
        },
        communitySettingUpdate: async (jid, setting) => {
            await communityQuery(jid, 'set', [{ tag: setting, attrs: {} }]);
        },
        communityMemberAddMode: async (jid, mode) => {
            await communityQuery(jid, 'set', [{ tag: 'member_add_mode', attrs: {}, content: mode }]);
        },
        communityJoinApprovalMode: async (jid, mode) => {
            await communityQuery(jid, 'set', [{ tag: 'membership_approval_mode', attrs: {}, content: [{ tag: 'community_join', attrs: { state: mode } }] }]);
        },
        communityFetchAllParticipating
    };
};
exports.makeCommunitiesSocket = makeCommunitiesSocket;
const extractCommunityMetadata = (result) => {
    const community = (0, index_js_4.getBinaryNodeChild)(result, 'group');
    const descChild = (0, index_js_4.getBinaryNodeChild)(community, 'description');
    let desc;
    let descId;
    if (descChild) {
        desc = (0, index_js_4.getBinaryNodeChildString)(descChild, 'body');
        descId = descChild.attrs.id;
    }
    const mode = community.attrs.addressing_mode;
    const communityId = community.attrs.id.includes('@') ? community.attrs.id : (0, index_js_4.jidEncode)(community.attrs.id, 'g.us');
    const eph = (0, index_js_4.getBinaryNodeChild)(community, 'ephemeral')?.attrs.expiration;
    const memberAddMode = (0, index_js_4.getBinaryNodeChildString)(community, 'member_add_mode') === 'all_member_add';
    const metadata = {
        id: communityId,
        addressingMode: mode,
        subject: community.attrs.subject,
        subjectOwner: mode === 'lid' ? community.attrs.s_o_pn : community.attrs.s_o,
        subjectTime: +community.attrs.s_t,
        size: community.attrs?.size ? +community.attrs.size : (0, index_js_4.getBinaryNodeChildren)(community, 'participant').length,
        creation: +community.attrs.creation,
        owner: community.attrs.creator ? (0, index_js_4.jidNormalizedUser)(mode === 'lid' ? community.attrs.creator_pn : community.attrs.creator) : undefined,
        desc,
        descId,
        linkedParent: (0, index_js_4.getBinaryNodeChild)(community, 'linked_parent')?.attrs.jid || undefined,
        restrict: !!(0, index_js_4.getBinaryNodeChild)(community, 'locked'),
        announce: !!(0, index_js_4.getBinaryNodeChild)(community, 'announcement'),
        isCommunity: !!(0, index_js_4.getBinaryNodeChild)(community, 'parent'),
        isCommunityAnnounce: !!(0, index_js_4.getBinaryNodeChild)(community, 'default_sub_group'),
        joinApprovalMode: !!(0, index_js_4.getBinaryNodeChild)(community, 'membership_approval_mode'),
        memberAddMode,
        participants: (0, index_js_4.getBinaryNodeChildren)(community, 'participant').map(({ attrs }) => {
            return {
                id: mode === 'lid' ? community.phone_number : attrs.jid,
                lid: mode === 'lid' ? community.jid : attrs.lid,
                admin: (attrs.type || null),
            };
        }),
        ephemeralDuration: eph ? +eph : undefined,
    };
    return metadata;
};
exports.extractCommunityMetadata = extractCommunityMetadata;



