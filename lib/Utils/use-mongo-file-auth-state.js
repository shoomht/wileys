"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.useMongoFileAuthState = void 0;
const index_js_1 = require("../../WAProto/index.js");
const auth_utils_js_1 = require("./auth-utils.js");
const generics_js_1 = require("./generics.js");
/*
code from amiruldev readjusted by @irull2nd, don't delete WM!
*/
const useMongoFileAuthState = async (collection) => {
    const writeData = (data, id) => {
        const informationToStore = JSON.parse(JSON.stringify(data, generics_js_1.BufferJSON.replacer));
        const update = {
            $set: {
                ...informationToStore,
            },
        };
        return collection.updateOne({ _id: id }, update, { upsert: true });
    };
    const readData = async (id) => {
        try {
            const data = JSON.stringify(await collection.findOne({ _id: id }));
            return JSON.parse(data, generics_js_1.BufferJSON.reviver);
        }
        catch (err) {
            console.log(err);
        }
    };
    const removeData = async (id) => {
        try {
            await collection.deleteOne({ _id: id });
        }
        catch (err) {
            console.log('error', err);
        }
    };
    const creds = (await readData('creds')) || (0, auth_utils_js_1.initAuthCreds)();
    return {
        state: {
            creds,
            keys: {
                get: async (type, ids) => {
                    const data = {};
                    await Promise.all(ids.map(async (id) => {
                        let value = await readData(`${type}-${id}`);
                        if (type === 'app-state-sync-key') {
                            value = index_js_1.proto.Message.AppStateSyncKeyData.fromObject(data);
                        }
                        data[id] = value;
                    }));
                    return data;
                },
                set: async (data) => {
                    const tasks = [];
                    for (const category of Object.keys(data)) {
                        for (const id of Object.keys(data[category])) {
                            const value = data[category][id];
                            const key = `${category}-${id}`;
                            tasks.push(value ? writeData(value, key) : removeData(key));
                        }
                    }
                    await Promise.all(tasks);
                },
            },
        },
        saveCreds: () => {
            return writeData(creds, 'creds');
        }
    };
};
exports.useMongoFileAuthState = useMongoFileAuthState;



