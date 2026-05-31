"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.makeCacheManagerAuthState = void 0;
const cache_manager_1 = require("cache-manager");
const index_js_1 = require("../../WAProto/index.js");
const index_js_2 = require("../Utils/index.js");
const logger_js_1 = __importDefault(require("../Utils/logger.js"));
const makeCacheManagerAuthState = async (store, sessionKey) => {
    const defaultKey = (file) => `${sessionKey}:${file}`;
    const databaseConn = await (0, cache_manager_1.caching)(store);
    const writeData = async (file, data) => {
        let ttl = undefined;
        if (file === 'creds') {
            ttl = 63115200; // 2 years
        }
        await databaseConn.set(defaultKey(file), JSON.stringify(data, index_js_2.BufferJSON.replacer), ttl);
    };
    const readData = async (file) => {
        try {
            const data = await databaseConn.get(defaultKey(file));
            if (data) {
                return JSON.parse(data, index_js_2.BufferJSON.reviver);
            }
            return null;
        }
        catch (error) {
            logger_js_1.default.error(error);
            return null;
        }
    };
    const removeData = async (file) => {
        try {
            return await databaseConn.del(defaultKey(file));
        }
        catch (_a) {
            logger_js_1.default.error(`Error removing ${file} from session ${sessionKey}`);
        }
    };
    const clearState = async () => {
        try {
            const result = await databaseConn.store.keys(`${sessionKey}*`);
            await Promise.all(result.map(async (key) => await databaseConn.del(key)));
        }
        catch (err) {
        }
    };
    const creds = (await readData('creds')) || (0, index_js_2.initAuthCreds)();
    return {
        clearState,
        saveCreds: () => writeData('creds', creds),
        state: {
            creds,
            keys: {
                get: async (type, ids) => {
                    const data = {};
                    await Promise.all(ids.map(async (id) => {
                        let value = await readData(`${type}-${id}`);
                        if (type === 'app-state-sync-key' && value) {
                            value = index_js_1.proto.Message.AppStateSyncKeyData.fromObject(value);
                        }
                        data[id] = value;
                    }));
                    return data;
                },
                set: async (data) => {
                    const tasks = [];
                    for (const category in data) {
                        for (const id in data[category]) {
                            const value = data[category][id];
                            const key = `${category}-${id}`;
                            tasks.push(value ? writeData(key, value) : removeData(key));
                        }
                    }
                    await Promise.all(tasks);
                },
            }
        }
    };
};
exports.makeCacheManagerAuthState = makeCacheManagerAuthState;



