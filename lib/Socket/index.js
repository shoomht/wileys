"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const index_js_1 = require("../Defaults/index.js");
const communities_js_1 = require("./communities.js");
// export the last socket layer
const makeWASocket = (config) => {
    const newConfig = {
        ...index_js_1.DEFAULT_CONNECTION_CONFIG,
        ...config
    };
    return (0, communities_js_1.makeCommunitiesSocket)(newConfig);
};
exports.default = makeWASocket;



