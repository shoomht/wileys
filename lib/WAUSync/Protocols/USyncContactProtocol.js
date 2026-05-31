"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.USyncContactProtocol = void 0;
const index_js_1 = require("../../WABinary/index.js");
const USyncUser_js_1 = require("../USyncUser.js");
class USyncContactProtocol {
    constructor() {
        this.name = 'contact';
    }
    getQueryElement() {
        return {
            tag: 'contact',
            attrs: {}
        };
    }
    getUserElement(user) {
        //TODO: Implement type / username fields (not yet supported)
        return {
            tag: 'contact',
            attrs: {},
            content: user.phone
        };
    }
    parser(node) {
        if (node.tag === 'contact') {
            (0, index_js_1.assertNodeErrorFree)(node);
            return node?.attrs?.type === 'in';
        }
        return false;
    }
}
exports.USyncContactProtocol = USyncContactProtocol;



