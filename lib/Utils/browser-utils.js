"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getPlatformId = exports.Browsers = exports.PLATFORM_VERSIONS = exports.PLATFORM_MAP = void 0;
const os_1 = require("os");
const index_js_1 = require("../../WAProto/index.js");
exports.PLATFORM_MAP = {
    'aix': 'AIX', 'darwin': 'Mac OS', 'win32': 'Windows', 'android': 'Android',
    'freebsd': 'FreeBSD', 'openbsd': 'OpenBSD', 'sunos': 'Solaris', 'linux': 'Linux',
    'ubuntu': 'Ubuntu', 'ios': 'iOS', 'astrabail': 'AstraBail', 'chromeos': 'Chrome OS',
    'tizen': 'Tizen', 'watchos': 'watchOS', 'wearos': 'Wear OS', 'harmonyos': 'HarmonyOS',
    'kaios': 'KaiOS', 'smarttv': 'Smart TV', 'raspberrypi': 'Raspberry Pi OS',
    'symbian': 'Symbian', 'blackberry': 'Blackberry OS', 'windowsphone': 'Windows Phone'
};
exports.PLATFORM_VERSIONS = {
    'ubuntu': '22.04.4', 'darwin': '18.5', 'win32': '10.0.22631', 'android': '14.0.0',
    'freebsd': '13.2', 'openbsd': '7.3', 'sunos': '11', 'linux': '6.5', 'ios': '18.2',
    'astrabail': '6.5.0', 'chromeos': '117.0.5938.132', 'tizen': '6.5', 'watchos': '10.1',
    'wearos': '4.1', 'harmonyos': '4.0.0', 'kaios': '3.1', 'smarttv': '23.3.1',
    'raspberrypi': '11 (Bullseye)', 'symbian': '3', 'blackberry': '10.3.3', 'windowsphone': '8.1'
};
exports.Browsers = {
    ubuntu: (browser) => [exports.PLATFORM_MAP['ubuntu'], browser, exports.PLATFORM_VERSIONS['ubuntu']],
    macOS: (browser) => [exports.PLATFORM_MAP['darwin'], browser, exports.PLATFORM_VERSIONS['darwin']],
    windows: (browser) => [exports.PLATFORM_MAP['win32'], browser, exports.PLATFORM_VERSIONS['win32']],
    linux: (browser) => [exports.PLATFORM_MAP['linux'], browser, exports.PLATFORM_VERSIONS['linux']],
    solaris: (browser) => [exports.PLATFORM_MAP['sunos'], browser, exports.PLATFORM_VERSIONS['sunos']],
    astrabail: (browser) => [exports.PLATFORM_MAP['astrabail'], browser, exports.PLATFORM_VERSIONS['astrabail']],
    android: (browser) => [exports.PLATFORM_MAP['android'], browser, exports.PLATFORM_VERSIONS['android']],
    iOS: (browser) => [exports.PLATFORM_MAP['ios'], browser, exports.PLATFORM_VERSIONS['ios']],
    kaiOS: (browser) => [exports.PLATFORM_MAP['kaios'], browser, exports.PLATFORM_VERSIONS['kaios']],
    chromeOS: (browser) => [exports.PLATFORM_MAP['chromeos'], browser, exports.PLATFORM_VERSIONS['chromeos']],
    appropriate: (browser) => {
        const p = (0, os_1.platform)();
        return [exports.PLATFORM_MAP[p] || 'Unknown OS', browser, exports.PLATFORM_VERSIONS[p] || 'latest'];
    },
    custom: (p, browser, version) => {
        return [exports.PLATFORM_MAP[p.toLowerCase()] || p, browser, version || exports.PLATFORM_VERSIONS[p] || 'latest'];
    }
};
const getPlatformId = (browser) => {
    const platformType = index_js_1.proto.DeviceProps.PlatformType[browser.toUpperCase()];
    return platformType ? platformType.toString() : '1';
};
exports.getPlatformId = getPlatformId;



