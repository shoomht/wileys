export type HKDFOptions = {
    salt?: Uint8Array | Buffer | string;
    info?: Uint8Array | Buffer | string;
};
export declare function md5(data: Uint8Array | Buffer | string): Buffer;
export declare function hkdf(key: Uint8Array | Buffer | string, length: number, options?: HKDFOptions): Buffer;
export declare function expandAppStateKeys(keyData: Uint8Array | Buffer | string): {
    indexKey: Buffer;
    valueEncryptionKey: Buffer;
    valueMacKey: Buffer;
    snapshotMacKey: Buffer;
    patchMacKey: Buffer;
};
export declare class LTHashAntiTampering {
    private readonly info;
    private readonly size;
    constructor(info?: string | Uint8Array | Buffer, size?: number);
    subtractThenAdd(base: Uint8Array | Buffer, subtract?: Array<Uint8Array | Buffer>, add?: Array<Uint8Array | Buffer>): Buffer;
    subtractThenAddInPlace(base: Buffer, subtract?: Array<Uint8Array | Buffer>, add?: Array<Uint8Array | Buffer>): Buffer;
    private multipleOp;
    private performPointwiseWithOverflow;
}



