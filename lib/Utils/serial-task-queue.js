"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
class SerialTaskQueue {
    constructor(options = {}) {
        this.concurrency = Math.max(1, Number(options.concurrency || 1));
        this.running = 0;
        this.queue = [];
    }
    add(task) {
        return new Promise((resolve, reject) => {
            this.queue.push({ task, resolve, reject });
            this._drain();
        });
    }
    _drain() {
        while (this.running < this.concurrency && this.queue.length) {
            const item = this.queue.shift();
            this.running += 1;
            Promise.resolve()
                .then(() => item.task())
                .then(value => item.resolve(value), error => item.reject(error))
                .finally(() => {
                this.running -= 1;
                this._drain();
            });
        }
    }
}
exports.default = SerialTaskQueue;



