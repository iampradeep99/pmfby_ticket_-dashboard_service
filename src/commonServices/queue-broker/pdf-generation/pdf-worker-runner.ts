import { Worker } from 'worker_threads';
import * as path from 'path';
import * as fs from 'fs';
const Logger = require("../../../commonServices/logger");

const logger = new Logger('worker-runner.log');

export function runWorker(payload: any, timeoutMs = 300000): Promise<any> {
  return new Promise((resolve, reject) => {
    if (payload == null) return reject(new Error('Invalid payload'));

    let settled = false;

    const workerPath = path.resolve(__dirname, './pdf-generation-middleware.js');

    if (!fs.existsSync(workerPath)) {
      return reject(new Error('Worker file not found'));
    }

    const timer = setTimeout(() => {
      if (!settled) {
        settled = true;
        reject(new Error('Worker timeout exceeded'));
      }
    }, timeoutMs);

    let worker: Worker;

    try {
      worker = new Worker(workerPath, { workerData: { payload } });
    } catch (err) {
      clearTimeout(timer);
      return reject(err);
    }

    const finalize = (result: any, isError = false) => {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        isError ? reject(result) : resolve(result);
      }
    };

    worker.on('message', (msg) => finalize(msg));
    worker.on('error', (err) => finalize(err, true));
    worker.on('exit', (code) => {
      if (code !== 0) finalize(new Error(`Worker exited with code ${code}`), true);
      else finalize(null);
    });
  });
}
