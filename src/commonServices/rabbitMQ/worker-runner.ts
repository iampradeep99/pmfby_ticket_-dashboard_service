// worker-runner.ts
import { Worker } from 'worker_threads';
import path from 'path';

export function runWorker(payload: any): Promise<any> {
  return new Promise((resolve, reject) => {
    const worker = new Worker(path.resolve(__dirname, 'ticket-excel-worker.js'), {
      workerData: payload,
    });

    worker.on('message', (msg) => resolve(msg));
    worker.on('error', (err) => reject(err));
    worker.on('exit', (code) => {
      if (code !== 0) {
        reject(new Error(`Worker stopped with exit code ${code}`));
      }
    });
  });
}
