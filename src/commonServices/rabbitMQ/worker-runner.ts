import { Worker } from 'worker_threads';
import * as path from 'path';

export function runWorker(payload: any): Promise<any> {
  return new Promise((resolve, reject) => {
    // Note: use .js because dist contains JS after build
    const workerPath = path.resolve(__dirname, './ticket-excel-worker.js');

    console.log('[WorkerRunner] Spawning worker at:', workerPath);

    const worker = new Worker(workerPath, {
      workerData: payload,
    });

    worker.on('message', (msg) => {
      console.log('[WorkerRunner] Worker finished:', msg);
      resolve(msg);
    });

    worker.on('error', (err) => {
      console.error('[WorkerRunner] Worker error:', err);
      reject(err);
    });

    worker.on('exit', (code) => {
      if (code !== 0) {
        reject(new Error(`Worker stopped with exit code ${code}`));
      }
    });
  });
}
