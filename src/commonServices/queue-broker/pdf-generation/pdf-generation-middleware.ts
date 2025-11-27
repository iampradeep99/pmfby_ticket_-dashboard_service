const { parentPort, workerData } = require("worker_threads");
const { PDFGenerationWorkerService } = require("../pdf-generation/pdf-generation-worker");

async function run() {
  try {
    const payload = workerData.payload;

    const service = new PDFGenerationWorkerService(); // No argument
    const result = await service.ProcessInformationForFarmer(payload);

    parentPort.postMessage(result);
  } catch (err) {
    parentPort.postMessage({ error: err.message || String(err) });
  }
}

run();
