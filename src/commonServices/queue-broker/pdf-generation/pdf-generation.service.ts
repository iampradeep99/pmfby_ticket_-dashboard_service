import { Injectable, OnModuleInit, OnApplicationShutdown } from '@nestjs/common';
import * as amqp from 'amqplib';
import { runWorker } from './pdf-worker-runner';
import config from '../../../environment/config';
const Logger = require("../../../commonServices/logger");

@Injectable()
export class PdfGenerationService implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger('pdf-generation-rmq.log');

  private readonly RABBITMQ_URL = config.rabbitmq;
  private readonly QUEUE_NAME = 'krph_support_ticket_pdf_generation_queue_1';
  private  PREFETCH = 5;
  private readonly RECONNECT_DELAY = 5000;
  private readonly MAX_RETRIES = 5;

  private connection: amqp.Connection | null = null;
  private consumerChannel: amqp.Channel | null = null;
  private producerChannel: amqp.Channel | null = null;
  private monitorChannel: amqp.Channel | null = null;

  private activeJobs = 0;
  private shuttingDown = false;
  private initialized = false;

  constructor() {}

  async onModuleInit() {
    console.log(`[PDF-Generation][Init] Starting module initialization`);
    await this.initializeRabbitMQ();
  }

  private async initializeRabbitMQ(attempt = 1): Promise<void> {
    console.log(`[PDF-Generation][RMQ-Init] Connecting to RabbitMQ ${this.RABBITMQ_URL} | Attempt: ${attempt}`);

    if (attempt > this.MAX_RETRIES) {
      console.log(`[PDF-Generation][RMQ-Init] Max connection retries exceeded`);
      return;
    }

    try {
      this.connection = await amqp.connect(this.RABBITMQ_URL);
      console.log(`[PDF-Generation][RMQ-Init] Connection established`);

      this.connection.on('error', (e) => {
        console.log(`[PDF-Generation][RMQ-Error] Connection error ${e}`);
        this.reconnect();
      });

      this.connection.on('close', () => {
        console.log(`[PDF-Generation][RMQ-Error] Connection closed`);
        this.reconnect();
      });

      this.producerChannel = await this.connection.createChannel();
      await this.producerChannel.assertQueue(this.QUEUE_NAME, { durable: true });
      console.log(`[PDF-Generation][RMQ-Producer] Producer channel ready`);

      this.consumerChannel = await this.connection.createChannel();
      await this.consumerChannel.assertQueue(this.QUEUE_NAME, { durable: true });
      this.consumerChannel.prefetch(this.PREFETCH);
      console.log(`[PDF-Generation][RMQ-Consumer] Consumer channel ready`);

      this.monitorChannel = await this.connection.createChannel();
      console.log(`[PDF-Generation][RMQ-Monitor] Monitoring channel ready`);

      console.log(`[PDF-Generation][RMQ-Init] Listening on queue "${this.QUEUE_NAME}"`);

      this.consumeMessages();
      this.monitorQueueAndAdjustConcurrency();
      this.initialized = true;
    } catch (err) {
      console.log(`[PDF-Generation][RMQ-Init] Connection failed. Retrying in ${this.RECONNECT_DELAY}ms`);
      await new Promise((r) => setTimeout(r, this.RECONNECT_DELAY));
      await this.initializeRabbitMQ(attempt + 1);
    }
  }

  private async reconnect() {
    if (this.shuttingDown) return;
    if (!this.initialized) return;

    console.log(`[PDF-Generation][RMQ-Reconnect] Reconnecting...`);
    this.initialized = false;

    await this.cleanupConnections();
    await new Promise((r) => setTimeout(r, this.RECONNECT_DELAY));
    await this.initializeRabbitMQ();
  }

  async sendToQueue(message: any) {
    if (!this.producerChannel) {
      console.log(`[PDF-Generation][RMQ-Producer] Producer channel not ready`);
      return;
    }

    try {
      const payload = JSON.stringify(message ?? {});
      this.producerChannel.sendToQueue(this.QUEUE_NAME, Buffer.from(payload), { persistent: true });
      console.log(`[PDF-Generation][RMQ-Producer] Message dispatched to queue`);
    } catch (err) {
      console.log(`[PDF-Generation][RMQ-Producer] Error sending message ${err}`);
    }
  }

  private consumeMessages() {
    if (!this.consumerChannel) return;

    console.log(`[PDF-Generation][RMQ-Consumer] Consumer started`);

    this.consumerChannel.consume(
      this.QUEUE_NAME,
      async (msg) => {
        if (!msg || this.shuttingDown) return;

        console.log(`[PDF-Generation][Worker] Message received`);

        let payload: any = null;

        try {
          payload = JSON.parse(msg.content.toString());
        } catch {
          console.log(`[PDF-Generation][Worker] Invalid JSON payload discarded`);
          this.consumerChannel.nack(msg, false, false);
          return;
        }

        this.activeJobs++;
        console.log(`[PDF-Generation][Worker] Worker started | Active jobs: ${this.activeJobs}`);

        try {
          await runWorker(payload);
          console.log(`[PDF-Generation][Worker] Processing completed`);
          this.consumerChannel.ack(msg);
        } catch (err) {
          console.log(err, "pradeep")
          return
          console.log(`[PDF-Generation][Worker] Processing failed ${err}`);
          this.consumerChannel.nack(msg, false, true);
        } finally {
          this.activeJobs--;
          console.log(`[PDF-Generation][Worker] Job finished | Active jobs: ${this.activeJobs}`);
        }
      },
      { noAck: false }
    );
  }

  // private monitorQueueAndAdjustConcurrency() {
  //   setInterval(async () => {
  //     if (!this.monitorChannel || this.shuttingDown) return;

  //     try {
  //       const q = await this.monitorChannel.checkQueue(this.QUEUE_NAME);
  //       console.log(`[PDF-Generation][Queue-Monitor] Pending: ${q.messageCount} | Active: ${this.activeJobs}`);
  //     } catch (err) {
  //       console.log(`[PDF-Generation][Queue-Monitor] Queue check failed ${err}`);
  //     }
  //   }, 5000);
  // }
  private monitorQueueAndAdjustConcurrency() {
  setInterval(async () => {
    if (!this.monitorChannel || !this.consumerChannel || this.shuttingDown) return;

    try {
      const q = await this.monitorChannel.checkQueue(this.QUEUE_NAME);
      const pending = q.messageCount;

      console.log(`[PDF-Generation][Queue-Monitor] Pending: ${pending} | Active: ${this.activeJobs}`);

      // Dynamic Prefetch Logic
      let newPrefetch = pending >= 100 ? 10 : 5;

      // Apply only if changed
      if (newPrefetch !== this.PREFETCH) {
        console.log(`[PDF-Generation][Queue-Monitor] Updating prefetch to ${newPrefetch}`);
        this.consumerChannel.prefetch(newPrefetch);
        this.PREFETCH = newPrefetch;
      }

    } catch (err) {
      console.log(`[PDF-Generation][Queue-Monitor] Queue check failed ${err}`);
    }
  }, 5000);
}


  async onApplicationShutdown() {
    console.log(`[PDF-Generation][Shutdown] Shutdown initiated`);
    this.shuttingDown = true;

    while (this.activeJobs > 0) {
      await new Promise((r) => setTimeout(r, 100));
    }

    console.log(`[PDF-Generation][Shutdown] All tasks done. Closing connections`);
    await this.cleanupConnections();
  }

  private async cleanupConnections() {
    console.log(`[PDF-Generation][Cleanup] Cleaning up RabbitMQ connections`);
    try { await this.consumerChannel?.close(); } catch {}
    try { await this.producerChannel?.close(); } catch {}
    try { await this.monitorChannel?.close(); } catch {}
    try { await this.connection?.close(); } catch {}

    this.consumerChannel = null;
    this.producerChannel = null;
    this.monitorChannel = null;
    this.connection = null;
  }
}
