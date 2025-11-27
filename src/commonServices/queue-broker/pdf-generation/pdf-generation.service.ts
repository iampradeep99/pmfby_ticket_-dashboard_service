
// import { Injectable, OnModuleInit, OnApplicationShutdown } from '@nestjs/common';
// import * as amqp from 'amqplib';
// import { runWorker } from './pdf-worker-runner';
// import config from '../../../environment/config';
// const Logger = require("../../../commonServices/logger");

// interface ProcessingLog {
//   SupportTicketNo: string;
//   AttemptNumber: number;
//   Status: 'processing' | 'success' | 'failed' | 'retry';
//   ErrorMessage?: string;
//   ProcessingTime?: number;
//   GCSUrl?: string;
//   CreatedAt: Date;
//   UpdatedAt: Date;
// }

// interface MessageWithRetry {
//   payload: any;
//   retryCount: number;
//   maxRetries: number;
//   originalMessageId: string;
//   firstAttemptTime?: string;
// }

// @Injectable()
// export class PdfGenerationService implements OnModuleInit, OnApplicationShutdown {
//   private readonly logger = new Logger('pdf-generation-rmq.log');
//   private readonly RABBITMQ_URL = config.rabbitmq;
//   private readonly QUEUE_NAME = 'krph_support_ticket_pdf_generation_queue_1';
//   private readonly DLQ_NAME = 'krph_support_ticket_pdf_generation_dlq';
//   private PREFETCH = 5;
//   private readonly RECONNECT_DELAY = 5000;
//   private readonly MAX_RETRIES = 5;
//   private readonly MESSAGE_MAX_RETRIES = 3;
//   private readonly RETRY_DELAY = 5000;

//   private connection: amqp.Connection | null = null;
//   private consumerChannel: amqp.Channel | null = null;
//   private producerChannel: amqp.Channel | null = null;
//   private monitorChannel: amqp.Channel | null = null;

//   private activeJobs = 0;
//   private shuttingDown = false;
//   private initialized = false;

//   constructor() {}

//   async onModuleInit() {
//     console.log(`[PDF-Generation][Init] Starting module initialization`);
//     await this.initializeRabbitMQ();
//   }

//   private async initializeRabbitMQ(attempt = 1): Promise<void> {
//     console.log(`[PDF-Generation][RMQ-Init] Connecting to RabbitMQ ${this.RABBITMQ_URL} | Attempt: ${attempt}`);

//     if (attempt > this.MAX_RETRIES) {
//       console.log(`[PDF-Generation][RMQ-Init] Max connection retries exceeded`);
//       return;
//     }

//     try {
//       this.connection = await amqp.connect(this.RABBITMQ_URL);
//       console.log(`[PDF-Generation][RMQ-Init] Connection established`);

//       this.connection.on('error', (e) => {
//         console.log(`[PDF-Generation][RMQ-Error] Connection error ${e}`);
//         this.reconnect();
//       });

//       this.connection.on('close', () => {
//         console.log(`[PDF-Generation][RMQ-Error] Connection closed`);
//         this.reconnect();
//       });

//       this.producerChannel = await this.connection.createChannel();
//       await this.producerChannel.assertQueue(this.QUEUE_NAME, { durable: true });
//       await this.producerChannel.assertQueue(this.DLQ_NAME, { durable: true });
//       console.log(`[PDF-Generation][RMQ-Producer] Producer channel ready with DLQ`);

//       this.consumerChannel = await this.connection.createChannel();
//       await this.consumerChannel.assertQueue(this.QUEUE_NAME, { durable: true });
//       await this.consumerChannel.assertQueue(this.DLQ_NAME, { durable: true });
//       this.consumerChannel.prefetch(this.PREFETCH);
//       console.log(`[PDF-Generation][RMQ-Consumer] Consumer channel ready`);

//       this.monitorChannel = await this.connection.createChannel();
//       console.log(`[PDF-Generation][RMQ-Monitor] Monitoring channel ready`);

//       console.log(`[PDF-Generation][RMQ-Init] Listening on queue "${this.QUEUE_NAME}"`);

//       this.consumeMessages();
//       this.monitorQueueAndAdjustConcurrency();
//       this.initialized = true;
//     } catch (err) {
//       console.log(`[PDF-Generation][RMQ-Init] Connection failed. Retrying in ${this.RECONNECT_DELAY}ms`);
//       await new Promise((r) => setTimeout(r, this.RECONNECT_DELAY));
//       await this.initializeRabbitMQ(attempt + 1);
//     }
//   }

//   private async reconnect() {
//     if (this.shuttingDown) return;
//     if (!this.initialized) return;

//     console.log(`[PDF-Generation][RMQ-Reconnect] Reconnecting...`);
//     this.initialized = false;

//     await this.cleanupConnections();
//     await new Promise((r) => setTimeout(r, this.RECONNECT_DELAY));
//     await this.initializeRabbitMQ();
//   }

//   async sendToQueue(message: any) {
//     if (!this.producerChannel) {
//       console.log(`[PDF-Generation][RMQ-Producer] Producer channel not ready`);
//       return;
//     }

//     try {
//       const wrappedMessage: MessageWithRetry = {
//         payload: message,
//         retryCount: 0,
//         maxRetries: this.MESSAGE_MAX_RETRIES,
//         originalMessageId: this.generateMessageId(),
//         firstAttemptTime: new Date().toISOString()
//       };

//       const payload = JSON.stringify(wrappedMessage);
//       this.producerChannel.sendToQueue(
//         this.QUEUE_NAME,
//         Buffer.from(payload),
//         {
//           persistent: true,
//           messageId: wrappedMessage.originalMessageId
//         }
//       );
//       console.log(`[PDF-Generation][RMQ-Producer] Message dispatched: ${message.SupportTicketNo}`);
//     } catch (err) {
//       console.log(`[PDF-Generation][RMQ-Producer] Error sending message ${err}`);
//     }
//   }

//     private consumeMessages() {
//       if (!this.consumerChannel) return;

//       console.log(`[PDF-Generation][RMQ-Consumer] Consumer started`);

//       this.consumerChannel.consume(
//         this.QUEUE_NAME,
//         async (msg) => {
//           if (!msg || this.shuttingDown) return;

//           console.log(`[PDF-Generation][Worker] Message received`);

//           let wrappedMessage: MessageWithRetry | null = null;

//           try {
//             const content = msg.content.toString();
//             const parsed = JSON.parse(content);

//             if (parsed.payload && parsed.retryCount !== undefined) {
//               wrappedMessage = parsed;
//             } else {
//               wrappedMessage = {
//                 payload: parsed,
//                 retryCount: 0,
//                 maxRetries: this.MESSAGE_MAX_RETRIES,
//                 originalMessageId: msg.properties.messageId || this.generateMessageId(),
//                 firstAttemptTime: new Date().toISOString()
//               };
//             }
//           } catch {
//             console.log(`[PDF-Generation][Worker] Invalid JSON payload discarded`);
//             this.consumerChannel.nack(msg, false, false);
//             return;
//           }

//           this.activeJobs++;
//           const ticketNo = wrappedMessage.payload.SupportTicketNo;
//           const currentAttempt = wrappedMessage.retryCount + 1;

//           console.log(`[PDF-Generation][Worker] Processing ${ticketNo} | Attempt ${currentAttempt}/${this.MESSAGE_MAX_RETRIES} | Active: ${this.activeJobs}`);

//           await this.logProcessingAttempt(ticketNo, currentAttempt, 'processing');

//           const startTime = Date.now();

//           try {
//             const result = await runWorker(wrappedMessage.payload);
//             const processingTime = Date.now() - startTime;

//             console.log(`[PDF-Generation][Worker] ✓ Completed ${ticketNo} in ${processingTime}ms`);

//             await this.logProcessingAttempt(ticketNo, currentAttempt, 'success', undefined, processingTime, result?.gcsUrl);

//             this.consumerChannel.ack(msg);

//           } catch (err) {
//             const processingTime = Date.now() - startTime;
//             const errorMessage = err?.message || 'Unknown error';

//             console.log(`[PDF-Generation][Worker] ✗ Failed ${ticketNo}: ${errorMessage}`);

//             await this.handleFailedMessage(msg, wrappedMessage, errorMessage, processingTime);

//           } finally {
//             this.activeJobs--;
//             console.log(`[PDF-Generation][Worker] Job finished | Active: ${this.activeJobs}`);
//           }
//         },
//         { noAck: false }
//       );
//     }

//   private async handleFailedMessage(
//     msg: amqp.Message,
//     wrappedMessage: MessageWithRetry,
//     errorMessage: string,
//     processingTime: number
//   ) {
//     const ticketNo = wrappedMessage.payload.SupportTicketNo;
//     const currentAttempt = wrappedMessage.retryCount + 1;

//     await this.logProcessingAttempt(
//       ticketNo,
//       currentAttempt,
//       currentAttempt < this.MESSAGE_MAX_RETRIES ? 'retry' : 'failed',
//       errorMessage,
//       processingTime
//     );

//     if (currentAttempt < this.MESSAGE_MAX_RETRIES) {
//       console.log(`[PDF-Generation][Retry] ⚠ Retry ${currentAttempt + 1}/${this.MESSAGE_MAX_RETRIES} for ${ticketNo}`);

//       wrappedMessage.retryCount = currentAttempt;

//       setTimeout(async () => {
//         try {
//           if (this.producerChannel && !this.shuttingDown) {
//             this.producerChannel.sendToQueue(
//               this.QUEUE_NAME,
//               Buffer.from(JSON.stringify(wrappedMessage)),
//               {
//                 persistent: true,
//                 messageId: wrappedMessage.originalMessageId
//               }
//             );
//             console.log(`[PDF-Generation][Retry] ✓ Requeued ${ticketNo}`);
//             this.consumerChannel.ack(msg);
//           } else {
//             this.consumerChannel.nack(msg, false, false);
//           }
//         } catch (requeueError) {
//           console.log(`[PDF-Generation][Retry] ✗ Requeue failed: ${requeueError}`);
//           this.consumerChannel.nack(msg, false, false);
//         }
//       }, this.RETRY_DELAY);

//     } else {
//       console.log(`[PDF-Generation][DLQ] ✗ Max retries for ${ticketNo}. Moving to DLQ.`);

//       try {
//         const dlqPayload = {
//           ...wrappedMessage,
//           finalError: errorMessage,
//           failedAt: new Date().toISOString(),
//           totalAttempts: currentAttempt
//         };

//         if (this.producerChannel) {
//           this.producerChannel.sendToQueue(
//             this.DLQ_NAME,
//             Buffer.from(JSON.stringify(dlqPayload)),
//             { persistent: true }
//           );
//           console.log(`[PDF-Generation][DLQ] ✓ Moved ${ticketNo} to DLQ`);
//         }

//         await this.sendFailureAlert(ticketNo, errorMessage, currentAttempt);

//       } catch (dlqError) {
//         console.log(`[PDF-Generation][DLQ] ✗ Failed to move to DLQ: ${dlqError}`);
//       } finally {
//         this.consumerChannel.ack(msg);
//       }
//     }
//   }

//   private async logProcessingAttempt(
//     ticketNo: string,
//     attemptNumber: number,
//     status: 'processing' | 'success' | 'failed' | 'retry',
//     errorMessage?: string,
//     processingTime?: number,
//     gcsUrl?: string
//   ): Promise<void> {
//     try {
//       const logEntry: ProcessingLog = {
//         SupportTicketNo: ticketNo,
//         AttemptNumber: attemptNumber,
//         Status: status,
//         ErrorMessage: errorMessage,
//         ProcessingTime: processingTime,
//         GCSUrl: gcsUrl,
//         CreatedAt: new Date(),
//         UpdatedAt: new Date()
//       };

//       console.log(`[PDF-Generation][Log] ${status} | ${ticketNo} | Attempt ${attemptNumber}`);
//       this.logger.info(`Ticket: ${ticketNo} | Attempt: ${attemptNumber} | Status: ${status} | Error: ${errorMessage || 'N/A'}`);

//     } catch (logError) {
//       console.log(`[PDF-Generation][Log] Logging failed: ${logError}`);
//     }
//   }

//   private async sendFailureAlert(
//     ticketNo: string,
//     errorMessage: string,
//     totalAttempts: number
//   ): Promise<void> {
//     try {
//       console.log(`[PDF-Generation][Alert] CRITICAL: ${ticketNo} failed after ${totalAttempts} attempts`);
//       console.log(`[PDF-Generation][Alert] Error: ${errorMessage}`);
//     } catch (alertError) {
//       console.log(`[PDF-Generation][Alert] Alert failed: ${alertError}`);
//     }
//   }

//   private generateMessageId(): string {
//     return `msg_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
//   }

//   private monitorQueueAndAdjustConcurrency() {
//     setInterval(async () => {
//       if (!this.monitorChannel || !this.consumerChannel || this.shuttingDown) return;

//       try {
//         const q = await this.monitorChannel.checkQueue(this.QUEUE_NAME);
//         const dlq = await this.monitorChannel.checkQueue(this.DLQ_NAME);
//         const pending = q.messageCount;
//         const dlqCount = dlq.messageCount;

//         console.log(`[PDF-Generation][Monitor] Queue: ${pending} | Active: ${this.activeJobs} | DLQ: ${dlqCount}`);

//         let newPrefetch = pending >= 100 ? 10 : 5;

//         if (newPrefetch !== this.PREFETCH) {
//           console.log(`[PDF-Generation][Monitor] Updating prefetch to ${newPrefetch}`);
//           this.consumerChannel.prefetch(newPrefetch);
//           this.PREFETCH = newPrefetch;
//         }

//         if (dlqCount > 10) {
//           console.log(`[PDF-Generation][Monitor] WARNING: DLQ has ${dlqCount} messages`);
//         }

//       } catch (err) {
//         console.log(`[PDF-Generation][Monitor] Check failed: ${err}`);
//       }
//     }, 5000);
//   }

//   async onApplicationShutdown() {
//     console.log(`[PDF-Generation][Shutdown] Shutdown initiated`);
//     this.shuttingDown = true;

//     console.log(`[PDF-Generation][Shutdown] Waiting for ${this.activeJobs} jobs...`);
//     while (this.activeJobs > 0) {
//       await new Promise((r) => setTimeout(r, 100));
//     }

//     console.log(`[PDF-Generation][Shutdown] All tasks done. Closing connections`);
//     await this.cleanupConnections();
//   }

//   private async cleanupConnections() {
//     console.log(`[PDF-Generation][Cleanup] Cleaning up RabbitMQ connections`);
//     try { await this.consumerChannel?.close(); } catch {}
//     try { await this.producerChannel?.close(); } catch {}
//     try { await this.monitorChannel?.close(); } catch {}
//     try { await this.connection?.close(); } catch {}

//     this.consumerChannel = null;
//     this.producerChannel = null;
//     this.monitorChannel = null;
//     this.connection = null;
//   }
// }



import { Injectable, OnModuleInit, OnApplicationShutdown } from '@nestjs/common';
import * as amqp from 'amqplib';
import { runWorker } from './pdf-worker-runner';
import config from '../../../environment/config';
const Logger = require("../../../commonServices/logger");

interface ProcessingLog {
  SupportTicketNo: string;
  AttemptNumber: number;
  Status: 'processing' | 'success' | 'failed' | 'retry';
  ErrorMessage?: string;
  ProcessingTime?: number;
  GCSUrl?: string;
  CreatedAt: Date;
  UpdatedAt: Date;
}

interface MessageWithRetry {
  payload: any;
  retryCount: number;
  maxRetries: number;
  originalMessageId: string;
  firstAttemptTime?: string;
}

@Injectable()
export class PdfGenerationService implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger('pdf-generation-rmq.log');
  private readonly RABBITMQ_URL = config.rabbitmq;
  private readonly QUEUE_NAME = 'krph_support_ticket_pdf_generation_queue_1';
  private readonly DLQ_NAME = 'krph_support_ticket_pdf_generation_dlq';
  private PREFETCH = 5;
  private readonly RECONNECT_DELAY = 5000;
  private readonly MAX_RETRIES = 5;
  private readonly MESSAGE_MAX_RETRIES = 3;
  private readonly RETRY_DELAY = 5000;

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
      await this.producerChannel.assertQueue(this.DLQ_NAME, { durable: true });
      console.log(`[PDF-Generation][RMQ-Producer] Producer channel ready with DLQ`);

      this.consumerChannel = await this.connection.createChannel();
      await this.consumerChannel.assertQueue(this.QUEUE_NAME, { durable: true });
      await this.consumerChannel.assertQueue(this.DLQ_NAME, { durable: true });
      this.consumerChannel.prefetch(this.PREFETCH);
      console.log(`[PDF-Generation][RMQ-Consumer] Consumer channel ready`);

      this.monitorChannel = await this.connection.createChannel();
      console.log(`[PDF-Generation][RMQ-Monitor] Monitoring channel ready`);

      console.log(`[PDF-Generation][RMQ-Init] Listening on queue "${this.QUEUE_NAME}"`);

      this.consumeMessages();
      this.consumeDLQMessages(); // Added DLQ consumer
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
      const wrappedMessage: MessageWithRetry = {
        payload: message,
        retryCount: 0,
        maxRetries: this.MESSAGE_MAX_RETRIES,
        originalMessageId: this.generateMessageId(),
        firstAttemptTime: new Date().toISOString()
      };

      const payload = JSON.stringify(wrappedMessage);
      this.producerChannel.sendToQueue(
        this.QUEUE_NAME,
        Buffer.from(payload),
        {
          persistent: true,
          messageId: wrappedMessage.originalMessageId
        }
      );
      console.log(`[PDF-Generation][RMQ-Producer] Message dispatched: ${message.SupportTicketNo}`);
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

        let wrappedMessage: MessageWithRetry | null = null;

        try {
          const content = msg.content.toString();
          const parsed = JSON.parse(content);

          if (parsed.payload && parsed.retryCount !== undefined) {
            wrappedMessage = parsed;
          } else {
            wrappedMessage = {
              payload: parsed,
              retryCount: 0,
              maxRetries: this.MESSAGE_MAX_RETRIES,
              originalMessageId: msg.properties.messageId || this.generateMessageId(),
              firstAttemptTime: new Date().toISOString()
            };
          }
        } catch {
          console.log(`[PDF-Generation][Worker] Invalid JSON payload discarded`);
          this.consumerChannel.nack(msg, false, false);
          return;
        }

        this.activeJobs++;
        const ticketNo = wrappedMessage.payload.SupportTicketNo;
        const currentAttempt = wrappedMessage.retryCount + 1;

        console.log(`[PDF-Generation][Worker] Processing ${ticketNo} | Attempt ${currentAttempt}/${this.MESSAGE_MAX_RETRIES} | Active: ${this.activeJobs}`);

        await this.logProcessingAttempt(ticketNo, currentAttempt, 'processing');

        const startTime = Date.now();

        try {
          const result = await runWorker(wrappedMessage.payload);
          const processingTime = Date.now() - startTime;

          console.log(`[PDF-Generation][Worker] ✓ Completed ${ticketNo} in ${processingTime}ms`);

          await this.logProcessingAttempt(ticketNo, currentAttempt, 'success', undefined, processingTime, result?.gcsUrl);

          this.consumerChannel.ack(msg);

        } catch (err) {
          const processingTime = Date.now() - startTime;
          const errorMessage = err?.message || 'Unknown error';

          console.log(`[PDF-Generation][Worker] ✗ Failed ${ticketNo}: ${errorMessage}`);

          await this.handleFailedMessage(msg, wrappedMessage, errorMessage, processingTime);

        } finally {
          this.activeJobs--;
          console.log(`[PDF-Generation][Worker] Job finished | Active: ${this.activeJobs}`);
        }
      },
      { noAck: false }
    );
  }

  // NEW METHOD: DLQ Consumer
  private consumeDLQMessages() {
    if (!this.consumerChannel) return;

    console.log(`[PDF-Generation][DLQ-Consumer] DLQ Consumer started`);

    this.consumerChannel.consume(
      this.DLQ_NAME,
      async (msg) => {
        if (!msg || this.shuttingDown) return;

        console.log(`[PDF-Generation][DLQ-Worker] Message received from DLQ`);

        let wrappedMessage: MessageWithRetry | null = null;

        try {
          const content = msg.content.toString();
          const parsed = JSON.parse(content);

          if (parsed.payload && parsed.retryCount !== undefined) {
            wrappedMessage = parsed;
            // Reset retry count for DLQ messages to give them fresh attempts
            console.log(`[PDF-Generation][DLQ] Reprocessing message from DLQ, resetting retry count`);
            wrappedMessage.retryCount = 0;
          } else {
            wrappedMessage = {
              payload: parsed,
              retryCount: 0,
              maxRetries: this.MESSAGE_MAX_RETRIES,
              originalMessageId: msg.properties.messageId || this.generateMessageId(),
              firstAttemptTime: new Date().toISOString()
            };
          }
        } catch {
          console.log(`[PDF-Generation][DLQ-Worker] Invalid JSON payload discarded from DLQ`);
          this.consumerChannel.nack(msg, false, false);
          return;
        }

        this.activeJobs++;
        const ticketNo = wrappedMessage.payload.SupportTicketNo;
        const currentAttempt = wrappedMessage.retryCount + 1;

        console.log(`[PDF-Generation][DLQ-Worker] Processing ${ticketNo} | Attempt ${currentAttempt}/${this.MESSAGE_MAX_RETRIES} | Active: ${this.activeJobs}`);

        await this.logProcessingAttempt(ticketNo, currentAttempt, 'processing');

        const startTime = Date.now();

        try {
          const result = await runWorker(wrappedMessage.payload);
          const processingTime = Date.now() - startTime;

          console.log(`[PDF-Generation][DLQ-Worker] ✓ Completed ${ticketNo} in ${processingTime}ms`);

          await this.logProcessingAttempt(ticketNo, currentAttempt, 'success', undefined, processingTime, result?.gcsUrl);

          this.consumerChannel.ack(msg);

        } catch (err) {
          const processingTime = Date.now() - startTime;
          const errorMessage = err?.message || 'Unknown error';

          console.log(`[PDF-Generation][DLQ-Worker] ✗ Failed ${ticketNo}: ${errorMessage}`);

          await this.handleFailedMessage(msg, wrappedMessage, errorMessage, processingTime);

        } finally {
          this.activeJobs--;
          console.log(`[PDF-Generation][DLQ-Worker] Job finished | Active: ${this.activeJobs}`);
        }
      },
      { noAck: false }
    );
  }

  private async handleFailedMessage(
    msg: amqp.Message,
    wrappedMessage: MessageWithRetry,
    errorMessage: string,
    processingTime: number
  ) {
    const ticketNo = wrappedMessage.payload.SupportTicketNo;
    const currentAttempt = wrappedMessage.retryCount + 1;

    await this.logProcessingAttempt(
      ticketNo,
      currentAttempt,
      currentAttempt < this.MESSAGE_MAX_RETRIES ? 'retry' : 'failed',
      errorMessage,
      processingTime
    );

    if (currentAttempt < this.MESSAGE_MAX_RETRIES) {
      console.log(`[PDF-Generation][Retry] ⚠ Retry ${currentAttempt + 1}/${this.MESSAGE_MAX_RETRIES} for ${ticketNo}`);

      wrappedMessage.retryCount = currentAttempt;

      setTimeout(async () => {
        try {
          if (this.producerChannel && !this.shuttingDown) {
            this.producerChannel.sendToQueue(
              this.QUEUE_NAME,
              Buffer.from(JSON.stringify(wrappedMessage)),
              {
                persistent: true,
                messageId: wrappedMessage.originalMessageId
              }
            );
            console.log(`[PDF-Generation][Retry] ✓ Requeued ${ticketNo}`);
            this.consumerChannel.ack(msg);
          } else {
            this.consumerChannel.nack(msg, false, false);
          }
        } catch (requeueError) {
          console.log(`[PDF-Generation][Retry] ✗ Requeue failed: ${requeueError}`);
          this.consumerChannel.nack(msg, false, false);
        }
      }, this.RETRY_DELAY);

    } else {
      console.log(`[PDF-Generation][DLQ] ✗ Max retries for ${ticketNo}. Moving to DLQ.`);

      try {
        const dlqPayload = {
          ...wrappedMessage,
          finalError: errorMessage,
          failedAt: new Date().toISOString(),
          totalAttempts: currentAttempt
        };

        if (this.producerChannel) {
          this.producerChannel.sendToQueue(
            this.DLQ_NAME,
            Buffer.from(JSON.stringify(dlqPayload)),
            { persistent: true }
          );
          console.log(`[PDF-Generation][DLQ] ✓ Moved ${ticketNo} to DLQ`);
        }

        await this.sendFailureAlert(ticketNo, errorMessage, currentAttempt);

      } catch (dlqError) {
        console.log(`[PDF-Generation][DLQ] ✗ Failed to move to DLQ: ${dlqError}`);
      } finally {
        this.consumerChannel.ack(msg);
      }
    }
  }

  private async logProcessingAttempt(
    ticketNo: string,
    attemptNumber: number,
    status: 'processing' | 'success' | 'failed' | 'retry',
    errorMessage?: string,
    processingTime?: number,
    gcsUrl?: string
  ): Promise<void> {
    try {
      const logEntry: ProcessingLog = {
        SupportTicketNo: ticketNo,
        AttemptNumber: attemptNumber,
        Status: status,
        ErrorMessage: errorMessage,
        ProcessingTime: processingTime,
        GCSUrl: gcsUrl,
        CreatedAt: new Date(),
        UpdatedAt: new Date()
      };

      console.log(`[PDF-Generation][Log] ${status} | ${ticketNo} | Attempt ${attemptNumber}`);
      this.logger.info(`Ticket: ${ticketNo} | Attempt: ${attemptNumber} | Status: ${status} | Error: ${errorMessage || 'N/A'}`);

    } catch (logError) {
      console.log(`[PDF-Generation][Log] Logging failed: ${logError}`);
    }
  }

  private async sendFailureAlert(
    ticketNo: string,
    errorMessage: string,
    totalAttempts: number
  ): Promise<void> {
    try {
      console.log(`[PDF-Generation][Alert] CRITICAL: ${ticketNo} failed after ${totalAttempts} attempts`);
      console.log(`[PDF-Generation][Alert] Error: ${errorMessage}`);
    } catch (alertError) {
      console.log(`[PDF-Generation][Alert] Alert failed: ${alertError}`);
    }
  }

  private generateMessageId(): string {
    return `msg_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  private monitorQueueAndAdjustConcurrency() {
    setInterval(async () => {
      if (!this.monitorChannel || !this.consumerChannel || this.shuttingDown) return;

      try {
        const q = await this.monitorChannel.checkQueue(this.QUEUE_NAME);
        const dlq = await this.monitorChannel.checkQueue(this.DLQ_NAME);
        const pending = q.messageCount;
        const dlqCount = dlq.messageCount;

        console.log(`[PDF-Generation][Monitor] Queue: ${pending} | Active: ${this.activeJobs} | DLQ: ${dlqCount}`);

        let newPrefetch = pending >= 100 ? 10 : 5;

        if (newPrefetch !== this.PREFETCH) {
          console.log(`[PDF-Generation][Monitor] Updating prefetch to ${newPrefetch}`);
          this.consumerChannel.prefetch(newPrefetch);
          this.PREFETCH = newPrefetch;
        }

        if (dlqCount > 10) {
          console.log(`[PDF-Generation][Monitor] WARNING: DLQ has ${dlqCount} messages`);
        }

      } catch (err) {
        console.log(`[PDF-Generation][Monitor] Check failed: ${err}`);
      }
    }, 5000);
  }

  async onApplicationShutdown() {
    console.log(`[PDF-Generation][Shutdown] Shutdown initiated`);
    this.shuttingDown = true;

    console.log(`[PDF-Generation][Shutdown] Waiting for ${this.activeJobs} jobs...`);
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