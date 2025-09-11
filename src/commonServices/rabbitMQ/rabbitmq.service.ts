import { Injectable, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib';
import { TicketDashboardService } from '../../ticket-dashboard/ticket-dashboard.service';

@Injectable()
export class RabbitMQService implements OnModuleInit {
  private readonly RABBITMQ_URL = 'amqp://user:password@10.128.60.11:5672';
  private readonly QUEUE_NAME = 'support_ticket_download';
  private connection: amqp.Connection;
  private channel: amqp.Channel;

  constructor(private readonly ticketDashboardService: TicketDashboardService) {}

  async onModuleInit() {
    console.log('[RabbitMQ] Connecting to RabbitMQ server...');
    this.connection = await amqp.connect(this.RABBITMQ_URL);
    this.channel = await this.connection.createChannel();
    await this.channel.assertQueue(this.QUEUE_NAME, { durable: true });
    console.log(`[RabbitMQ] Connected. Listening to queue "${this.QUEUE_NAME}"...`);

    this.consumeMessages();
  }

  async sendToQueue(message: any) {
    console.log('[RabbitMQ] Sending message to queue:', message);
    await this.channel.sendToQueue(
      this.QUEUE_NAME,
      Buffer.from(JSON.stringify(message)),
      { persistent: true }
    );
    console.log('[RabbitMQ] Message successfully sent to queue');
  }

  private async consumeMessages() {
    console.log(`[RabbitMQ] Starting to consume messages from "${this.QUEUE_NAME}" queue...`);
    this.channel.prefetch(1);
    this.channel.consume(this.QUEUE_NAME, async (msg) => {
      if (msg) {
        const ticketPayload = JSON.parse(msg.content.toString());
        console.log('[RabbitMQ] Received job from queue:', ticketPayload);

        try {
          await this.ticketDashboardService.processTicketHistoryAndGenerateZip(ticketPayload);
          console.log('[RabbitMQ] Job processed successfully');
          this.channel.ack(msg);
        } catch (err) {
          console.error('[RabbitMQ] Job processing failed:', err);
          this.channel.nack(msg, false, true); // Requeue job in case of failure
        }
      }
    });
  }
}
