import { Injectable, OnModuleInit, OnApplicationShutdown } from '@nestjs/common';
import * as amqp from 'amqplib';
import config from '../../environment/config';

@Injectable()
export class NewTestBroker implements OnModuleInit, OnApplicationShutdown {

  private readonly RABBITMQ_URL = config.rabbitmq;
  private readonly QUEUE_NAME = 'cropLossIntimationStatusUpdate';

  private connection: amqp.Connection;
  private producerChannel: amqp.Channel;

  async onModuleInit() {
    console.log('---------------------------------------------');
    console.log(`[RabbitMQ] 🟡 Initialization started...`);
    
    try {
      console.log(`[RabbitMQ] 🔗 Connecting to: ${this.RABBITMQ_URL}`);
      this.connection = await amqp.connect(this.RABBITMQ_URL);
      console.log(`[RabbitMQ] ✅ Connection established`);

      console.log(`[RabbitMQ] 📦 Creating producer channel...`);
      this.producerChannel = await this.connection.createChannel();
      console.log(`[RabbitMQ] ✅ Channel created`);

      console.log(`[RabbitMQ] 🔍 Checking/Creating Queue: "${this.QUEUE_NAME}"`);
      await this.producerChannel.assertQueue(this.QUEUE_NAME, { durable: true });
      console.log(`[RabbitMQ] 🆗 Queue ready → "${this.QUEUE_NAME}"`);

      console.log(`[RabbitMQ] 🚀 Initialization complete`);
      console.log('---------------------------------------------');

    } catch (err) {
      console.error(`[RabbitMQ ❌ ERROR during setup]:`, err);
    }
  }

  async sendToQueueInfo(payload: any) {
    console.log(payload, "yes")
    console.log('\n---------------------------------------------');
    console.log(`[RabbitMQ] 📨 Preparing message...`);

    if (!this.producerChannel) {
      console.error(`[RabbitMQ ❌ ERROR]: Producer channel missing!`);
      throw new Error('[RabbitMQ] Producer channel not initialized!');
    }

    const jsonString = JSON.stringify(payload);
    console.log(`[RabbitMQ] 📦 Payload:`, jsonString);

    const bufferMsg = Buffer.from(jsonString);

    console.log(`[RabbitMQ] 📤 Sending message to queue "${this.QUEUE_NAME}"...`);
    this.producerChannel.sendToQueue(this.QUEUE_NAME, bufferMsg, { persistent: true });

    console.log(`[RabbitMQ] ✅ Message successfully queued`);
    console.log('---------------------------------------------\n');
  }

  async onApplicationShutdown() {
    console.log('---------------------------------------------');
    console.log(`[RabbitMQ] ⛔ Application shutting down...`);

    try {
      console.log(`[RabbitMQ] 🔄 Closing channel...`);
      await this.producerChannel?.close();
      console.log(`[RabbitMQ] 🆗 Channel closed`);

      console.log(`[RabbitMQ] 🔌 Closing connection...`);
      await this.connection?.close();
      console.log(`[RabbitMQ] 🆗 Connection closed`);

    } catch (err) {
      console.error(`[RabbitMQ ❌ ERROR closing connection]:`, err);
    }

    console.log(`[RabbitMQ] 🔚 Shutdown complete`);
    console.log('---------------------------------------------');
  }
}
