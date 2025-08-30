import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import * as express from 'express';
import * as path from 'path';
import { RedisWrapper } from '../src/commonServices/redisWrapper'; // ✅ adjust this

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
   const redis = new RedisWrapper();
    app.use('/downloads', express.static(path.join(__dirname, '../downloads')));
    try {
    await redis.connectionInit({ url: "redis://10.128.60.9:6379" });
    console.log('✅ Redis connected and subscriber started');
  } catch (err) {
    console.error('❌ Redis init error:', err);
  }
  await app.listen(process.env.PORT ?? 5500);
}
bootstrap();
