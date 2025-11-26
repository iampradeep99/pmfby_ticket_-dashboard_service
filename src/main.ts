import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import * as express from 'express';
import * as path from 'path';
import { RedisWrapper } from './commonServices/redisWrapper';
import * as crypto from 'crypto';
import config from './environment/config';
import { ValidationPipe } from '@nestjs/common';

(global as any).crypto = crypto;

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  app.setGlobalPrefix('krphdashboard');

  app.use('/downloads', express.static(path.join(__dirname, '../downloads')));
 app.use('/krphdashboard/assets', express.static(path.join(__dirname, '../assets')));


  app.enableCors({
    origin: '*',
    methods: 'GET,HEAD,PUT,PATCH,POST,DELETE,OPTIONS',
    allowedHeaders: '*',
  });

  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
    })
  );

  const redis = new RedisWrapper();
  try {
    await redis.connectionInit({ url: config.redis });
    console.log('Redis connected and subscriber started');
  } catch (err) {
    console.error('Redis init error:', err);
  }

  const port = process.env.PORT ? Number(process.env.PORT) : 5500;
  await app.listen(port);
  console.log(`Server running on port ${port}`);
}

bootstrap();
