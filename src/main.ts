import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import * as express from 'express';
import * as path from 'path';


async function bootstrap() {
  const app = await NestFactory.create(AppModule);
    app.use('/downloads', express.static(path.join(__dirname, '../downloads')));
  await app.listen(process.env.PORT ?? 5500);
}
bootstrap();
