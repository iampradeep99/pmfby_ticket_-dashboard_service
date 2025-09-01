// app.controller.ts
import { Controller, Get, Res } from '@nestjs/common';
import { Response } from 'express';

@Controller()
export class AppController {
  @Get()
  healthCheck(@Res() res: Response) {
    // This will be at: GET /krphdashboard
    res.send({ message: 'Hello World!', status: 'OK' });
  }
}
