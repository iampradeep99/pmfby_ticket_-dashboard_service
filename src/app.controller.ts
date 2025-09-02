import { Controller, Get, Res } from '@nestjs/common';
import { Response } from 'express';

@Controller()
export class AppController {
  // Health check route
  @Get('health')
  healthCheck(@Res() res: Response) {
    res.send({ message: 'Hello World!', status: 'OK' });
  }

  // Redirect root /krphdashboard → /krphdashboard/ticket-dashboard
  @Get()
  redirectToDashboard(@Res() res: Response) {
    res.redirect('/krphdashboard/ticket-dashboard');
  }
}
