import { Controller, Post, Body, UseGuards, Req, Res } from '@nestjs/common';
import { AuthService, UserRole } from './auth.service';
import { JwtAuthGuard } from './guards/jwt-auth.guard';
import { Request, Response } from 'express';
import { UtilService } from '../commonServices/utilService';
import {
  jsonErrorHandler,
  jsonResponseHandler, jsonResponseHandlerCopy,jsonResponseHandlerReport
} from '../commonServices/responseHandler';



@Controller('auth')
export class AuthController {
  constructor(private readonly authService: AuthService,private readonly utilService: UtilService,) {}

  @Post('register')
  async register(
    @Body() payload: { name: string; email: string; password: string; mobile: string; role: UserRole },
    @Req() req: Request,
    @Res({ passthrough: false }) res: Response
  ) {
    try {
      let {data,message} = await this.authService.register(
        payload.name,
        payload.email,
        payload.password,
        payload.mobile,
        payload.role
      );
      if (data) data = await this.utilService.GZip(data);

      return jsonResponseHandler(data, message, req, res, () => {});
    } catch (err) {
      return jsonErrorHandler(err, req, res, () => {});
    }
  }

  @Post('login')
  async login(
    @Body() payload: { email: string; password: string },
    @Req() req: Request,
    @Res({ passthrough: false }) res: Response
  ) {
    try {
      let {data, message} = await this.authService.login(payload.email, payload.password);
      if (data) data = await this.utilService.GZip(data);
      return jsonResponseHandler(data, message, req, res, () => {});
    } catch (err) {
      return jsonErrorHandler(err, req, res, () => {});
    }
  }


  

  @Post('update-password')
  async updatePassword(
    @Body() payload: { email: string; newPassword: string },
    @Req() req: Request,
    @Res({ passthrough: false }) res: Response
  ) {
    try {
      let {data, message} = await this.authService.updatePassword(payload.email, payload.newPassword);
      if (data) data = await this.utilService.GZip(data);

      return jsonResponseHandler(data, message, req, res, () => {});
    } catch (err) {
      return jsonErrorHandler(err, req, res, () => {});
    }
  }

  @Post('protected')
  @UseGuards(JwtAuthGuard)
  async protected(
    @Req() req: Request,
    @Res({ passthrough: false }) res: Response
  ) {
    try {
      return jsonResponseHandler({ message: 'This is a protected route' }, 'Success', req, res, () => {});
    } catch (err) {
      return jsonErrorHandler(err, req, res, () => {});
    }
  }
}