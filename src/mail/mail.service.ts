// src/mail/mail.service.ts
import { Injectable } from '@nestjs/common';
import * as nodemailer from 'nodemailer';

@Injectable()
export class MailService {
  private transporter: nodemailer.Transporter;

  constructor() {
    this.transporter = nodemailer.createTransport({
      host: 'smtp.example.com',
      port: 587,
      secure: false,
      auth: {
        user: process.env.SEND_MAIL_USER,
        pass: process.env.APP_PASSWORD_GMAIL,
      },
    });
  }

  async sendMail(payload): Promise<void> {
    await this.transporter.sendMail({
      from: process.env.SEND_MAIL_USER,
      to:payload?.to,
      subject:payload?.subject,
      text:payload?.text,
      html:payload?.html,
    });
  }
}
