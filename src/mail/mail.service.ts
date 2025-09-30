// src/mail/mail.service.ts
import { Injectable } from '@nestjs/common';
import * as nodemailer from 'nodemailer';

interface SendMailPayload {
  to: any;
  subject: string;
  text?: string;
  html?: string;
}

@Injectable()
export class MailService {
  private transporter: nodemailer.Transporter;

  constructor() {
    this.transporter = nodemailer.createTransport({
      host: 'smtp.gmail.com',            // ✅ Correct Gmail SMTP host
      port: 587,
      secure: false,                     // TLS on port 587
      auth: {
        user: process.env.SEND_MAIL_USER,
        pass: process.env.APP_PASSWORD_GMAIL, // ✅ Gmail App Password
      },
    });
  }

  async sendMail(payload: SendMailPayload): Promise<void> {
    const { to, subject, text, html } = payload;

    await this.transporter.sendMail({
      from: `"Support Team" <${process.env.SEND_MAIL_USER}>`, // Better sender name
      to,
      subject,
      text: text || subject, // fallback to subject if text is missing
      html,
    });
  }
}
