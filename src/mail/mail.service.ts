// src/mail/mail.service.ts
import { Injectable } from '@nestjs/common';
import * as nodemailer from 'nodemailer';
import config from '../environment/config'; // import dynamic config


interface SendMailPayload {
  to: any;
  subject: string;
  text?: string;
  html?: string;
}

@Injectable()
export class MailService {
  private transporter: nodemailer.Transporter;
      private readonly emailUser = config.mail?.user
      private readonly emailPassword = config.mail?.password

  

  constructor() {
    this.transporter = nodemailer.createTransport({
      host: 'smtp.gmail.com',            // ✅ Correct Gmail SMTP host
      port: 587,
      secure: false,                     // TLS on port 587
      auth: {
        user: this.emailUser,
        pass: this.emailPassword, // ✅ Gmail App Password
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
