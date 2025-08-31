/**
 * Generates HTML email content for support ticket download.
 * 
 * @param username - Name of the user receiving the email
 * @param requestDateTime - Date & time of the support request
 * @param downloadLink - Secure link to download the report
 * @returns HTML content as a string
 */
export function generateSupportTicketEmailHTML(
  username: string,
  requestDateTime: string,
  downloadLink: string
): string {
  return `
    <!DOCTYPE html>
    <html>
      <head>
        <style>
          body {
            font-family: Arial, sans-serif;
            color: #333;
            background-color: #f9f9f9;
            padding: 20px;
          }
          .container {
            max-width: 600px;
            margin: auto;
            background-color: #fff;
            border-radius: 8px;
            padding: 30px;
            box-shadow: 0 0 10px rgba(0, 0, 0, 0.05);
          }
          .btn {
            display: inline-block;
            padding: 10px 20px;
            margin-top: 20px;
            background-color: #007bff;
            color: white;
            text-decoration: none;
            border-radius: 4px;
          }
          .footer {
            margin-top: 30px;
            font-size: 12px;
            color: #777;
          }
        </style>
      </head>
      <body>
        <div class="container">
          <h2>Hi ${username},</h2>
          <p>We hope this message finds you well.</p>
          <p>
            As per your request on <strong>${requestDateTime}</strong> regarding your support ticket,
            we have compiled the relevant report for your reference.
          </p>
          <p>Please click the button below to securely download your report:</p>
          <a href="${downloadLink}" class="btn" target="_blank">Download Report</a>
          <p class="footer">
            If you have any further questions or need additional assistance, feel free to contact our support team.<br><br>
            Regards,<br>
            <strong>PMFBY Support Team</strong>
          </p>
        </div>
      </body>
    </html>
  `;
}


export function getCurrentFormattedDateTime(): string {
  const now = new Date();

  const pad = (num: number) => num.toString().padStart(2, '0');

  const day = pad(now.getDate());
  const month = pad(now.getMonth() + 1); // Months are 0-based
  const year = now.getFullYear();

  const hours = pad(now.getHours());
  const minutes = pad(now.getMinutes());
  const seconds = pad(now.getSeconds());

  return `${day}-${month}-${year} ${hours}:${minutes}:${seconds}`;
}
