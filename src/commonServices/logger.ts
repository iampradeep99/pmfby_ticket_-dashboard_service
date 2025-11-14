import fs = require("fs");
import path = require("path");

const logDirectory = path.join(__dirname, "logs");
if (!fs.existsSync(logDirectory)) fs.mkdirSync(logDirectory, { recursive: true });

class Logger {
  private logFile: string;

  constructor(filename: string = "app.log") {
    this.logFile = path.join(logDirectory, filename);
  }

  private writeLog(level: string, message: string) {
    const timestamp = new Date().toISOString();
    const logMessage = `${timestamp} [${level}] ${message}\n`;
    fs.appendFileSync(this.logFile, logMessage, { encoding: "utf8" });
  }

  info(message: string) { this.writeLog("INFO", message); }
  warn(message: string) { this.writeLog("WARN", message); }
  error(message: string) { this.writeLog("ERROR", message); }
}

export = Logger; // CommonJS export
