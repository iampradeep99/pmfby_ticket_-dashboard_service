import fs = require("fs");
import path = require("path");

const logDirectory = path.join(__dirname, "logs");
if (!fs.existsSync(logDirectory)) {
  fs.mkdirSync(logDirectory, { recursive: true });
}

class Logger {
  private logFile: string;
  private currentDate: string;

  constructor() {
    this.currentDate = this.getDateString();
    this.logFile = this.getLogFilePath();
  }

  private getDateString() {
    return new Date().toISOString().split("T")[0];
  }

  private getLogFilePath() {
    return path.join(logDirectory, `application-${this.currentDate}.log`);
  }

  private rotateLogFileIfNeeded() {
    const today = this.getDateString();
    if (today !== this.currentDate) {
      this.currentDate = today;
      this.logFile = this.getLogFilePath();
    }
  }

  private writeLog(level: string, message: string) {
    try {
      this.rotateLogFileIfNeeded();

      const timestamp = new Date().toISOString();
      const logMessage = `${timestamp} [${level}] ${message}\n`;

      fs.appendFileSync(this.logFile, logMessage, { encoding: "utf8" });
    } catch (err) {
      console.error("Logger failed:", err);
      console.log(`${level}:`, message);
    }
  }

  info(message: string) { this.writeLog("INFO", message); }
  warn(message: string) { this.writeLog("WARN", message); }
  error(message: string) { this.writeLog("ERROR", message); }
}

export = Logger;
