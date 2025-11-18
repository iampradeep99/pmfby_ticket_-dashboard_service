import * as zlib from "zlib";
import jwt from "jsonwebtoken";
import { Injectable } from "@nestjs/common";
import axios from 'axios'
import * as moment from "moment";
import { fixedHolidays, floatingHolidays } from './holidays'; 
const Logger = require("../commonServices/logger");


@Injectable()
export class UtilService {
  private logger: InstanceType<typeof Logger>;

    constructor(

    ) {
      this.logger = new Logger("Utility.log");

    }

    /**
     * Generate JWT Token
     */
    static async generateJwtToken(id: string | number, username: string): Promise<string> {
        const timestamp = Math.floor(Date.now() / 1000);

        const token = jwt.sign(
            {
                id,
                username,
                iat: timestamp
            },
            process.env.JWT_SECRET as string,
            { expiresIn: "30d" }
        );

        return token;
    }

    /**
     * Placeholder for password hashing
     */
    async hashPassword(password: string): Promise<string> {
        // TODO: Implement hashing logic (e.g., bcrypt)
        return password;
    }

    /**
     * Compress payload to Base64 GZIP
     */
    async GZip(payload: Record<string, unknown>): Promise<string> {
        const stringify = JSON.stringify(payload);
        const buffer = zlib.gzipSync(stringify);
        return buffer.toString("base64");
    }

    /**
     * Compress payload and return Buffer
     */
    async GZipBI(payload: Record<string, unknown>): Promise<Buffer> {
        const stringify = JSON.stringify(payload);
        return zlib.gzipSync(stringify);
    }

    /**
     * Decompress Base64 GZIP data
     */
    async unGZip(data: string): Promise<Record<string, unknown>> {
        const buffer = Buffer.from(data, "base64");

        const uncompressedBuffer = await new Promise<Buffer>((resolve, reject) => {
            zlib.gunzip(buffer, (err, result) => {
                if (err) reject(err);
                else resolve(result);
            });
        });

        const jsonString = uncompressedBuffer.toString("utf-8");
        return JSON.parse(jsonString);
    }

    /**
     * Convert string to Unicode hex
     */
    GetSingleUnicodeHex(x: string): string {
        let result = "";
        for (let i = 0; i < x.length; i++) {
            result += ("000" + x.charCodeAt(i).toString(16)).slice(-4);
        }
        return result;
    }

      async convertStringToArray(str) {
    return str.split(",").map(Number);
  }

  async getSupportTicketUserDetail(userID) {
    const data = { userID };
    const TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHBpcmVzSW4iOiIyMDI0LTEwLTA5VDE4OjA4OjA4LjAyOFoiLCJpYXQiOjE3Mjg0NjEyODguMDI4LCJpZCI6NzA5LCJ1c2VybmFtZSI6InJhamVzaF9iYWcifQ.niMU8WnJCK5SOCpNOCXMBeDrsr2ZqC96LUzQ5Z9MoBk'

    const url = 'https://pmfby.gov.in/krphapi/FGMS/GetSupportTicketUserDetail'
    return axios.post(url, data, {
      headers: {
        'Content-Type': 'application/json',
        'Authorization': TOKEN
      }
    })
      .then(response => {
        return response.data;
      })
      .catch(error => {
        console.error('Error:', error);
        throw error;
      });
  };

  async  GetDetailsForDistrictUsers(userID:any) {
  const data = {
    filterID: userID,
    filterID1: 0,
    masterName: "DISTASIGN",
    searchText: "#ALL",
    searchCriteria: "AW",
    objCommon: {
      insertedUserID: userID?.toString() || "3",
      insertedIPAddress: "10.234.55.44",
      dateShort: "yyyy-MM-dd",
      dateLong: "yyyy-MM-dd HH:mm:ss"
    }
  };

  const TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHBpcmVzSW4iOiIyMDI0LTEwLTA5VDE4OjA4OjA4LjAyOFoiLCJpYXQiOjE3Mjg0NjEyODguMDI4LCJpZCI6NzA5LCJ1c2VybmFtZSI6InJhamVzaF9iYWcifQ.niMU8WnJCK5SOCpNOCXMBeDrsr2ZqC96LUzQ5Z9MoBk';

  const url = 'https://pmfby.gov.in/krphapi/FGMS/GetMasterDataBinding';

  try {
    const response = await axios.post(url, data, {
      headers: {
        'Content-Type': 'application/json',
        'Authorization': TOKEN
      }
    });
    return response.data;
  } catch (error) {
    console.error('Error:', error.response?.data || error.message);
    throw error;
  }
}


async generateCacheKey(prefix: string, payload: any) {
  if (!prefix) prefix = 'cache';
  let payloadString: string;
  try {
    payloadString = JSON.stringify(payload || {});
  } catch {
    payloadString = String(payload);
  }
  const normalized = payloadString.replace(/\s+/g, '').toLowerCase();
  const hash = require('crypto').createHash('sha256').update(normalized).digest('hex');
  return `${prefix}:${hash}`;
}

async getStatusName(statusID: any): Promise<string> {
  let statusName = '';
    statusID = Number(statusID)
  switch (statusID) {
    case 109301:
      statusName = 'Open';
      break;
    case 109302:
      statusName = 'In-Progress';
      break;
    case 109303:
      statusName = 'Resolved';
      break;
    case 109304:
      statusName = 'Re-Open';
      break;
    default:
      statusName = 'Unknown Status';
      break;
  }

  return statusName;
}



async getWorkingDays(year, month) {
    let workingDays = 0;

    const start = moment(`${year}-${String(month).padStart(2, "0")}-01`, "YYYY-MM-DD");
    const end = start.clone().endOf("month");

    const current = start.clone();

    while (current.isSameOrBefore(end)) {
        workingDays++;
        current.add(1, "day");
    }

    return workingDays;
}





getNationalHolidays(year: number, month: number) {
  const allHolidays: moment.Moment[] = [];

  for (const key of Object.keys(fixedHolidays)) {
    const [holidayMonth, day] = key.split("-");
    allHolidays.push(moment(`${year}-${holidayMonth.padStart(2, "0")}-${day.padStart(2, "0")}`, "YYYY-MM-DD"));
  }

  if (floatingHolidays[year]) {
    for (const key of Object.keys(floatingHolidays[year])) {
      const [holidayMonth, day] = key.split("-");
      allHolidays.push(moment(`${year}-${holidayMonth.padStart(2, "0")}-${day.padStart(2, "0")}`, "YYYY-MM-DD"));
    }
  }

  return allHolidays.filter(h => h.month() + 1 === month);
}


    shuffleTotalMinutes(totalMinutes) {
    const variation = Math.random() * 0.2 - 0.1;
    return totalMinutes * (1 + variation);
}

 cleanupCollection = async (db, collectionName, filter) => {
  if (!db) {
    throw new Error('Database connection is required');
  }
  if (!collectionName || typeof collectionName !== 'string') {
    throw new Error('Collection name must be a non-empty string');
  }
  if (!filter || typeof filter !== 'object' || Object.keys(filter).length === 0) {
    this.logger.info(`No valid filter provided for deleting records in '${collectionName}'. Skipping operation.`);
    return {
      success: true,
      skipped: true,
      deletedCount: 0,
      message: 'Operation skipped due to empty or invalid filter'
    };
  }

  try {
    const collection = db.collection(collectionName);
    if (!collection) {
      throw new Error(`Collection '${collectionName}' not found`);
    }

    const result = await collection.deleteMany(filter);
    const deletedCount = result?.deletedCount || 0;
    
    this.logger.info(`Deleted ${deletedCount} records from '${collectionName}'.`);
    
    return {
      success: true,
      skipped: false,
      deletedCount,
      message: `Successfully deleted ${deletedCount} records`
    };
  } catch (err) {
    const errorMsg = `Error deleting from collection '${collectionName}': ${err.message}`;
    this.logger.info(errorMsg, err);
    throw new Error(errorMsg);
  }
};


}
