import * as zlib from "zlib";
import jwt from "jsonwebtoken";

export class UtilService {
    constructor() {}

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
}
