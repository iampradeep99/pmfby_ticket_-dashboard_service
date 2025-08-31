// GCPServices.ts

import axios from 'axios';
import * as FormData from 'form-data';
interface UploadFileData {
  filePath: string;
  uploadedBy: string;
  file: {
    buffer: Buffer;
    originalname: string;
  };
}

interface UploadResponse {
  success: boolean;
  message: string;
  url?: string;
  [key: string]: any; 
}

export class GCPServices {
  async uploadFileToGCP(fileData: UploadFileData): Promise<UploadResponse> {
    try {
      const { filePath, uploadedBy, file } = fileData;

      if (!filePath || !uploadedBy || !file) {
        throw new Error("Missing required fields: filePath, uploadedBy, or file");
      }

      const formData = new FormData();
      formData.append('filePath', filePath);
      formData.append('documents', file.buffer, file.originalname);
      formData.append('uploadedBy', uploadedBy);

      const uploadUrl = process.env.GCP_UPLOAD_URL;
      if (!uploadUrl) throw new Error("Missing GCP_UPLOAD_URL environment variable");

      const response = await axios.post(uploadUrl, formData, {
        headers: {
          ...formData.getHeaders()
        }
      });

      if (response.status === 200) {
        return response.data;
      } else {
        throw new Error('Failed to upload file to GCP');
      }
    } catch (error: any) {
      console.error(error);
      throw new Error(`Error uploading file to GCP: ${error.message}`);
    }
  }
}
