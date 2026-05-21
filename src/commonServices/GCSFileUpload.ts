// GCPServices.ts

import axios from 'axios';
import * as FormData from 'form-data';
import * as fs from 'fs';
import config from '../environment/config'



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
    private readonly gcpFileUploadUrl = config.gcpUpload

  constructor(){

  }
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

      const uploadUrl = this.gcpFileUploadUrl;
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

  async uploadFilePathToGCP(fileData: {
    filePath: string;
    uploadedBy: string;
    localFilePath: string;
    originalname: string;
  }): Promise<UploadResponse> {
    try {
      const { filePath, uploadedBy, localFilePath, originalname } = fileData;

      if (!filePath || !uploadedBy || !localFilePath || !originalname) {
        throw new Error("Missing required fields: filePath, uploadedBy, localFilePath, or originalname");
      }

      const uploadUrl = this.gcpFileUploadUrl;
      if (!uploadUrl) throw new Error("Missing GCP_UPLOAD_URL environment variable");

      const formData = new FormData();
      formData.append('filePath', filePath);
      formData.append('documents', fs.createReadStream(localFilePath), originalname);
      formData.append('uploadedBy', uploadedBy);

      const response = await axios.post(uploadUrl, formData, {
        headers: {
          ...formData.getHeaders()
        },
        maxBodyLength: Infinity,
        maxContentLength: Infinity,
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
