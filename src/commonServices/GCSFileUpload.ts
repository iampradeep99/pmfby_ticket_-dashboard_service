// GCPServices.ts

import axios from 'axios';
import * as FormData from 'form-data';
import config from '../environment/config';
import { UtilService } from './utilService';


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

      const uploadUrl = this.gcpFileUploadUrl || 'https://pmfby.gov.in/krphapi/FGMS/GCPFileUploadForCDR';
      const response = await axios.post(uploadUrl, formData, {
        headers: {
          ...formData.getHeaders()
        },
        maxBodyLength: Infinity,
        maxContentLength: Infinity,
        timeout: Number(process.env.GCP_UPLOAD_TIMEOUT_MS) || 1800000
      });

      if (response.status !== 200) {
        throw new Error('Failed to upload file to GCP');
      }

      if (response?.data?.responseCode && response.data.responseCode !== '1') {
        throw new Error(response.data.responseMessage || 'GCP upload failed');
      }

      const responseDynamic = response?.data?.responseDynamic;
      const decodedResponse = typeof responseDynamic === 'string'
        ? await new UtilService().unGZip(responseDynamic)
        : responseDynamic;

      const uploadedFile = Array.isArray(decodedResponse)
        ? decodedResponse[0]
        : decodedResponse;
      const uploadedUrl =
        uploadedFile?.gcsUrl ||
        uploadedFile?.gcsURL ||
        uploadedFile?.url ||
        uploadedFile?.path;

      if (!uploadedUrl) {
        throw new Error('GCP upload completed but file URL was not returned');
      }

      return {
        ...response.data,
        url: uploadedUrl,
        file: [
          {
            ...uploadedFile,
            gcsUrl: uploadedUrl
          }
        ],
        fileInfo: uploadedFile,
        rawResponse: response.data
      };
    } catch (error: any) {
      console.error(error);
      throw new Error(`Error uploading file to GCP: ${error.message}`);
    }
  }
}
