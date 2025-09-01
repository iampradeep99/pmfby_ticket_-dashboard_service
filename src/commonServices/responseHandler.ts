import replace from "lodash/replace.js";
import { Request, Response, NextFunction } from "express";
import { constant } from "./constant"; // Make sure this has TS types

export const jsonErrorHandler = (
  err: any,
  req: Request,
  res: Response,
  next: NextFunction
) => {
  if (typeof err.code === "string" && (err.code.startsWith("42") || err.code.startsWith("22"))) {
    err.message = "Internal server error: Database error during request";
  } else if (err.code?.startsWith("400")) {
    err.message = "Error : Bad Request ";
  } else if (err.code?.startsWith("401")) {
    err.message = "Error : Un Authorized Request";
  } else if (err.code?.startsWith("403")) {
    err.message = "Error : Forbidden Request";
  } else if (err.code?.startsWith("404")) {
    err.message = "Error : Request Not Found ";
  } else if (err.code?.startsWith("405")) {
    err.message = "Error : Request Method Not Allowed";
  } else if (err.code?.startsWith("408")) {
    err.message = "Error : Request Timeout ";
  } else if (err.code?.startsWith("409")) {
    err.message = "Error : Request Conflict";
  } else if (err.code?.startsWith("500")) {
    err.message = "Error : Internal Server Error ";
  } else if (err.code?.startsWith("502")) {
    err.message = "Error : Bad Gateway ";
  } else if (err.code?.startsWith("503")) {
    err.message = "Error : Service Unavailable";
  } else if (err.code?.startsWith("504")) {
    err.message = "Error : Gateway Timeout ";
  }

  const message = err.message?.replace("Error: ", "");

  return res.json({
    responseObject: null,
    responseDynamic: null,
    responseCode: "0",
    responseMessage: message || "Unhandled error",
    jsonString: null,
    recordCount: 0,
  });
};

export const sendErrorResponse = (
  res: Response,
  message: string,
  code: number | string
) => {
  return res.status(200).json({
    rmessage: message,
    rcode: code,
  });
};

export const joiErrorHandler = (error: any) => {
  const errors: { field: string; message: string }[] = [];

  if (error?.details?.length > 0) {
    error.details.forEach((err: any) => {
      const message = replace(err.message, /"/g, "");
      const key = err.context.key;
      errors.push({ field: key, message });
    });
  }

  return errors;
};

export const jsonResponseHandler = (
  data: any,
  message: any,

  req: Request,
  res: Response,
  next: NextFunction
) => {
  const messages = message?.msg || message;
  const code = message?.code;

  res.status(200).send({
    responseObject: null,
    responseDynamic: data || null,
    responseCode: code || "1",
    responseMessage: messages || constant.ResponseStatus.SUCCESS,
    jsonString: null,
    recordCount: 0,
  });
};




export const jsonResponseHandlerCopy = (
  data?: any,
  message?: any,
  pagination?: any,
  req?: Request,
  res?: Response,
  next?: NextFunction
) => {
  // Handle message as string or object
  const messages = message?.msg || message || constant.ResponseStatus.SUCCESS;
  const code = message?.code || "1";

  // Calculate record count
  const recordCount = Array.isArray(data) ? data.length : data ? 1 : 0;

  const responsePayload = {
    responseObject: null,
    responseDynamic: data || null,
    responseCode: code,
    responseMessage: messages,
    jsonString: null,
    recordCount,
    pagination: pagination || null
  };

  // Only send if `res` is passed
  if (res) {
    res.status(200).send(responsePayload);
  } else {
    // fallback: just return the object
    return responsePayload;
  }
};


export const jsonResponseHandlerOther = (
  data: any,
  message: any,
  req: Request,
  res: Response,
  next: NextFunction
) => {
  const messages = message?.msg || message;
  const code = message?.code;

  res.status(200).send({
    responseObject: null,
    responseDynamic: data || null,
    responseCode: code || "1",
    responseMessage: messages || constant.ResponseStatus.SUCCESS,
    jsonString: null,
  });
};
