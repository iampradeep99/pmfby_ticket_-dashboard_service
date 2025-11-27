import * as dotenv from "dotenv";
dotenv.config();

interface DBConfig {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
}

interface MailConfig {
  user: string;
  password: string;
}

interface GupshupConfig {
  userid: string;
  password: string;
  version: string;
  format: string;
  msg_type: string;
  method: string;
  caption: string;
  gupshupAPIUrl : string;
}

interface Config {
  mongodb: string;
  redis: string;
  mysql: DBConfig;
  rabbitmq: string;
  mail: MailConfig;
  gcpUpload: string;
  pmfbyRoleURL: string;
  krphPDFTicketInfoURL: string;
  krphPDFTicketToken: string;
  gupshupConfig: GupshupConfig;
}

const env = process.env.NODE_ENV || "uat";

const config: { [key: string]: Config } = {
  local: {
    mongodb: process.env.LOCAL_MONGO!,
    redis: process.env.LOCAL_REDIS!,
    mysql: {
      host: process.env.LOCAL_DB_HOST!,
      port: Number(process.env.LOCAL_DB_PORT),
      user: process.env.LOCAL_DB_USER!,
      password: process.env.LOCAL_DB_PASS!,
      database: process.env.LOCAL_DB_NAME!
    },
    rabbitmq: process.env.LOCAL_RABBIT!,
    mail: {
      user: process.env.LOCAL_MAIL_USER!,
      password: process.env.LOCAL_MAIL_PASS!
    },
    gcpUpload: process.env.LOCAL_GCP_UPLOAD!,
    pmfbyRoleURL: process.env.PMFBY_ROLE_URL!,
    krphPDFTicketInfoURL: process.env.KPRH_PDF_TICKET_INFO_URL!,
    krphPDFTicketToken: process.env.KPRH_PDF_TICKET_TOKEN!,
    gupshupConfig: {
      userid: process.env.GUPSHUP_USERID!,
      password: process.env.GUPSHUP_PASSWORD!,
      version: process.env.GUPSHUP_VERSION!,
      format: process.env.GUPSHUP_FORMAT!,
      msg_type: process.env.GUPSHUP_MSG_TYPE!,
      method: process.env.GUPSHUP_METHOD!,
      caption: process.env.GUPSHUP_CAPTION!,
      gupshupAPIUrl:process.env.GUPSHUP_API_URL!
    }
  },
  uat: {
    mongodb: process.env.UAT_MONGO!,
    redis: process.env.UAT_REDIS!,
    mysql: {
      host: process.env.UAT_DB_HOST!,
      port: Number(process.env.UAT_DB_PORT),
      user: process.env.UAT_DB_USER!,
      password: process.env.UAT_DB_PASS!,
      database: process.env.UAT_DB_NAME!
    },
    rabbitmq: process.env.UAT_RABBIT!,
    mail: {
      user: process.env.UAT_MAIL_USER!,
      password: process.env.UAT_MAIL_PASS!
    },
    gcpUpload: process.env.UAT_GCP_UPLOAD!,
    pmfbyRoleURL: process.env.PMFBY_ROLE_URL!,
    krphPDFTicketInfoURL: process.env.KPRH_PDF_TICKET_INFO_URL!,
    krphPDFTicketToken: process.env.KPRH_PDF_TICKET_TOKEN!,
    gupshupConfig: {
      userid: process.env.GUPSHUP_USERID!,
      password: process.env.GUPSHUP_PASSWORD!,
      version: process.env.GUPSHUP_VERSION!,
      format: process.env.GUPSHUP_FORMAT!,
      msg_type: process.env.GUPSHUP_MSG_TYPE!,
      method: process.env.GUPSHUP_METHOD!,
      caption: process.env.GUPSHUP_CAPTION!,
      gupshupAPIUrl:process.env.GUPSHUP_API_URL!

    }
  },
  prod: {
    mongodb: process.env.PROD_MONGO!,
    redis: process.env.PROD_REDIS!,
    mysql: {
      host: process.env.PROD_DB_HOST!,
      port: Number(process.env.PROD_DB_PORT),
      user: process.env.PROD_DB_USER!,
      password: process.env.PROD_DB_PASS!,
      database: process.env.PROD_DB_NAME!
    },
    rabbitmq: process.env.PROD_RABBIT!,
    mail: {
      user: process.env.PROD_MAIL_USER!,
      password: process.env.PROD_MAIL_PASS!
    },
    gcpUpload: process.env.PROD_GCP_UPLOAD!,
    pmfbyRoleURL: process.env.PMFBY_ROLE_URL!,
    krphPDFTicketInfoURL: process.env.KPRH_PDF_TICKET_INFO_URL!,
    krphPDFTicketToken: process.env.KPRH_PDF_TICKET_TOKEN!,
    gupshupConfig: {
      userid: process.env.GUPSHUP_USERID!,
      password: process.env.GUPSHUP_PASSWORD!,
      version: process.env.GUPSHUP_VERSION!,
      format: process.env.GUPSHUP_FORMAT!,
      msg_type: process.env.GUPSHUP_MSG_TYPE!,
      method: process.env.GUPSHUP_METHOD!,
      caption: process.env.GUPSHUP_CAPTION!,
      gupshupAPIUrl:process.env.GUPSHUP_API_URL!

    }
  }
};

export default config[env];
