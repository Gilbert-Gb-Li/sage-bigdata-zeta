connect 'jdbc:derby:../db/master;create=true';

DROP TABLE APP.CONFIG;
CREATE TABLE APP.CONFIG
(
   ID VARCHAR(64) PRIMARY KEY NOT NULL,
   NAME VARCHAR(64),
   DATA CLOB(1073741823),
   STATUS VARCHAR(64),
   LAST_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
   CREATE_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
   IS_SAMPLE INT DEFAULT 0 NOT NULL
);

DROP TABLE APP.PARSER;
CREATE TABLE APP.PARSER(
   ID VARCHAR(64) PRIMARY KEY NOT NULL,
   NAME VARCHAR(64),
   SAMPLE CLOB(1073741823),
   PARSER CLOB(1073741823),
   PROPERTIES CLOB(1073741823),
   DATASOURCE VARCHAR(64),
   LAST_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
   CREATE_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
   IS_SAMPLE INT DEFAULT 0 NOT NULL
);

DROP TABLE APP.ANALYZER;
CREATE TABLE APP.ANALYZER(
    ID VARCHAR(64) PRIMARY KEY NOT NULL,
    NAME VARCHAR(64),
    DATA CLOB,
    CHANNEL VARCHAR(64),
    LAST_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CREATE_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    IS_SAMPLE INT NOT NULL DEFAULT 0
);

DROP TABLE APP.DATASOURCE;
CREATE TABLE APP.DATASOURCE
(
   ID VARCHAR(64) PRIMARY KEY NOT NULL,
   NAME VARCHAR(64),
   DATA CLOB(1073741823),
   COLLECTOR VARCHAR(64),
   LAST_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
   CREATE_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
   IS_SAMPLE INT DEFAULT 0 NOT NULL
);

DROP TABLE APP.KNOWLEDGE;
CREATE TABLE APP.KNOWLEDGE
(
   ID VARCHAR(64) PRIMARY KEY NOT NULL,
   NAME VARCHAR(64),
   DRIVERID VARCHAR(64),
   COLLECTORID VARCHAR(64),
   PARSERID VARCHAR(64),
   STATUS VARCHAR(64),
   LAST_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
   CREATE_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
   IS_SAMPLE INT DEFAULT 0 NOT NULL
);

DROP TABLE APP.WRITER;
CREATE TABLE APP.WRITER
(
   ID VARCHAR(64) PRIMARY KEY NOT NULL,
   NAME VARCHAR(64),
   WRITERTYPE VARCHAR(20),
   DATA CLOB(1073741823),
   LAST_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
   CREATE_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
   IS_SAMPLE INT DEFAULT 0 NOT NULL
);
exit;