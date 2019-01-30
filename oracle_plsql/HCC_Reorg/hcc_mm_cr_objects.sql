/*
** Daniel Moya Copyright 2015 - All rights reserved.
** hcc_mm_cr_objects.sql :
**
**    Create objects for HCC_MANAGEMENT & MONITOR tools.
**
** D.Moya, 08.05.2015.
*/
DROP SEQUENCE HCO_ID_SEQ;
CREATE SEQUENCE HCO_ID_SEQ START WITH 1 MAXVALUE 9999999999999999 NOCYCLE;

DROP TABLE HCC_OPERATIONS;
  
  CREATE TABLE "HCC_OPERATIONS" 
   (	"HCO_ID" NUMBER, 
	"OPERATION_NAME" VARCHAR2(80 BYTE), 
	"OPERATION_TYPE" VARCHAR2(80 BYTE), 
	"OPERATION_STATUS" VARCHAR2(30 BYTE) DEFAULT 'READY', 
	"OBJECT_OWNER" VARCHAR2(30 BYTE), 
	"OBJECT_TYPE" VARCHAR2(30 BYTE), 
	"OBJECT_NAME" VARCHAR2(30 BYTE), 
	"OBJECT_SUB_TYPE" VARCHAR2(30 BYTE), 
	"OBJECT_SUB_NAME" VARCHAR2(30 BYTE), 
	"SQL_STEP" NUMBER, 
	"SQL_STATEMENT" CLOB, 
	"SQL_STATUS" VARCHAR2(30 BYTE) DEFAULT 'READY', 
	"SQL_SQLERRM" VARCHAR2(4000 BYTE), 
	"SESSION_IDENTIFIER" VARCHAR2(32 BYTE), 
	"MARKUP" TIMESTAMP (6), 
	"USE_FOR_RECOVERY" NUMBER(1,0) DEFAULT 0, 
	"BYTESIZE" NUMBER
)
 PCTFREE 15 STORAGE( FREELISTS 4 )
  TABLESPACE "USERS" 
 LOB ("SQL_STATEMENT") STORE AS BASICFILE (
  TABLESPACE "USERS" ENABLE STORAGE IN ROW CHUNK 32768 RETENTION 
  NOCACHE LOGGING 
  STORAGE( FREELISTS 4 ))
    PARTITION BY HASH( HCO_ID )
  PARTITIONS 8 ;

CREATE INDEX IX_HCC_OPERATIONS_01 ON HCC_OPERATIONS ( HCO_ID, SQL_STEP )
  LOCAL STORAGE( FREELISTS 4) PCTFREE 15 INITRANS 2 MAXTRANS 255  
  TABLESPACE "USERS" PARALLEL 8 ;

ALTER TABLE HCC_OPERATIONS PARALLEL 8;

CREATE TABLE "SYS"."HCC_OPE_GROUPS" 
   (	"HCG_ID" NUMBER, 
	"HCG_NAME" VARCHAR2(256 BYTE), 
	"HCO_ID" NUMBER
   ) 
  TABLESPACE "USERS" ;
 