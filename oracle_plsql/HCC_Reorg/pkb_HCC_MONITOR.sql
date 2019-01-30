create or replace PACKAGE BODY     "HCC_MONITOR" Is
  /*
  ** Daniel Moya Copyright 2015 - All rights reserved.
  ** HCC_MONITOR :   Procedure & functions to monitor the HCC compression tasks.
  **
  ** Created :		D. Moya	08/05/2015.
  ** Modified :
  **  D. Moya   08/05/2015    First deployed version.
  **
  ** 
  */
  -- Global exposed constants
  -- Cursors Definitions
  -- ---------------------------------------------------------------------------------------------------------------------------------
  -- HCC_RUNNING_TASKS : Shows all the current running tasks.
  -- ---------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE HCC_RUNNING_TASKS
  IS
  BEGIN
    NULL;
  END HCC_RUNNING_TASKS;
END HCC_MONITOR;
/

-- SQL Statements that could be added to follow current running operations :

SELECT OBJECT_SUB_NAME, SQL_STEP, SQL_STATUS, SESSION_IDENTIFIER, MARKUP, SQL_STATEMENT 
FROM HCC_OPERATIONS WHERE OPERATION_STATUS IN ( 'RUNNING', 'ERROR') ORDER BY HCO_ID, SQL_STEP ;

