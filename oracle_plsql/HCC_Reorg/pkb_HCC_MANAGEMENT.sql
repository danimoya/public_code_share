create or replace PACKAGE BODY       "HCC_MANAGEMENT" As
  /*
  ** Daniel Moya Copyright 2015 - All rights reserved.
  ** HCC_MANAGEMENT : Manage the HCC compression tasks.
  **
  ** Created :		D. Moya	08/05/2015.
  ** Modified :
  **  D. Moya   08/05/2015    First deployed version.
  **  D. Moya	  13/05/2015	  Added Recover Tasks procedure to re-execute index creation in case of breaking the parallelization.
  */
  gn_DEBUG        NUMBER := 9;     -- Sets Debug Mode (prints some debug trace on screen), 0: disabled.
  gn_PARALLELISM  NUMBER := 64;    -- Parallel level for the executions (force DML and force QUERY).
 
  -- Default Session Identifier
  gs_SESSION_IDENTIFIER HCC_OPERATIONS.SESSION_IDENTIFIER%TYPE DEFAULT 'HCC-OPS-'||TO_CHAR(SYSTIMESTAMP,'YYYYMMDDHH24MISSFF');
                              
  -- Global exposed constants

  -- HCC_OPERATION Statuses taken in account or modified by this package :
  --  OPERATION_STATUS      ACTIONS/MOD. BY:
  --    READY                 CAN BE EXECUTED BY EXECUTE_TAKS or EXECUTE_ALL_TASKS
  --    OBJ.IN USE            At execution of task, object of task was found in use, all task was marked as skipped.
  --    RUNNING               When execution of SQL step starts, this status is used. Remains RUNNING till end of exection. 
  --    DONE                  At end of execution of SQL step, if no error occured, SQL Step is setup with this status. 
  --    ERROR                 At execution, error occured, so SQL STEP and whole task was put in error.
  --    SKIP                  Any other status will be ignored by the PL/SQL procs, so used SKIP to mark them (or other as you see fit)
  gks_READY         constant  varchar2(20) := 'READY';
  gks_OBJ_IN_USE    constant  varchar2(20) := 'OBJ. IN USE';
  gks_RUNNING       constant  varchar2(20) := 'RUNNING';
  gks_DONE          constant  varchar2(20) := 'DONE';
  gks_ERROR         constant  varchar2(20) := 'ERROR';
  
  -- Cursors Definitions

  -- ---------------------------------------------------------------------------------------------------------------------------------
  -- TRACE_LOG : Trace the sent comment to the output if it is asked (level of trace<current trace level defined by gn_DEBUG).
  -- ---------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE TRACE_LOG( pi_TRACE_LEVEL NUMBER, pis_TRACE_TEXT VARCHAR2 )
  IS 
  BEGIN
    IF pi_TRACE_LEVEL < gn_DEBUG THEN
      DBMS_OUTPUT.PUT_LINE( pis_TRACE_TEXT );
    END IF;
  END TRACE_LOG;

  -- ---------------------------------------------------------------------------------------------------------------------------------
  -- UPDATE_TASK : Update the Task data if it changed, using autonomous transaction to avoid loosing the status in case of error
  --                in the SQL Statment executed.
  -- ---------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE UPDATE_TASK( pit_OPERATION IN OUT HCC_OPERATIONS%ROWTYPE, pis_UPDATE_TYPE VARCHAR2, pis_INFO VARCHAR2, pii_STEP NUMBER default NULL )
  IS 
    PRAGMA AUTONOMOUS_TRANSACTION;
    SESS_ID NUMBER := 1;
    SESS_SERIAL NUMBER := 1;
  BEGIN
    -- If Serial/SID Information on task not set, update them.
    IF pit_OPERATION.SESSION_IDENTIFIER IS NULL THEN
      -- Select DISTINCT SID, SERIAL# INTO SESS_ID, SESS_SERIAL From SYS.V_$SESSION Where AUDSID = userenv('SESSIONID'); 
      --
      pit_OPERATION.SESSION_IDENTIFIER := gs_SESSION_IDENTIFIER;
      UPDATE HCC_OPERATIONS SET SESSION_IDENTIFIER = gs_SESSION_IDENTIFIER
                          WHERE HCO_ID = pit_OPERATION.HCO_ID;
    END IF;
   
    -- If we update because we had an error, flag the current SQL statement in error, flag all task statuses in error, keep error msg.
    CASE pis_UPDATE_TYPE
      WHEN 'ERROR' THEN
        TRACE_LOG( 4, 'Updated Status of OPERATION and STEP To :'|| pis_INFO ) ;
        -- Update Step
        UPDATE HCC_OPERATIONS 
           SET OPERATION_STATUS = gks_ERROR, SQL_STATUS = gks_ERROR,
               SQL_SQLERRM = pis_INFO, MARKUP = SYSTIMESTAMP
         WHERE HCO_ID = pit_OPERATION.HCO_ID AND SQL_STEP = pit_OPERATION.SQL_STEP;
        -- Put whole task in error
        UPDATE HCC_OPERATIONS 
           SET OPERATION_STATUS = gks_ERROR
         WHERE HCO_ID = pit_OPERATION.HCO_ID;
      WHEN 'STATUS' THEN
        TRACE_LOG( 5, 'Updated Status of STEP To :'|| pis_INFO ) ;
        IF pii_STEP IS NULL THEN
         -- Update status of the task with the sent information.
         UPDATE HCC_OPERATIONS 
            SET OPERATION_STATUS = pis_INFO
          WHERE HCO_ID = pit_OPERATION.HCO_ID;
        ELSE
         -- Update only the status of the step with the sent information.
         TRACE_LOG( 4, 'Updated Status of OPERATION To :'|| pis_INFO ) ;
         UPDATE HCC_OPERATIONS 
            SET SQL_STATUS = pis_INFO, MARKUP = SYSTIMESTAMP
          WHERE HCO_ID = pit_OPERATION.HCO_ID AND SQL_STEP = pii_STEP;
        END IF;
    END CASE;
    -- Commit the autonomous transaction
    COMMIT;	
  END UPDATE_TASK;

  -- ---------------------------------------------------------------------------------------------------------------------------------
  -- EXEC_IMMEDIATE : Do an execute immediate on the SQL command sent as parameter. This procedure is PRAGMA AUTONOMOUS
  --                   for avoiding killing the inner cursor with the DML autocommit.
  -- ---------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE EXEC_IMMEDIATE( pioc_SQL_STATEMENT IN CLOB )
  IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    TRACE_LOG(7, 'Executing : '|| REPLACE( pioc_SQL_STATEMENT, CHR(10), CHR(20)));
    -- Comment this if you want to be sure nothing is executed in the DB (full security in PROD environment.
    -- EXECUTE IMMEDIATE pioc_SQL_STATEMENT;
    -- Commit the autonomous transaction
    COMMIT;
  END EXEC_IMMEDIATE;

  -- ---------------------------------------------------------------------------------------------------------------------------------
  -- GET_OBJECT_STATUS : Return the status of an object (true = in use, false = not in use)
  -- ---------------------------------------------------------------------------------------------------------------------------------
  FUNCTION GET_OBJECT_STATUS( pi_OWNER VARCHAR2, pi_OBJECT_NAME VARCHAR2, pi_OBJECT_TYPE VARCHAR2 ) RETURN NUMBER
  IS
    LI_STATUS NUMBER(1) := 0;     -- Secure setting : consider object always locked.
  BEGIN
	  RETURN( LI_STATUS );
  END GET_OBJECT_STATUS;

  -- ---------------------------------------------------------------------------------------------------------------------------------
  -- EXECUTE_TASK : Execute a tasks (all SQL statements attached to the task, following steps order, and if they are not de-activated)
  -- ---------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE EXECUTE_TASK( pi_HCO_ID HCC_OPERATIONS.HCO_ID%TYPE )
  IS
    CURSOR C_GET_TASK( pi_HCO_ID HCC_OPERATIONS.HCO_ID%TYPE ) IS
			 SELECT *                     -- Keep * because I use %ROW type as parameter.
			   FROM HCC_OPERATIONS
			  WHERE HCO_ID = pi_HCO_ID
          AND OPERATION_STATUS = gks_READY
        ORDER BY SQL_STEP;
    -- 
    LI_OBJ_IN_USE NUMBER;
    LI_ERROR_IN_STEP NUMBER;
    LS_APP_INFO VARCHAR2(200);
  BEGIN
    -- Set Session ID to the session 
    DBMS_SESSION.SET_IDENTIFIER(gs_SESSION_IDENTIFIER);
    -- Ensure parallelization is set on the session.
    EXECUTE IMMEDIATE 'ALTER SESSION FORCE PARALLEL DDL PARALLEL '||gn_PARALLELISM;
    EXECUTE IMMEDIATE 'ALTER SESSION FORCE PARALLEL QUERY PARALLEL '||gn_PARALLELISM;
    
    -- Get the task to be executed
    FOR TASK IN C_GET_TASK( pi_HCO_ID )
    LOOP
      -- Will allow us to break if as step fails
      LI_ERROR_IN_STEP := 0;
      
      -- Sets information on the session for monitoring.
      LS_APP_INFO := 'Operation ['||TASK.OPERATION_NAME||'] Step ['||TASK.SQL_STEP||']';
      TRACE_LOG( 3, 'Execution of ' ||LS_APP_INFO ) ;
      DBMS_APPLICATION_INFO.SET_ACTION(LS_APP_INFO);
    
      -- Execute the task if object is not 'in use'
      LI_OBJ_IN_USE := GET_OBJECT_STATUS ( TASK.OBJECT_OWNER, TASK.OBJECT_NAME, TASK.OBJECT_TYPE ); 
      IF LI_OBJ_IN_USE = 0 THEN
        -- Before execution, update the status of the task (consider it running as long as there's a step running).
        UPDATE_TASK( TASK, 'STATUS', gks_RUNNING );

        -- Execute SQL Statement of the task
        BEGIN
          UPDATE_TASK( TASK, 'STATUS', gks_RUNNING, TASK.SQL_STEP );
          EXEC_IMMEDIATE( TASK.SQL_STATEMENT );

        -- Handle exception in executing the SQL Statement. 
        EXCEPTION WHEN OTHERS THEN 
          -- An Error occured, update the status of the task, put the SQL statement in error, keep the error message
          UPDATE_TASK( TASK, 'ERROR', SQLERRM, TASK.SQL_STEP );
          -- If you want to break of first error on SQL Statement, uncomment this.
          LI_ERROR_IN_STEP := SQLCODE ;
          RAISE; 
        END;
        -- Stop execution task if subtask failed.
        EXIT WHEN LI_ERROR_IN_STEP != 0;
        
        -- Move to next step.
        UPDATE_TASK( TASK, 'STATUS', gks_DONE, TASK.SQL_STEP );
      ELSE
        -- Put the current step in 'OBJ. IN USE' because object is locked
        UPDATE_TASK( TASK, 'STATUS', gks_OBJ_IN_USE, TASK.SQL_STEP );
      END IF;

      -- If we are done with all steps in task (or not if obj is used), the setup the status of the whole task
      IF LI_OBJ_IN_USE = 0 THEN
        UPDATE_TASK( TASK, 'STATUS', gks_DONE );
      ELSE
        -- Put whole task in gks_OBJ_IN_USE because object is locked
        UPDATE_TASK( TASK, 'STATUS', gks_OBJ_IN_USE );
      END IF;
    END LOOP;
  END EXECUTE_TASK;
--
  -- ---------------------------------------------------------------------------------------------------------------------------------
  -- EXECUTE_ALL_TASKS : Execute all 'READY' tasks.
  -- ---------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE EXECUTE_ALL_TASKS
  IS
    CURSOR C_GET_ALL_TASKS IS
			 SELECT DISTINCT HCO_ID, OPERATION_NAME
			   FROM HCC_OPERATIONS
			  WHERE OPERATION_STATUS = gks_READY
          AND ROWNUM < 1; -- security
  BEGIN
    -- Get the task to be executed
    FOR TASK IN C_GET_ALL_TASKS
    LOOP
      TRACE_LOG( 1, 'Executing Operation ['||TASK.OPERATION_NAME||']');
      EXECUTE_TASK( TASK.HCO_ID );
    END LOOP;
  END EXECUTE_ALL_TASKS;
--
  -- ---------------------------------------------------------------------------------------------------------------------------------
  -- EXECUTE_GROUP : Execute all 'READY' tasks from a Group.
  -- ---------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE EXECUTE_GROUP( pi_HCG_ID HCC_OPE_GROUPS.HCG_ID%TYPE )
  IS
    CURSOR C_GET_GRP_TASKS IS
			 SELECT HCO.HCO_ID, HCG_NAME, OPERATION_NAME 
			   FROM HCC_OPERATIONS HCO, HCC_OPE_GROUPS HCG
			  WHERE HCG.HCO_ID = HCO.HCO_ID 
          AND HCO.OPERATION_STATUS = gks_READY;
  BEGIN
    -- Get the task to be executed
    FOR TASK IN C_GET_GRP_TASKS
    LOOP
      TRACE_LOG( 1, 'Executing Operation ['||TASK.OPERATION_NAME||'] from Group ['||TASK.HCG_NAME||']');
      EXECUTE_TASK( TASK.HCO_ID );
    END LOOP;
  END EXECUTE_GROUP;
  -- ---------------------------------------------------------------------------------------------------------------------------------
  -- RECOVER : Find all operations that are 'in error', then run all RECOVERY 
  -- ---------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE RECOVER_TASKS
  IS
    CURSOR C_TASKS_TO_RECOVER IS
			 SELECT * 
			   FROM HCC_OPERATIONS HCO
			  WHERE HCO.OPERATION_STATUS IN ( gks_RUNNING, gks_ERROR ) AND OPERATION_TYPE = 'ALTER TABLE MOVE SUBPART'
        ORDER BY HCO_ID, SQL_STEP
          FOR UPDATE;
    PREV_HCO_ID NUMBER := 0;
    PREV_STEP_DONE NUMBER := 0;
  BEGIN
    -- Get the task to be executed
    FOR TASK IN C_TASKS_TO_RECOVER
    LOOP
      TRACE_LOG( 1, 'RECOVERING Operation ['||TASK.OPERATION_NAME||']');
      
      -- If we changed the HCO, then reset booleans
      IF TASK.HCO_ID <> PREV_HCO_ID THEN
        PREV_HCO_ID := TASK.HCO_ID;
        -- If 1st task in error then no need to recover the other ones
        IF TASK.SQL_STATUS = 'ERROR' THEN
          PREV_STEP_DONE := 0;
        ELSE
          PREV_STEP_DONE := 1;
        END IF;
      END IF;
    
      -- If SQL Statement is "USE_FOR_RECOVERY" enabled, then execute it for recovering the task.
      IF TASK.USE_FOR_RECOVERY = 1 AND PREV_STEP_DONE = 1 THEN
        BEGIN
          EXEC_IMMEDIATE(TASK.SQL_STATEMENT);
        EXCEPTION WHEN OTHERS THEN
          TRACE_LOG( 1, 'ERROR EXECUTING SQL_STATEMENT !');
        END;
      END IF;
      
      -- Sets the status back to ready
      UPDATE HCC_OPERATIONS SET OPERATION_STATUS = gks_READY, 
                                SQL_STATUS = NULL, SQL_SQLERRM = NULL, MARKUP = NULL
        WHERE CURRENT OF C_TASKS_TO_RECOVER;
    END LOOP;
  END RECOVER_TASKS;
--
END HCC_MANAGEMENT;