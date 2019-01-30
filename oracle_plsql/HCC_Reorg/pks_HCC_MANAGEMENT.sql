create or replace PACKAGE       "HCC_MANAGEMENT" AS
  /*
  ** Daniel Moya Copyright 2015 - All rights reserved.
  ** HCC_MANAGEMENT : Manage the HCC compression tasks.
  **
  ** Created :		D. Moya	08/05/2015.
  ** Modified :
  **  D. Moya   08/05/2015    First deployed version.
  **  D. Moya	  13/05/2015	Added Recover Tasks procedure to re-execute index creation in case of breaking the parallelization.
  */
  -- Global exposed constants
  -- Cursors Definitions
  -- ---------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE EXECUTE_TASK( pi_HCO_ID HCC_OPERATIONS.HCO_ID%TYPE );
  -- ---------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE EXECUTE_ALL_TASKS;
  -- ---------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE EXECUTE_GROUP( pi_HCG_ID HCC_OPE_GROUPS.HCG_ID%TYPE );
  -- ---------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE RECOVER_TASKS;
  -- ---------------------------------------------------------------------------------------------------------------------------------
END HCC_MANAGEMENT;