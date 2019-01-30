-- December 2012. Procedure to re-organize fragmented objects in SIEBEL.
-----------------------------------------------------------------------------------
------------------------------PACKAGE ---------------------------------------------
-----------------------------------------------------------------------------------

CREATE OR REPLACE PACKAGE REORG AS
	--Segment Manager Package
	--Set job_queue_processes to control parallelism while executing non-reporting procedures.
	
	FUNCTION check_version return number;
	--Set / Enable +10g features in execute statements.

	PROCEDURE set_prefs(v_opt varchar2);
	-- CALL Example: exec reorg.set_prefs('OPTION');
	-- Options: INSTALL,RESET,DROP.  // TRUE= enable OFFLINE operations. FALSE, disable.

	--alter session set nls_date_format='dd-mm-yyyy hh24:mi:ss';
	--select * from segment_manager_info; -- SMI Table.

	FUNCTION check_space (v_owner varchar2, v_obj_name varchar2,v_obj_part varchar2 default NULL, v_obj_type varchar2) return varchar2;
	/*       Función func_size_obj(isidro): devuelve TRUE si el objeto que le llega por parámetro tiene espacio sufciente en el TBS.   */

	PROCEDURE check_segment (v_owner varchar2, v_obj_name varchar2,v_obj_part varchar2 default NULL, v_obj_type varchar2);
	-- CALL Example: exec reorg.check_segment('SIEBEL', 'S_CLASS_SCRPT',NULL,'TABLE');
	-- CALL Example: exec reorg.check_segment('SIEBEL', 'GCOMDW_INF_SUM','GCOMDW_PP_01','TABLE PARTITION');
	-- CALL Example: exec reorg.check_segment('SIEBEL', 'T_FINAN_INTERNA_M1',NULL,'INDEX');
	-- CALL Example: exec reorg.check_segment('DELTA_ADMINIS', 'IDX_GCCOMBILLATR_28','P_BEFORE_2014_2','INDEX PARTITION');

	PROCEDURE check_viability (v_owner varchar2, v_obj_name varchar2,v_obj_part varchar2 default NULL, v_obj_type varchar2);
	--CALL Example: exec reorg.check_viability('SIEBEL', 'S_CLASS_SCRPT',NULL,'TABLE');
	--CALL Example: exec reorg.check_viability('SIEBEL', 'GCOMDW_INF_SUM',NULL,'TABLE');
	--CALL Example: exec reorg.check_viability('SIEBEL', 'GCOMDW_INF_SUM','GCOMDW_PP_01','TABLE PARTITION');
	--CALL Example: exec reorg.check_viability('SIEBEL', 'CX_GN_AUD_WF',NULL,'TABLE');
	
	PROCEDURE shrink (v_owner varchar2, v_obj_name varchar2,v_obj_part varchar2 default NULL, v_obj_type varchar2);
	--CALL Example: exec reorg.shrink('SIEBEL','GEM_DWEXP_DATA_MARZO',NULL,'TABLE');
	--CALL Example: exec reorg.shrink('SIEBEL', 'GCOMDW_INF_SUM','GCOMDW_PP_01','TABLE PARTITION');
	
	PROCEDURE datapump (v_owner varchar2, v_obj_name varchar2,v_obj_part varchar2 default NULL, v_obj_type varchar2);
	--CALL Example: exec datapump('SIEBEL', 'GCOMDW_INF_SUM','GCOMDW_PP_01','TABLE PARTITION');
	--CALL Example: exec datapump('SIEBEL','S_UPG_COMP',NULL,'TABLE');
	
	PROCEDURE bulkcollect (v_owner varchar2, v_obj_name varchar2,v_obj_part varchar2 default NULL, v_obj_type varchar2);
	--CALL Example: exec bulkcollect ('SIEBEL', 'S_CLASS_SCRPT',NULL,'TABLE');
	--CALL Example: exec bulkcollect ('SIEBEL', 'GCOMDW_INF_SUM',NULL,'TABLE'); --Trampa Tabla particionada.
	--CALL Example: exec bulkcollect ('SIEBEL', 'GCOMDW_INF_SUM','GCOMDW_PP_01','TABLE PARTITION');
	--CALL Example: exec bulkcollect ('SIEBEL','GEM_DWEXP_DATA_MARZO',NULL,'TABLE');
	
	--PROCEDURE online_redef (v_owner varchar2, v_obj_name varchar2,v_obj_part varchar2 default NULL, v_obj_type varchar2);
	
	
	--PROCEDURE move (v_owner varchar2, v_obj_name varchar2,v_obj_part varchar2 default NULL, v_obj_type varchar2);
	
	
end reorg;
/
-----------------------------------------------------------------------------------
------------------------------PACKAGE BODY---------------------------------------
-----------------------------------------------------------------------------------

CREATE OR REPLACE PACKAGE BODY REORG IS

	--Global variables:
	gv_max_size_to_dump number := 2500; --Max size in MEGABYTES for a segment to determine by defrag_advisor which method is preferred.
	gv_dir_name varchar2(30) := 'FRAG'; -- DIRECTORY for DATAPUMP Ops. (Datapump Method)
	
	gv_version number := NULL; --Enable +10g features in execute statements.
	gv_allow_offline_ops boolean := FALSE; --For security reasons. Only modifiable explictly via "exec reorg.set_prefs('TRUE');" Set false to deny.
	gv_scope varchar2(4000) := ''; --SQL Cursor to limit the scope of the operation. (MOVE method)
	gv_tablespace_dat varchar2(30) := ''; --New (optional) tablespace for data segments.
	gv_tablespace_idx varchar2(30) := ''; --New (optional) tablespace for index segments.
	--Global variables END

--------------------------------------------------NEXT-----------------------------------

FUNCTION check_version return number AS
	v_version integer;
begin
  select TO_NUMBER(substr(version,1,2)) into v_version from dba_registry where comp_name like '%Packages and Types%';
	if v_version = 10 then return 10;
	elsif v_version = 11 then return 11;
	elsif v_version = 12 then return 12;
		else RAISE_APPLICATION_ERROR (-20001, 'Package not compatible, only for 10g or higher');
	end if;
end check_version;

PROCEDURE set_prefs(v_opt varchar2) AS
		v_sql        varchar2(4000);
		v_cntrl number := NULL;
BEGIN
-- TABLE SYS.SEGMENT_MANAGER_INFO
v_sql := 
'CREATE TABLE SEGMENT_MANAGER_INFO (OBJECT_ID number,OWNER varchar2(30),OBJECT_NAME varchar2(128),PARTITION_NAME varchar2(30),OBJECT_TYPE varchar2(19),
TABLESPACE_NAME VARCHAR2(30),PCT_FREE number,LAST_STATS_ANALYZED date,AVG_ROW_LEN number,
LAST_DEFRAG_ANALYZED date,DEFRAG_TYPE_USED varchar2(30),DEFRAG_START_TIME date,DEFRAG_END_TIME date,
JOB_NAME varchar2(61),JOB_STATUS varchar2(15),
MB_USED   NUMBER(10),MB_ALLOC  NUMBER(10),MB_FREE   NUMBER(10),CLAIMABLE_SPACE_PCT NUMBER(10), CHAIN_PCENT NUMBER(10),
IS_SHRINKABLE varchar2(1),IS_ONLINE_REDEF varchar2(1),IS_MOVABLE varchar2(1),IS_BULKCOLLECTABLE varchar2(1),IS_SMALL_DATAPUMP varchar2(1),
ERROR_MSG varchar2(4000))';
		if v_opt = ('INSTALL') then
				    select count(*) into v_cntrl from all_objects where object_name = 'SEGMENT_MANAGER_INFO' and owner = 'SYS';
					if v_cntrl = 0 then
					execute immediate (v_sql);
					DBMS_SCHEDULER.CREATE_JOB_CLASS('SGMTMGR',logging_level=>DBMS_SCHEDULER.LOGGING_FULL,comments=>'Job Class for package REORG');
					else
					RAISE_APPLICATION_ERROR (-20003, 'The table already exists, use RESET option instead of INSTALL, sorry!');
					end if;
		elsif v_opt = ('RESET') then
					execute immediate 'TRUNCATE TABLE SYS.SEGMENT_MANAGER_INFO';
		elsif v_opt = ('DROP') then
			dbms_output.put_line('To clean up this package, execute: ');
			dbms_output.put_line('DROP PACKAGE SYS.REORG;');
			dbms_output.put_line('DROP TABLE SYS.SEGMENT_MANAGER_INFO;');
			DBMS_SCHEDULER.DROP_JOB_CLASS('SGMTMGR',TRUE);
--Global variables modify.
elsif v_opt = ('TRUE') then
		gv_allow_offline_ops := TRUE;
		dbms_output.put_line('OFFLINE Operations allowed.');
elsif v_opt = ('FALSE') then
		gv_allow_offline_ops := FALSE;
		dbms_output.put_line('ONLINE Operations only.');
else
RAISE_APPLICATION_ERROR (-20004, 'Invalid option entered, sorry!');
end if;

end set_prefs;

--------------------------------------------------NEXT-----------------------------------

FUNCTION check_space (v_owner varchar2, v_obj_name varchar2,v_obj_part varchar2 default NULL, v_obj_type varchar2) return varchar2 AS

         v_size number(15);
         v_size_tbs number(15);

  BEGIN
    if (v_obj_type = 'TABLE') OR (v_obj_type = 'INDEX') then

       select bytes / 1024 / 1024 into v_size from dba_segments where segment_name = v_obj_name and segment_type = v_obj_type and owner = v_owner;

       select trunc(sum(bytes) / 1024 / 1024) MB_FREE into v_size_tbs from dba_free_space where tablespace_name =
		(select tablespace_name from dba_segments where segment_name = v_obj_name and segment_type = v_obj_type and owner = v_owner);
	end if;

    if (v_obj_type = 'TABLE PARTITION') OR (v_obj_type = 'INDEX PARTITION') then

       select bytes / 1024 / 1024 into v_size from dba_segments where segment_name = v_obj_name
          and segment_type = v_obj_type and partition_name = v_obj_part and owner = v_owner;

       select trunc(sum(bytes) / 1024 / 1024) MB_FREE into v_size_tbs from dba_free_space where tablespace_name =
		(select tablespace_name from dba_segments where segment_name = v_obj_name and segment_type = v_obj_type and owner = v_owner);
    end if;

            if  (v_size * 1.75) < v_size_tbs then 
				return 'YES';
			else return 'NO';
            end if;
return 'N/A';
END check_space;

--------------------------------------------------NEXT-----------------------------------

PROCEDURE check_segment (v_owner varchar2, v_obj_name varchar2,v_obj_part varchar2 default NULL, v_obj_type varchar2) AS
v_error_msg varchar2(4000);
v_obj_id number;
v_tablespace varchar2(30);
v_pct_free number;
v_last_analyzed date;
v_avg_row_len number;

BEGIN
if (v_obj_type = 'TABLE') then

		--(DBA_TABLES.tablespace_name,pct_free,last_analyzed,avg_row_len)
		select obj.object_id, tablespace_name,pct_free,last_analyzed,avg_row_len
		  into v_obj_id, v_tablespace,v_pct_free,v_last_analyzed,v_avg_row_len
		from dba_tables tab, dba_objects obj where
		tab.owner=obj.owner and tab.table_name=obj.object_name and object_type=v_obj_type and
		tab.owner=v_owner and tab.table_name=v_obj_name;
		
		MERGE INTO SEGMENT_MANAGER_INFO smi
		USING
		(SELECT SYSDATE as LAST_DEFRAG_ANALYZED,
			   ROUND((SPACE_USED) / 1024 / 1024) AS MB_USED,
			   ROUND((SPACE_ALLOCATED) / 1024 / 1024) AS MB_ALLOC,
			   ROUND((SPACE_ALLOCATED - SPACE_USED) / 1024 / 1024) AS MB_FREE,
			   decode (round((space_allocated) / 1024 / 1024), 0,NULL, round((round((space_allocated - space_used) / 1024 / 1024)*100)/round((space_allocated) / 1024 / 1024))) AS CLAIMABLE_SPACE_PCT, CHAIN_PCENT
		FROM TABLE(DBMS_SPACE.OBJECT_SPACE_USAGE_TBF(v_owner, v_obj_name, v_obj_type, NULL))		) s
		ON (smi.object_id=v_obj_id)
		WHEN MATCHED THEN
			UPDATE SET smi.tablespace_name=v_tablespace,smi.pct_free=v_pct_free,smi.LAST_STATS_ANALYZED=v_last_analyzed,smi.avg_row_len=v_avg_row_len,
			smi.LAST_DEFRAG_ANALYZED=s.LAST_DEFRAG_ANALYZED,smi.mb_used=s.MB_USED,smi.mb_alloc=s.MB_ALLOC,smi.mb_free=s.MB_FREE,
			smi.claimable_space_pct=s.CLAIMABLE_SPACE_PCT,smi.chain_pcent=s.CHAIN_PCENT
		WHEN NOT MATCHED THEN
			INSERT (object_id,owner,object_name,object_type,tablespace_name,pct_free,last_stats_analyzed,avg_row_len,
			last_defrag_analyzed,mb_used,mb_alloc,mb_free,claimable_space_pct,chain_pcent)
			VALUES (v_obj_id,v_owner,v_obj_name,v_obj_type, v_tablespace,v_pct_free,v_last_analyzed,v_avg_row_len,
			s.LAST_DEFRAG_ANALYZED,s.MB_USED,s.MB_ALLOC,s.MB_FREE,s.CLAIMABLE_SPACE_PCT,s.CHAIN_PCENT);
		commit;
elsif (v_obj_type = 'INDEX') then

		--(DBA_INDEXES.tablespace_name,pct_free,last_analyzed,avg_row_len)
		select obj.object_id, tablespace_name,pct_free,last_analyzed
		  into v_obj_id, v_tablespace,v_pct_free,v_last_analyzed
		from dba_indexes idx, dba_objects obj where
		idx.owner=obj.owner and idx.index_name=obj.object_name and object_type=v_obj_type and
		idx.owner=v_owner and idx.index_name=v_obj_name;
		
		MERGE INTO SEGMENT_MANAGER_INFO smi
		USING
		(SELECT SYSDATE as LAST_DEFRAG_ANALYZED,
			   ROUND((SPACE_USED) / 1024 / 1024) AS MB_USED,
			   ROUND((SPACE_ALLOCATED) / 1024 / 1024) AS MB_ALLOC,
			   ROUND((SPACE_ALLOCATED - SPACE_USED) / 1024 / 1024) AS MB_FREE,
			   decode (round((space_allocated) / 1024 / 1024), 0,NULL, round((round((space_allocated - space_used) / 1024 / 1024)*100)/round((space_allocated) / 1024 / 1024))) AS CLAIMABLE_SPACE_PCT, CHAIN_PCENT
		FROM TABLE(DBMS_SPACE.OBJECT_SPACE_USAGE_TBF(v_owner, v_obj_name, v_obj_type, NULL))		) s
		ON (smi.object_id=v_obj_id)
		WHEN MATCHED THEN
			UPDATE SET smi.tablespace_name=v_tablespace,smi.pct_free=v_pct_free,smi.LAST_STATS_ANALYZED=v_last_analyzed,
			smi.LAST_DEFRAG_ANALYZED=s.LAST_DEFRAG_ANALYZED,smi.mb_used=s.MB_USED,smi.mb_alloc=s.MB_ALLOC,smi.mb_free=s.MB_FREE,
			smi.claimable_space_pct=s.CLAIMABLE_SPACE_PCT,smi.chain_pcent=s.CHAIN_PCENT
		WHEN NOT MATCHED THEN
			INSERT (object_id,owner,object_name,object_type,tablespace_name,pct_free,last_stats_analyzed,
			last_defrag_analyzed,mb_used,mb_alloc,mb_free,claimable_space_pct,chain_pcent)
			VALUES (v_obj_id,v_owner,v_obj_name,v_obj_type, v_tablespace,v_pct_free,v_last_analyzed,
			s.LAST_DEFRAG_ANALYZED,s.MB_USED,s.MB_ALLOC,s.MB_FREE,s.CLAIMABLE_SPACE_PCT,s.CHAIN_PCENT);
		commit;

elsif (v_obj_type = 'TABLE PARTITION') then

		--(DBA_TAB_PARTITIONS.tablespace_name,pct_free,last_analyzed,avg_row_len)
		select obj.object_id, tablespace_name,pct_free,last_analyzed,avg_row_len
		  into v_obj_id, v_tablespace,v_pct_free,v_last_analyzed,v_avg_row_len
		from dba_tab_partitions tab, dba_objects obj where
		tab.table_owner=obj.owner and tab.table_name=obj.object_name and tab.partition_name=obj.subobject_name
		and obj.owner=v_owner and obj.object_name=v_obj_name and obj.subobject_name=v_obj_part;
		
		MERGE INTO SEGMENT_MANAGER_INFO smi
		USING
		(SELECT SYSDATE as LAST_DEFRAG_ANALYZED,
			   ROUND((SPACE_USED) / 1024 / 1024) AS MB_USED,
			   ROUND((SPACE_ALLOCATED) / 1024 / 1024) AS MB_ALLOC,
			   ROUND((SPACE_ALLOCATED - SPACE_USED) / 1024 / 1024) AS MB_FREE,
			   decode (round((space_allocated) / 1024 / 1024), 0,NULL, round((round((space_allocated - space_used) / 1024 / 1024)*100)/round((space_allocated) / 1024 / 1024))) AS CLAIMABLE_SPACE_PCT, CHAIN_PCENT
		FROM TABLE(DBMS_SPACE.OBJECT_SPACE_USAGE_TBF(v_owner, v_obj_name, v_obj_type, NULL, v_obj_part))		) s
		ON (smi.object_id=v_obj_id)
		WHEN MATCHED THEN
			UPDATE SET smi.tablespace_name=v_tablespace,smi.pct_free=v_pct_free,smi.LAST_STATS_ANALYZED=v_last_analyzed,smi.avg_row_len=v_avg_row_len,
			smi.LAST_DEFRAG_ANALYZED=s.LAST_DEFRAG_ANALYZED,smi.mb_used=s.MB_USED,smi.mb_alloc=s.MB_ALLOC,smi.mb_free=s.MB_FREE,
			smi.claimable_space_pct=s.CLAIMABLE_SPACE_PCT,smi.chain_pcent=s.CHAIN_PCENT
		WHEN NOT MATCHED THEN
			INSERT (object_id,owner,object_name,object_type,PARTITION_NAME,tablespace_name,pct_free,last_stats_analyzed,avg_row_len,
			last_defrag_analyzed,mb_used,mb_alloc,mb_free,claimable_space_pct,chain_pcent)
			VALUES (v_obj_id,v_owner,v_obj_name,v_obj_type,v_obj_part,v_tablespace,v_pct_free,v_last_analyzed,v_avg_row_len,
			s.LAST_DEFRAG_ANALYZED,s.MB_USED,s.MB_ALLOC,s.MB_FREE,s.CLAIMABLE_SPACE_PCT,s.CHAIN_PCENT);
		commit;

elsif (v_obj_type = 'INDEX PARTITION') then

		--(DBA_IND_PARTITIONS.tablespace_name,pct_free,last_analyzed,avg_row_len)
		select obj.object_id, tablespace_name,pct_free,last_analyzed
		  into v_obj_id, v_tablespace,v_pct_free,v_last_analyzed
		from dba_ind_partitions idx, dba_objects obj where
		idx.index_owner=obj.owner and idx.index_name=obj.object_name and idx.partition_name=obj.subobject_name
		and obj.owner=v_owner and obj.object_name=v_obj_name and obj.subobject_name=v_obj_part;
		
		MERGE INTO SEGMENT_MANAGER_INFO smi
		USING
		(SELECT SYSDATE as LAST_DEFRAG_ANALYZED,
			   ROUND((SPACE_USED) / 1024 / 1024) AS MB_USED,
			   ROUND((SPACE_ALLOCATED) / 1024 / 1024) AS MB_ALLOC,
			   ROUND((SPACE_ALLOCATED - SPACE_USED) / 1024 / 1024) AS MB_FREE,
			   decode (round((space_allocated) / 1024 / 1024), 0,NULL, round((round((space_allocated - space_used) / 1024 / 1024)*100)/round((space_allocated) / 1024 / 1024))) AS CLAIMABLE_SPACE_PCT, CHAIN_PCENT
		FROM TABLE(DBMS_SPACE.OBJECT_SPACE_USAGE_TBF(v_owner, v_obj_name, v_obj_type, NULL, v_obj_part))		) s
		ON (smi.object_id=v_obj_id)
		WHEN MATCHED THEN
			UPDATE SET smi.tablespace_name=v_tablespace,smi.pct_free=v_pct_free,smi.LAST_STATS_ANALYZED=v_last_analyzed,
			smi.LAST_DEFRAG_ANALYZED=s.LAST_DEFRAG_ANALYZED,smi.mb_used=s.MB_USED,smi.mb_alloc=s.MB_ALLOC,smi.mb_free=s.MB_FREE,
			smi.claimable_space_pct=s.CLAIMABLE_SPACE_PCT,smi.chain_pcent=s.CHAIN_PCENT
		WHEN NOT MATCHED THEN
			INSERT (object_id,owner,object_name,object_type,PARTITION_NAME,tablespace_name,pct_free,last_stats_analyzed,
			last_defrag_analyzed,mb_used,mb_alloc,mb_free,claimable_space_pct,chain_pcent)
			VALUES (v_obj_id,v_owner,v_obj_name,v_obj_type,v_obj_part,v_tablespace,v_pct_free,v_last_analyzed,
			s.LAST_DEFRAG_ANALYZED,s.MB_USED,s.MB_ALLOC,s.MB_FREE,s.CLAIMABLE_SPACE_PCT,s.CHAIN_PCENT);
		commit;

else
	RAISE_APPLICATION_ERROR (-20002, 'The object checked is not a TABLE or INDEX (Partitioned/Non-Partitioned): '||v_owner||'.'||v_obj_name||' '||v_obj_part||' '||v_obj_type);
end if;

END check_segment;

--------------------------------------------------NEXT-----------------------------------

PROCEDURE check_viability (v_owner varchar2, v_obj_name varchar2,v_obj_part varchar2 default NULL, v_obj_type varchar2) AS
--Variables to insert/update.
v_chk_shrink varchar2(1);
v_chk_onlineref varchar2(1);
v_chk_bulkcoll varchar2(1);
v_chk_smallimpdp varchar2(1);
v_chk_move varchar2(1);

v_obj_id number;
--SELECT INTO Variables.
v_check1 varchar2(3); --For PARTITIONED
v_check2 varchar2(12); --For IOT_TYPE
v_check3 varchar2(8); --For COMPRESSION
v_check4 varchar2(1); --For INDEX_TYPE - IOT OR FUNCTION-BASED
v_check5 varchar2(1); --For Temporary tables.
v_check6 number; --For LONG/LONG RAW Columns counter.
v_check7 number; --For SECURE Lobs counter.
v_check8 number := 2500; --(MAX MBSIZE TO DUMP) Substituted by GLOBAL VARIABLE. gv_max_size_to_dump
v_check82 number;
v_check9 varchar2(6);
v_check91 number;

v_error_msg varchar2(4000);
BEGIN

IF v_obj_type IN ('TABLE','TABLE PARTITION') then
v_chk_shrink :='Y';
v_chk_onlineref :='Y';
v_chk_bulkcoll :='Y';
v_chk_smallimpdp :='Y';
v_chk_move :='Y';

	IF v_obj_type = 'TABLE' then
			select PARTITIONED into v_check1 from dba_tables where table_name=v_obj_name and owner=v_owner;
			if v_check1 = 'YES' then
				v_chk_move := 'N';
			end if;
			select IOT_TYPE into v_check2 from dba_tables where table_name=v_obj_name and owner=v_owner;
			if v_check2 is not null then
				v_chk_shrink := 'N';
			end if;
			select COMPRESSION into v_check3 from dba_tables where table_name=v_obj_name and owner=v_owner;
			if v_check3='ENABLED' then
				v_chk_shrink := 'N';
			end if;
			select TEMPORARY into v_check5 from dba_tables where table_name=v_obj_name and owner=v_owner;
			if v_check3='Y' then
				v_chk_onlineref := 'N';
			end if;
			
			select obj.object_id into v_obj_id from dba_tables tab, dba_objects obj
			where tab.owner=obj.owner and tab.table_name=obj.object_name and object_type=v_obj_type and
			tab.owner=v_owner and tab.table_name=v_obj_name;
			
			--Small DataPump check:
			if v_check1 = 'YES' then
			--sum bytes partitions
			select round(sum(bytes/1024/1024),0) MBSIZE into v_check82 from dba_segments
			where owner=v_owner and segment_name=v_obj_name and partition_name in (select partition_name from dba_tab_partitions where table_name=v_obj_name and table_owner=v_owner);
			-- Tablespace_name ASSM check.
			select count((CASE WHEN SEGMENT_SPACE_MANAGEMENT='MANUAL' then 'MANUAL' WHEN SEGMENT_SPACE_MANAGEMENT='AUTO' then NULL END)) into v_check91
			from dba_tablespaces where tablespace_name in (select tablespace_name from dba_tab_partitions where table_name=v_obj_name and table_owner=v_owner);
				if v_check91 > 0 then
					RAISE_APPLICATION_ERROR (-20004, 'Object indicated as:'||v_obj_type||': is partitioned table '||v_owner||'.'||v_obj_name||' and contains MSSM partitions.');
				end if;
			else
			--Non-partitioned.
			select round(bytes/1024/1024,0) MBSIZE into v_check82 from dba_segments where owner=v_owner and segment_name=v_obj_name and segment_type=v_obj_type;
			-- Tablespace_name ASSM check.
			select SEGMENT_SPACE_MANAGEMENT into v_check9 from dba_tablespaces
			where tablespace_name=(select tablespace_name from dba_tables where table_name=v_obj_name and owner=v_owner);
			end if;
			if v_check82 > v_check8 then
					v_chk_smallimpdp :='N';
			end if;
			
			if v_check9 = 'MANUAL' then
				v_chk_shrink := 'N';
			end if;
			
	END IF;
	IF v_obj_type = 'TABLE PARTITION' then
			--Compression needed, duplicate with intention.
			select COMPRESSION into v_check3 from dba_tab_partitions where table_name=v_obj_name and table_owner=v_owner and partition_name=v_obj_part;
			if v_check3='ENABLED' then
				v_chk_shrink := 'N';
			end if;
			
			--Small DataPump check for specific partition.
			select round(sum(bytes/1024/1024),0) MBSIZE into v_check82 from dba_segments
			where owner=v_owner and segment_name=v_obj_name and partition_name=v_obj_part;
			if v_check82 > v_check8 then
					v_chk_smallimpdp :='N';
			end if;
			
			select object_id into v_obj_id from dba_objects where owner=v_owner
			and object_name=v_obj_name and object_type=v_obj_type and subobject_name=v_obj_part;
		
			select SEGMENT_SPACE_MANAGEMENT into v_check9 from dba_tablespaces
			where tablespace_name=(select tablespace_name from dba_segments where owner=v_owner and segment_name=v_obj_name and partition_name=v_obj_part);
			if v_check9 = 'MANUAL' then
				v_chk_shrink := 'N';
			end if;
	END IF;
	
	if (v_obj_type = 'TABLE') OR (v_obj_type = 'TABLE PARTITION') then
			--DBMS_OUTPUT.PUT_LINE('CHECK!!');
			select count(1) into v_check6 from dba_tab_columns where table_name=v_obj_name and owner=v_owner and data_type like 'LONG%';
			if v_check6 > 0 then
				v_chk_onlineref := 'N';
				v_chk_move := 'N';
			end if;

			SELECT CASE WHEN IDX=0 THEN 'N' WHEN IDX>0 THEN 'Y' END into v_check4
			FROM (select count(index_type) IDX from dba_indexes where table_owner=v_owner and table_name=v_obj_name
			and ((index_type like 'FUNCT%') or (index_type like 'IOT%'))) idx;
				if v_check4='Y' then 
					v_chk_shrink := 'N'; --Table has Funtion-based or IOT Indexes.
				end if;

			SELECT count(1) into v_check7 FROM DBA_LOBS WHERE (TABLE_NAME,COLUMN_NAME) IN
			( SELECT T.TABLE_NAME,C.COLUMN_NAME FROM DBA_TABLES T,DBA_TAB_COLUMNS C WHERE T.OWNER=C.OWNER AND T.TABLE_NAME=C.TABLE_NAME
			and t.table_name=v_obj_name and t.owner=v_owner and C.DATA_TYPE like '%LOB' ) and SECUREFILE='YES';
			if v_check6 > 0 then
				v_chk_shrink := 'N';
			end if;

	END IF;

ELSE
			RAISE_APPLICATION_ERROR (-20004, 'Object not valid for viability check: '||v_owner||'.'||v_obj_name||' '||v_obj_part||' '||v_obj_type);
END IF;

--Variables values after execution for DEBUG/CHECK.
--DBMS_OUTPUT.PUT_LINE('SHRINK '||v_chk_shrink );
--DBMS_OUTPUT.PUT_LINE('OREDEF '||v_chk_onlineref );
--DBMS_OUTPUT.PUT_LINE('BULKCC '||v_chk_bulkcoll );
--DBMS_OUTPUT.PUT_LINE('SIMPDP '||v_chk_smallimpdp );
--DBMS_OUTPUT.PUT_LINE('MOVERB '||v_chk_move );

		MERGE INTO SEGMENT_MANAGER_INFO smi
		USING
		(SELECT v_chk_shrink as IS_SHRINKABLE, v_chk_onlineref as IS_ONLINE_REDEF,
		v_chk_move as IS_MOVABLE, v_chk_bulkcoll as IS_BULKCOLLECTABLE,v_chk_smallimpdp as IS_SMALL_DATAPUMP
		FROM DUAL) s
		ON (smi.object_id=v_obj_id)
		WHEN MATCHED THEN
			UPDATE SET smi.IS_SHRINKABLE=s.IS_SHRINKABLE,smi.IS_ONLINE_REDEF=s.IS_ONLINE_REDEF,
			smi.IS_MOVABLE=s.IS_MOVABLE,smi.IS_BULKCOLLECTABLE=s.IS_BULKCOLLECTABLE,smi.IS_SMALL_DATAPUMP=s.IS_SMALL_DATAPUMP, smi.mb_alloc=v_check82
		WHEN NOT MATCHED THEN
			INSERT (object_id,owner,object_name,object_type,PARTITION_NAME,IS_SHRINKABLE,IS_ONLINE_REDEF,IS_MOVABLE,IS_BULKCOLLECTABLE,IS_SMALL_DATAPUMP,MB_ALLOC)
			VALUES (v_obj_id,v_owner,v_obj_name,v_obj_type,v_obj_part,v_chk_shrink,v_chk_onlineref,v_chk_move,v_chk_bulkcoll,v_chk_smallimpdp,v_check82);
		commit;

EXCEPTION
when others then
v_error_msg := sqlerrm;
		MERGE INTO SEGMENT_MANAGER_INFO smi
		USING
		(SELECT v_chk_shrink as IS_SHRINKABLE, v_chk_onlineref as IS_ONLINE_REDEF,
		v_chk_move as IS_MOVABLE, v_chk_bulkcoll as IS_BULKCOLLECTABLE,v_chk_smallimpdp as IS_SMALL_DATAPUMP
		FROM DUAL) s
		ON (smi.object_id=v_obj_id)
		WHEN MATCHED THEN
			UPDATE SET smi.IS_SHRINKABLE=s.IS_SHRINKABLE,smi.IS_ONLINE_REDEF=s.IS_ONLINE_REDEF,
			smi.IS_MOVABLE=s.IS_MOVABLE,smi.IS_BULKCOLLECTABLE=s.IS_BULKCOLLECTABLE,smi.IS_SMALL_DATAPUMP=s.IS_SMALL_DATAPUMP, smi.mb_alloc=v_check82,smi.error_msg=v_error_msg
		WHEN NOT MATCHED THEN
			INSERT (object_id,owner,object_name,object_type,PARTITION_NAME,IS_SHRINKABLE,IS_ONLINE_REDEF,IS_MOVABLE,IS_BULKCOLLECTABLE,IS_SMALL_DATAPUMP,MB_ALLOC,error_msg)
			VALUES (v_obj_id,v_owner,v_obj_name,v_obj_type,v_obj_part,v_chk_shrink,v_chk_onlineref,v_chk_move,v_chk_bulkcoll,v_chk_smallimpdp,v_check82,v_error_msg);
		commit;
dbms_output.put_line('ERROR check_viability '||v_owner||'.'||v_obj_name||' '||v_obj_part||' '||v_obj_type||' '||v_error_msg);
END check_viability;

--------------------------------------------------NEXT-----------------------------------

PROCEDURE shrink(v_owner varchar2, v_obj_name varchar2,v_obj_part varchar2 default NULL, v_obj_type varchar2) AS
v_error_msg varchar2(4000);
v_defrag_type varchar2(30) :='IS_SHRINKABLE';
v_obj_id number;
v_start_time DATE;
v_end_time DATE;
v_is_partitioned boolean;
v_sql varchar2(4000);
v_smi_present number;
v_chk_space varchar2(3);
v_compatible varchar2(1);
v_check1 number;

BEGIN
		if v_obj_part is null then
		v_is_partitioned := FALSE;
		select object_id into v_obj_id from dba_objects where owner=v_owner
		and object_name=v_obj_name and object_type=v_obj_type;
		else
		--PARTITION
		v_is_partitioned := TRUE;
		select object_id into v_obj_id from dba_objects where owner=v_owner
		and object_name=v_obj_name and object_type=v_obj_type and subobject_name=v_obj_part;
		end if;
		if v_obj_id is null then
			RAISE_APPLICATION_ERROR (-20005, 'Object not found in DBA*_OBJECTS/_TABLES/TAB_PARTITIONS: '||v_owner||'.'||v_obj_name||' '||v_obj_part||' '||v_obj_type);
		end if;
		--If not present on SMI, call check_space,check_segment,check_viability.
		select count(1) COUNT into v_smi_present from segment_manager_info where object_id=v_obj_id;
		if v_smi_present = 0 then
			v_chk_space := REORG.CHECK_SPACE(v_owner,v_obj_name,v_obj_part,v_obj_type);
			REORG.CHECK_SEGMENT(v_owner,v_obj_name,v_obj_part,v_obj_type);
		end if;
		if v_defrag_type IN ('MOVERB','OREDEF','BULKCC') AND v_chk_space = 'NO' then
					RAISE_APPLICATION_ERROR (-20006, 'Not enought space to make the operation '||v_defrag_type||': '||v_owner||'.'||v_obj_name||' '||v_obj_part||' '||v_obj_type);
		end if;
		select CHAIN_PCENT into v_check1 from segment_manager_info where object_id=v_obj_id;
		if v_check1 is null then
			REORG.CHECK_SEGMENT(v_owner,v_obj_name,v_obj_part,v_obj_type);
		end if;
		select IS_SHRINKABLE into v_compatible from segment_manager_info where object_id=v_obj_id;
		if v_compatible is null then
			REORG.CHECK_VIABILITY(v_owner,v_obj_name,v_obj_part,v_obj_type);
		END IF;
		select IS_SHRINKABLE into v_compatible from segment_manager_info where object_id=v_obj_id;
		IF v_compatible = 'N' then
			RAISE_APPLICATION_ERROR (-20007, 'Not compatible operation '||v_defrag_type||': '||v_owner||'.'||v_obj_name||' '||v_obj_part||' '||v_obj_type);
		END IF;
		-- CODE STARTS HERE
		v_start_time := sysdate;
		-- CODE STARTS HERE
		if v_obj_type = 'TABLE' then
				v_sql := 'ALTER TABLE '||v_owner||'.'||v_obj_name||' ENABLE ROW MOVEMENT';
				execute immediate (v_sql);
				v_sql := 'ALTER TABLE '||v_owner||'.'||v_obj_name||' SHRINK SPACE CASCADE';
				execute immediate (v_sql);
				v_sql := 'ALTER TABLE '||v_owner||'.'||v_obj_name||' DISABLE ROW MOVEMENT';
				execute immediate (v_sql);

		elsif v_obj_type = 'TABLE PARTITION' then
				v_sql := 'ALTER TABLE '||v_owner||'.'||v_obj_name||' ENABLE ROW MOVEMENT';
				execute immediate (v_sql);
				v_sql := 'ALTER TABLE '||v_owner||'.'||v_obj_name||' MODIFY PARTITION '||v_obj_part||' SHRINK SPACE CASCADE';
				execute immediate (v_sql);
				v_sql := 'ALTER TABLE '||v_owner||'.'||v_obj_name||' DISABLE ROW MOVEMENT';
				execute immediate (v_sql);
		else
		RAISE_APPLICATION_ERROR (-20004, 'Object not valid for viability check: '||v_owner||'.'||v_obj_name||' '||v_obj_part||' '||v_obj_type);
		end if;
		-- CODE ENDS HERE
		v_end_time := sysdate;
		-- CODE ENDS HERE
		
 		MERGE INTO SEGMENT_MANAGER_INFO smi
		USING (SELECT 1 from dual) s ON (smi.object_id=v_obj_id)
		WHEN MATCHED THEN
			UPDATE SET smi.defrag_type_used=v_defrag_type,smi.defrag_start_time=v_start_time,smi.defrag_end_time=v_end_time
		WHEN NOT MATCHED THEN
			INSERT (object_id,owner,object_name,object_type,PARTITION_NAME,defrag_type_used,defrag_start_time,defrag_end_time)
			VALUES (v_obj_id,v_owner,v_obj_name,v_obj_type,v_obj_part,v_defrag_type,v_start_time,v_end_time);
		commit;
		
EXCEPTION
when others then
v_error_msg := sqlerrm;
		v_end_time := sysdate;

 		MERGE INTO SEGMENT_MANAGER_INFO smi
		USING (SELECT 1 from dual) s ON (smi.object_id=v_obj_id)
		WHEN MATCHED THEN
			UPDATE SET smi.defrag_type_used=v_defrag_type,smi.defrag_start_time=v_start_time,smi.defrag_end_time=v_end_time,smi.error_msg=v_error_msg
		WHEN NOT MATCHED THEN
			INSERT (object_id,owner,object_name,object_type,PARTITION_NAME,defrag_type_used,defrag_start_time,defrag_end_time,error_msg)
			VALUES (v_obj_id,v_owner,v_obj_name,v_obj_type,v_obj_part,v_defrag_type,v_start_time,v_end_time,v_error_msg);
		commit;
dbms_output.put_line('ERROR shrink :'||v_owner||'.'||v_obj_name||' '||v_obj_part||' '||v_obj_type||' Details: '||v_error_msg);
END shrink;

---------------------------------------------------------NEXT-----------------------------------------------
PROCEDURE datapump (v_owner varchar2, v_obj_name varchar2,v_obj_part varchar2 default NULL, v_obj_type varchar2) AS
v_error_msg varchar2(4000);
v_defrag_type varchar2(30) :='IS_SMALL_DATAPUMP';
v_obj_id number;
v_start_time DATE;
v_end_time DATE;
v_is_partitioned boolean;
v_sql varchar2(4000);
v_smi_present number;
v_chk_space varchar2(3);
v_compatible varchar2(1);
v_check1 number;

--Datapump Variables:
v_handler number;
v_percent_done number;
v_job_status VARCHAR2(30);
	ind NUMBER;              -- Loop index
	le ku$_LogEntry;         -- For WIP and error messages
	js ku$_JobStatus;        -- The job status from get_status
	jd ku$_JobDesc;          -- The job description from get_status
	sts ku$_Status;          -- The status object returned by get_status

v_job_exists number;
v_error_counter number;
v_readonly varchar2(3);

BEGIN
		if v_obj_part is null then
		v_is_partitioned := FALSE;
		select object_id into v_obj_id from dba_objects where owner=v_owner
		and object_name=v_obj_name and object_type=v_obj_type;
		else
		--PARTITION
		v_is_partitioned := TRUE;
		select object_id into v_obj_id from dba_objects where owner=v_owner
		and object_name=v_obj_name and object_type=v_obj_type and subobject_name=v_obj_part;
		end if;
		if v_obj_id is null then
			RAISE_APPLICATION_ERROR (-20005, 'Object not found in DBA*_OBJECTS/_TABLES/TAB_PARTITIONS: '||v_owner||'.'||v_obj_name||' '||v_obj_part||' '||v_obj_type);
		end if;
		--If not present on SMI, call check_space,check_segment,check_viability.
		select count(1) COUNT into v_smi_present from segment_manager_info where object_id=v_obj_id;
		if v_smi_present = 0 then
			v_chk_space := REORG.CHECK_SPACE(v_owner,v_obj_name,v_obj_part,v_obj_type);
			REORG.CHECK_SEGMENT(v_owner,v_obj_name,v_obj_part,v_obj_type);
		end if;
		if v_defrag_type IN ('MOVERB','OREDEF','BULKCC') AND v_chk_space = 'NO' then
					RAISE_APPLICATION_ERROR (-20006, 'Not enought space to make the operation '||v_defrag_type||': '||v_owner||'.'||v_obj_name||' '||v_obj_part||' '||v_obj_type);
		end if;
		select CHAIN_PCENT into v_check1 from segment_manager_info where object_id=v_obj_id;
		if v_check1 is null then
			REORG.CHECK_SEGMENT(v_owner,v_obj_name,v_obj_part,v_obj_type);
		end if;
		select IS_SMALL_DATAPUMP into v_compatible from segment_manager_info where object_id=v_obj_id;
		if v_compatible is null then
			REORG.CHECK_VIABILITY(v_owner,v_obj_name,v_obj_part,v_obj_type);
		END IF;
		select IS_SMALL_DATAPUMP into v_compatible from segment_manager_info where object_id=v_obj_id;
		IF v_compatible = 'N' then
			RAISE_APPLICATION_ERROR (-20007, 'Not compatible operation '||v_defrag_type||': '||v_owner||'.'||v_obj_name||' '||v_obj_part||' '||v_obj_type);
		END IF;
		-- CODE STARTS HERE
		v_start_time := sysdate;
		-- CODE STARTS HERE
		select count(1) COUNT into v_job_exists from DBA_DATAPUMP_JOBS where job_name=v_obj_name;
		IF v_job_exists > 0 then
			RAISE_APPLICATION_ERROR (-20008, 'Another DATAPUMP job running for the same table: '||v_defrag_type||': '||v_owner||'.'||v_obj_name||' '||v_obj_part||' '||v_obj_type);
		END IF;

		update SEGMENT_MANAGER_INFO set job_status='RUNNING'
		where object_id=v_obj_id and defrag_type_used=v_defrag_type;
		
		v_handler := DBMS_DATAPUMP.OPEN('EXPORT','TABLE',NULL,v_obj_name,NULL);
		--dbms_output.put_line(v_handler);
		
		
		DBMS_DATAPUMP.ADD_FILE(v_handler,'exp_reorg_'||v_owner||'_'||v_obj_name||'_'||v_obj_part||'.dmp','FRAG',reusefile=>1); -- Overwrites the DUMP file.
		DBMS_DATAPUMP.ADD_FILE(v_handler,'exp_reorg_'||v_owner||'_'||v_obj_name||'_'||v_obj_part||'_'||v_start_time||'.log','FRAG',filetype=>DBMS_DATAPUMP.KU$_FILE_TYPE_LOG_FILE);
		
		--Defining Parameters.
		--dbms_datapump.set_parameter(v_handler,'ESTIMATE','BLOCKS');
		--dbms_datapump.set_parameter(v_handler,'COMPRESSION','ALL');
		
		if v_obj_type = 'TABLE' then
		DBMS_DATAPUMP.METADATA_FILTER(v_handler,'SCHEMA_EXPR','IN ('''||v_owner||''')');
		DBMS_DATAPUMP.METADATA_FILTER(v_handler,'NAME_EXPR','IN ('''||v_obj_name||''')');
		
		elsif v_obj_type = 'TABLE PARTITION' then
		DBMS_DATAPUMP.METADATA_FILTER(v_handler,'SCHEMA_EXPR','IN ('''||v_owner||''')');
		DBMS_DATAPUMP.METADATA_FILTER(v_handler,'NAME_EXPR','IN ('''||v_obj_name||''')');
				DBMS_DATAPUMP.DATA_FILTER(v_handler,'PARTITION_EXPR','IN ('''||v_obj_part||''')',v_obj_name,v_owner);

		else
		RAISE_APPLICATION_ERROR (-20004, 'Object not valid for viability check: '||v_owner||'.'||v_obj_name||' '||v_obj_part||' '||v_obj_type);
		end if;
		select READ_ONLY into v_readonly from dba_tables where owner=v_owner and table_name=v_obj_name;
		if v_readonly = 'NO' then
			v_sql := 'ALTER TABLE '||v_owner||'.'||v_obj_name||' read only';
			execute immediate(v_sql);
			DBMS_DATAPUMP.LOG_ENTRY(v_handler,'Table altered as READ ONLY');
		end if;
		DBMS_DATAPUMP.START_JOB(v_handler,cluster_ok=>0);

 		MERGE INTO SEGMENT_MANAGER_INFO smi
		USING (SELECT 1 from dual) s ON (smi.object_id=v_obj_id)
		WHEN MATCHED THEN
			UPDATE SET smi.defrag_type_used=v_defrag_type,smi.defrag_start_time=v_start_time,smi.defrag_end_time=v_end_time,smi.error_msg=v_error_msg
		WHEN NOT MATCHED THEN
			INSERT (object_id,owner,object_name,object_type,PARTITION_NAME,defrag_type_used,defrag_start_time,defrag_end_time,error_msg)
			VALUES (v_obj_id,v_owner,v_obj_name,v_obj_type,v_obj_part,v_defrag_type,v_start_time,v_end_time,v_error_msg);
		commit;

		DBMS_DATAPUMP.WAIT_FOR_JOB(v_handler,v_job_status);
		DBMS_DATAPUMP.DETACH(v_handler);
		--dbms_output.put_line('FIN Export: '||v_job_status);

		if v_job_status = 'COMPLETED' then
			update SEGMENT_MANAGER_INFO set job_status='EXPORTED'
			where object_id=v_obj_id and defrag_type_used=v_defrag_type;
		end if;
		--RESET
		v_job_status := NULL;
		v_handler := NULL;
		
		select READ_ONLY into v_readonly from dba_tables where owner=v_owner and table_name=v_obj_name;
		--dbms_output.put_line('FIN Import, read_only state: '||v_readonly);
		if v_readonly = 'YES' then
			v_sql := 'ALTER TABLE '||v_owner||'.'||v_obj_name||' read write';
			execute immediate(v_sql);
			DBMS_DATAPUMP.LOG_ENTRY(v_handler,'Table altered as READ WRITE');
		end if;
		
		--Import / TRUNCATE.
		v_handler := DBMS_DATAPUMP.OPEN('IMPORT','TABLE',NULL,v_obj_name,NULL);
		-- DUMPFILE LOGFILE
		DBMS_DATAPUMP.ADD_FILE(v_handler,'exp_reorg_'||v_owner||'_'||v_obj_name||'_'||v_obj_part||'.dmp','FRAG');
		DBMS_DATAPUMP.ADD_FILE(v_handler,'imp_reorg_'||v_owner||'_'||v_obj_name||'_'||v_obj_part||'_'||v_start_time||'.log','FRAG',filetype=>DBMS_DATAPUMP.KU$_FILE_TYPE_LOG_FILE);
		--TABLE_EXISTS_ACTION
		DBMS_DATAPUMP.SET_PARAMETER(v_handler,'TABLE_EXISTS_ACTION','TRUNCATE');

		DBMS_DATAPUMP.START_JOB(v_handler);
		
		DBMS_DATAPUMP.WAIT_FOR_JOB(v_handler,v_job_status);
		DBMS_DATAPUMP.DETACH(v_handler);
		--dbms_output.put_line('FIN Import: '||v_job_status);

		if v_job_status = 'COMPLETED' then
			update SEGMENT_MANAGER_INFO set job_status='FINISHED'
			where object_id=v_obj_id and defrag_type_used=v_defrag_type;
		else
			update SEGMENT_MANAGER_INFO set job_status='WARNING',error_msg='Check Log: ' || 'imp_reorg_'||v_owner||'_'||v_obj_name||'_'||v_obj_part||'_'||v_start_time||'.log'
			where object_id=v_obj_id and defrag_type_used=v_defrag_type;
		end if;
		
		-- CODE ENDS HERE
		-- CODE ENDS HERE
		v_end_time := sysdate;

 		MERGE INTO SEGMENT_MANAGER_INFO smi
		USING (SELECT 1 from dual) s ON (smi.object_id=v_obj_id)
		WHEN MATCHED THEN
			UPDATE SET smi.defrag_type_used=v_defrag_type,smi.defrag_start_time=v_start_time,smi.defrag_end_time=v_end_time,smi.error_msg=v_error_msg
		WHEN NOT MATCHED THEN
			INSERT (object_id,owner,object_name,object_type,PARTITION_NAME,defrag_type_used,defrag_start_time,defrag_end_time,error_msg)
			VALUES (v_obj_id,v_owner,v_obj_name,v_obj_type,v_obj_part,v_defrag_type,v_start_time,v_end_time,v_error_msg);
		commit;

EXCEPTION
when others then
v_error_msg := sqlerrm;
		v_end_time := sysdate;
		
 		MERGE INTO SEGMENT_MANAGER_INFO smi
		USING (SELECT 1 from dual) s ON (smi.object_id=v_obj_id)
		WHEN MATCHED THEN
			UPDATE SET smi.defrag_type_used=v_defrag_type,smi.defrag_start_time=v_start_time,smi.defrag_end_time=v_end_time,smi.error_msg=v_error_msg
		WHEN NOT MATCHED THEN
			INSERT (object_id,owner,object_name,object_type,PARTITION_NAME,defrag_type_used,defrag_start_time,defrag_end_time,error_msg)
			VALUES (v_obj_id,v_owner,v_obj_name,v_obj_type,v_obj_part,v_defrag_type,v_start_time,v_end_time,v_error_msg);
		commit;
		dbms_datapump.detach (v_handler);
dbms_output.put_line('ERROR datapump:'||v_owner||'.'||v_obj_name||' '||v_obj_part||' '||v_obj_type||' Details: '||v_error_msg);
END datapump;

---------------------------------------------------------NEXT-----------------------------------------------
PROCEDURE bulkcollect(v_owner varchar2, v_obj_name varchar2,v_obj_part varchar2 default NULL, v_obj_type varchar2) AS
v_error_msg varchar2(4000);
v_defrag_type varchar2(30) :='IS_BULKCOLLECTABLE';
v_obj_id number;
v_start_time DATE;
v_end_time DATE;
v_is_partitioned varchar2(3);
v_sql varchar2(4000);
v_smi_present number;
v_chk_space varchar2(3);
v_compatible varchar2(1);
v_check1 number;

v_job_status varchar2(30);

--Bulk Collect variables:
v_bulkcollect clob;

CURSOR c_cols (v_owner varchar2,v_obj_name varchar2) is select column_name from dba_tab_columns where owner= v_owner and table_name = v_obj_name;
v_linea dba_tab_columns.column_name%TYPE;
v_columns varchar2(4000);
v_values varchar2(4000);
v_numcols number;
v_cols_cnt number;

v_create clob;
v_archivo utl_file.file_type;
v_readonly varchar2(3);
v_newname varchar2(30);
v_numrows number;

v_logging varchar2(3);
v_exists number;

BEGIN
		if v_obj_part is null then
		select object_id into v_obj_id from dba_objects where owner=v_owner
		and object_name=v_obj_name and object_type=v_obj_type;
		else
		--PARTITION
		select object_id into v_obj_id from dba_objects where owner=v_owner
		and object_name=v_obj_name and object_type=v_obj_type and subobject_name=v_obj_part;
		end if;
		if v_obj_id is null then
			RAISE_APPLICATION_ERROR (-20005, 'Object not found in DBA*_OBJECTS/_TABLES/TAB_PARTITIONS: '||v_owner||'.'||v_obj_name||' '||v_obj_part||' '||v_obj_type);
		end if;
		--If not present on SMI, call check_space,check_segment,check_viability.
		select count(1) COUNT into v_smi_present from segment_manager_info where object_id=v_obj_id;
		if v_smi_present = 0 then
			v_chk_space := REORG.CHECK_SPACE(v_owner,v_obj_name,v_obj_part,v_obj_type);
			REORG.CHECK_SEGMENT(v_owner,v_obj_name,v_obj_part,v_obj_type);
		end if;
		if v_defrag_type IN ('MOVERB','OREDEF','BULKCC') AND v_chk_space = 'NO' then
					RAISE_APPLICATION_ERROR (-20006, 'Not enought space to make the operation '||v_defrag_type||': '||v_owner||'.'||v_obj_name||' '||v_obj_part||' '||v_obj_type);
		end if;
		select CHAIN_PCENT into v_check1 from segment_manager_info where object_id=v_obj_id;
		if v_check1 is null then
			REORG.CHECK_SEGMENT(v_owner,v_obj_name,v_obj_part,v_obj_type);
		end if;
		select IS_BULKCOLLECTABLE into v_compatible from segment_manager_info where object_id=v_obj_id;
		if v_compatible is null then
			REORG.CHECK_VIABILITY(v_owner,v_obj_name,v_obj_part,v_obj_type);
		END IF;
		select IS_BULKCOLLECTABLE into v_compatible from segment_manager_info where object_id=v_obj_id;
		IF v_compatible = 'N' then
			RAISE_APPLICATION_ERROR (-20007, 'Not compatible operation '||v_defrag_type||': '||v_owner||'.'||v_obj_name||' '||v_obj_part||' '||v_obj_type);
		END IF;

		if v_obj_type = 'TABLE' then
			select PARTITIONED into v_is_partitioned from dba_tables where table_name=v_obj_name and owner=v_owner;
			if v_is_partitioned = 'YES' then
				RAISE_APPLICATION_ERROR (-20008, 'Trying to Bulk collect a partitioned table: '||v_defrag_type||': '||v_owner||'.'||v_obj_name||' '||v_obj_part||' '||v_obj_type);
			end if;
		end if;
		v_start_time := sysdate;
		
 		MERGE INTO SEGMENT_MANAGER_INFO smi
		USING (SELECT 1 from dual) s ON (smi.object_id=v_obj_id)
		WHEN MATCHED THEN
			UPDATE SET smi.defrag_type_used=v_defrag_type,smi.defrag_start_time=v_start_time,smi.defrag_end_time=v_end_time,smi.job_status=v_job_status,smi.error_msg=v_error_msg
		WHEN NOT MATCHED THEN
			INSERT (object_id,owner,object_name,object_type,PARTITION_NAME,defrag_type_used,defrag_start_time,defrag_end_time,job_status,error_msg)
			VALUES (v_obj_id,v_owner,v_obj_name,v_obj_type,v_obj_part,v_defrag_type,v_start_time,v_end_time,v_job_status,v_error_msg);
		commit;

		-- CODE STARTS HERE
		-- CODE STARTS HERE

		-- Create DDL File to generate the _TMP table;
		--v_archivo := utl_file.fopen ('FRAG', 'bulkcollect_'||v_owner||'_'||v_obj_name||'_'||v_obj_part||'.sql', 'w',32767);
		if v_obj_type = 'TABLE' then
			-- if length = 30, use alias for operation.'SIEBEL', 'S_CLASS_SCRPT',NULL,'TABLE'); -- RENAME at the end.
			if length(v_obj_name) > 25 then
			v_newname := SUBSTR(v_obj_name,1,25) || '_TMP';
			else
			v_newname := v_obj_name || '_TMP';
			end if;
			-- select REPLACE(dbms_metadata.get_ddl('TABLE','S_CLASS_SCRPT','SIEBEL'),'S_CLASS_SCRPT','S_CLASS_SCRPT'||'_TMP') from dual;
			select REPLACE(dbms_metadata.get_ddl(v_obj_type,v_obj_name,v_owner),v_obj_name,v_newname) into v_create from dual;
			--utl_file.put_line (v_archivo, v_create || ';');
					--Dependent objects (DDL gen). + read only.
			select READ_ONLY, LOGGING into v_readonly, v_logging from dba_tables where owner=v_owner and table_name=v_obj_name;
			if v_readonly = 'NO' then
				v_sql := 'ALTER TABLE '||v_owner||'.'||v_obj_name||' read only';
				execute immediate(v_sql);
			end if;
			select count(1) COUNT into v_exists from dba_tables where table_name=v_newname and owner=v_owner;
			if v_exists > 0 then
				execute immediate('DROP TABLE '||v_owner||'.'||v_newname||' CASCADE CONSTRAINTS');
				execute immediate(v_create);
			else
				execute immediate(v_create);
			end if;
			if v_logging = 'YES' then
					execute immediate('ALTER TABLE '||v_owner||'.'||v_newname||' NOLOGGING');
			end if;

			select round(num_rows / 10,0) into v_numrows from dba_tables where table_name=v_obj_name and owner=v_owner;
			select count(column_name) into v_numcols from dba_tab_columns where owner= v_owner and table_name = v_obj_name;

			if v_numrows is null then
				v_numrows := 10000;
			end if;
v_bulkcollect := 'DECLARE
CURSOR tab IS SELECT * FROM '||v_owner||'.'||v_obj_name||';
TYPE nt_type IS TABLE OF '||v_owner||'.'||v_obj_name||'%ROWTYPE;
l_arr nt_type;
cnt number;
BEGIN
cnt := 0;
OPEN tab;
LOOP
FETCH tab BULK COLLECT INTO l_arr LIMIT '||v_numrows||';
EXIT WHEN l_arr.count = 0;
FORALL i IN 1 .. l_arr.count
INSERT /*+ APPEND */ INTO '||v_owner||'.'||v_newname||'
';

				v_cols_cnt := 0;
				FOR v_linea IN c_cols (v_owner,v_obj_name) LOOP
							if v_cols_cnt = 0 then
								v_columns := v_linea.column_name || ', ';
								v_values := 'l_arr(i).' || v_linea.column_name || ', ';
							elsif v_cols_cnt < v_numcols - 1 then
								v_columns := v_columns || v_linea.column_name || ', ';
								v_values := v_values || ' l_arr(i).'|| v_linea.column_name || ', ';
							elsif v_cols_cnt + 1 = v_numcols then
								v_columns := v_columns || v_linea.column_name;
								v_values := v_values || ' l_arr(i).'|| v_linea.column_name;
								--utl_file.put_line (v_archivo,'CNT: '|| v_cols_cnt ||' COLUMNS: '|| v_columns || ' VALUES: '||v_values);   -- DEBUG
							end if;
						v_cols_cnt := v_cols_cnt +1;
				END LOOP;

v_bulkcollect := v_bulkcollect ||
'('||v_columns||')
VALUES
('||v_values||');
COMMIT;
cnt := cnt + 10;
update SEGMENT_MANAGER_INFO set job_status='||''''||'LOAD'||''''||'||cnt||'||''''||'%'||''''||'
where object_id='||v_obj_id||' and defrag_type_used='||''''||'IS_BULKCOLLECTABLE'||''''||';
commit;
END LOOP;
END;
';
			--DBMS_OUTPUT.PUT_LINE('Executing:'|| v_bulkcollect);   --DEBUG
			-- EXECUTING THE BULK COLLECT.
			update SEGMENT_MANAGER_INFO set job_status='LOADING'
			where object_id=v_obj_id and defrag_type_used=v_defrag_type;
			commit;
			
			execute immediate(v_bulkcollect);

			update SEGMENT_MANAGER_INFO set job_status='LOADED'
			where object_id=v_obj_id and defrag_type_used=v_defrag_type;
			commit;
						
			select READ_ONLY, LOGGING into v_readonly, v_logging from dba_tables where owner=v_owner and table_name=v_obj_name;
			if v_readonly = 'YES' then
				v_sql := 'ALTER TABLE '||v_owner||'.'||v_obj_name||' read write';
				execute immediate(v_sql);
			end if;
			
			--Truncate original table and load back.
			execute immediate('TRUNCATE TABLE '||v_owner||'.'||v_obj_name);
			
			update SEGMENT_MANAGER_INFO set job_status='LOADBACK'
			where object_id=v_obj_id and defrag_type_used=v_defrag_type;
			commit;
			
v_bulkcollect := 'DECLARE
CURSOR tab IS SELECT * FROM '||v_owner||'.'||v_newname||';
TYPE nt_type IS TABLE OF '||v_owner||'.'||v_obj_name||'%ROWTYPE;
l_arr nt_type;
cnt number;
BEGIN
cnt := 0;
OPEN tab;
LOOP
FETCH tab BULK COLLECT INTO l_arr LIMIT '||v_numrows||';
EXIT WHEN l_arr.count = 0;
FORALL i IN 1 .. l_arr.count
INSERT /*+ APPEND */ INTO '||v_owner||'.'||v_obj_name||'
';

v_bulkcollect := v_bulkcollect ||
'('||v_columns||')
VALUES
('||v_values||');
COMMIT;
cnt := cnt + 10;
update SEGMENT_MANAGER_INFO set job_status='||''''||'BACK'||''''||'||cnt||'||''''||'%'||''''||'
where object_id='||v_obj_id||' and defrag_type_used='||''''||'IS_BULKCOLLECTABLE'||''''||';
commit;
END LOOP;
END;
';
			
			-- Loading back to source table.
			execute immediate(v_bulkcollect);
			
			if v_logging = 'YES' then
					execute immediate('ALTER TABLE '||v_owner||'.'||v_obj_name||'_TMP'||' LOGGING');
			end if;
			
			-- Clean up.
			execute immediate('DROP TABLE '||v_owner||'.'||v_newname||' CASCADE CONSTRAINTS');
			
		elsif v_obj_type = 'TABLE PARTITION' then
		--drop partition/exchange...
			DBMS_OUTPUT.PUT_LINE('PARTITIONED!!');
		else
		RAISE_APPLICATION_ERROR (-20004, 'Object not valid for viability check: '||v_owner||'.'||v_obj_name||' '||v_obj_part||' '||v_obj_type);
		end if;
		
		v_end_time := sysdate;
		v_job_status := 'FINISHED';
		--utl_file.fclose(v_archivo);
		-- CODE ENDS HERE
		-- CODE ENDS HERE
		
 		MERGE INTO SEGMENT_MANAGER_INFO smi
		USING (SELECT 1 from dual) s ON (smi.object_id=v_obj_id)
		WHEN MATCHED THEN
			UPDATE SET smi.defrag_type_used=v_defrag_type,smi.defrag_start_time=v_start_time,smi.defrag_end_time=v_end_time,smi.job_status=v_job_status,smi.error_msg=v_error_msg
		WHEN NOT MATCHED THEN
			INSERT (object_id,owner,object_name,object_type,PARTITION_NAME,defrag_type_used,defrag_start_time,defrag_end_time,job_status,error_msg)
			VALUES (v_obj_id,v_owner,v_obj_name,v_obj_type,v_obj_part,v_defrag_type,v_start_time,v_end_time,v_job_status,v_error_msg);
		commit;

EXCEPTION
when others then
v_error_msg := sqlerrm;
		v_end_time := sysdate;
		v_job_status := 'FAILED';
		
 		MERGE INTO SEGMENT_MANAGER_INFO smi
		USING (SELECT 1 from dual) s ON (smi.object_id=v_obj_id)
		WHEN MATCHED THEN
			UPDATE SET smi.defrag_type_used=v_defrag_type,smi.defrag_start_time=v_start_time,smi.defrag_end_time=v_end_time,smi.job_status=v_job_status,smi.error_msg=v_error_msg
		WHEN NOT MATCHED THEN
			INSERT (object_id,owner,object_name,object_type,PARTITION_NAME,defrag_type_used,defrag_start_time,defrag_end_time,job_status,error_msg)
			VALUES (v_obj_id,v_owner,v_obj_name,v_obj_type,v_obj_part,v_defrag_type,v_start_time,v_end_time,v_job_status,v_error_msg);
		commit;
dbms_output.put_line('ERROR bulkcollect :'||v_owner||'.'||v_obj_name||' '||v_obj_part||' '||v_obj_type||' Details: '||v_error_msg);
END bulkcollect;


end reorg;
/

