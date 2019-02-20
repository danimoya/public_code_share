# README - BACKUP #
#   Daniel Moya   #

# Objective
> Get a minimalistic script that it is flexible and easy to maintain.

Files description
------------------------
crosscheck - Make a crosscheck backup.
full - make a full backup.
incr_arch - To make a incremental backup. Several options.
tbs - To do a section backup of a single tablespace.
validate - Validate the database backups' consistency. After this, check: select * from V$DATABASE_BLOCK_CORRUPTION;

logs - Logs Directory

Backup recommendations
--------------------------------------
Ensure that the database that is being backup it up has the following pre-requisites
to approach backup optimization:

- Enable BCT to get the most of the incremental backups.
	ALTER DATABASE ENABLE BLOCK CHANGE TRACKING USING FILE '/dir/sid_block_change_file';
	How to check if it's active when making backups:
	select file#, datafile_blocks, (blocks_read / datafile_blocks) * 100 pct_read_for_backup
	from v$backup_datafile where used_change_tracking='YES' and incremental_level > 0;

- CONFIGURE command that helps optimization:
	CONFIGURE BACKUP OPTIMIZATION ON;
	CONFIGURE COMPRESSION ALGORITHM 'MEDIUM' as of release 'DEFAULT' OPTIMIZE FOR LOAD TRUE; --Adv. Compression license.
	CONFIGURE DEVICE TYPE [disk | sbt] PARALLELISM 16 BACKUP TYPE TO COMPRESSED BACKUPSET;
	CONFIGURE CONTROLFILE AUTOBACKUP ON;

- RMAN ASYNC operations: requires init. parameter BACKUP_TAPE_IO_SLAVES=TRUE and DBWR_IO_SLAVES set to the number of CPU.
	SELECT filename, long_wait_time_total, long_waits/io_count waitratio FROM V$BACKUP_ASYNC_IO;

	
ALWAYS REMEMBER
----------------------------------
	The degree of parallelism (DOP) achieved will be constrained by the lowest of these factors:
	•	The number of channels including MAXOPENFILES Default:8 (ALLOCATE CHANNEL disk1 DEVICE TYPE DISK FORMAT '/disk1/%U' MAXOPENFILES 8;)
	•	The number of backup sets (influence: FILESPERSET)
	•	The number of input files (influence: MAXOPENFILES)
