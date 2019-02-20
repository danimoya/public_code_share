connect target &1/oractl;
run
{
BACKUP INCREMENTAL LEVEL 1 as compressed backupset database filesperset 3 format '/backup/incr_%d_%U_%s_%p';
}
