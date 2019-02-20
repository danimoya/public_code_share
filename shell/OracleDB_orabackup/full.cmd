connect target &1/oractl;
run
{
backup as compressed backupset full database filesperset 3 format '/backup/full_%d_%U_%s_%p';
backup spfile format '/backup/spfile_%d_%U_%s_%p';
backup archivelog all delete input  format '/backup/arch_%d_%U_%s_%t';
}
