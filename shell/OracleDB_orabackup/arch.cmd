connect target &1/oractl;
run
{
BACKUP ARCHIVELOG ALL delete all input TO DESTINATION '/backup/archives';
}
