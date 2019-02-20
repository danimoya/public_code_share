connect target &1/oractl;
run
{
BACKUP SECTION SIZE 10M TABLESPACE &2 TO DESTINATION '/backup';
}
