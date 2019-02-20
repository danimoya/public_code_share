connect target &1/oractl;
run
{
sql 'select * from v$instance';
}
