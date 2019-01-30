# Application Variables

# START APP. SPECIFIC VARIABLES (by environment)

DEFAULT_STORE_NAME=nosqltest

# Multi-Instance. NOSQLTAB is loaded from UNIX Profile. KVROOT must be unique.
# KVHOME=/u00/app/oracle/product/kv-4.3.11
# KVROOT=/u00/app/oracle/admin
# NOSQLTAB=/etc/nosqltab

LSOF=/sbin/lsof
JAVA=${JAVA_HOME}/bin/java

SECDIR=security
TMPDIR=/u00/app/oracle/tmp
NOSQL_EXPORT_DIR=/u00/app/oracle/admin/scripts/releases/export_dir

TIMEOUT_MS=20000

USERNAME=sys
NOSQL_ADMIN_ALIAS=sys

DEFAULT_DATA_SUFFIX=nosqldata
DEFAULT_DEPLOY_POOL=AllStorageNodes
DEFAULT_MAX_REP_FACTOR=3
DEFAULT_NUM_PARTITIONS=21

# Set NSQL_FS_CAPACITY to 0  for auto-detect.

NSQL_FS_CAPACITY=\"1000 MB\"
PCT_RAM_USED=0.5

# SNMP Servers

SNMPHOST1=snmp_host1
SNMPHOST2=snmp_host2

# Graphite Server (Monitoring)

NC_HOST=graphite_server
NC_PORT=2003

# Ports configured to support up to 100 Shards (Around 100 Primary Servers + Replicas, capacity 5)

NSQL_REGISTRY_PORT=5000
NSQL_HARANGE=5001,5599
NSQL_SERVICERANGE=5601,5999

APP_PARAMS=XX_PASSWORD_HERE_XX

# END APP. SPECIFIC VARIABLES

# Tag:parameters
# Application Users: _user_:UserName:Password
# Password should not contain delimiter: :
# The first _sn_ should have admin process
# _zone_:name:rf:type:arbiter_mode
# _pool_create_:name
# _sn_:zone_name:server_name:registry_port:admin
# _pool_join_:pool_name:sn_id

# TOPOLOGY 1

#_user_:USER_HERE:PASSWORD_HERE
#_zone_:ZoneONE:3:primary:-no-arbiters
#_zone_:ZoneTWO:3:secondary
#_pool_create_:PoolONE
#_pool_create_:PoolTWO
#_sn_:ZoneONE:srv01zn1:5000:admin
#_sn_:ZoneONE:srv02zn1:5000:admin
#_sn_:ZoneONE:srv03zn1:5000:admin
#_sn_:ZoneTWO:srv01zn2:5000:admin
#_sn_:ZoneTWO:srv02zn2:5000:admin
#_sn_:ZoneTWO:srv03zn2:5000:admin
#_pool_join_:PoolONE:sn1
#_pool_join_:PoolONE:sn2
#_pool_join_:PoolTWO:sn3
#_pool_join_:PoolONE:sn4
#_pool_join_:PoolTWO:sn5
#_pool_join_:PoolTWO:sn6

## TOPOLOGY 2 (Switched over scenario)
##_swback:SwitchBackTopo
##_swtopo:SwitchOverTopo
##_swtopo_config:change-zone-type:ZoneTWO:primary
##_swtopo_config:change-zone-type:ZoneONE:secondary

### TOPOLOGY 3 (Failed over scenario)
###_swback_config:change-zone-type:ZoneTWO:secondary
###_swback_config:change-zone-type:ZoneONE:primary