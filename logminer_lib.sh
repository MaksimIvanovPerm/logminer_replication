# Reference ###########################################################
# Misc
#log_info()
# myexit()

## Aux ###############################################################
# exec_sql_script()	db_name="$1" sql_script="$2"
# set_logmnr_param()	v_pname="$1" v_pvalue="$2" db_name="$LONGMNR_DB"
# get_logmnr_param()	v_pname="$1" db_name="$LONGMNR_DB"
# get_arclog_attribute()	v_fill_name="$1" v_attr_name="$2"
# register_archivelog()	local v_fill_name="$1"
# register_all_archivelog_in_dir()	v_logdir="$1" v_seq_from="$2"
# check_and_del_expired_arclog_registrations	Checks all records in SYSTEM.arclogs: if according arclog-files really is, or not;
# check_sequence_continuity
# rotate_unnecessary_arclogs	$1 - current seq#
#	Remove, after processing current arclog, all arclogs which is not necessary to retain for long-tx

## Data-processing ####################################################
# mine_arclog()	$1 - seq# of arclog to mine; $2 - code which designates options for mining arlog(s) by dbms_logmnr;
#	Mining tx-describing data from arclog
# apply_dml_totable
#	Get mined dml, and apply it to a table
# check_prev_opened_tx	$1 - current arclog seq#
#	Check: if, in current-arclog data, is|are xid of long-tx, registered in system.active_transactions;
#	And, if is|are, process that records, in system.active_transactions, in according xid-status, found in arclog;
# delete_applied_tx
#	Delete, from system.active_transactions, records about long-tx, which was|were commited in current arclog;
# apply_long_commited_tx	$1 - seq# from;	$2 - seq# to; from&to border-values are included in range;
#	Mine range of arclogs which contains info about long-tx;
#	Get out this info and apply it to replcated table;
#######################################################################
get_arclog_attribute() {
local module="get_arclog_attribute"
local v_fill_name="$1"
local v_attr_name="$2"
local v_arclogdir

if [ -z "$v_fill_name" ]
then
	log_info "${module} fill name of arclog-file is empty;"
	return "1"
fi

if [ -z "$v_attr_name" ]
then
        log_info "${module} attribute-name is empty;"
        return "1"
fi

if [ ! -f "$v_fill_name" ]
then
	log_info "${module} file ${v_fill_name} is absent;"
	return 1	
fi

case "$v_attr_name" in
	"dirname")
		v_arclogdir=$(dirname ${v_fill_name})
		v_arclogdir=${v_arclogdir%"/"}; v_arclogdir="${v_arclogdir}/";
		echo "$v_arclogdir"
	;;
	"filename")
		echo $(basename ${v_fill_name})
	;;
	"seq#")
		echo $(basename ${v_fill_name}) | cut -f 2 -d "_" | awk '{printf "%d", $0;}'
	;;
	"rid")
		echo $(basename ${v_fill_name}) | cut -f 3 -d "_" | awk '{printf "%d", $0;}'
	;;
	*)
		log_info "${module} unexpected attribute-name: ${v_attr_name}"
		return 1
	;;

esac

return 0
}

log_info() {
local datetime=`date +%Y.%m.%d:%H.%M.%S`
local data_source="$2"

if [ "$data_source" == "logfile" ]
then
	[ "$CONSOLE_OUTPUT" == "Y" ] && cat $1 | awk -v runid=$RUNID '{print runid": "$0}'
	cat $1 | awk -v runid=$RUNID '{print runid": "$0}' >> $CURRENT_LOG_FILE
else
	[ -e "$CURRENT_LOG_FILE" ] && echo "${RUNID}:${datetime}: $1" >> $CURRENT_LOG_FILE
	[ "$CONSOLE_OUTPUT" == "Y" ] && echo "${RUNID}:${datetime}: $1"
fi

if [ "$V_LOG2JIRA" == "1" ]
then
	post_comment2jiratask "${RUNID}:${datetime}: $1"
fi
}

myexit() {
local module="myexit"
	log_info "${module}: ${1}"
	${RM} -f "$LOCK_FILE" 1>/dev/null 2>&1
        [ -f "$CURRENT_LOG_FILE" ] && cat "$CURRENT_LOG_FILE" >> "$LOG_FILE"
	${RM} -f "$CURRENT_LOG_FILE" 1>/dev/null 2>&1
	${RM} -f "$TMP_FILE" 1>/dev/null 2>&1
	${RM} -f "$SQL_SPOOL_FILE" 1>/dev/null 2>&1
	${RM} -f "$SQL_FILE" 1>/dev/null 2>&1
	exit "$1"
}

exec_sql_script() {
local rc
local module="exec_sql_statement"
local db_name="$1"
local sql_script="$2"

if [ -z "$db_name" ]
then
 log_info "$module ERROR: arg with db-name: is empty"
 return 1
fi

if [ ! -f "${sql_script}" ]
then
 log_info "$module ERROR: sql-script: ${sql_script} isn't file or not found"
 return 2
fi

log_info "$module ok try to run ${sql_script} for db ${db_name}"
[ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"
[ -f "$TMP_FILE" ] && rm -f $TMP_FILE

su -l oracle << __EOF__
export ORACLE_SID=${db_name}
$ORACLE_HOME/bin/sqlplus -S /nolog @${sql_script} "${db_name}" 1>${SQL_SPOOL_FILE} 2>&1
__EOF__
rc=$?

if [ -f "${SQL_SPOOL_FILE}" ]
then
 log_info "${SQL_SPOOL_FILE}" "logfile"
 rm -f ${SQL_SPOOL_FILE}
fi
log_info "$module exiting with code: $rc"
return $rc

}

set_logmnr_param() {
local module="set_logmnr_param"
local v_pname="$1"
local v_pvalue="$2"
local db_name="$LONGMNR_DB"
local rc

if [ -z "$v_pname" ]
then
        log_info "${module} pname is empty"
        return 1
fi

if [ -z "$db_name" ]
then
        log_info "${module} db_name is empty"
        return 1
fi

log_info "${module} try to set value of parameter ${v_pname} to ${v_pvalue} in ${db_name}"
[ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"
[ -f "$TMP_FILE" ] && rm -f $TMP_FILE

cat << __EOF__ > "$TMP_FILE"
whenever sqlerror exit 1
conn / as sysdba
set echo on
set head off
set feedback off

declare
 v_count integer;
begin
 select count(*) into v_count from system.logmnr_conf where pname='${v_pname}';
 if v_count = 0 then
  insert into system.logmnr_conf (pname, pvalue) values ('${v_pname}', '${v_pvalue}');
 else
  update system.logmnr_conf t set t.pvalue='${v_pvalue}' where t.pname='${v_pname}';
 end if;
 commit write nowait batch;
end;
/

exit;
__EOF__

su -l oracle << __EOF__
export ORACLE_SID=${db_name}
$ORACLE_HOME/bin/sqlplus -S /nolog @$TMP_FILE 1>${SQL_SPOOL_FILE} 2>&1
__EOF__
rc=$?

if [ -f "${SQL_SPOOL_FILE}" ]
then
 log_info "${SQL_SPOOL_FILE}" "logfile"
 rm -f ${SQL_SPOOL_FILE}
fi
log_info "$module exiting with code: $rc"
return $rc
}

get_logmnr_param() {
local module="get_logmnr_param"
local v_pname="$1"
local db_name="$LONGMNR_DB"
local rc

if [ -z "$v_pname" ]
then
	log_info "${module} pname is empty"
	return 1
fi

if [ -z "$db_name" ]
then
        log_info "${module} db_name is empty"
        return 1
fi


log_info "${module} try to ask value of parameter ${v_pname} in ${db_name}"
[ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"
[ -f "$TMP_FILE" ] && rm -f $TMP_FILE

cat << __EOF__ > "$TMP_FILE"
whenever sqlerror exit 1
conn / as sysdba
set echo off
set head off
set feedback off
select pvalue from system.logmnr_conf where pname='${v_pname}';
exit;
__EOF__

su -l oracle << __EOF__
export ORACLE_SID=${db_name}
$ORACLE_HOME/bin/sqlplus -S /nolog @$TMP_FILE 1>${SQL_SPOOL_FILE} 2>&1
__EOF__
rc=$?

}

mine_arclog() {
local module="mine_arclog"
local v_seq="$1"
local v_fullarclog_name=""
local v_option="$2"
local db_name="$LONGMNR_DB"
local v_ext_table_name=""
local rc

if [ -z "$v_option" -o -z "$v_seq" ]
then
	log_info "${module} v_option|v_seq is empty;"; return 1
fi

if ! [[ "$v_seq" =~ ^[0-9]+$ ]]
then
	log_info "${module} v_seq is not an int-digit;"; return 1
fi
if ! [[ "$v_option" =~ ^[0-9]+$ ]]
then
	log_info "${module} v_option is not an int-digit;"; return 1
fi
v_seq=$(echo "$v_seq" | awk '{printf "%d", $0;}')
v_option=$(echo "$v_option" | awk '{printf "%d", $0;}')

log_info "${module} ok try to process ${v_seq} with option: ${v_option}"
log_info "${module} try to decode ${v_option} to dbms_logmnr-terms;"
case "$v_option" in
	"0")
	v_option="DBMS_LOGMNR.NO_SQL_DELIMITER+DBMS_LOGMNR.NO_ROWID_IN_STMT"
	v_ext_table_name="ALL_TX_DATA"
	;;
	"1")
	v_option="DBMS_LOGMNR.COMMITTED_DATA_ONLY+DBMS_LOGMNR.NO_SQL_DELIMITER+DBMS_LOGMNR.NO_ROWID_IN_STMT"
	v_ext_table_name="COMMITED_ONLY_TX_DATA"
	;;
	"*")
	log_info "${module} unexpected value for v_option: ${v_option}"; return "1"
	;;
esac

log_info "${module} try to obtain full-name of arclog with seq#: ${v_seq}"
cat << __EOF__ > "$SQL_FILE"
whenever sqlerror exit 1
conn / as sysdba
set echo off
set head off
set feedback off
set newp none
set pagesize 0
set verify off
set appinfo logmnr
select FS_DIRECTORY||'/'||NAME as col from system.arclogs where seq=${v_seq};
exit
__EOF__
chown oracle:oracle "$SQL_FILE"
[ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"
su -l oracle << __EOF__
export ORACLE_SID=${LONGMNR_DB}
$ORACLE_HOME/bin/sqlplus -S /nolog @${SQL_FILE} 1>${SQL_SPOOL_FILE} 2>&1
__EOF__
rc="$?"
log_info "${SQL_SPOOL_FILE}" "logfile";
if [ "$rc" -ne "0" ]
then
	rm -f ${SQL_SPOOL_FILE}; rm -f ${SQL_FILE}; return "1"
fi
v_fullarclog_name=$(cat "$SQL_SPOOL_FILE" | tr -d [:cntrl:]); rm -f ${SQL_SPOOL_FILE}
if [ ! -f "$v_fullarclog_name" ]
then
	log_info "${module} file ${v_fullarclog_name} does not exist;"; return 1
fi

log_info "${module} ok ${v_seq} decoded to ${v_fullarclog_name}"
log_info "${module} ok ${2} decoded to ${v_option}"

cat << __EOF__ > "$SQL_FILE"
whenever sqlerror exit 1
conn / as sysdba
set echo on
set head off
set feedback off
set newp none
set pagesize 0
set verify off
set appinfo logmnr

--https://www.giac.org/paper/gcfa/159/oracle-database-forensics-logminer/105140
ALTER SESSION SET NLS_DATE_FORMAT='DD-MON-YYYY HH24:MI:SS';
alter session set NLS_TIMESTAMP_FORMAT='yyyy-mm-dd hh:mi:ssxff';
exec DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => '${v_fullarclog_name}', OPTIONS => DBMS_LOGMNR.NEW);

exec DBMS_LOGMNR.START_LOGMNR(OPTIONS => ${v_option});

begin
execute immediate 'drop table SYS.${v_ext_table_name}';
exception when others then null;
end;
/

column dir_path new_value dir_path noprint
select directory_path as dir_path from sys.dba_directories where directory_name='${DIR_OBJECT}';
host rm -f &&dir_path/${v_ext_table_name}* 2>/dev/null

create table SYS.${v_ext_table_name}
ORGANIZATION EXTERNAL
(TYPE ORACLE_DATAPUMP DEFAULT DIRECTORY ${DIR_OBJECT} LOCATION  ('${v_ext_table_name}.dmp'))
as select * from V\$LOGMNR_CONTENTS;

EXECUTE DBMS_LOGMNR.END_LOGMNR();

exit;
__EOF__

chown oracle:oracle "$SQL_FILE"
[ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"
su -l oracle << __EOF__
export ORACLE_SID=${LONGMNR_DB}
$ORACLE_HOME/bin/sqlplus /nolog @${SQL_FILE} 1>${SQL_SPOOL_FILE} 2>&1
__EOF__
rc="$?"
#cat ${SQL_FILE}
#cat ${SQL_SPOOL_FILE}
[ -f "$SQL_FILE" ] && rm -f "$SQL_FILE"
if [ -f "${SQL_SPOOL_FILE}" ]
then
 log_info "${SQL_SPOOL_FILE}" "logfile";  rm -f ${SQL_SPOOL_FILE}
fi
return "$rc"

}

register_archivelog() {
local module="register_archivelog"
local v_fill_name="$1"
local rc

if [ ! -f "$v_fill_name" ]
then
	log_info "${module} arclog-file ${v_fill_name} is absent;"
	return 1
fi

local v_logdir=$(get_arclog_attribute "$v_fill_name" "dirname")
local v_arclogname=$(get_arclog_attribute "$v_fill_name" "filename")
local v_arclog_seq=$(get_arclog_attribute "$v_fill_name" "seq#")
local v_arclog_rid=$(get_arclog_attribute "$v_fill_name" "rid")

log_info "${module} ok try to process ${v_fill_name}"
log_info "${module} arclogdir:...${v_arclogdir}"
log_info "${module} arclogname:..${v_arclogname}"
log_info "${module} arclog seq#:.${v_arclog_seq}"
log_info "${module} arclog rid:..${v_arclog_rid}"

cat << __EOF__ > "$SQL_FILE"
whenever sqlerror exit 1
conn / as sysdba
set echo on
set head off
set feedback off
set newp none
set serveroutput on
set verify off
set appinfo logmnr

column rcount new_value rcount noprint;
select count(*) as rcount from SYSTEM.arclogs t
where fs_directory='${v_logdir}' and t.name='${v_arclogname}' and t.seq=${v_arclog_seq} and t.resetlogs_id=${v_arclog_rid};

prompt record count is: &&rcount
begin
if &&rcount=0 then
	insert into SYSTEM.arclogs(fs_directory, name, seq, resetlogs_id, status)
	values('${v_logdir}', '${v_arclogname}', ${v_arclog_seq}, ${v_arclog_rid}, 'new');
	commit;
	dbms_output.put_line('new record added');
else
	dbms_output.put_line('record already exists');
end if;
end;
/

exit;
__EOF__

chown oracle:oracle "$SQL_FILE"
exec_sql_script "$LONGMNR_DB" "$SQL_FILE"
rc="$?"
[ -f "$SQL_FILE" ] && rm -f "$SQL_FILE"
if [ -f "${SQL_SPOOL_FILE}" ]
then
 log_info "${SQL_SPOOL_FILE}" "logfile"
 rm -f ${SQL_SPOOL_FILE}
fi
log_info "$module exiting with code: $rc"
return "$rc"
}


register_all_archivelog_in_dir() {
local module="register_all_archivelog_in_dir"
local v_logdir="$1"
local v_seq_from="$2"
local v_arclog_seq
local rc

if [ ! -d "$v_logdir" ]
then
	log_info "$module ${v_logdir} is not a directory"; return 1
fi

[ -z "$v_seq_from" ] && v_seq_from="1"
if ! [[ "$v_seq_from" =~ ^[0-9]+$ ]]
then
	log_info "$module ${v_seq_from} is not a int-digit"; return 1
fi
v_seq_from=$(echo "$v_seq_from" | awk '{printf "%d", $0;}')
log_info "$module try to find and register all arclogs in ${v_logdir} from seq#: ${v_seq_from}"

for i in $(find "$v_logdir" -name "$ARCLOG_NAME_FORMAT" )
do
	v_arclog_seq=$(get_arclog_attribute "$i" "seq#")
	[ "$?" -ne "0" ] && {
		log_info "$module can not get seq# from name: ${i}"; return 1
	}
	if [ "$v_arclog_seq" -ge "$v_seq_from" ]
	then
		log_info "$module processing: ${i} ${v_arclog_seq}"
		register_archivelog "$i"
		if [ "$?" -ne "0" ]
		then
			log_info "$module for some reason it's impossible to register in repository arclog ${i}"
			log_info "$module exiting with fail code"
			return "1"
		fi
	fi
done

log_info "$module done"
return "0"
}

check_and_del_expired_arclog_registrations() {
local module="check_expired_arclog_registrations"
#local v_seq="$1"
local x
local rc

#if [ ! -z "$v_seq" ]
#then
#	log_info "$module seq# is provided with value: ${v_seq}"
#	if ! [[ "$v_seq" =~ ^[0-9]+$ ]]
#	then
#		log_info "$module ${v_seq} is not a int-digit"; return 1
#	fi
#	v_seq=$(echo "$v_seq" | awk '{printf "%d", $0;}')
#fi

log_info "$module try to ask ${LONGMNR_DB} about list of full-path of arclogs"
cat << __EOF__ > "$SQL_FILE"
whenever sqlerror exit 1
conn / as sysdba
set echo off
set head off
set feedback off
set newp none
set linesize 1024
set appinfo logmnr
select FS_DIRECTORY||'/'||NAME as col from SYSTEM.arclogs order by seq asc;
exit;
__EOF__

chown oracle:oracle "$SQL_FILE"
[ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"; [ -f "${TMP_FILE}" ] && rm -f "${TMP_FILE}"

su -l oracle << __EOF__
export ORACLE_SID=${LONGMNR_DB}
$ORACLE_HOME/bin/sqlplus -S /nolog @${SQL_FILE} 1>${SQL_SPOOL_FILE} 2>&1
__EOF__
rc="$?"
log_info "$SQL_SPOOL_FILE" "logfile"
[ "$rc" -ne "0" ] && {
	log_info "$module can not ask ${$LONGMNR_DB} about arclog-records, exiting with fail"
	[ -f "$SQL_FILE" ] && rm -f "$SQL_FILE"
	[ -f "${SQL_SPOOL_FILE}" ] && rm -f ${SQL_SPOOL_FILE}
	log_info "$module exiting with fail"
	return 1
}

[ -f "$SQL_FILE" ] && rm -f "$SQL_FILE"
if [ -f "${SQL_SPOOL_FILE}" ]
then
	#log_info "${SQL_SPOOL_FILE}" "logfile"
	log_info "$module ok, check records in obtained list of full-path of arclog-files"
	while read line
	do
		log_info "$module check record: ${line}"	
		[ ! -f "$line" ] && { 
			log_info "$module expired record: ${line}"
			echo "$line" >> "$TMP_FILE"
		}
	done < <(cat "$SQL_SPOOL_FILE")
	rm -f ${SQL_SPOOL_FILE}
fi

if [ -f "$TMP_FILE" ]
then
	log_info "$module arclog-files which is(are) registered in repository, but is(are) absent as OS-file(s):"
	log_info "${TMP_FILE}" "logfile"
	x=$(cat "$TMP_FILE" | wc -l)
	log_info "$module amount of expired repository-records is: ${x}"
else
	x="0"
fi

if [ "$x" -gt "0" ]
then
	#log_info "$module some arclogs-registrations are expired:"
	log_info "$TMP_FILE" "logfile"
	cat << __EOF__ > "$SQL_FILE"
whenever sqlerror exit 1
conn / as sysdba
set echo off
set head off
set feedback off
set newp none
set linesize 1024
set appinfo logmnr
__EOF__
	while read line
	do
		echo "delete from SYSTEM.arclogs where FS_DIRECTORY||'/'||NAME='${line}';" >> "$SQL_FILE"
	done < <(cat "$TMP_FILE")
	cat << __EOF__ >> "$SQL_FILE"
commit;
exit
__EOF__
	chown oracle:oracle "$SQL_FILE"
	exec_sql_script "$LONGMNR_DB" "$SQL_FILE"
	rc="$?"
	[ -f "$SQL_FILE" ] && rm -f "$SQL_FILE"
	if [ -f "${SQL_SPOOL_FILE}" ]
	then
		 log_info "${SQL_SPOOL_FILE}" "logfile"
		 rm -f ${SQL_SPOOL_FILE}
	fi
	[ "$rc" -ne "0" ] && {
		log_info "$module can not delete expired arclog-records from repository"; return "$rc"
	}
else
	log_info "$module no-one arclog-records, registered in repository, is|are found as expired"
fi

log_info "$module done"
return "0"

}

check_sequence_continuity() {
local module="check_sequence_continuity"
local v_seq_from="$1"
local v_seq_to="$2"
local rc

if [ -z "$v_seq_from" -o -z "$v_seq_to" ]
then
	log_info "$module v_seq_from and|or v_seq_to is|are empty"; return 1
fi
log_info "$module v_seq_from: ${v_seq_from}; v_seq_to: ${v_seq_to}"

if ! [[ "$v_seq_from" =~ ^[0-9]+$ ]]
then
	log_info "$module v_seq_from is not an int-digit;"; return 1
fi

if ! [[ "$v_seq_to" =~ ^[0-9]+$ ]]
then
        log_info "$module v_seq_to is not an int-digit;"; return 1
fi
v_seq_from=$(echo "$v_seq_from" | awk '{printf "%d", $0;}')
v_seq_to=$(echo "$v_seq_to" | awk '{printf "%d", $0;}')

if [ "$v_seq_from" -gt "$v_seq_to" ]
then
	log_info "$module v_seq_from has to ne less or equial to v_seq_to"; return 1
fi

for((i=${v_seq_from};i<=${v_seq_to};i++))
do
	log_info "$module asking about seq# ${i}"
	cat << __EOF__ > "$SQL_FILE"
whenever sqlerror exit 1
conn / as sysdba
set echo off
set head off
set feedback off
set newp none
set linesize 1024
set appinfo logmnr
declare
 x integer;
begin
 select seq into x from SYSTEM.arclogs where seq=${i};
end;
/

exit;
__EOF__
	chown oracle:oracle "$SQL_FILE"
	[ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"
	su -l oracle << __EOF__
export ORACLE_SID=${LONGMNR_DB}
$ORACLE_HOME/bin/sqlplus -S /nolog @${SQL_FILE} 1>${SQL_SPOOL_FILE} 2>&1
__EOF__
	rc="$?"
	[ "$rc" -ne "0" ] && {
	        log_info "$module about arclog-records about ${i} seq# was not found"
        	[ -f "$SQL_FILE" ] && rm -f "$SQL_FILE"
	        [ -f "${SQL_SPOOL_FILE}" ] && rm -f ${SQL_SPOOL_FILE}
        	return 1
	}
done
log_info "$module done"
return 0
}

log_tx_metadata_from_arclog() {
local module="log_tx_metadata_from_arclog"
local v_seq="$1"
local rc

if [ -z "$v_seq" ]
then
	log_info "$module v_seq is empty;"; return 1
fi

if ! [[ "$v_seq" =~ ^[0-9]+$ ]]
then
	log_info "$module v_seq: ${v_seq} is not an int-digit;"; return 1
fi
v_seq=$(echo "$v_seq" | awk '{printf "%d", $0;}')

log_info "$module start with v_seq: ${v_seq}"
cat << __EOF__ > "$SQL_FILE"
whenever sqlerror exit 1
conn / as sysdba
set echo off
set head off
set feedback off
set serveroutput on size unlimited
set appinfo logmnr

DECLARE
 v_arclog_seq    NUMBER := ${v_seq};
 v_sid           NUMBER;
 v_serial        NUMBER;
 CURSOR c1 IS WITH tx AS (SELECT DISTINCT t.xid AS xid
FROM SYS.ALL_TX_DATA t
WHERE t.table_name='${TABLE_NAME}'
)
SELECT t1.xid AS opened_tx
FROM SYS.ALL_TX_DATA t1, tx
WHERE t1.xid=tx.xid AND Upper(t1.operation) = 'START'
MINUS
SELECT t2.xid AS closed_tx
FROM SYS.ALL_TX_DATA t2, tx
WHERE t2.xid=tx.xid AND Upper(t2.operation) IN ('COMMIT','ROLLBACK');
BEGIN
 FOR i IN c1
 LOOP
  Dbms_Output.put_line('processing: '||i.opened_tx);
  SELECT DISTINCT t.session#, t.serial# INTO v_sid, v_serial
  FROM SYS.ALL_TX_DATA t WHERE t.xid=i.opened_tx;
  INSERT INTO SYSTEM.active_transactions(opened_at_seq, xid, sid, serial#, status)
  VALUES (v_arclog_seq, i.opened_tx, v_sid, v_serial, 'START');
  COMMIT WRITE NOWAIT batch;
 END LOOP;
END;
/

exit;
__EOF__
chown oracle:oracle "$SQL_FILE"
[ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"
su -l oracle << __EOF__
export ORACLE_SID=${LONGMNR_DB}
$ORACLE_HOME/bin/sqlplus /nolog @${SQL_FILE} 1>${SQL_SPOOL_FILE} 2>&1
__EOF__
rc="$?"

[ -f "$SQL_FILE" ] && rm -f "$SQL_FILE"
if [ -f "${SQL_SPOOL_FILE}" ]
then
         log_info "${SQL_SPOOL_FILE}" "logfile"
         rm -f ${SQL_SPOOL_FILE}
fi

log_info "$module done"
return "$rc"
}

apply_dml_totable() {
local module="apply_dml_totable"
local rc

log_info "$module start"
log_info "$module just for logging&debug"
cat << __EOF__ > "$SQL_FILE"
whenever sqlerror exit 1
conn / as sysdba
set echo off
set head off
set feedback off
set newp none
set pagesize 0
set linesize 4000
set appinfo logmnr

SELECT t.commit_scn||' '||SYSTEM.rewrite_sql.process_statement(t.operation, t.table_name, t.sql_redo, 0) AS col1
FROM SYS.COMMITED_ONLY_TX_DATA t
WHERE t.table_name='${TABLE_NAME}'
  AND t.commit_scn IN (SELECT DISTINCT t1.commit_scn as cscn FROM SYS.COMMITED_ONLY_TX_DATA t1 WHERE t1.table_name='${TABLE_NAME}')
ORDER BY t.commit_scn asc, t.sequence# asc;

exit;
__EOF__
chown oracle:oracle "$SQL_FILE"
[ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"

su -l oracle << __EOF__
export ORACLE_SID=${LONGMNR_DB}
$ORACLE_HOME/bin/sqlplus -S /nolog @${SQL_FILE} 1>${SQL_SPOOL_FILE} 2>&1
__EOF__
log_info "${SQL_SPOOL_FILE}" "logfile"
[ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"

log_info "$module actual applying dml"
cat << __EOF__ > "$SQL_FILE"
whenever sqlerror exit 1
conn / as sysdba
set echo off
set head off
set feedback off
set newp none
set pagesize 0
set serveroutput on
set appinfo logmnr

declare
cursor c1 is select cscn as commit_scn from (
SELECT DISTINCT t.commit_scn as cscn FROM SYS.COMMITED_ONLY_TX_DATA t
WHERE t.table_name='${TABLE_NAME}'
ORDER BY t.commit_scn asc);

cursor c2(p1 in number) is SELECT t.sequence# as sttmnt_seq, SYSTEM.rewrite_sql.process_statement(t.operation, t.table_name, t.sql_redo, 0) AS sql_sttmnt
FROM SYS.COMMITED_ONLY_TX_DATA t
WHERE t.table_name='${TABLE_NAME}' AND t.commit_scn=p1
ORDER BY t.sequence# asc;

cursor_name    INTEGER;
rows_processed INTEGER;
e_wrong_num_of_rowsprocessed exception;
v_count        integer;
begin
 for i in c1
 loop
  dbms_output.put_line(i.commit_scn);
  v_count:=0;
  for j in c2(i.commit_scn)
  loop
   begin
    /*cursor_name := dbms_sql.open_cursor;
    DBMS_SQL.PARSE(cursor_name, j.sql_sttmnt, DBMS_SQL.NATIVE);
    rows_processed := DBMS_SQL.EXECUTE(cursor_name);
    DBMS_SQL.CLOSE_CURSOR(cursor_name);*/
    execute immediate j.sql_sttmnt;
    v_count:=v_count+1;
   exception
    when others then --DBMS_SQL.CLOSE_CURSOR(cursor_name);
     Raise_Application_Error(-20000,i.commit_scn||' '||j.sttmnt_seq);
   end;
  end loop; -- end of loop by statement sequence in given commit scn;
  if v_count>0 then
   commit;
  end if;
 end loop; --end of loop by commit scns
end;
/

exit
__EOF__

chown oracle:oracle "$SQL_FILE"
[ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"
[ -f "$TMP_FILE" ] && rm -f "$TMP_FILE"

su -l oracle << __EOF__
export ORACLE_SID=${LONGMNR_DB}
$ORACLE_HOME/bin/sqlplus -S /nolog @${SQL_FILE} 1>${SQL_SPOOL_FILE} 2>&1
__EOF__
rc="$?"
log_info "${SQL_SPOOL_FILE}" "logfile"
[ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"
[ -f "$SQL_FILE" ] && rm -f "$SQL_FILE"
log_info "$module exiting with ${rc}"
return "$rc"
}

check_prev_opened_tx() {
local module="check_prev_opened_tx"
local v_seq="$1"
local rc

if [ -z "$v_seq" ]
then
	log_info "$module v_seq is empty"; return 1
fi

if ! [[ "$v_seq" =~ ^[0-9]+$ ]]
then
	log_info "$module v_seq: ${v_seq} is not an int-digit"; return 1
fi
v_seq=$(echo "$v_seq" | awk '{printf "%d", $0;}')

# if SYSTEM.active_transactions is empty, or does not conatain started tx - we can skip next activities;
log_info "$module starting with v_seq: ${v_seq}"
cat << __EOF__ > "$SQL_FILE"
whenever sqlerror exit 1
conn / as sysdba
set echo off
set head off
set feedback off
set newp none
set pagesize 0
set appinfo logmnr

begin
 execute immediate 'drop table finally_closed_transactions';
exception when others then null;
end;
/


create global temporary table finally_closed_transactions 
on commit preserve rows
as
select t.xid as xid, t.operation as operation
from SYS.ALL_TX_DATA t
where t.xid in (select atx.xid from SYSTEM.active_transactions atx where atx.opened_at_seq<${v_seq});

select xid||' '||operation as closed_tx
from finally_closed_transactions;

update SYSTEM.active_transactions t
set t.STATUS='COMMIT'
where t.xid in (select f.xid from finally_closed_transactions f where f.operation='COMMIT');

delete from SYSTEM.active_transactions t
where t.xid in (select f.xid from finally_closed_transactions f where f.operation='ROLLBACK');

commit;

begin
 execute immediate 'drop table finally_closed_transactions';
exception when others then null;
end;
/

exit;
__EOF__

chown oracle:oracle "$SQL_FILE"
[ -f "${TMP_FILE}" ] && rm -f "${TMP_FILE}"
[ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"
su -l oracle << __EOF__
export ORACLE_SID=${LONGMNR_DB}
$ORACLE_HOME/bin/sqlplus /nolog @${SQL_FILE} 1>${SQL_SPOOL_FILE} 2>&1
__EOF__
rc="$?"
log_info "${SQL_SPOOL_FILE}" "logfile"
if [ "$rc" -ne "0" ]
then
        log_info "$module failed at processing previously opened tx"
        [ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"
        return "1"
fi

log_info "$module expected ending of the routine"
return "$rc"

}

delete_applied_tx() {
local module="delete_applied_tx"
local rc

log_info "$module starting"
cat << __EOF__ > "$SQL_FILE"
whenever sqlerror exit 1
conn / as sysdba
set echo off
set head off
set feedback off
set newp none
set pagesize 0
set appinfo logmnr

Prompt xid which'll be deleted from active_transactions as commited (will be printed, if are):
select atx.OPENED_AT_SEQ||' '||atx.xid||' '||atx.sid||','||atx.serial#||' '||atx.status as active_tx_info
from SYSTEM.active_transactions atx
where atx.xid in (SELECT distinct t.xid AS xid
FROM SYS.COMMITED_ONLY_TX_DATA t
WHERE t.table_name='${TABLE_NAME}');

delete from SYSTEM.active_transactions atx
where atx.xid in (SELECT distinct t.xid AS xid
FROM SYS.COMMITED_ONLY_TX_DATA t
WHERE t.table_name='${TABLE_NAME}');
commit;

exit
__EOF__
chown oracle:oracle "$SQL_FILE"
[ -f "${TMP_FILE}" ] && rm -f "${TMP_FILE}"
[ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"
su -l oracle << __EOF__
export ORACLE_SID=${LONGMNR_DB}
$ORACLE_HOME/bin/sqlplus /nolog @${SQL_FILE} 1>${SQL_SPOOL_FILE} 2>&1
__EOF__
rc="$?"
log_info "${SQL_SPOOL_FILE}" "logfile"
if [ "$rc" -ne "0" ]
then
        log_info "$module failed at deleting commited and previously opened tx"
        [ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"
        return "1"
fi

log_info "$module expected ending of the routine"
return "$rc"
}

apply_long_commited_tx() {
local module="apply_long_commited_tx"
local v_seq="$1"
local v_seq_from=""
local rc

if [ -z "$v_seq" ]
then
        log_info "$module v_seq is empty"; return 1
fi

if ! [[ "$v_seq" =~ ^[0-9]+$ ]]
then
        log_info "$module v_seq: ${v_seq} is not an int-digit"; return 1
fi
v_seq=$(echo "$v_seq" | awk '{printf "%d", $0;}')

log_info "$module starting with v_seq: ${v_seq}"
log_info "$module try to get out oldest seq, which related to commited tx"
cat << __EOF__ > "$SQL_FILE"
whenever sqlerror exit 1
conn / as sysdba
set echo off
set head off
set feedback off
set newp none
set pagesize 0
set appinfo logmnr
select min(OPENED_AT_SEQ) as min_seq from system.active_transactions where upper(STATUS)='COMMIT';
exit
__EOF__
chown oracle:oracle "$SQL_FILE"
[ -f "${TMP_FILE}" ] && rm -f "${TMP_FILE}"
[ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"
su -l oracle << __EOF__
export ORACLE_SID=${LONGMNR_DB}
$ORACLE_HOME/bin/sqlplus -S /nolog @${SQL_FILE} 1>${SQL_SPOOL_FILE} 2>&1
__EOF__
rc="$?"
log_info "${SQL_SPOOL_FILE}" "logfile"
if [ "$rc" -ne "0" ]
then
        log_info "$module failed at deleting commited and previously opened tx"
        [ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"
        return "1"
fi
v_seq_from=$(cat ${SQL_SPOOL_FILE} | tr -d [:cntrl:] | tr -d [:space:]); [ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"

if [ -z "$v_seq_from" ]
then
        log_info "$module v_seq_from is empty"; return 0
fi

if ! [[ "$v_seq_from" =~ ^[0-9]+$ ]]
then
        log_info "$module v_seq_from: ${v_seq_from} is not an int-digit"; return 1
fi
v_seq_from=$(echo "$v_seq_from" | awk '{printf "%d", $0;}')
log_info "$module ok, seq#-range is: [${v_seq_from},${v_seq}]"
log_info "$module try to mine arclogs of that range"

cat << __EOF__ > "$SQL_FILE"
whenever sqlerror exit 1
conn / as sysdba
set echo off
set head off
set feedback off
set newp none
set pagesize 0
set linesize 1024
set appinfo logmnr
select FS_DIRECTORY||'/'||NAME as col from system.arclogs where seq between ${v_seq_from} and ${v_seq} order by seq asc;
exit
__EOF__
chown oracle:oracle "$SQL_FILE"
[ -f "${TMP_FILE}" ] && rm -f "${TMP_FILE}"
[ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"
su -l oracle << __EOF__
export ORACLE_SID=${LONGMNR_DB}
$ORACLE_HOME/bin/sqlplus -S /nolog @${SQL_FILE} 1>${TMP_FILE} 2>&1
__EOF__
rc="$?"
log_info "$TMP_FILE" "logfile"
if [ "$rc" -ne "0" ]
then
	[ -f "${TMP_FILE}" ] && rm -f "${TMP_FILE}"; [ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"; return 1
fi

cat << __EOF__ > "$SQL_FILE"
whenever sqlerror exit 1
conn / as sysdba
set echo on
set head off
set feedback off
set newp none
set pagesize 0
set verify off
set appinfo logmnr

ALTER SESSION SET NLS_DATE_FORMAT='DD-MON-YYYY HH24:MI:SS';
alter session set NLS_TIMESTAMP_FORMAT='yyyy-mm-dd hh:mi:ssxff';
__EOF__
rc="1"
while read line
do
	if [ "$rc" -eq "1" ]
	then
		echo "exec DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => '${line}', OPTIONS => DBMS_LOGMNR.NEW);">>"$SQL_FILE"
	else
		echo "exec DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => '${line}', OPTIONS => DBMS_LOGMNR.ADDFILE);">>"$SQL_FILE"
	fi
	rc=$((rc+1))
done < <(cat ${TMP_FILE})
local v_ext_table_name="COMMITED_ONLY_TX_DATA"
local v_option="DBMS_LOGMNR.COMMITTED_DATA_ONLY+DBMS_LOGMNR.NO_SQL_DELIMITER+DBMS_LOGMNR.NO_ROWID_IN_STMT"
cat << __EOF__ >> "$SQL_FILE"
exec DBMS_LOGMNR.START_LOGMNR(OPTIONS => ${v_option});
begin
execute immediate 'drop table SYS.${v_ext_table_name}';
exception when others then null;
end;
/

column dir_path new_value dir_path noprint
select directory_path as dir_path from sys.dba_directories where directory_name='${DIR_OBJECT}';
host rm -f &&dir_path/${v_ext_table_name}* 2>/dev/null

create table SYS.${v_ext_table_name}
ORGANIZATION EXTERNAL
(TYPE ORACLE_DATAPUMP DEFAULT DIRECTORY ${DIR_OBJECT} LOCATION  ('${v_ext_table_name}.dmp'))
as select * from V\$LOGMNR_CONTENTS;

EXECUTE DBMS_LOGMNR.END_LOGMNR();
exit;
__EOF__
log_info "$SQL_FILE" "logfile"
chown oracle:oracle "$SQL_FILE"
[ -f "${TMP_FILE}" ] && rm -f "${TMP_FILE}"; [ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"
su -l oracle << __EOF__
export ORACLE_SID=${LONGMNR_DB}
$ORACLE_HOME/bin/sqlplus /nolog @${SQL_FILE} 1>${SQL_SPOOL_FILE} 2>&1
__EOF__
rc="$?"
log_info "$SQL_SPOOL_FILE" "logfile"
if [ "$rc" -ne "0" ]
then
        log_info "$module failed at mining commited_tx data from range of arclogs"
        [ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"; [ -f "${TMP_FILE}" ] && rm -f "${TMP_FILE}"; [ -f "${SQL_FILE}" ] && rm -f "${SQL_FILE}"
        return "1"
fi
log_info "$module mining range of arclogs completed successfully"
log_info "$module let's extract&save to separate ext-table, from all amount of commited-tx metadata from given range of arclogs, metadata only about necessary long-tx;"
cat << __EOF__ > "$SQL_FILE"
whenever sqlerror exit 1
conn / as sysdba
set echo off
set head off
set feedback off
set newp none
set pagesize 0
set appinfo logmnr

begin
execute immediate 'drop table SYS.LONG_TX_DATA';
exception when others then null;
end;
/

column dir_path new_value dir_path noprint
select directory_path as dir_path from sys.dba_directories where directory_name='${DIR_OBJECT}';
host rm -f &&dir_path/LONG_TX_DATA* 2>/dev/null

create table SYS.LONG_TX_DATA
ORGANIZATION EXTERNAL
(TYPE ORACLE_DATAPUMP DEFAULT DIRECTORY ${DIR_OBJECT} LOCATION  ('LONG_TX_DATA.dmp'))
as select * from SYS.COMMITED_ONLY_TX_DATA t
where t.xid in (select t1.xid from system.active_transactions t1 where t1.status='COMMIT');

exit;
__EOF__
chown oracle:oracle "$SQL_FILE"
[ -f "${TMP_FILE}" ] && rm -f "${TMP_FILE}"; [ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"
su -l oracle << __EOF__
export ORACLE_SID=${LONGMNR_DB}
$ORACLE_HOME/bin/sqlplus -S /nolog @${SQL_FILE} 1>${SQL_SPOOL_FILE} 2>&1
__EOF__
rc="$?"
log_info "$SQL_SPOOL_FILE" "logfile"
if [ "$rc" -ne "0" ]
then
	log_info "$module can not create ext-table SYS.LONG_TX_DATA with necessary subset of data";
	[ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"; [ -f "${TMP_FILE}" ] && rm -f "${TMP_FILE}"; [ -f "${SQL_FILE}" ] && rm -f "${SQL_FILE}"
        return "1"
fi
log_info "$module ext-table SYS.LONG_TX_DATA with necessary subset of data created successfully"

log_info "$module try to get out xid(s) of commited-tx"
cat << __EOF__ > "$SQL_FILE"
whenever sqlerror exit 1
conn / as sysdba
set echo off
set head off
set feedback off
set newp none
set pagesize 0
set appinfo logmnr

select XID from system.active_transactions where status='COMMIT';

exit;
__EOF__
log_info "$SQL_FILE" "logfile"
chown oracle:oracle "$SQL_FILE"
[ -f "${TMP_FILE}" ] && rm -f "${TMP_FILE}"; [ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"
su -l oracle << __EOF__
export ORACLE_SID=${LONGMNR_DB}
$ORACLE_HOME/bin/sqlplus -S /nolog @${SQL_FILE} 1>${TMP_FILE} 2>&1
__EOF__
rc="$?"
log_info "$TMP_FILE" "logfile"
if [ "$rc" -ne "0" ]
then
        log_info "$module failed at mining commited_tx data from range of arclogs"
        [ -f "${SQL_SPOOL_FILE}" ] && rm -f "${SQL_SPOOL_FILE}"; [ -f "${TMP_FILE}" ] && rm -f "${TMP_FILE}"; [ -f "${SQL_FILE}" ] && rm -f "${SQL_FILE}"
        return "1"
fi

cat << __EOF__ > "$SQL_SPOOL_FILE"
whenever sqlerror exit 1
conn / as sysdba
set echo on
set head off
set feedback off
set newp none
set pagesize 0
set linesize 4000
set appinfo logmnr
__EOF__
chown oracle:oracle "$SQL_SPOOL_FILE"

while read line
do
	log_info "$module try to process metadata about tx: ${line}"
	cat << __EOF__ > "$SQL_FILE"
whenever sqlerror exit 1
conn / as sysdba
set echo off
set head off
set feedback off
set newp none
set pagesize 0
set linesize 4000
set appinfo logmnr

SELECT SYSTEM.rewrite_sql.process_statement(t.operation, t.table_name, t.sql_redo, 0)||';' AS sql_sttmnt
FROM SYS.LONG_TX_DATA t
WHERE t.table_name='${TABLE_NAME}' AND t.xid='${line}'
ORDER BY t.sequence# asc
;

exit
__EOF__
	chown oracle:oracle "$SQL_FILE"
	su -l oracle << __EOF__
export ORACLE_SID=${LONGMNR_DB}
$ORACLE_HOME/bin/sqlplus -S /nolog @${SQL_FILE} 1>>${SQL_SPOOL_FILE} 2>&1
__EOF__
	rc="$?"
	if [ "$?" -ne "0" ]
	then
		log_info "$module can not read sql-statments about tx: ${line}"; return 1
	fi
	echo "commit;" >> ${SQL_SPOOL_FILE}
done < <(cat ${TMP_FILE})
echo "exit;" >> ${SQL_SPOOL_FILE}
log_info "$SQL_SPOOL_FILE" "logfile"
su -l oracle << __EOF__
export ORACLE_SID=${LONGMNR_DB}
$ORACLE_HOME/bin/sqlplus -S /nolog @${SQL_SPOOL_FILE} 1>${SQL_FILE} 2>&1
__EOF__
rc="$?"
log_info "$SQL_FILE" "logfile"
if [ "$?" -ne "0" ]
then
	log_info "$module cannot apply long-tx to table"; return 1
fi
log_info "$module ok, commited-long tx was(were) applied to table successfully"
log_info "$module ok, try to delete metadata about commited-long tx;"

cat << __EOF__ > "$SQL_FILE"
whenever sqlerror exit 1
conn / as sysdba
set echo off
set head off
set feedback off
set newp none
set pagesize 0
set linesize 4000
set appinfo logmnr
delete from system.active_transactions where status='COMMIT';
commit;
exit
__EOF__
chown oracle:oracle "$SQL_FILE"
[ -f "$SQL_SPOOL_FILE" ] && rm -f "$SQL_SPOOL_FILE"
su -l oracle << __EOF__
export ORACLE_SID=${LONGMNR_DB}
$ORACLE_HOME/bin/sqlplus -S /nolog @${SQL_FILE} 1>${SQL_SPOOL_FILE} 2>&1
__EOF__
rc="$?"
if [ "$?" -ne "0" ]
then
        log_info "$module cannot delete metadata about commited-long tx"; return 1
fi
log_info "$module metadata about commited-long tx deleted successfully"
# delete ext-tables
# delete aux-files;
log_info "$module done"
return "0"
}

rotate_unnecessary_arclogs() {
local module="rotate_unnecessary_arclogs"
local v_seq="$1"
local v_x=""
local rc

if [ -z "$v_seq" ]
then
        log_info "$module v_seq is empty"; return 1
fi

if ! [[ "$v_seq" =~ ^[0-9]+$ ]]
then
        log_info "$module v_seq: ${v_seq} is not an int-digit"; return 1
fi
v_seq=$(echo "$v_seq" | awk '{printf "%d", $0;}')

log_info "$module start"
cat << __EOF__ > "$SQL_FILE"
whenever sqlerror exit 1
conn / as sysdba
set echo off
set head off
set feedback off
set newp none
set pagesize 0
set serveroutput on
set appinfo logmnr

declare
 x number;
begin
 select count(*) into x from system.active_transactions where status='START' and OPENED_AT_SEQ<${v_seq};
 if x=0 then
  dbms_output.put_line('none');
 else
  select min(OPENED_AT_SEQ) into x from system.active_transactions where status='START' and OPENED_AT_SEQ<${v_seq};
  dbms_output.put_line(x);
 end if;
end;
/

exit
__EOF__
chown oracle:oracle "$SQL_FILE"
[ -f "$SQL_SPOOL_FILE" ] && rm -f "$SQL_SPOOL_FILE"
su -l oracle << __EOF__
export ORACLE_SID=${LONGMNR_DB}
$ORACLE_HOME/bin/sqlplus -S /nolog @${SQL_FILE} 1>${SQL_SPOOL_FILE} 2>&1
__EOF__
rc="$?"
log_info "$SQL_SPOOL_FILE" "logfile"
if [ "$?" -ne "0" ]
then
        log_info "$module can not ask ${LONGMNR_DB} about minimal necessary arclog-seq#"
	[ -f "SQL_SPOOL_FILE" ] && rm -f "$SQL_SPOOL_FILE"; [ -f "$SQL_FILE" ] && rm -f "$SQL_FILE"; return 1
fi
v_seq=$(cat $SQL_SPOOL_FILE | tr -d [:cntrl:] | tr -d [:space:])
log_info "$module minimal necessary arclog-seq# obtained as: ${v_seq}"
if [ "$v_seq" == "none" ]
then
	log_info "$module well, we do not have any tx in 'START' status and launched in some previous seq#, more old than ${1}"
	v_seq="$1"
	log_info "$module so, just delete all arclogs with seq#<${v_seq}, that is: less of current seq#"
fi

log_info "$module ask in ${LONGMNR_DB} about full-path of arclogs-file which is(are) supposed to be deleted"
cat << __EOF__ > "$SQL_FILE"
whenever sqlerror exit 1
conn / as sysdba
set echo off
set head off
set feedback off
set newp none
set pagesize 0
set linesize 512
set appinfo logmnr
select FS_DIRECTORY||'/'||NAME as col from system.arclogs where seq < ${v_seq} order by seq asc;
exit
__EOF__
chown oracle:oracle "$SQL_FILE"
[ -f "$SQL_SPOOL_FILE" ] && rm -f "$SQL_SPOOL_FILE"; [ -f "$TMP_FILE" ] && rm -f "$TMP_FILE";
su -l oracle << __EOF__
export ORACLE_SID=${LONGMNR_DB}
$ORACLE_HOME/bin/sqlplus -S /nolog @${SQL_FILE} 1>${TMP_FILE} 2>&1
__EOF__
rc="$?"
log_info "$TMP_FILE" "logfile"
v_x="0"
while read line
do
	log_info "$module try to delete file: ${line}"
	if [ -f "$line" ]
	then
		rm -f "$line" 1>"$SQL_SPOOL_FILE" 2>&1
		#stat "$line" 1>"$SQL_SPOOL_FILE" 2>&1
		if [ "$?" -eq "0" ]
		then
			log_info "$module deleted successfully"
			v_x="1"
		else
			log_info "$module failed, see why:"
			log_info "$SQL_SPOOL_FILE" "logfile"
		fi
	#else
	#	log_info "$module ${line} is not a file, left untouched"
	fi
done < <(cat "$TMP_FILE")

if [ "$v_x" -eq "1" ]
then
	log_info "$module one, or more, arclogs was(were) erased; Crosscheck metadata about arclogs in ${LONGMNR_DB}"
	check_and_del_expired_arclog_registrations
fi

log_info "$module done"
return "0"
}

process_given_arclog() {
local module="process_given_arclog"
local v_arclog_seq="$1"
local rc

if [ -z "$v_arclog_seq" ]
then
        log_info "$module v_arclog_seq is empty"; return 1
fi

if ! [[ "$v_arclog_seq" =~ ^[0-9]+$ ]]
then
        log_info "$module v_arclog_seq: ${v_arclog_seq} is not an int-digit"; return 1
fi
v_arclog_seq=$(echo "$v_arclog_seq" | awk '{printf "%d", $0;}')


log_info "$module started with seq#: ${v_arclog_seq}"
# normal process routine of one, given arclog
# in a good way: we have to check: if directory-type db-object, with name in $DIR_OBJECT, really is and valid;
mine_arclog "$v_arclog_seq" "0" #"DBMS_LOGMNR.NO_SQL_DELIMITER+DBMS_LOGMNR.NO_ROWID_IN_STMT"
[ "$?" -ne "0" ] && {
	log_info "${module} mine_arclog for obtaining all-tx metadata failed;"; return "1"
}

# Log all opened at this seq#, and unclosed in this seq#, tx, in SYSTEM.active_transactions
log_tx_metadata_from_arclog "$v_arclog_seq"
[ "$?" -ne "0" ] && {
	log_info "${module} log_tx_metadata_from_arclog failed;"; return "1"
}

mine_arclog "$v_arclog_seq" "1" #"DBMS_LOGMNR.COMMITTED_DATA_ONLY+DBMS_LOGMNR.NO_SQL_DELIMITER+DBMS_LOGMNR.NO_ROWID_IN_STMT"
apply_dml_totable
[ "$?" -ne "0" ] && {
	log_info "${module} apply_dml_totable failed;"; return "1"
}

# becouse we're work with logmnr-data obtained in commited-only mode
delete_applied_tx
[ "$?" -ne "0" ] && {
	log_info "${module} delete_applied_tx failed;"; return "1"
}

# All-tx data already mined and saved in external tabe ALL_TX_DATA
# mine_arclog "$v_arclog_seq" "0" #"DBMS_LOGMNR.NO_SQL_DELIMITER+DBMS_LOGMNR.NO_ROWID_IN_STMT"
#[ "$?" -ne "0" ] && {
#       log_info "${module} mine_arclog for obtaining tx-metadata failed;"; myexit "1"
#}

check_prev_opened_tx "$v_arclog_seq"
[ "$?" -ne "0" ] && {
	log_info "${module} check_prev_opened_tx failed;"; return "1"
}

# apply_long_commited_tx: See if active_transactions, now, contains any COMMITED tx;
# That is: if some tx was(were) commited in current-processing seq#
# And it is(are): process it;
apply_long_commited_tx "$v_arclog_seq"
[ "$?" -ne "0" ] && {
	log_info "${module} apply_long_commited_tx failed;"; return "1"
}

# Update, in repository, seq# of last processed arclog to "$v_arclog_seq"
set_logmnr_param "last_processed_seq" "$v_arclog_seq"
[ "$?" -ne "0" ] && {
	log_info "${module} set_logmnr_param failed;"; return "1"
}

rotate_unnecessary_arclogs "$v_arclog_seq"
[ "$?" -ne "0" ] && {
	log_info "${module} rotate_unnecessary_arclogs failed;"; return "1"
}

log_info "$module done"
return "0"
}

