CREATE OR REPLACE PACKAGE BODY SYSTEM.rewrite_sql
AS
-- Private resources 
v_do_debug INTEGER := 0;

TYPE My_Rec IS RECORD (colname varchar2(30),
dict_name varchar2(30),
type_name varchar2(30),
colvalue_hex varchar2(128),
varchar2_value varchar2(128),
number_value number,
date_value date);
TYPE My_Rec_Type is table of My_Rec index by binary_integer;


PROCEDURE DEBUG(p1 IN varchar2)
IS
BEGIN
 IF v_do_debug=1 THEN
  Dbms_Output.put_line(To_Char(SYSDATE,'yyyy.mm.dd:hh24:mi:ss')||' '||p1);
 END IF; 
END;
-- Main --------------------------------------------------------------------------------------------------
FUNCTION get_dict_name(p_nodict_name IN VARCHAR2, p_return_remapped_name NUMBER DEFAULT 0) RETURN VARCHAR2
IS
 e_empty_parameter EXCEPTION;
 v_module          VARCHAR2(30) := 'get_dict_name';
 v_record          SYSTEM.logmnr_tabconf%ROWTYPE;
 v_retval          VARCHAR2(62) := '';          
BEGIN
 -- first of all: let's check input parameters; 
 IF p_nodict_name IS NULL THEN
  RAISE e_empty_parameter;
 END IF;

 SELECT t.* INTO v_record
 FROM SYSTEM.logmnr_tabconf t WHERE t.no_dict_name=p_nodict_name;

 IF v_record.map_owner IS NOT NULL AND p_return_remapped_name=0 THEN
  v_retval := v_record.map_owner;
 ELSE
  v_retval := v_record.orig_owner;
 END IF;  

 IF v_record.map_name IS NOT NULL AND p_return_remapped_name=0 THEN
  v_retval := v_retval||'.'||v_record.map_name;
 ELSE
  v_retval := v_retval||'.'||v_record.orig_name;
 END IF;  

 RETURN v_retval;
EXCEPTION 
 WHEN e_empty_parameter THEN Raise_Application_Error(-20000, v_module||': one or all parameter is|are empty');  
 WHEN no_data_found then Raise_Application_Error(-20001, v_module||': can not find data about '||p_nodict_name||' in SYSTEM.logmnr_tabconf');
END; -- get_dict_name
--set serveroutput on
--exec dbms_output.put_line( '>'||SYSTEM.rewrite_sql.get_dict_name('OBJ# 56770') );

function get_col_property(p_segname IN VARCHAR2, p_nodict_name in varchar2, p_what in varchar2 default 'name', p_dbg integer default 0) RETURN VARCHAR2
is
 e_empty_parameter EXCEPTION;
 e_wrong_what      exception;
 v_module          VARCHAR2(30) := 'get_col_name';
 v_tabrec          SYSTEM.logmnr_tabconf%ROWTYPE;
 v_colrec          SYSTEM.logmnr_coldefconf%ROWTYPE;
begin
 -- first of all: let's check input parameters;
 IF p_nodict_name IS NULL or p_nodict_name is null or p_what is null THEN
  RAISE e_empty_parameter;
 END IF;

 SELECT t.* INTO v_tabrec
 FROM SYSTEM.logmnr_tabconf t WHERE t.no_dict_name=p_segname;

 select t.* into v_colrec
 from SYSTEM.logmnr_coldefconf t where NO_DICT_NAME=p_nodict_name and PARENT_ID=v_tabrec.id;

 case lower(p_what)
  when 'name' then return v_colrec.NAME;
  when 'type' then return lower(v_colrec.TYPE);
  when 'data_length' then return v_colrec.DATA_LENGTH;
  when 'data_precision' then return v_colrec.DATA_PRECISION;
  when 'nullable' then return v_colrec.NULLABLE;
  else raise e_wrong_what;
 end case;

exception
 when e_wrong_what then Raise_Application_Error(-20002, v_module||': unexpected value of p_what formal parameter');
 when e_empty_parameter then Raise_Application_Error(-20000, v_module||': one or all parameter is|are empty');
 WHEN no_data_found then Raise_Application_Error(-20001, v_module||': can not find data about table '||p_segname||' and(or) about column '||p_nodict_name);
end; --get_col_property

function process_statement(p_operation in varchar2, p_segname IN VARCHAR2, p_statement IN varchar2, p_dbg integer default 0) return varchar2
is
 e_empty_parameter      EXCEPTION;
 e_unexpected_optype    exception;
 v_module               VARCHAR2(30) := 'process_statement';
begin
 if p_dbg > 0 then
  v_do_debug:=1;
 else
  v_do_debug:=0;
 end if;

 case upper(p_operation)
  when 'INSERT' then return process_insert_statement(p_segname, p_statement, p_dbg);
  when 'DELETE' then return process_delete_statement(p_segname, p_statement, p_dbg);
  when 'SELECT_FOR_UPDATE' then return '-- SELECT_FOR_UPDATE unsupported';
  when 'UPDATE' then return process_update_statement(p_segname, p_statement, p_dbg);
  else raise e_unexpected_optype;
 end case;
exception
 when e_unexpected_optype then Raise_Application_Error(-20000, v_module||': p_operation value is: '''||p_operation||'''; supported values are: INSERT|DELETE|UPDATE; SELECT_FOR_UPDATE is formaly supported');
 when e_empty_parameter then Raise_Application_Error(-20000, v_module||': one or all parameter is|are empty'); 
end; --process_statement

function process_delete_statement(p_segname IN VARCHAR2, p_statement IN varchar2, p_dbg integer default 0) RETURN VARCHAR2
is
 e_empty_parameter      EXCEPTION;
 e_unexpected_type_name exception;
 v_module               VARCHAR2(30) := 'process_delete_statement';
 v_object_name          varchar2(62);
 v_pattern              varchar2(128) := '"COL\s+\d+"\s+\=\s+HEXTORAW\(''[^'']+''\)';
 v_item                 varchar2(4000);
 v_retval               varchar2(4000):='';
 k                      binary_integer;
 v_structure            My_Rec_Type;
begin
 if p_dbg > 0 then
  v_do_debug:=1;
 else
  v_do_debug:=0;
 end if;
 -- first of all: let's check input parameters;
 IF p_segname IS NULL OR p_statement IS NULL THEN
  RAISE e_empty_parameter;
 END IF;
 DEBUG(v_module||' launched with p_segname: '||p_segname||'; p_statement: '||p_statement);

 --well; try to get out originale table owner and name, and map owner and name;
 v_object_name := get_dict_name(p_segname);
 DEBUG(v_module||' objname is: '||v_object_name);

 v_retval:=regexp_replace(p_statement,'delete\s+from\s+"[^"]+"\."[^"]+"\s+', 'delete from '||v_object_name||' ', 1, 1, 'i');
 k:=1;
 for j in (select regexp_substr(p_statement, v_pattern, 1, level) as item from dual
connect by regexp_substr(p_statement, v_pattern, 1, level) is not null)
 loop
  v_item:=regexp_substr(j.item,'^([^\=]+)\s+\=',1,1,'i',1);
  v_item:=replace(v_item,'"','');
  v_structure(k).colname:=v_item;
  v_item:=regexp_substr(j.item,'\=\s+(.*)',1,1,'i',1);
  v_item:=regexp_substr(v_item,'HEXTORAW\(''([^'']+)''\)',1,1,'i',1);
  v_structure(k).colvalue_hex:=v_item;
  DEBUG(v_module||' ('||k||') '||j.item||' '||v_structure(k).colname||' '||v_structure(k).colvalue_hex );

  v_structure(k).dict_name := get_col_property(p_segname, v_structure(k).colname, 'name');
  v_structure(k).type_name:=get_col_property(p_segname, v_structure(k).colname, 'type');
  case v_structure(k).type_name
   when 'number' then DBMS_STATS.CONVERT_RAW_VALUE( hextoraw(v_structure(k).colvalue_hex), v_structure(k).number_value);
   when 'date' then DBMS_STATS.CONVERT_RAW_VALUE( hextoraw(v_structure(k).colvalue_hex), v_structure(k).date_value);
   when 'varchar2' then DBMS_STATS.CONVERT_RAW_VALUE( hextoraw(v_structure(k).colvalue_hex), v_structure(k).varchar2_value);
   else raise e_unexpected_type_name;
  end case;

  v_item:='';
  case v_structure(k).type_name
   when 'number' then v_item:=v_structure(k).dict_name||'='||v_structure(k).number_value;
   when 'date' then v_item:=v_structure(k).dict_name||'=to_date('''||to_char(v_structure(k).date_value,'yyyy.mm.dd:hh24:mi:ss')||''',''yyyy.mm.dd:hh24:mi:ss'')';
   when 'varchar2' then v_item:=v_structure(k).dict_name||'='''||v_structure(k).varchar2_value||'''';
   else raise e_unexpected_type_name;
  end case;
  DEBUG(v_module||'('||k||') '||v_item);
  v_retval:=replace(v_retval, j.item, v_item);

  k:=k+1;
 end loop;

 return v_retval;
exception
 when e_unexpected_type_name then Raise_Application_Error(-20000, v_module||': unexpected dict type_name of some column of '||p_segname);
 when e_empty_parameter then Raise_Application_Error(-20000, v_module||': one or all parameter is|are empty');
end; --process_delete_statement

FUNCTION process_update_statement(p_segname IN VARCHAR2, p_statement IN varchar2, p_dbg integer default 0) RETURN VARCHAR2
is
 e_empty_parameter      EXCEPTION;
 e_unexpected_type_name exception;
 v_module               VARCHAR2(30) := 'process_update_statement';
 v_object_name          varchar2(62);
 v_pattern              varchar2(128) := '"COL\s+\d+"\s+\=\s+HEXTORAW\(''[^'']+''\)';
 v_item                 varchar2(4000);
 v_retval               varchar2(4000):='';
 k                      binary_integer;
 v_structure            My_Rec_Type;
begin
 if p_dbg > 0 then
  v_do_debug:=1;
 else
  v_do_debug:=0;
 end if;
 -- first of all: let's check input parameters;
 IF p_segname IS NULL OR p_statement IS NULL THEN
  RAISE e_empty_parameter;
 END IF;
 DEBUG(v_module||' launched with p_segname: '||p_segname||'; p_statement: '||p_statement);

 --well; try to get out originale table owner and name, and map owner and name;
 v_object_name := get_dict_name(p_segname);
 DEBUG(v_module||' objname is: '||v_object_name);

 v_retval:=regexp_replace(p_statement,'update\s+"[^"]+"\."[^"]+"\s+', 'update '||v_object_name||' ', 1, 1, 'i');
 k:=1;
 for j in (select regexp_substr(p_statement, v_pattern, 1, level) as item from dual
connect by regexp_substr(p_statement, v_pattern, 1, level) is not null)
 loop
  v_item:=regexp_substr(j.item,'^([^\=]+)\s+\=',1,1,'i',1);
  v_item:=replace(v_item,'"',''); 
  v_structure(k).colname:=v_item;
  v_item:=regexp_substr(j.item,'\=\s+(.*)',1,1,'i',1);
  v_item:=regexp_substr(v_item,'HEXTORAW\(''([^'']+)''\)',1,1,'i',1);
  v_structure(k).colvalue_hex:=v_item;
  DEBUG(v_module||' ('||k||') '||j.item||' '||v_structure(k).colname||' '||v_structure(k).colvalue_hex );

  v_structure(k).dict_name := get_col_property(p_segname, v_structure(k).colname, 'name');
  v_structure(k).type_name:=get_col_property(p_segname, v_structure(k).colname, 'type');
  case v_structure(k).type_name
   when 'number' then DBMS_STATS.CONVERT_RAW_VALUE( hextoraw(v_structure(k).colvalue_hex), v_structure(k).number_value);
   when 'date' then DBMS_STATS.CONVERT_RAW_VALUE( hextoraw(v_structure(k).colvalue_hex), v_structure(k).date_value);
   when 'varchar2' then DBMS_STATS.CONVERT_RAW_VALUE( hextoraw(v_structure(k).colvalue_hex), v_structure(k).varchar2_value);
   else raise e_unexpected_type_name;
  end case;
  
  v_item:='';
  case v_structure(k).type_name
   when 'number' then v_item:=v_structure(k).dict_name||'='||v_structure(k).number_value;
   when 'date' then v_item:=v_structure(k).dict_name||'=to_date('''||to_char(v_structure(k).date_value,'yyyy.mm.dd:hh24:mi:ss')||''',''yyyy.mm.dd:hh24:mi:ss'')';
   when 'varchar2' then v_item:=v_structure(k).dict_name||'='''||v_structure(k).varchar2_value||'''';
   else raise e_unexpected_type_name;
  end case;
  DEBUG(v_module||'('||k||') '||v_item);
  --v_retval:=regexp_replace(v_retval, v_pattern, v_item, 1, k, 'i'); 
  v_retval:=replace(v_retval, j.item, v_item);
  k:=k+1;
 end loop;
 
 v_pattern:='"COL\s+\d+"';
 v_item:=v_retval;
 k:=1;
 for j in (select regexp_substr(v_retval, v_pattern, 1, level) as item from dual
connect by regexp_substr(v_retval, v_pattern, 1, level) is not null)
 loop
  if j.item is not null then
   DEBUG(v_module||'('||k||') '||j.item);
   v_structure(k).dict_name := get_col_property(p_segname, replace(j.item,'"',''), 'name');
   v_item:=replace(v_item, j.item, v_structure(k).dict_name);
   k:=k+1;
  end if;
 end loop;
 v_retval:=v_item;

 return v_retval;
exception
 when e_unexpected_type_name then Raise_Application_Error(-20000, v_module||': unexpected dict type_name of some column of '||p_segname);
 when e_empty_parameter then Raise_Application_Error(-20000, v_module||': one or all parameter is|are empty');
end; --process_update_statement

FUNCTION process_insert_statement(p_segname IN VARCHAR2, p_statement IN varchar2, p_dbg integer default 0) RETURN VARCHAR2
IS
 e_empty_parameter EXCEPTION;
 e_empty_col_list  exception;
 e_empty_val_list  exception;
 e_unexpected_type_name exception;
 v_module          VARCHAR2(30) := 'process_insert_statement';
 v_object_name     varchar2(62);
 v_items           varchar2(512);
 v_values          varchar2(1024):='';

 v_structure       My_Rec_Type;
 i                 binary_integer;
 k                 integer;
BEGIN

 if p_dbg > 0 then
  v_do_debug:=1; 
 else
  v_do_debug:=0;
 end if;
 -- first of all: let's check input parameters; 
 IF p_segname IS NULL OR p_statement IS NULL THEN
  RAISE e_empty_parameter;
 END IF;
 DEBUG(v_module||' launched with p_segname: '||p_segname||'; p_statement: '||p_statement);

 --well; try to get out originale table owner and name, and map owner and name; 
 v_object_name := get_dict_name(p_segname);
 DEBUG(v_module||' objname is: '||v_object_name);

 --try to getout column(s)-list
 v_items := regexp_substr(p_statement,'\(([^\)]+)\)',1,1,'i',1);
 if v_items is null then
  raise e_empty_col_list;
 end if;
 DEBUG(v_module||' column list is: '||v_items);
 i:=1;
 for j in (select regexp_substr(v_items,'[^,]+', 1, level) as colname from dual
connect by regexp_substr(v_items, '[^,]+', 1, level) is not null)
 loop
  v_structure(i).colname:=replace(j.colname,'"','');
  DEBUG(v_module||' '||v_structure(i).colname);
  i:=i+1;
 end loop;
 DEBUG(v_module||': '||v_structure.count);

 v_items := regexp_substr(p_statement,'values\s+\((.*)',1,1,'i',1); 
 v_items := regexp_substr(v_items,'(.*)\)',1,1,'i',1);
 if v_items is null then
  raise e_empty_val_list;
 end if;
 --DEBUG(v_module||' values list is: '||v_items);
 i:=1;
 for j in (select regexp_substr(v_items,'[^,]+', 1, level) as colvalue from dual
connect by regexp_substr(v_items, '[^,]+', 1, level) is not null)
 loop
  --DEBUG(v_module||' '||regexp_substr(j.colvalue,'HEXTORAW\(''([^'']+)''\)',1,1,'i',1) );
  v_structure(i).colvalue_hex:=regexp_substr(j.colvalue,'HEXTORAW\(''([^'']+)''\)',1,1,'i',1);
  i:=i+1;
 end loop;
 
 /*well; right now we have to loop throught collection v_structure 
   and get out name, type of each column, and, according to type - cast value or string representation of value*/
 i:=v_structure.first;
 while i is not null
 loop
  v_structure(i).dict_name := get_col_property(p_segname, v_structure(i).colname, 'name');
  v_structure(i).type_name:=get_col_property(p_segname, v_structure(i).colname, 'type');
  case v_structure(i).type_name
   when 'number' then DBMS_STATS.CONVERT_RAW_VALUE( hextoraw(v_structure(i).colvalue_hex), v_structure(i).number_value);
   when 'date' then DBMS_STATS.CONVERT_RAW_VALUE( hextoraw(v_structure(i).colvalue_hex), v_structure(i).date_value);
   when 'varchar2' then DBMS_STATS.CONVERT_RAW_VALUE( hextoraw(v_structure(i).colvalue_hex), v_structure(i).varchar2_value);
   else raise e_unexpected_type_name;
  end case;
  debug(v_module||'i: '||i||' '||v_structure(i).colname);
  debug(v_module||'i: '||i||' '||v_structure(i).dict_name);
  debug(v_module||'i: '||i||' '||v_structure(i).type_name);
  debug(v_module||'i: '||i||'n '||v_structure(i).number_value);
  debug(v_module||'i: '||i||'v '||v_structure(i).varchar2_value);
  debug(v_module||'i: '||i||'d '||to_char(v_structure(i).date_value,'yyyy.mm.dd:hh24:mi:ss'));
  debug(v_module||'i: '||i||'hex: '||v_structure(i).colvalue_hex);
  i:=v_structure.next(i);
 end loop;

 k:=1;
 i:=v_structure.first;
 while i is not null
 loop
  if k=1 then
   v_items:=v_structure(i).dict_name;
   case v_structure(i).type_name
    when 'number' then v_values:=v_structure(i).number_value;
    when 'varchar2' then v_values:=v_structure(i).varchar2_value;
    when 'date' then v_values:='to_date('||to_char(v_structure(i).date_value,'yyyy.mm.dd:hh24:mi:ss')||',''yyyy.mm.dd:hh24:mi:ss'')';
   end case;
  else
   v_items:=v_items||','||v_structure(i).dict_name;
   case v_structure(i).type_name
    when 'number' then v_values:=v_values||','||v_structure(i).number_value;
    when 'varchar2' then v_values:=v_values||','''||v_structure(i).varchar2_value||'''';
    when 'date' then v_values:=v_values||','||'to_date('''||to_char(v_structure(i).date_value,'yyyy.mm.dd:hh24:mi:ss')||''',''yyyy.mm.dd:hh24:mi:ss'')';
   end case;
  end if;
  i:=v_structure.next(i);
  k:=k+1;
 end loop;

 RETURN 'insert into '||v_object_name||'('||v_items||') values ('||v_values||')';
EXCEPTION
 when e_unexpected_type_name then Raise_Application_Error(-20000, v_module||': unexpected dict type_name of some column of '||p_segname);
 WHEN e_empty_val_list then Raise_Application_Error(-20002, v_module||': can not get out values-list from: '||substr(p_statement,-1,128));
 WHEN e_empty_col_list then Raise_Application_Error(-20001, v_module||': can not get out column-list from: '||substr(p_statement,1,128));
 WHEN e_empty_parameter THEN Raise_Application_Error(-20000, v_module||': one or all parameter is|are empty');  
END; --process_insert_statement
END;
/

show err;

SELECT OWNER||' '||NAME||' '||TYPE||' '||SEQUENCE||' '||LINE||' '||POSITION||' '||TEXT||' '||ATTRIBUTE||' '||MESSAGE_NUMBER as error_info
FROM sys.dba_errors
WHERE owner='SYSTEM' AND name='REWRITE_SQL'
ORDER BY SEQUENCE desc;

