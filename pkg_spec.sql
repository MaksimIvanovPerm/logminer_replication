CREATE OR REPLACE PACKAGE SYSTEM.rewrite_sql
AS
--Aux
--p_return_remapped_name: 1 - do not return map-values, event if they are;
FUNCTION get_dict_name(p_nodict_name IN VARCHAR2, p_return_remapped_name NUMBER DEFAULT 0) RETURN VARCHAR2;
/*get out various column proprties; what to get - it's setted by p_what: name, type, nullable, length ...
*/
function get_col_property(p_segname IN VARCHAR2, p_nodict_name in varchar2, p_what in varchar2 default 'name',  p_dbg integer default 0) RETURN VARCHAR2;
--Main
FUNCTION process_insert_statement(p_segname IN VARCHAR2, p_statement IN varchar2, p_dbg integer default 0) RETURN VARCHAR2;
FUNCTION process_update_statement(p_segname IN VARCHAR2, p_statement IN varchar2, p_dbg integer default 0) RETURN VARCHAR2;
function process_delete_statement(p_segname IN VARCHAR2, p_statement IN varchar2, p_dbg integer default 0) RETURN VARCHAR2;
--p_operation: inser|update|delete; 
function process_statement(p_operation in varchar2, p_segname IN VARCHAR2, p_statement IN varchar2, p_dbg integer default 0) return varchar2;
END;
/

show err;
