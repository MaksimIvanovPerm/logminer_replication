# logminer_replication
prof-concept for one oracle table replication by logminer

It seems tome that it is possible: to replicate oracle table (or, even, tables)  with help of oracle-logminer;
So, here, a bunch of scripts, with help of this I managed to replicate one table from one, source oracle-db to another, dest-db;

The main idea is: source-db - works in arclog-mode, with force-mode;
Somehow (incrond+rsync, for example) transfer of arclogs of source-db should be setted up, to dest-db;
And there, at dest-db, the arclogs will be processed by logminer;

Logminer, at dest-db site, extracts information about transactions, about setted oracle-table;
This information, it's, in essential: dml-statements in rigth sequence, which should be, in right time, closed by commit or rollback statement;
In theory it is quite simple and straight: you just mine&filter transaction-data, about a table you're interested in, from arclogs, one-by-one;
That is: you can just mine arclogs in commited-mode (with `DBMS_LOGMNR.COMMITTED_DATA_ONLY` option);
And as soon as you obtain next commited sequence of dml-statements - you just apply it to according-table in the dest-db;

However, in practice there are two things against you and both are plcaed at dest-db site;
First - dictionary metadata, from the source-dn; 
With help of which logmainer are able to provide you with human-readable and ready-to-use text of dml-statements, mined from arclogs;
To be more specific: lack of the dictionary metatda;
It's possible to provide logminer with that dict-metadata;
But it turns out that it should be permanent provisioning: each new logminer-session should be provided with actual dict-metatda;
Well it's seems to me that it's possible to organize some kind of own dictionary and own process to build dml-statements without hex-formatted data and id-based notifiocation of object;

Second: long transactions;
Long - means here that transatcion desctiption was started in one arclog, and will be ended in some another arclog, sometime later;
If we just mine arclogs, one-by-one, without organizing logminer-sessions in which all arclog-sequence, which covers longs-transaction(s) are described - we will not obtain data about long-ransactions at all;
At least, and only if arclog was mined wthout `DBMS_LOGMNR.COMMITTED_DATA_ONLY` option we will be able to notice: that some transaction was|were closedm by commit, rollback;
But ther is a problem: it hard to say, at the given moment: when some transaction (which already is long or which will be long) starts and when it will be closed;
And we can not wait too long and mine too huge bunch of arclogs in hope that this bunch large enough for describing all long transactions, of their are;

So we have to process, next arclog when it arrives from the source db;
And  we have to do something some king of tracking: detecting and processing metadata about transactions which were opened, and this arclog (that is: in the given sequence);
But were not closed in the arclog;
We have to notice and register such tx and, when it were opened in what sequence;
Let me call that sequence: start sequence;
We have to retain this and some more recent arclogs, for mining from their metadata about thouse long-tx;
And when, later, when, at least, we will recieve, from the source-db, arclog, in which thouse tx will be closed - we notice sequence of that arclog;
Again let me call this sequence as close-sequence;
So when we notice what we recieved arclogs, by data in which some, already opened long-tx: are closed - we know (and have) all sequence of arclogs, which fully-describe that long-tx;
And, if tx-closing - it's `COMMIT` - we're able to mine all this sequence of arclog (with `DBMS_LOGMNR.COMMITTED_DATA_ONLY` option) and obtain all dml-sequence of that long-tx, and, of couse - apply it to replicated-version of table at dest-db;
Of couse if tx-closing was `ROLLBACK` - we don't have to do all this resource-consuming routine, we just forgot about this canceled long-tx;
And of couse, when long-tx were finished and processed, we have to correclty and carefully process our metadata about it and see: which arclog we have to still reain and which we one have to retin nay more and delete it;

First problem, with dictionary and binary-based dml-text, a resolve with help of my own "dictionary" and plsql-package - `SYSTEM.rewrite_sql`;
My, so-called dictionsty it's just two tables where I place information what does mean the given `OBJ# NNNNN` which I interested in, in logminer-metadata obtained from some arclog;
```
--drop table SYSTEM.logmnr_tabconf
CREATE TABLE SYSTEM.logmnr_tabconf(id NUMBER,
no_dict_name VARCHAR2(30) NOT NULL,
orig_owner VARCHAR2(30) NOT NULL,
orig_name VARCHAR2(30) NOT NULL,
map_owner VARCHAR2(30),
map_name VARCHAR2(30)) TABLESPACE excellent;

INSERT INTO SYSTEM.logmnr_tabconf(id, no_dict_name, orig_owner, orig_name, map_owner, map_name)
VALUES (1, 'OBJ# 56770', 'SOMEONE', 'TESTTAB', NULL, null);
COMMIT;

SELECT * FROM SYSTEM.logmnr_tabconf t;
```
```
--drop table SYSTEM.logmnr_coldefconf
CREATE TABLE SYSTEM.logmnr_coldefconf(id NUMBER,
parent_id NUMBER,
no_dict_name VARCHAR2(30),
name VARCHAR2(30),
type VARCHAR2(30),
data_length NUMBER,
data_precision NUMBER,
nullable CHAR(1));

INSERT INTO SYSTEM.logmnr_coldefconf(id, parent_id, no_dict_name, name, type, data_length, data_precision, nullable)
VALUES (1, 1, 'COL 1', 'COL1', 'NUMBER', 22, NULL, 'N');
INSERT INTO SYSTEM.logmnr_coldefconf(id, parent_id, no_dict_name, name, type, data_length, data_precision, nullable)
VALUES (2, 1, 'COL 2', 'COL2', 'NUMBER', 22, NULL, 'N');
INSERT INTO SYSTEM.logmnr_coldefconf(id, parent_id, no_dict_name, name, type, data_length, data_precision, nullable)
VALUES (3, 1, 'COL 3', 'COL3', 'NUMBER', 22, NULL, 'N');
INSERT INTO SYSTEM.logmnr_coldefconf(id, parent_id, no_dict_name, name, type, data_length, data_precision, nullable)
VALUES (4, 1, 'COL 4', 'COL4', 'DATE', 7, NULL, 'N');
INSERT INTO SYSTEM.logmnr_coldefconf(id, parent_id, no_dict_name, name, type, data_length, data_precision, nullable)
VALUES (5, 1, 'COL 5', 'COL5', 'DATE', 7, NULL, 'Y');
COMMIT;
SELECT * from SYSTEM.logmnr_coldefconf;
```

Spec|body-code of `SYSTEM.rewrite_sql` package are placed in 'pkg_body.sql', `pkg_spec.sql` files;
So, current version of this package, and as consequence: of all this project: restricted by working with scalar oracle-datatypes only - number|date|varchar;

The main mechanic of processing arclogs: it's shell script `logminer-trigger`
By my intent and code-time design it firest by incron-daemon, at dest-db server, when next arclog-file arrives from the source-db site;
So first and one argument of `logminer-trigger` - is full name of the next arclog-file, `$@/$#` in incrond-terms;
`logminer-trigger` uses and sources two files: `logminer.conf` and `logminer_lib.sh`


