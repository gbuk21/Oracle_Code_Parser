# Oracle Code Parser

Oracle query parser
This article will show how to parse Oracle queries and report all the modules and queries that utilize a given table. It will also how to report implicit references by parsing the where clauses in a query. The intent is to create a reference database for Oracle SQL code analysis.

Some of the Python libraries used are pandas,sqlite3, os,re and cx_Oracle.
To reduce the impact on Oracle database, Oracle tables like dba_tab_comments, dba_tab_col_comments, dba_tab_columns, gv$sql will be copied over to SQLITE3.

Run the python program sqlparse_eq_table_up_del_ins.py that does Oracle SQL parsing and reports back various reference data like modules that does insert, update, delete, create, drop, select and also all the tables that join with a given table.

Python3 sqlparse_eq_table_up_del_ins.py.

There is a flask website component that can be used to display the contents from SQL parsing that also allows searching table, module or table comments.

Search for the table of interest or using comments on a table or a module to fetch the details.

Sample data that will be displayed.
 
 1) Table columns and comments
 2) Joins to other tables/columns
 3) Modules that insert
 4) Modules that delete
 5) Modules that update
 6) Modules that select
 7) Modules that create
 8) Modules that drop
 9) Columns in the where clauses
 
 There are links that can be used to pull a specific query using query id and all the queries using a module id.
 
 Set module on PL/SQL programs using DBMS_APPLICATION_INFO.SET_MODULE

 


 


