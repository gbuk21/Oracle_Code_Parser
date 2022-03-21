import sqlite3

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d
    
def connect():
   conn = sqlite3.connect('one_sqlparse.db',check_same_thread=False)
   return conn

def close(conn):
   conn.close()


def get_where_clauses(conn , table_name,module ):
    conn.row_factory = dict_factory
    query="select table_name, column_name, sql_id, module, service from WHERE_CLAUSE_REFERENCE_DATA where 1==1 "
    if table_name.strip()!='':    
       query=query+" and upper(table_name)='"+table_name.upper()+"'"
    if module.strip()!='':    
       query=query+" and upper(module) like '%"+module.upper()+"%' "
    query=query+"  limit 1"
    print(query)   
    cursor = conn.execute(query)  
    return(cursor.fetchall())
    
def get_select_clauses(conn, table_name,module  ):
    conn.row_factory = dict_factory
    query="select table_name,  sql_id, module, service from SELECT_REFERENCE_DATA where 1==1 "
    if table_name.strip()!='':    
       query=query+" and upper(table_name)='"+table_name.upper()+"'"
    if module.strip()!='':    
       query=query+" and upper(module) like  '%"+module.upper()+"%'"
    query=query+"  limit 1"
    print(query)   
    
    cursor = conn.execute(query)  
    return(cursor.fetchall())

def get_create_clauses(conn, table_name,module  ):
    conn.row_factory = dict_factory
    query="select table_name,  sql_id, module, service from CREATE_REFERENCE_DATA where 1==1 "
    if table_name.strip()!='':    
       query=query+" and upper(table_name)='"+table_name.upper()+"'"
    if module.strip()!='':    
       query=query+" and upper(module)like '%"+module.upper()+"%' "
    query=query+"  limit 1"
    print(query)   
    cursor = conn.execute(query)  
    return(cursor.fetchall())
    
def get_drop_clauses(conn, table_name,module  ):
    conn.row_factory = dict_factory
    query="select table_name,  sql_id, module, service from DROP_REFERENCE_DATA where 1==1 "
    if table_name.strip()!='':    
       query=query+" and upper(table_name)='"+table_name.upper()+"'"
    if module.strip()!='':    
       query=query+" and upper(module) like'%"+module.upper()+"%' "
    query=query+"  limit 1"
    print(query)   
    cursor = conn.execute(query)  
    return(cursor.fetchall())

def get_update_clauses(conn, table_name,module  ):
    conn.row_factory = dict_factory
    query="select table_name,  sql_id, module, service from UPDATE_REFERENCE_DATA where 1==1 "
    if table_name.strip()!='':    
       query=query+" and upper(table_name)='"+table_name.upper()+"'"
    if module.strip()!='':    
       query=query+" and upper(module) like '%"+module.upper()+"%' "
    query=query+"  limit 1"
    print(query)   
    cursor = conn.execute(query)  
    return(cursor.fetchall()) 
    
def get_delete_clauses(conn, table_name,module  ):
    conn.row_factory = dict_factory
 
    query="select table_name,  sql_id, module, service from DELETE_REFERENCE_DATA where 1==1 "
    if table_name.strip()!='':    
       query=query+" and upper(table_name)='"+table_name.upper()+"'"
    if module.strip()!='':    
       query=query+" and upper(module) like '%"+module.upper()+"%'  "
    query=query+"  limit 1"
    print(query)   
    cursor = conn.execute(query)  
    return(cursor.fetchall()) 
    
def get_insert_clauses(conn, table_name,module  ):
    conn.row_factory = dict_factory

    query="select table_name,  sql_id, module, service from INSERT_REFERENCE_DATA where 1==1 "
    if table_name.strip()!='':    
       query=query+" and upper(table_name)='"+table_name.upper()+"'"
    if module.strip()!='':    
       query=query+" and upper(module)like '%"+module.upper()+"%'  "
    query=query+"  limit 1"
    print(query)   
    cursor = conn.execute(query)  
    return(cursor.fetchall()) 

def get_implicit_dependencies(conn, table_name  ):
    conn.row_factory = dict_factory
    print("select left_table, left_column, right_table, right_column from implicit_dependencies where upper(left_table)='"+table_name.upper()+"' or upper(right_table)='"+table_name.upper()+"'")
    cursor = conn.execute("select left_table, left_column, right_table, right_column from implicit_dependencies where upper(left_table)='"+table_name.upper()+"' or upper(right_table)='"+table_name.upper()+"' limit 1" )
    return(cursor.fetchall())  
    
def get_all_tables(conn  ):
    conn.row_factory = dict_factory
    print("select distinct table_name from dba_tab_columns")
    cursor = conn.execute("select distinct table_name from dba_tab_columns  limit 1")    
    return(cursor.fetchall())

def get_table_columns(conn, table_name, table_comment ):
    conn.row_factory = dict_factory
    
    query="select distinct owner, table_name, comments from dba_tab_columns where 1==1 "
    if table_name.strip()!='':    
       query=query+" and upper(table_name)='"+table_name.upper()+"'"
    if table_comment.strip()!='':    
       query=query+" and upper(comments) like '%"+table_comment.upper()+"%'  "
    query=query+"  limit 1"
    print(query)   
    cursor = conn.execute(query)  
    return(cursor.fetchall())
    
def get_query_data(conn, query_id ):
    conn.row_factory = dict_factory
    print("select distinct * from dg_gv_sql_snapshot where upper(SQL_ID)='"+query_id.upper()+"'")  
    cursor = conn.execute("select distinct * from dg_gv_sql_snapshot where upper(SQL_ID)='"+query_id.upper()+"'  limit 1")     
    return(cursor.fetchall()) 
    
def get_module_data(conn, module ):
    conn.row_factory = dict_factory
    print("select distinct * from dg_gv_sql_snapshot where upper(module)='"+module.upper()+"'")  
    cursor = conn.execute("select distinct * from dg_gv_sql_snapshot where upper(module)='"+module.upper()+"'  limit 1")     
    return(cursor.fetchall())
