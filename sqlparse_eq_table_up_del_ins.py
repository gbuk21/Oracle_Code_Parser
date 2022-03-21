import pandas as pd,sqlite3,time
#import spacy
import re,os,cx_Oracle
ignore_key_words=[' ',',','"','','NVL(','IN','AND','OR','WITH','TRIM(','BETWEEN','BY','PRIOR','START','CONNECT','DECODE','','INTO']
remove_key_words=[' ',',','"','','NVL','SUM','TRUNC','TO_DATE','+','>','-','<','/','*']
owd = os.getcwd()
try:
   os.chdir(r'/usr/lib/oracle/11.2/')# change to where oracle connector libraries exist

index_string='create index stomvt_i1 on stomvt(stmsite, stmcinl)'
index_split=re.split(r'[(), ]',index_string)
#print(index_split)
index_ignore_key_words=['','CREATE','INDEX']
columns_from_index=[]
table_from_index=[]
processed_sql_id=[]
on_started=False
table_identified=False
for i in index_split:
    if i.strip().upper() not in index_ignore_key_words:
        #print(i)
        x=i.strip().upper()
        if 'ON' in x:
            on_started=True
        if    on_started and 'ON' not in x:
            table_from_index.append(x)
            table_identified=True
        if    table_identified and 'ON' not in x:
            columns_from_index.append(x)
#print(table_from_index[0])
conn_str = '#Oracle_uid/pwd_and_connection_info)'
conn = cx_Oracle.connect(conn_str)
c = conn.cursor()

conn1 = sqlite3.connect('one_sqlparse.db') # change to where you would like to create sqlite3 database
c1 = conn1.cursor()
if 1==1:
   try:
       table1="create table implicit_dependencies (left_Table text, left_column text, right_table text, right_column text)"
       conn1.execute(table1)
   except:
       pass

   sql_text1  = "select com.owner, com.table_name, com.column_name, data_type, data_length, data_precision, data_scale, nullable, comments  from dba_tab_columns col, dba_col_comments com where com.owner=col.owner and com.table_name=col.table_name and com.column_name =col.column_name"
   #print(sql_text1)
   df_dba_tab_columns=pd.read_sql(sql_text1, con=conn)
   table1="dba_tab_columns"
   df_dba_tab_columns.to_sql(table1,  conn1,   if_exists='replace')

   try:
       c1.execute("create index dba_tab_columns_idx01 on dba_tab_columns(table_name, column_name)")
   except:
        pass

   sql_text1  = "select owner, table_name, comments from dba_tab_comments"
   #print(sql_text1)
   df_dba_tab_columns=pd.read_sql(sql_text1, con=conn)
   table1="dba_tab_comments"
   df_dba_tab_columns.to_sql(table1,  conn1,   if_exists='replace')

   try:
       c1.execute("create index dba_tab_comments_idx01 on dba_tab_comments(table_name, comments)")
   except:
        pass

   
   sql_text1  = "select   distinct sql_text, sql_id, module, service from gv$sql where upper(sql_texT) not like 'BEGIN%' and upper(sql_text) not like 'DECLARE%' and  upper(sql_text) not like 'LOCK%' and upper(sql_text) not like 'CREATE%' and sql_text not like '%chartorowid%' and sql_text not like '%xmlindex_sel_idx_tbl%' and sql_text not like '%OPT_DYN_SAMP%' and sql_text not like '%SYS.PARTOBJ%' and upper(sql_text) like '%STOMVT%' "
   df_dba_tab_columns=pd.read_sql(sql_text1, con=conn)
   table1="dg_gv_sql_snapshot"
   df_dba_tab_columns.to_sql(table1,  conn1,   if_exists='replace')

   try:
       c1.execute("create index dg_gv_sql_snapshot_idx01 on dg_gv_sql_snapshot(sql_text)")
   except:
        pass


print('before')
processed_s=[]
update_hold=[]
table_hold=[]
col_hold_full=[]
col_hold_temp=[]
insert_hold=[]
delete_hold=[]
create_hold=[]
drop_hold=[]
update_hold1=[]
select_hold=[]
update1_hold_parent=[]
insert_hold_parent=[]
delete_hold_parent=[]
create_hold_parent=[]
drop_hold_parent=[]
select_hold_parent=[]
where_columns_hold=[]
where_columns_hold_full=[]
module_string="select dbms_lob.substr( sql_fulltext, 9999, 1 )   from gv$sql where upper(sql_texT) not like 'BEGIN%' and upper(sql_text) not like 'DECLARE%' and  upper(sql_text) not like 'LOCK%' and upper(sql_text) not like 'CREATE%' and rownum <2"
module_string="select      sql_text, sql_id,module,service    from dg_gv_sql_snapshot where upper(sql_texT) not like 'BEGIN%' and upper(sql_text) not like 'DECLARE%' and  upper(sql_text) not like 'LOCK%' and upper(sql_text) not like 'CREATE%' and sql_text not like '%chartorowid%' and sql_text not like '%xmlindex_sel_idx_tbl%' and sql_text not like '%OPT_DYN_SAMP%' and sql_text not like '%SYS.PARTOBJ%'  "
final_result=[]
for df in pd.read_sql(module_string, con=conn1, chunksize=1):

 #print(df)
#with open ('C://temp/Sqlparse/sql1.sql') as f:
#    List_1 =f.read()
 for index, row1 in df.iterrows():
 #List_1=row1[0].read().upper()
  update_hold=[]
  table_hold=[]
  col_hold_full=[]
  col_hold_temp=[]
  List_1=row1['SQL_TEXT']
  sql_id=row1['SQL_ID']
  module=row1['MODULE']
  service=row1['SERVICE']
 #if table_from_index[0] not in List_1.upper():
 #       continue
  if row1['SQL_ID']  in processed_sql_id:
        continue
  else:
    processed_sql_id.append(row1['SQL_ID'])
  #print(List_1)
  splitted=re.split(r'(-|,|=|"|\s)\s*',List_1)
  #print(splitted)
  if 'TBL$OR$IDX$PART$NUM' in List_1 or 'DBMS_STATS' in List_1 or 'ROWIDTOCHARROWID' in  List_1:
    continue
  keep_vars=[]
  keep_from_tables=[]
  keep_update_tables=[]
  keep_insert_tables=[]
  keep_delete_tables=[]
  keep_create_tables=[]
  keep_drop_tables=[]
  for i in splitted:
    if i.strip().upper() not in ignore_key_words:
       keep_vars.append(i.strip().upper())
       #print(keep_vars)
       if len(keep_vars)==3:
          if keep_vars[1]=="=":
             #print("left - "+ str (keep_vars[0])+" right - "+ str (keep_vars[2]))
             col_hold_temp.append(str (keep_vars[0]))
             col_hold_temp.append(str (keep_vars[2]))
             col_hold_full.append(col_hold_temp)
             col_hold_temp=[]

          keep_vars.pop(0)

       keep_update_tables.append(i.strip().upper())
       #print(keep_update_tables)
       if len(keep_update_tables)>1:
          if keep_update_tables[0]=="UPDATE":
             #print("update table - "+ str (keep_update_tables[1]))
             update_hold.append(str (keep_update_tables[1]))
             update_hold1.append(str (keep_update_tables[1]))
             update_hold1.append(sql_id)
             update_hold1.append(module)
             update_hold1.append(service)
             update1_hold_parent.append(update_hold1)
             update_hold1=[]

          keep_update_tables.pop(0)
       keep_from_tables.append(i.strip().upper())
       #print('keep_from_tables')
       #print(keep_from_tables)

       keep_insert_tables.append(i.strip().upper())
       if len(keep_insert_tables)==2:
          if keep_insert_tables[0]=="INSERT" :
             #print("update table - "+ str (keep_insert_tables[1]))
             insert_hold.append(str (keep_insert_tables[1]))
             print(insert_hold)
             insert_hold.append(sql_id)
             insert_hold.append(module)

             insert_hold.append(service)
             insert_hold_parent.append(insert_hold)
             insert_hold=[]

          keep_insert_tables.pop(0)

       keep_delete_tables.append(i.strip().upper())
       #print(keep_delete_tables)
       if len(keep_delete_tables)==3:
          if keep_delete_tables[0]=="DELETE" and keep_delete_tables[1]=="FROM":
             #print("update table - "+ str (keep_delete_tables[1]))
             delete_hold.append(str (keep_delete_tables[2]))
             print(delete_hold)
             delete_hold.append(sql_id)
             delete_hold.append(module)
             delete_hold.append(service)
             delete_hold_parent.append(delete_hold)
             delete_hold=[]

          keep_delete_tables.pop(0)

       keep_create_tables.append(i.strip().upper())
       #print(keep_create_tables)
       if len(keep_create_tables)==3:
          if keep_create_tables[0]=="CREATE" and keep_create_tables[1]=="TABLE":
             #print("update table - "+ str (keep_create_tables[1]))
             create_hold.append(str (keep_create_tables[2]))
             #print(create_hold)
             create_hold.append(sql_id)
             create_hold.append(module)
             create_hold.append(service)
             create_hold_parent.append(create_hold)
             create_hold=[]
          keep_create_tables.pop(0)

       keep_drop_tables.append(i.strip().upper())
       #print(keep_drop_tables)
       if len(keep_drop_tables)==3:
          if keep_drop_tables[0]=="DROP" and keep_drop_tables[1]=="TABLE":
             #print("update table - "+ str (keep_drop_tables[1]))
             drop_hold.append(str (keep_drop_tables[2]))
             drop_hold.append(sql_id)
             drop_hold.append(module)
             drop_hold.append(service)
             drop_hold_parent.append(drop_hold)
             drop_hold=[]
             #print(drop_hold)

          keep_drop_tables.pop(0)


       if keep_from_tables[0] =='FROM' or keep_from_tables[0] =='JOIN':
          #print(keep_from_tables)
          if i.strip().upper()=='(' or i.strip().upper()=='(SELECT'or i.strip().upper()==')' or i.strip().upper()=='WHERE' or i.strip().upper()==';' or i.strip().upper()=='ON':
             keep_from_tables.pop(0)
          else:
             if len(keep_from_tables)>0:
                if keep_from_tables[0]=="FROM" and len(keep_from_tables)>1:
                   #print("from table - "+ str (keep_from_tables[1]))
                   table_hold.append(str (keep_from_tables[1]))

                   sql_text= "select distinct table_name from dba_Tab_columns where  1=1 and "

                   sql_text  =sql_text+ "(table_name = '"+str(keep_from_tables[1])+"' ) "
                   df2=pd.read_sql(sql_text, con=conn1)
                   if len(df2)>0:
                      select_hold.append(str (keep_from_tables[1]))

                   #select_hold.append(str (keep_from_tables[1]))

                   keep_from_tables.pop(-1)
                   select_hold.append(sql_id)
                   select_hold.append(module)
                   select_hold.append(service)
                   select_hold_parent.append(select_hold)
                   select_hold=[]
       else:
             keep_from_tables.pop(0)
 table_hold1=[]
 j=0
 sql_text= "select distinct table_name, column_name from dba_Tab_columns where  1=1 and "
 for i  in update_hold:
     table_hold1.append(i)
 for i  in table_hold:
     table_hold1.append(i)
 #print('table_hold1')
 #print(table_hold1)
 if len(table_hold1)>0:
    j=0
    for i in table_hold1:
        if len(table_hold1)==1:
           sql_text  =sql_text+ "(table_name = '"+str(i)+"' ) "
        elif j==0:
           sql_text=sql_text+ "( "

           sql_text  =sql_text+ "table_name = '"+str(i)+"' OR "

        elif j<len(table_hold1)-1:
           sql_text  =sql_text+ "table_name = '"+str(i)+"' OR "
        else:
           sql_text  =sql_text+ "table_name = '"+str(i)+"'"
           sql_text =sql_text+" )"
        j=j+1
 k=0
 i1_processed=[]

 if len(col_hold_full)>0:
    for i1 in col_hold_full:
      try:
        i2=[]
        if "." in i1[0]:
           i2.append(str(i1[0]).split(0)[1])
        else:
           i2.append(str(i1[0]))
        if "." in i1[1]:
           i2.append(str(i1[1]).split(0)[1])
        else:
           i2.append(str(i1[1]))
        if i2 not in i1_processed:
           i1_processed.append(i2)
        else:

            continue
        i1=i2
        if "'" not in str(i1[0]) and "'" not in str(i1[1]) and ":" not in str(i1[0]) and ":" not in str(i1[1]):
            sql_text1  = sql_text +" and ('%'||column_name||'%' like '%"+str(i1[0])+"%')  order by 1,2"
            #print(sql_text1)
            df2=pd.read_sql(sql_text1, con=conn1)
            try:
               #print(df2['TABLE_NAME'][0])
               #print(df2['COLUMN_NAME'][0])
               where_columns_hold.append(df2['TABLE_NAME'][0])
               where_columns_hold.append(df2['COLUMN_NAME'][0])
               where_columns_hold.append(sql_id)
               where_columns_hold.append(module)
               where_columns_hold.append(service)
               where_columns_hold_full.append(where_columns_hold)
               where_columns_hold=[]

               sql_text1  = sql_text+" and ('%'||column_name||'%' like '%"+str(i1[1])+"%') order by 1,2"
               #print(sql_text1)
               df21=pd.read_sql(sql_text1, con=conn)
               #print(df21)
               #print(df21['TABLE_NAME'][0])
               #print(df21['COLUMN_NAME'][0])
               where_columns_hold.append(df21['TABLE_NAME'][0])
               where_columns_hold.append(df21['COLUMN_NAME'][0])
               where_columns_hold.append(sql_id)
               where_columns_hold.append(module)
               where_columns_hold.append(service)
               where_columns_hold_full.append(where_columns_hold)
               where_columns_hold=[]
               interim_result=[]
               interim_result.append(df2['TABLE_NAME'][0])
               interim_result.append(df2['COLUMN_NAME'][0])
               interim_result.append(df21['TABLE_NAME'][0])
               interim_result.append(df21['COLUMN_NAME'][0])
               #print(interim_result)
               if interim_result not in final_result:
                  final_result.append(interim_result)

            except:
               continue
      except:
         continue
print(final_result)
for s1 in final_result:
 sql_insert="insert into implicit_dependencies values('replace1','replace2', 'replace3', 'replace4')"
 if 1==1:
 #try:
    sql_insert=sql_insert.replace('replace1',s1[0]).replace('replace2',s1[1]).replace('replace3',s1[2]).replace('replace4',s1[3])
    print(sql_insert)
    c1.execute(sql_insert)
 #except:
 #   continue
 conn1.commit()

df_u = pd.DataFrame (update1_hold_parent, columns = ['table_name', 'sql_id','module','service'])
table1="UPDATE_REFERENCE_DATA"
df_u.to_sql(table1,  conn1,   if_exists='replace')
df_i = pd.DataFrame (insert_hold_parent, columns = ['table_name', 'sql_id','module','service'])
table1="INSERT_REFERENCE_DATA"

df_i.to_sql(table1,  conn1,   if_exists='replace')
df_d = pd.DataFrame (delete_hold_parent, columns = ['table_name', 'sql_id','module','service'])
table1="DELETE_REFERENCE_DATA"
df_d.to_sql(table1,  conn1,   if_exists='replace')
df_p = pd.DataFrame (drop_hold_parent, columns = ['table_name', 'sql_id','module','service'])
table1="DROP_REFERENCE_DATA"
df_p.to_sql(table1,  conn1,   if_exists='replace')

df_c = pd.DataFrame (create_hold_parent, columns = ['table_name', 'sql_id','module','service'])
table1="CREATE_REFERENCE_DATA"
df_c.to_sql(table1,  conn1,   if_exists='replace')

df_s = pd.DataFrame (select_hold_parent, columns = ['table_name', 'sql_id','module','service'])
table1="SELECT_REFERENCE_DATA"
df_s.to_sql(table1,  conn1,   if_exists='replace')

df_s = pd.DataFrame (where_columns_hold_full, columns = ['table_name', 'column_name', 'sql_id','module','service'])
table1="WHERE_CLAUSE_REFERENCE_DATA"
df_s.to_sql(table1,  conn1,   if_exists='replace')

