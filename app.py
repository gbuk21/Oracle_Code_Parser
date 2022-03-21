import flask
from flask import Flask
from flask import request,  jsonify, render_template, url_for , make_response, flash, redirect,send_file
import json,os,re
import requests

import db_ops as dops

import functools

app = Flask(__name__)

app.secret_key="Oracle Table metadata"

from flask_paginate import Pagination, get_page_parameter

from werkzeug.utils import secure_filename


conn=dops.connect()

@app.route('/')
@app.route('/index/')
def main_index():

    posts=dops.get_all_tables(conn)

    return render_template('index1.html',  posts=posts)
    
@app.route('/get_table_data/<string:table_name>', methods=['GET','POST'])
def get_table_data(table_name):
    column_names=dops.get_table_columns(conn,table_name,'')
    join_columns=dops.get_implicit_dependencies(conn,table_name)
    print(join_columns)
    insert_modules=dops.get_insert_clauses(conn,table_name,'')
    print(insert_modules)
    update_modules=dops.get_update_clauses(conn,table_name,'')
    delete_modules=dops.get_delete_clauses(conn,table_name,'')
    select_modules=dops.get_select_clauses(conn,table_name,'')
    create_modules=dops.get_create_clauses(conn,table_name,'')
    drop_modules=dops.get_drop_clauses(conn,table_name,'')
    where_clauses=dops.get_where_clauses(conn,table_name,'')
    return render_template('table_data1.html',  column_names=column_names,insert_modules=insert_modules,update_modules=update_modules, delete_modules=delete_modules,select_modules=select_modules,create_modules=create_modules,drop_modules=drop_modules, join_columns=join_columns,where_clauses=where_clauses)      
    
@app.route('/get_query_data/<string:query_id>', methods=['GET','POST'])
def get_query_data(query_id):
    query=dops.get_query_data(conn,query_id)
    print(query)
    return render_template('query_data1.html',  query=query)

@app.route('/get_module_data/<string:module>', methods=['GET','POST'])
def get_module_data(module):
    query=dops.get_module_data(conn,module)
    print(query)
    return render_template('query_data1.html',  query=query)
    
    
@app.route('/get_search_details/', methods=['GET','POST'])
def get_search_details():
    table_name=request.form.get('table_name')
    table_comment=request.form.get('table_comment')
    module=request.form.get('module')
    column_names=dops.get_table_columns(conn,table_name,table_comment)
    print(table_name)
    print(table_comment)
    print(module)
    join_columns=dops.get_implicit_dependencies(conn,table_name)
    print(join_columns)
    insert_modules=dops.get_insert_clauses(conn,table_name,module)
    print(insert_modules)
    update_modules=dops.get_update_clauses(conn,table_name,module)
    delete_modules=dops.get_delete_clauses(conn,table_name,module)
    select_modules=dops.get_select_clauses(conn,table_name,module)
    create_modules=dops.get_create_clauses(conn,table_name,module)
    drop_modules=dops.get_drop_clauses(conn,table_name,module)
    where_clauses=dops.get_where_clauses(conn,table_name,module)
    return render_template('table_data1.html',  column_names=column_names,insert_modules=insert_modules,update_modules=update_modules, delete_modules=delete_modules,select_modules=select_modules,create_modules=create_modules,drop_modules=drop_modules, join_columns=join_columns,where_clauses=where_clauses)      
