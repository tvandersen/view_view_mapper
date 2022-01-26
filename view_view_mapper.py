# -*- coding: utf-8 -*- 
""" Created on Tue Jan 25 17:22:22 2022 @author: tbär """

# =============================================================================
#  maps and prints (to json string and DB) the dependencies of each views 
#  to other tables/views, as well as other view's dependencies of that
# =============================================================================
import sqlite3
import json

f_db = input("give f of DB to check VIEW depends on...\n>:").replace('"','')

con = sqlite3.connect(f_db)
cur = con.cursor()

# get type names...
type_by_name = dict(
    cur.execute('''
    select name, type 
    from sqlite_master
    where type!='index'
    ''').fetchall()
)

# get SQL
sql_by_view = dict(
    cur.execute('''
    select name, sql 
    from sqlite_master
    where type='view'
    ''').fetchall()
)
    
# cleanup...
for k,v in sql_by_view.items():
    # v=sql_by_view['rms_by_ept_SHARE']
    sql_by_view[k] = (
        v.replace('\r','')
         .replace('\t','    ')
         .replace('"','')
         .replace("'",'')
    )
    print(v)
         

# sort out views from tables...
views, tables = (
    sorted(k for k,v in type_by_name.items() if v==typ)
    for typ in ['view','table']
)

# find reading from tables/views
tbls_by_view = {}
for v in views:
    qr = sql_by_view[v]
    tbls_by_view[v] = {}
    tbls_by_view[v]['reads from'] = [
        ln.split('from ')[1].split(' ')[0] 
        for ln in qr.split('\n')
        if ('from ' in ln) * (not 'from (' in ln) 
    ]

# finally, add what other views read from view
for v in views:
    tbls_by_view[v]['read by'] = [
        # vv = 'CO2_per_el_heat_by_fuel'
        vv for vv in views 
        if v in tbls_by_view[vv]['reads from']    
    ]
ui = input(json.dumps(tbls_by_view, indent=4) + '\n\n dump in DB? [no action=yes!]\n:>')

if not ui:
    vals = ',\n    '.join(
        sorted(
            str((k0,k1, i, x)) 
            for k0 in tbls_by_view 
            for k1,v1 in tbls_by_view[k0].items() 
            for i,x in enumerate(v1)
        )
    )

    qrs = f'''
    drop table if exists VIEW_VIEW
    ;
    create table if not exists VIEW_VIEW (
        view0, depend, ix, view1
    )
    ;
    insert into VIEW_VIEW
    values
        {vals}
    '''
    for qr in qrs.split(';'):    
        print(qr)
        cur.execute(qr)
        con.commit()
        
con.close()
