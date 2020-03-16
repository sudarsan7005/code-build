#!/usr/bin/env python
# coding: utf-8

# In[3]:




# -*- coding: utf-8 -*-data
"""
Created on Tue Oct  8 19:21:38 2019

@author: saikiranu
"""

#!/usr/bin/env python
# coding: utf-8

import time
from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import calendar
import json
import psycopg2
pd.options.mode.chained_assignment = None 
import re

def source_db_connection():
    hostname = "petramai-uat-src.cu9bamjzuaw5.us-east-1.rds.amazonaws.com"
    portno = "5432"
    dbname = "petramai_uat_src"
    dbusername = "petramai_uat"
    dbpassword = "NMVg4h_g3T"
    conn = create_engine('postgresql://' + dbusername + ':' + dbpassword + '@' + hostname + ':' + portno + '/' + dbname)
    return conn

def target_db_connection():
    hostname = "petramai-uat-trgt.cu9bamjzuaw5.us-east-1.rds.amazonaws.com"
    portno = "5432"
    dbname = "petramai_uat_trgt"
    dbusername = "petramai_uat"
    dbpassword = "4AzM5q_FjE"
    conn = psycopg2.connect(host=hostname, port=portno, database=dbname, user=dbusername, password=dbpassword)
    return conn

def segmentorderlevel(i):
    if i == 'Enthusiasts':
        return 1
    elif i == 'Thrifty':
        return 3
    elif i == 'Core':
        return 4
    else:
        return 2

def getsourceelement(engine):
    data= pd.read_sql_query('select o.order_id,o."channel",c."customer_id",o."total", o."order_date",c."state" from petram.customer_details c inner join petram."order" o on o.customer_id=c.customer_id ', con =engine )
    employees = pd.read_sql_query('SELECT e."Employee_id" FROM petram.employee_details e ', con =engine )
    employees.rename(columns={'Employee_id':'customer_id'},inplace=True)
    employee_customer = data.merge(employees,on=['customer_id'], how = 'inner')
    final_data = data[(~data.customer_id.isin(employee_customer.customer_id))]
    final_data["Total_c_final"] = np.where(final_data['total'] <= 0, np.nan , final_data['total'])
    final_data.dropna(subset=['Total_c_final'],inplace=True)
    final_data.dropna(subset=['total'],inplace=True)
    final_data['order_date']= pd.to_datetime(final_data['order_date'])
    final_data.dropna(subset=['order_date'],inplace=True)
    final_data['year'] =  final_data['order_date'].dt.year
    final_data['quarter'] =  final_data['order_date'].dt.quarter
    final_data['month'] = final_data['order_date'].dt.strftime('%b')
    final_data['week'] =  final_data['order_date'].dt.weekofyear
    final_data['month_num'] =  final_data['order_date'].dt.month
    final_data = final_data[final_data['year'] > 2014]
    order_details= pd.read_sql_query('select ord."Order_ID",ord."Total_Computed" from  petram.order_details ord', con =engine )
    order_details.rename(columns={'Order_ID': 'order_id'}, inplace=True)
    order_details["Total_Computed_final"] = np.where(order_details['Total_Computed'] <= 0, np.nan , order_details['Total_Computed'])
    order_details.dropna(subset=['Total_Computed_final'],inplace=True)
    order_details.dropna(subset=['Total_Computed'],inplace=True)
    customer_order_details = pd.merge(final_data,order_details,how = 'inner',on='order_id')
    cust_traits= pd.read_sql_query('select cus."customer_id",cus."Traits_segment",cus."Micro_segments" from petram."Customer_segmentation_traits" cus', con =engine )
    customer_traits_order_details = pd.merge(customer_order_details,cust_traits,how = 'inner',on='customer_id')
    customer_traits_order_details['state'].fillna(value='others', inplace = True)
    customer_traits_order_details['state'] = customer_traits_order_details['state'].astype(str)
    customer_traits_order_details['state'] = customer_traits_order_details['state'].apply(lambda x: x.strip())
    customer_traits_order_details['state'] = customer_traits_order_details['state'].apply(lambda x: x.lower())
    customer_traits_order_details['state'] = customer_traits_order_details['state'].map(lambda x: re.sub(r'\W', '',x))
#     region= pd.read_csv('https://raw.githubusercontent.com/cphalpert/census-regions/master/us%20census%20bureau%20regions%20and%20divisions.csv')
    region= pd.read_sql_query('select * from petram.region_details',con =engine)
    region['State Code'] = region['State Code'].astype(str)
    region['State Code'] = region['State Code'].apply(lambda x: x.strip())
    region['State Code'] = region['State Code'].apply(lambda x: x.lower())
    region['State Code'] = region['State Code'].map(lambda x: re.sub(r'\W', '',x))
    region.rename(columns={'State Code':'state'}, inplace = True)
    df_final_data = customer_traits_order_details.merge(region[['state','Region']], on = 'state', how = 'left')
    df_final_data['Region'] = df_final_data['Region'].fillna("others")
    donught_year = df_final_data.groupby(['Traits_segment','Micro_segments','Region','year']).agg({'customer_id' : pd.Series.nunique,'Total_Computed_final': lambda x: x.sum(), 'order_id':  pd.Series.nunique}).reset_index()
    donught_year.rename(columns={'customer_id': 'No_of_customers', 'Total_Computed_final': 'Revenue', 'order_id': 'No_of_transaction'}, inplace=True)
    donught_quarter = df_final_data.groupby(['Traits_segment','Micro_segments','Region','year','quarter']).agg({'customer_id' : pd.Series.nunique,'Total_Computed_final': lambda x: x.sum(), 'order_id':  pd.Series.nunique}).reset_index()
    donught_quarter.rename(columns={'customer_id': 'No_of_customers', 'Total_Computed_final': 'Revenue', 'order_id': 'No_of_transaction'}, inplace=True)                 
    donught_month = df_final_data.groupby(['Traits_segment','Micro_segments','Region','year','month','month_num']).agg({'customer_id' : pd.Series.nunique,'Total_Computed_final': lambda x: x.sum(), 'order_id':  pd.Series.nunique}).reset_index()
    donught_month.rename(columns={'customer_id': 'No_of_customers', 'Total_Computed_final': 'Revenue', 'order_id': 'No_of_transaction'}, inplace=True)
    donught_week = df_final_data.groupby(['Traits_segment','Micro_segments','Region','year','week']).agg({'customer_id' : pd.Series.nunique,'Total_Computed_final': lambda x: x.sum(), 'order_id':  pd.Series.nunique}).reset_index()
    donught_week.rename(columns={'customer_id': 'No_of_customers', 'Total_Computed_final': 'Revenue', 'order_id': 'No_of_transaction'}, inplace=True)
    return donught_year,donught_quarter,donught_month,donught_week
    

def loadtargetelement1(_targetconnection,donught_year,projectid, projectsubmissionid,widgetid,widgetname,createdby):
    targetcursor = _targetconnection.cursor()
    print(donught_year.head(0))
    ''' Check if already Executed and set the active flag to N '''
    postgres_update_query = """ UPDATE petram.kycyearanalysis SET active = 'N' WHERE projectid = %s AND widgetid = %s AND widgetname = %s """
    record_to_update = (str(projectid),str(widgetid),str(widgetname))
    targetcursor.execute(postgres_update_query,record_to_update)

    for index,row in donught_year.iterrows():
        postgres_insert_query = """ INSERT INTO petram.kycyearanalysis(projectid, sno, widgetid, widgetname, traitssegment, traitssegment2, years, noofcustomers, revenue, nooftransaction, segmentlstorder,Region) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
        record_to_insert = (str(projectid),index,str(widgetid),str(widgetname),row['Traits_segment'],row['Micro_segments'], row['year'], row['No_of_customers'], row['Revenue'],row['No_of_transaction'],segmentorderlevel(str(row['Traits_segment'])),row['Region'])
        targetcursor.execute(postgres_insert_query,record_to_insert)
    _targetconnection.commit()
    print("year analysis over")
    targetcursor.close()
    

def loadtargetelement2(_targetconnection,donught_quarter,projectid, projectsubmissionid,widgetid,widgetname,createdby):
    targetcursor = _targetconnection.cursor()
    print(donught_quarter.head(0))
    ''' Check if already Executed and set the active flag to N '''
    postgres_update_query = """ UPDATE petram.kycquarteranalysis SET active = 'N' WHERE projectid = %s AND widgetid = %s AND widgetname = %s """
    record_to_update = (str(projectid),str(widgetid),str(widgetname))
    targetcursor.execute(postgres_update_query,record_to_update)

    for index,row in donught_quarter.iterrows():
        postgres_insert_query = """ INSERT INTO petram.kycquarteranalysis(projectid, sno, widgetid, widgetname, traitssegment, traitssegment2, years,quarters, noofcustomers, revenue, nooftransaction, segmentlstorder,Region) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
        record_to_insert = (str(projectid),index,str(widgetid),str(widgetname),row['Traits_segment'],row['Micro_segments'], row['year'],row['quarter'], row['No_of_customers'], row['Revenue'],row['No_of_transaction'],segmentorderlevel(str(row['Traits_segment'])),row['Region'])
        targetcursor.execute(postgres_insert_query,record_to_insert)
    _targetconnection.commit()
    print("quarter analysis over")
    targetcursor.close()
    
def loadtargetelement3(_targetconnection,donught_month,projectid, projectsubmissionid,widgetid,widgetname,createdby):
    targetcursor = _targetconnection.cursor()
    print(donught_month.head(0))
    ''' Check if already Executed and set the active flag to N '''
    postgres_update_query = """ UPDATE petram.kycmonthlyanalysis SET active = 'N' WHERE projectid = %s AND widgetid = %s AND widgetname = %s """
    record_to_update = (str(projectid),str(widgetid),str(widgetname))
    targetcursor.execute(postgres_update_query,record_to_update)

    for index,row in donught_month.iterrows():
        postgres_insert_query = """ INSERT INTO petram.kycmonthlyanalysis(projectid, sno, widgetid, widgetname, traitssegment, traitssegment2, years,months, noofcustomers, revenue, nooftransaction, segmentlstorder,Region,month_num) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
        record_to_insert = (str(projectid),index,str(widgetid),str(widgetname),row['Traits_segment'],row['Micro_segments'], row['year'],row['month'], row['No_of_customers'], row['Revenue'],row['No_of_transaction'],segmentorderlevel(str(row['Traits_segment'])),row['Region'],row['month_num'])
        targetcursor.execute(postgres_insert_query,record_to_insert)
    _targetconnection.commit()
    print("monthly analysis over")
    targetcursor.close()
    
def loadtargetelement4(_targetconnection,donught_week,projectid, projectsubmissionid,widgetid,widgetname,createdby):
    targetcursor = _targetconnection.cursor()
    print(donught_week.head(0))
    ''' Check if already Executed and set the active flag to N '''
    postgres_update_query = """ UPDATE petram.kycweeklyanalysis SET active = 'N' WHERE projectid = %s AND widgetid = %s AND widgetname = %s """
    record_to_update = (str(projectid),str(widgetid),str(widgetname))
    targetcursor.execute(postgres_update_query,record_to_update)

    for index,row in donught_week.iterrows():
        postgres_insert_query = """ INSERT INTO petram.kycweeklyanalysis(projectid, sno, widgetid, widgetname, traitssegment, traitssegment2, years,weeks, noofcustomers, revenue, nooftransaction, segmentlstorder,Region) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
        record_to_insert = (str(projectid),index,str(widgetid),str(widgetname),row['Traits_segment'],row['Micro_segments'], row['year'],row['week'], row['No_of_customers'], row['Revenue'],row['No_of_transaction'],segmentorderlevel(str(row['Traits_segment'])),row['Region'])
        targetcursor.execute(postgres_insert_query,record_to_insert)
    _targetconnection.commit()
    print("weekly analysis over")
    targetcursor.close()

def setsubmissionstatus(_targetconnection,projectid, projectsubmissionid,widgetid,status):
    _targetcursor = _targetconnection.cursor()
    status_update_query = """ update petram.tprojectwidget set status = %s where projectid = %s and widgetid = %s  """
    record_to_update = (str(status),str(projectid),str(widgetid))
    _targetcursor.execute(status_update_query, record_to_update)
    _targetconnection.commit()
    _targetcursor.close()

def process(projectid, projectsubmissionid,widgetid,widgetname,createdby):
    _sourceconnection = None
    
    # Set the Process to InProgress
    # Get the Source Connection
    _sourceconnection = source_db_connection()
    # Get Source Element
    donught_year,donught_quarter,donught_month,donught_week = getsourceelement(_sourceconnection)
    # Get the Target Connection
    _targetconnection = target_db_connection()
    # Load the data in Target Element
    loadtargetelement1(_targetconnection,donught_year,projectid, projectsubmissionid,widgetid,widgetname,createdby)
    loadtargetelement2(_targetconnection,donught_quarter,projectid, projectsubmissionid,widgetid,widgetname,createdby)
    loadtargetelement3(_targetconnection,donught_month,projectid, projectsubmissionid,widgetid,widgetname,createdby)
    loadtargetelement4(_targetconnection,donught_week,projectid, projectsubmissionid,widgetid,widgetname,createdby)
    # Set the Process to Completed
    setsubmissionstatus(_targetconnection,projectid, projectsubmissionid,widgetid,'COM')
    # Close all the Connection

def handler():
    #inputobject = json.loads(json.dumps(event))
    process("02254cee-ff7b-483b-aaba-61ba688c5caa", "668c21ba-ebdd-44ed-a5e9-570df29ba85d","SEGMENTATIONLIST","KYCDONUT","0dbf75db-1a46-4544-a70c-b301b0a70eb1")
    #process(inputobject['projectid'], inputobject['projectsubmissionid'],inputobject['widgetid'],inputobject['createdby'])
    return {"statusCode":200,"body":json.dumps({'status':'updated success'})}

handler()


# In[9]:


import time
from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import calendar
import json
import psycopg2
pd.options.mode.chained_assignment = None 
import re

# def source_db_connection():
hostname = "petramai-uat-src.cu9bamjzuaw5.us-east-1.rds.amazonaws.com"
portno = "5432"
dbname = "petramai_uat_src"
dbusername = "petramai_uat"
dbpassword = "NMVg4h_g3T"
engine = create_engine('postgresql://' + dbusername + ':' + dbpassword + '@' + hostname + ':' + portno + '/' + dbname)
#     return conn


def segmentorderlevel(i):
    if i == 'Enthusiasts':
        return 1
    elif i == 'Thrifty':
        return 3
    elif i == 'Core':
        return 4
    else:
        return 2


data= pd.read_sql_query('select o.order_id,o."channel",c."customer_id",o."total", o."order_date",c."state" from petram.customer_details c inner join petram."order" o on o.customer_id=c.customer_id ', con =engine )
employees = pd.read_sql_query('SELECT e."Employee_id" FROM petram.employee_details e ', con =engine )
employees.rename(columns={'Employee_id':'customer_id'},inplace=True)
employee_customer = data.merge(employees,on=['customer_id'], how = 'inner')
final_data = data[(~data.customer_id.isin(employee_customer.customer_id))]
final_data["Total_c_final"] = np.where(final_data['total'] <= 0, np.nan , final_data['total'])
final_data.dropna(subset=['Total_c_final'],inplace=True)
final_data.dropna(subset=['total'],inplace=True)
final_data['order_date']= pd.to_datetime(final_data['order_date'])
final_data.dropna(subset=['order_date'],inplace=True)
final_data['year'] =  final_data['order_date'].dt.year
final_data['quarter'] =  final_data['order_date'].dt.quarter
final_data['month'] = final_data['order_date'].dt.strftime('%b')
final_data['week'] =  final_data['order_date'].dt.weekofyear
final_data['month_num'] =  final_data['order_date'].dt.month
final_data = final_data[final_data['year'] > 2014]
order_details= pd.read_sql_query('select ord."Order_ID",ord."Total_Computed" from  petram.order_details ord', con =engine )
order_details.rename(columns={'Order_ID': 'order_id'}, inplace=True)
order_details["Total_Computed_final"] = np.where(order_details['Total_Computed'] <= 0, np.nan , order_details['Total_Computed'])
order_details.dropna(subset=['Total_Computed_final'],inplace=True)
order_details.dropna(subset=['Total_Computed'],inplace=True)
customer_order_details = pd.merge(final_data,order_details,how = 'inner',on='order_id')
cust_traits= pd.read_sql_query('select cus."customer_id",cus."Traits_segment",cus."Micro_segments" from petram."Customer_segmentation_traits" cus', con =engine )
customer_traits_order_details = pd.merge(customer_order_details,cust_traits,how = 'inner',on='customer_id')
customer_traits_order_details['state'].fillna(value='others', inplace = True)
customer_traits_order_details['state'] = customer_traits_order_details['state'].astype(str)
customer_traits_order_details['state'] = customer_traits_order_details['state'].apply(lambda x: x.strip())
customer_traits_order_details['state'] = customer_traits_order_details['state'].apply(lambda x: x.lower())
customer_traits_order_details['state'] = customer_traits_order_details['state'].map(lambda x: re.sub(r'\W', '',x))
#     region= pd.read_csv('https://raw.githubusercontent.com/cphalpert/census-regions/master/us%20census%20bureau%20regions%20and%20divisions.csv')
region= pd.read_sql_query('select * from petram.region_details',con =engine)
region['State Code'] = region['State Code'].astype(str)
region['State Code'] = region['State Code'].apply(lambda x: x.strip())
region['State Code'] = region['State Code'].apply(lambda x: x.lower())
region['State Code'] = region['State Code'].map(lambda x: re.sub(r'\W', '',x))
region.rename(columns={'State Code':'state'}, inplace = True)
df_final_data = customer_traits_order_details.merge(region[['state','Region']], on = 'state', how = 'left')
df_final_data['Region'] = df_final_data['Region'].fillna("others")
donught_year = df_final_data.groupby(['Traits_segment','Micro_segments','Region','year']).agg({'customer_id' : pd.Series.nunique,'Total_Computed_final': lambda x: x.sum(), 'order_id':  pd.Series.nunique}).reset_index()
#     return donught_year,donught_quarter,donught_month,donught_week


# In[12]:


len(df_final_data['customer_id'].unique())


# In[11]:


data.customer_id.count()


# In[ ]:




