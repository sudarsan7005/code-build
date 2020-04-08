#!/usr/bin/env python
# coding: utf-8

import time
from datetime import datetime
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sqlalchemy import create_engine
import calendar
import json
import psycopg2

def source_db_connection():
    hostname = "petramsourcedb.chpqwttbstha.us-east-1.rds.amazonaws.com"
    portno = "5432"
    dbname = "postgres"
    dbusername = "postgres"
    dbpassword = "sUca4_frad"
    engine = create_engine('postgresql://'+dbusername+':'+dbpassword+'@'+hostname+':'+portno+'/'+dbname)
    #conn = create_engine('postgresql://'+dbusername+':'+dbpassword+'@'+hostname+':'+portno+'/'+dbname)
    return engine

 

def target_db_connection():
    hostname = "petramtargetdb.chpqwttbstha.us-east-1.rds.amazonaws.com"
    portno = "5432"
    dbname = "petramtargetdb"
    dbusername = "postgres"
    dbpassword = "sUca4_frad"
    engine = psycopg2.connect(host=hostname,port=portno,database=dbname, user=dbusername, password=dbpassword)
    return engine



def RScore(x,p,d):
    if x <= d[p][0.25]:
        return 1
    elif x <= d[p][0.50]:
        return 2
    elif x <= d[p][0.75]: 
        return 3
    else:
        return 4

def FMScore(x,p,d):
    if x <= d[p][0.25]:
        return 4
    elif x <= d[p][0.50]:
        return 3
    elif x <= d[p][0.75]: 
        return 2
    else:
        return 1
    
def segment_me2(rfmr_df):
    if (rfmr_df['r_quartile'] == 1) and (rfmr_df['f_quartile'] == 1) and (rfmr_df['m_quartile'] == 1):
        return 'Best Customer'
    elif (rfmr_df['f_quartile'] == 1):
        return 'Loyal Customers'
    elif (rfmr_df['m_quartile'] == 1):
        return 'Big Spenders'
    elif ((rfmr_df['r_quartile'] == 1) or (rfmr_df['r_quartile'] == 2)) and ((rfmr_df['f_quartile'] == 3) or (rfmr_df['f_quartile'] == 4)) and ((rfmr_df['m_quartile'] == 1) or (rfmr_df['m_quartile'] == 2)):
        return 'New High Spenders'
    elif ((rfmr_df['r_quartile'] == 1) or (rfmr_df['r_quartile'] == 2)) and ((rfmr_df['f_quartile'] == 3) or (rfmr_df['f_quartile'] == 4)) and ((rfmr_df['m_quartile'] == 4) or (rfmr_df['m_quartile'] == 3)):
        return 'New Low Spenders'
    elif ((rfmr_df['f_quartile'] == 2) or (rfmr_df['f_quartile'] == 3)) and ((rfmr_df['m_quartile'] == 3) or (rfmr_df['m_quartile'] == 4)):
        return 'Low Loyal Customers'
    elif ((rfmr_df['f_quartile'] == 3) or (rfmr_df['f_quartile'] == 4)) and ((rfmr_df['m_quartile'] == 1) or (rfmr_df['m_quartile'] == 2)):
        return 'Rare High Spenders'
    elif ((rfmr_df['r_quartile'] == 3) or (rfmr_df['r_quartile'] == 3)) and (rfmr_df['f_quartile'] == 4) and (rfmr_df['m_quartile'] == 3):
        return 'At Risk Customers'
    elif ((rfmr_df['r_quartile'] == 4) or (rfmr_df['r_quartile'] == 3)) and (rfmr_df['f_quartile'] == 4) and (rfmr_df['m_quartile'] == 3):
        return 'Churned Customers'
    elif ((rfmr_df['r_quartile'] == 4) or (rfmr_df['r_quartile'] == 3)) and ((rfmr_df['f_quartile'] == 3) or (rfmr_df['f_quartile'] == 4)) and ((rfmr_df['m_quartile'] == 3) or (rfmr_df['m_quartile'] == 4)):
        return 'Churned Cheap Customers'
    else:
        return 'Failed'
    
def DNA_VPV(rfmr_df):
    if (rfmr_df['Monetary/Frequency'] <= 80):
        return 'Frugal'
    elif ((rfmr_df['Monetary/Frequency'] > 80) & (rfmr_df['Monetary/Frequency'] <= 300)):
        return 'Prudent'
    elif ((rfmr_df['Monetary/Frequency'] > 300) & (rfmr_df['Monetary/Frequency'] <= 1000)):
        return 'Premium'
    elif ((rfmr_df['Monetary/Frequency'] > 1000) & (rfmr_df['Monetary/Frequency'] <= 2000)):
        return 'Splurge'
    else:
        return 'Lavish'

def DNA_Frequency(rfmr_df):
    if (rfmr_df['Frequency'] == 1):
        return 'Oneoff'
    elif ((rfmr_df['Frequency'] > 1) & (rfmr_df['Frequency'] <= 4)):
        return 'Occasional'
    elif (rfmr_df['Frequency'] > 4):
        return 'Regular'


def DNA_range(rfmr_df):
    if ((rfmr_df['Range'] == 1) & (rfmr_df['Range'] == 0)):
        return 'Singleton'
    elif ((rfmr_df['Range'] > 1) & (rfmr_df['Range'] <= 4)):
        return 'Assortment'
    elif ((rfmr_df['Range'] > 4) & (rfmr_df['Range'] <= 15)):
        return 'Collector'
    elif (rfmr_df['Range'] > 15):
        return 'Medley'
    else:
        return 'Singleton'
    
    
def DNA_ON_OFF_Pro(rfmr_df):
    if (rfmr_df['DNA_ON_OFF_Pro_val'] == 0):
        return 'Always Offline'
    elif ((rfmr_df['DNA_ON_OFF_Pro_val'] > 0) & (rfmr_df['DNA_ON_OFF_Pro_val'] <= 0.50)):
        return 'Mostly Offline'
    elif ((rfmr_df['DNA_ON_OFF_Pro_val'] > 0.50) & (rfmr_df['DNA_ON_OFF_Pro_val'] < 1)):
        return 'Mostly Online'
    elif (rfmr_df['DNA_ON_OFF_Pro_val'] == 1):
        return 'Always Online'
    else :
        return 'missing'
    

def DNA_Recency(rfmr_df):
    if (rfmr_df['Recency'] > 658):
        return 'Lost'
    elif ((rfmr_df['Recency'] >= 202) & (rfmr_df['Recency'] <= 658)):
        return 'Dormant'
    elif (rfmr_df['Recency'] <= 201):
        return 'Active'

def DNA_Calender(rfmr_df):
        if ((rfmr_df['Jan']/rfmr_df['All']) > 0.5):
            return 'January Shoppers'
        elif ((rfmr_df['Feb']/rfmr_df['All']) > 0.5):
            return 'February Shoppers'
        elif ((rfmr_df['Mar']/rfmr_df['All']) > 0.5):
            return 'March Shoppers'
        elif ((rfmr_df['Apr']/rfmr_df['All']) > 0.5):
            return 'April Shoppers'
        elif ((rfmr_df['May']/rfmr_df['All']) > 0.5):
            return 'May Shoppers'
        elif ((rfmr_df['Jun']/rfmr_df['All']) > 0.5):
            return 'June Shoppers'
        elif ((rfmr_df['Jul']/rfmr_df['All']) > 0.5):
            return 'July Shoppers'
        elif ((rfmr_df['Aug']/rfmr_df['All']) > 0.5):
            return 'August Shoppers'
        elif ((rfmr_df['Sep']/rfmr_df['All']) > 0.5):
            return 'September Shoppers'
        elif ((rfmr_df['Oct']/rfmr_df['All']) > 0.5):
            return 'October Shoppers'
        elif ((rfmr_df['Nov']/rfmr_df['All']) > 0.5):
            return 'November Shoppers'
        elif ((rfmr_df['Dec']/rfmr_df['All']) > 0.5):
            return 'December Shoppers'
        else:
            return 'Sporadic Shoppers' 


def DNA_CAGR(rfmr_df):
    if (rfmr_df['CAGR_val'] == 1):
        return 'Single Transaction'
    elif ((rfmr_df['CAGR_val'] >= 0) & (rfmr_df['CAGR_val'] <= 5)):
        return 'Stagnant'
    elif (rfmr_df['CAGR_val'] > 5):
        return 'Growing'
    elif (rfmr_df['CAGR_val'] < 0):
        return 'Declining'
    else:
        return 'Declining'
        
        

def Traits_segment(rfmr_df):
    if (rfmr_df['Potential Enthusiasts'] == 'Yes'):
        return 'Potential Enthusiasts'
    elif (rfmr_df['Enthusiasts'] == 'Yes'):
        return 'Enthusiasts'
    elif (rfmr_df['Thrifty'] == 'Yes'):
        return 'Thrifty'
    elif (rfmr_df['Core'] == 'Yes'):
        return 'Core'
    else:
        return 'Core'

def incomegroup(i):
    if (i == 0):
        return ''
    elif (i < 50):
        return '<$50K'
    elif ((i >= 50) & (i <= 100)):
        return '$50k-$100K'
    elif ((i >= 101) & (i <= 150)):
        return '$100k-$150K'
    elif (i >= 151):
        return '>150K'         


def segmentorderlevel(i):
    if i == 'Enthusiasts':
        return 1
    elif i == 'Thrifty':
        return 3
    elif i == 'Core':
        return 4
    else:
        return 2
    
def age_group(rfmr_df):
    if (rfmr_df['Age_Group'] == '<35'):
        return '<35'
    elif (rfmr_df['Age_Group'] == '35-54'):
        return '35-54'
    elif (rfmr_df['Age_Group'] == '55-64'):
        return '55-65'
    elif (rfmr_df['Age_Group'] == '55-65'):
        return '55-65'
    elif (rfmr_df['Age_Group'] == '65+'):
        return '65+'
    
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
    print("program starts")
    data= pd.read_sql_query('select o.order_id,o."channel",c."customer_id",o."total", o."order_date" from public.customer_details c inner join public."order" o on o.customer_id=c.customer_id ', con =engine )
    employees = pd.read_sql_query('SELECT e."Employee_id" FROM public.employee_details e ', con =engine )
    employees.rename(columns={'Employee_id':'customer_id'},inplace=True)
    employee_customer = data.merge(employees,on=['customer_id'], how = 'inner')
    final_data = data[(~data.customer_id.isin(employee_customer.customer_id))]
    final_data["Total_c_final"] = np.where(final_data['total'] <= 0, np.nan , final_data['total'])
    final_data.dropna(subset=['Total_c_final'],inplace=True)
    final_data.dropna(subset=['total'],inplace=True)
    final_data['order_date']= pd.to_datetime(final_data['order_date'])
    final_data.dropna(subset=['order_date'],inplace=True)
    final_data['year'] =  final_data['order_date'].dt.year
    order_details= pd.read_sql_query('select ord."Product_ID", ord."Status", ord."Item_ID" ,ord."Discount",ord."Order_ID",ord."Total_Computed" from  public.order_details ord', con =engine )
    order_details.rename(columns={'Order_ID': 'order_id'}, inplace=True)
    order_details.dropna(subset=['Product_ID'],inplace=True)
    order_details["Total_Computed_final"] = np.where(order_details['Total_Computed'] <= 0, np.nan , order_details['Total_Computed'])
    order_details.dropna(subset=['Total_Computed_final'],inplace=True)
    order_details.dropna(subset=['Total_Computed'],inplace=True)
    customer_order_details = pd.merge(final_data,order_details,how = 'inner',on='order_id')
    timestamp = int(time.mktime(max(customer_order_details['order_date']).timetuple()))
    max_order_date = datetime.fromtimestamp(timestamp)
    print("program cleaning over")
    rfmTable1 = customer_order_details.groupby(['customer_id']).agg({'order_date': lambda x: (max_order_date - x.max()).days,'order_id': pd.Series.nunique,'Total_Computed_final': lambda x: x.sum()}).reset_index()
    rfmTable1.rename(columns={'order_date': 'Recency', 'order_id': 'Frequency', 'Total_Computed_final': 'Monetary'}, inplace=True)
    rfmTable2 = customer_order_details.groupby(['customer_id']).agg({'Product_ID' : pd.Series.nunique,'Discount' : lambda x:x.sum()}).reset_index()
    rfmTable2.rename(columns={'Product_ID' : 'Range'}, inplace=True)
    rfmTable = rfmTable1.merge(rfmTable2, on ='customer_id', how = 'left')
    rfm_df = pd.DataFrame(rfmTable).reset_index()
    rfm_df['Range'] = rfm_df['Range'].fillna(0)
    rfm_df['Monetary/Frequency'] = rfm_df['Monetary']/rfm_df['Frequency']
    #rfm_df['Monetary/Frequency']  = rfm_df['Monetary/Frequency'].round(0).astype(int)
    rfm_df['Discount_ratio'] = (rfm_df['Monetary']/rfm_df['Discount'])*100
    rfm_df['Discount_ratio'] = rfm_df['Discount_ratio'].fillna(0)
    no_of_unique_customers = rfm_df.shape[0]
    total_orders = rfm_df.Frequency.sum()
    rfm_df['average_purchase_value'] = rfm_df['Monetary/Frequency']/no_of_unique_customers
    average_frequency_rate = total_orders/no_of_unique_customers
    rfm_df['average_customer_value'] = rfm_df['average_purchase_value'] * average_frequency_rate
    max_year = customer_order_details['year'].max()
    min_year = customer_order_details['year'].min()
    avg_total_no_of_years = (max_year-min_year)/2
    rfm_df['life_time_value'] = rfm_df['average_customer_value'] * avg_total_no_of_years
    X=rfm_df[['life_time_value','average_purchase_value', 'average_customer_value']]
    # Feature Scaling
    scaler = StandardScaler()
    X_train = scaler.fit_transform(X)
    # Fitting K-Means to the dataset
    kmeans = KMeans(n_clusters = 3, init = 'k-means++', random_state = 42)
    loyalty = kmeans.fit_predict(X_train)
    loyalty1=loyalty
    loyalty1=loyalty+1
    rfm_df['loyalty_range'] =  loyalty1
    di ={1:'Low Loyalty',2:'Medium Loyalty',3:'High Loyalty'}
    rfm_df['loyalty_range'] = rfm_df['loyalty_range'].map(di)
    minimum_transaction_value = final_data.groupby('customer_id')['year'].min().reset_index()
    first_sale_value = pd.merge(minimum_transaction_value,final_data[['customer_id','year','total']], on =['customer_id','year'], how = 'left')
    first_year_sales = first_sale_value.groupby('customer_id')['total'].sum().reset_index()
    first_year_sales.columns = ['customer_id','first_year_sales_value']
    rfm_df_first_year = pd.merge(rfm_df,first_year_sales, on ='customer_id' ,how = 'left')
    customer_order_details['month'] = customer_order_details['order_date'].dt.month
    customer_order_details['month'] = customer_order_details['month'].fillna(0)
    customer_order_details['month'] =customer_order_details['month'].astype(int)
    customer_order_details['month'] = customer_order_details['month'].apply(lambda x: calendar.month_abbr[x])
    customer_month_x = pd.crosstab(customer_order_details.customer_id, customer_order_details.month,rownames=['customer_id'], colnames=['month'],margins=True).reset_index()
    customer_month_df = pd.DataFrame(customer_month_x)
    customer_month_df =customer_month_df[:-1]
    customer_month_df['customer_id'] = customer_month_df['customer_id'].astype(str)
    rfm_df['customer_id'] = rfm_df['customer_id'].astype(str)
    rfm_month = pd.merge(rfm_df_first_year,customer_month_df, on ='customer_id', how = 'left')
    online_offline= pd.crosstab(data.customer_id, data.channel,rownames=['customer_id'], colnames=['channel'],margins=True).reset_index()
    online_offline_df = pd.DataFrame(online_offline)
    online_offline_df=online_offline_df[:-1]
    online_offline_df['customer_id'] = online_offline_df['customer_id'].astype(str)
    rfm_month['customer_id'] = rfm_month['customer_id'].astype(str)
    rfmr_df = pd.merge(rfm_month,online_offline_df[['customer_id','Retail','Website']], on ='customer_id', how = 'left')
    rfmr_df['DNA_ON_OFF_Pro_val'] = (rfmr_df['Website'])/(rfmr_df['Retail']+rfmr_df['Website'])
    ecohert_data= pd.read_sql_query('SELECT "customer_id", "Income_Group",  "Age_Group" FROM public."Ecohert_cohort_explained"', con =engine )
    rfmr_df = pd.merge(rfmr_df,ecohert_data, on ='customer_id',how = 'left')
    rfmr_df['Income'] = rfmr_df.Income_Group.str.extract('(\d+)')
    last_year = customer_order_details['order_date'].max().year
    first_year = customer_order_details['order_date'].min().year
    No_of_years = last_year - first_year
    No_of_years = (1/No_of_years)
    rfmr_df['CAGR_val_a'] = rfmr_df['Monetary'] /rfmr_df['first_year_sales_value']
    rfmr_df['CAGR_val_b'] = rfmr_df['CAGR_val_a'].pow(No_of_years)
    rfmr_df['CAGR_val'] = (rfmr_df['CAGR_val_b']-1)*100
    rfmr_df['CAGR_val'] = rfmr_df['CAGR_val'].round(0)
    quantiles = rfmr_df.quantile(q=[0.25,0.5,0.75])
    quantiles = quantiles.to_dict()
    rfmr_df['r_quartile'] = rfmr_df['Recency'].apply(RScore, args=('Recency',quantiles,))
    rfmr_df['f_quartile'] = rfmr_df['Frequency'].apply(FMScore, args=('Frequency',quantiles,))
    rfmr_df['m_quartile'] = rfmr_df['Monetary'].apply(FMScore, args=('Monetary',quantiles,))
    rfmr_df['RFMScore'] = rfmr_df.r_quartile.map(str) + rfmr_df.f_quartile.map(str) + rfmr_df.m_quartile.map(str)
    rfmr_df['Total Score'] = rfmr_df['r_quartile'] + rfmr_df['f_quartile'] +rfmr_df['m_quartile']
    print('function starts')
    rfmr_df['Micro_segments'] = rfmr_df.apply(segment_me2, axis=1)
    rfmr_df['DNA_VPV'] = rfmr_df.apply(DNA_VPV, axis = 1)
    rfmr_df['DNA_Frequency'] = rfmr_df.apply(DNA_Frequency, axis = 1)
    rfmr_df['DNA_range'] = rfmr_df.apply(DNA_range, axis = 1)
    rfmr_df['DNA_Recency'] = rfmr_df.apply(DNA_Recency, axis = 1)
    rfmr_df['DNA_ON_OFF_Pro'] = rfmr_df.apply(DNA_ON_OFF_Pro, axis = 1)
    rfmr_df['DNA_Calender'] = rfmr_df.apply(DNA_Calender, axis = 1)
    rfmr_df['DNA_CAGR'] = rfmr_df.apply(DNA_CAGR, axis = 1)
    print('Function ends')
    rfmr_df['DNA_Discount_offline'] = np.nan
    rfmr_df.loc[(rfmr_df['Discount_ratio'] >100) & (rfmr_df['DNA_ON_OFF_Pro'].str.contains('Offline',case = False)),'DNA_Discount_offline'] = 'Bargain Hunter offline'
    rfmr_df.loc[(rfmr_df['Discount_ratio'] > 0) & (rfmr_df['Discount_ratio'] <= 100) & (rfmr_df['DNA_ON_OFF_Pro'].str.contains('Offline',case = False)),'DNA_Discount_offline'] = 'Occasional Discounter Offline'
    rfmr_df.loc[(rfmr_df['Discount_ratio'] ==0) & (rfmr_df['DNA_ON_OFF_Pro'].str.contains('Offline',case = False)),'DNA_Discount_offline'] = 'Full Price Shopper Offline'
    rfmr_df['DNA_Discount_Online'] = np.nan
    rfmr_df.loc[(rfmr_df['Discount_ratio'] >100) & (rfmr_df['DNA_ON_OFF_Pro'].str.contains('Online', case = False)),'DNA_Discount_Online'] = 'Bargain Hunter Online'
    rfmr_df.loc[(rfmr_df['Discount_ratio'] > 0) & (rfmr_df['Discount_ratio'] <= 100) & (rfmr_df['DNA_ON_OFF_Pro'].str.contains('Online',case = False)),'DNA_Discount_Online'] = 'Occasional Discounter Online'
    rfmr_df.loc[(rfmr_df['Discount_ratio'] ==0) & (rfmr_df['DNA_ON_OFF_Pro'].str.contains('Online',case = False)),'DNA_Discount_Online'] = 'Full Price Shopper Online'
    #     rfmr_df['Potential Enthusiasts'] = np.where((rfmr_df['DNA_Frequency'].isin(['Regular','Occasional'])) & (rfmr_df['DNA_VPV'].isin(['Lavish','Splurge'])) & (rfmr_df['DNA_range'].isin(['Medley','Collector'])),'Yes','No')
    #     rfmr_df['Enthusiasts'] = np.where((rfmr_df['DNA_Frequency'].isin(['Regular','Occasional'])) &(rfmr_df['DNA_range'].isin(['Medley','Collector'])) &((rfmr_df['DNA_VPV'].isin(['Premium','Lavish','Splurge']))),'Yes','No')
    #     rfmr_df['Thrifty'] = np.where((rfmr_df['DNA_VPV'].isin(['Frugal','Prudent'])) &(rfmr_df['DNA_Frequency'].str.contains('Oneoff', case = False)) & (rfmr_df['DNA_range'].str.contains('Singleton', case = False)),'Yes','No')
    #     rfmr_df['Core'] = np.where((rfmr_df['DNA_VPV'].isin(['Frugal','Prudent'])) &(rfmr_df['DNA_Frequency'].str.contains('Oneoff', case = False)) & (rfmr_df['DNA_range'].str.contains('Singleton', case = False)),'No','Yes') 
    rfmr_df['Potential Enthusiasts'] = np.where((rfmr_df['DNA_Frequency'].isin(['Regular','Occasional'])) & (rfmr_df['DNA_VPV'].isin(['Premium','Lavish','Splurge'])) & (rfmr_df['DNA_range'].isin(['Assortment'])),'Yes','No')
    rfmr_df['Enthusiasts'] = np.where((rfmr_df['DNA_Frequency'].isin(['Regular','Occasional'])) &(rfmr_df['DNA_range'].isin(['Medley','Collector'])) &((rfmr_df['DNA_VPV'].isin(['Lavish','Splurge']))),'Yes','No')
    rfmr_df['Thrifty'] = np.where((rfmr_df['DNA_VPV'].isin(['Frugal','Prudent'])) &(rfmr_df['DNA_Frequency'].str.contains('Oneoff', case = False)) & (rfmr_df['DNA_range'].str.contains('Singleton', case = False)),'Yes','No')
    rfmr_df['Core'] = np.where((rfmr_df['DNA_VPV'].isin(['Premium'])) &(rfmr_df['DNA_Frequency'].str.contains('Oneoff', case = False)) & (rfmr_df['DNA_range'].isin(['Singleton','Assortment'])),'Yes','No')
    rfmr_df['Traits_segment'] = rfmr_df.apply(Traits_segment, axis = 1)
    rfmr_df['Age_Group'] = rfmr_df.apply(age_group, axis = 1)
    Customer_segmentation_traits = rfmr_df[['customer_id', 'Recency', 'Frequency', 'Monetary', 'Range','r_quartile', 'f_quartile', 'm_quartile', 'RFMScore', 'Total Score','Micro_segments','CAGR_val', 'DNA_VPV','DNA_Frequency', 'DNA_range', 'DNA_Recency', 'DNA_ON_OFF_Pro','DNA_Calender', 'DNA_CAGR','DNA_Discount_offline','DNA_Discount_Online','Traits_segment','Income','Age_Group','loyalty_range']]
    print("loading to source db ")
    t=time.time()
    Customer_segmentation_traits.to_sql(name='Customer_segmentation_traits',con=engine, if_exists = 'replace', index=False,method ='multi',chunksize=1000)
    print(time.time()-t)
    print("loading to source db ends")
    return Customer_segmentation_traits
    
    
    
    
def loadtargetelement(_targetconnection,_sourceelement,projectid, projectsubmissionid,widgetid,createdby):
    targetcursor = _targetconnection.cursor()
    ''' Check if already Executed and set the active flag to N '''
    postgres_update_query = """ UPDATE petram.segmenttraits SET active = 'N' WHERE projectid = %s AND widgetid = %s """
    record_to_update = (str(projectid),str(widgetid))
    targetcursor.execute(postgres_update_query,record_to_update)
    my_list = []
    ##
#     _targetconnection.autocommit=False
#     postgres_insert_query = """ INSERT INTO segmenttraits(projectid, sno, widgetid, customerid,receny,frequency,monetary,ranges,cagrval,dnavpv,dnafrequency,dnarange,dnarecency,dnaonoffpro,dnacalender,dnacagr,dnadiscountoffline,dnadiscountonline,loyaltyrange,traitssegment,income,agegroup,incomegroup,segmentlstorder) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
    ##
    increment = 0
    start_1 = time.time()
    for index,row in _sourceelement.iterrows():
        i = row['Income']
        if str(row['Income']) == 'nan':
            i = 0
        incomgroup = incomegroup(int(i)) 
        print(index)
        record_to_insert = (str(projectid),index,str(widgetid),row['customer_id'],row['Recency'],row['Frequency'],row['Monetary'],row['Range'],row['DNA_VPV'],row['DNA_Frequency'],row['DNA_range'],row['DNA_Recency'],row['DNA_Calender'],row['DNA_CAGR'],row['loyalty_range'],row['Traits_segment'],row['Age_Group'],incomgroup,segmentorderlevel(str(row['Traits_segment'])),row['Micro_segments'])
        my_list.append(tuple(record_to_insert))
        increment = increment + 1
        if increment == 10000:
            t=time.time()
            args_str = ','.join(targetcursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", x).decode("utf-8") for x in my_list)
            targetcursor.execute("INSERT INTO petram.segmenttraits(projectid, sno, widgetid, customerid,receny,frequency,monetary,ranges,dnavpv,dnafrequency,dnarange,dnarecency,dnacalender,dnacagr,loyaltyrange,traitssegment,agegroup,incomegroup,segmentlstorder,traitssegment2) VALUES" + args_str)
#             targetcursor.executemany(postgres_insert_query,my_list)
            _targetconnection.commit()
            print('commit done')
            increment = 0
            my_list = []
            print(time.time()-t)
            
    #postgres_insert_query = """ INSERT INTO segmenttraits(projectid, sno, widgetid, customerid,receny,frequency,monetary,ranges,cagrval,dnavpv,dnafrequency,dnarange,dnarecency,dnaonoffpro,dnacalender,dnacagr,dnadiscountoffline,dnadiscountonline,loyaltyrange,traitssegment,income,agegroup,incomegroup,segmentlstorder) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
    args_str = ','.join(targetcursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", x).decode("utf-8") for x in my_list)
    targetcursor.execute("INSERT INTO petram.segmenttraits(projectid, sno, widgetid, customerid,receny,frequency,monetary,ranges,dnavpv,dnafrequency,dnarange,dnarecency,dnacalender,dnacagr,loyaltyrange,traitssegment,agegroup,incomegroup,segmentlstorder,traitssegment2) VALUES" + args_str)
    _targetconnection.commit()
    targetcursor.close()


def setsubmissionstatus(_targetconnection,projectid, projectsubmissionid,widgetid,status):
    _targetcursor = _targetconnection.cursor()
    status_update_query = """ update petram.tprojectwidget set status = %s where projectid = %s and widgetid = %s """
    record_to_update = (str(status),str(projectid),str(widgetid))
    _targetcursor.execute(status_update_query, record_to_update)
    _targetconnection.commit()
    _targetcursor.close()

def process(projectid, projectsubmissionid,widgetid,createdby):
    _sourceconnection = None
    
    # Set the Process to InProgress
    # Get the Source Connection
    _sourceconnection = source_db_connection()
    # Get Source Element
    _sourceelement = getsourceelement(_sourceconnection)
    # Get the Target Connection
    _targetconnection = target_db_connection()
    # Load the data in Target Element
    loadtargetelement(_targetconnection,_sourceelement,projectid, projectsubmissionid,widgetid,createdby)
    # Set the Process to Completed
    setsubmissionstatus(_targetconnection,projectid, projectsubmissionid,widgetid,'COM')
    # Close all the Connection

def handler():
    #inputobject = json.loads(json.dumps(event))
    process("02254cee-ff7b-483b-aaba-61ba688c5caa", "668c21ba-ebdd-44ed-a5e9-570df29ba85d","SEGMENTATIONLIST","0dbf75db-1a46-4544-a70c-b301b0a70eb1")
    # process(inputobject['projectid'], inputobject['projectsubmissionid'],inputobject['widgetid'],inputobject['createdby'])
    return {"statusCode":200,"body":json.dumps({'status':'updated success'})}

handler()
