# -*- coding: utf-8 -*-
import config 
import pandas as pd
import pickle
import math
import urllib
import pyodbc
from datetime import timedelta
from sqlalchemy import create_engine

#Model testing
server=config.deployconfig['server_name']
database=config.deployconfig['database_name']
uat="DRIVER={SQL Server Native Client 11.0} ;SERVER=%s;DATABASE=%s;Trusted_Connection=yes;"%(server,database)
params = urllib.parse.quote_plus(uat)
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
schemaname=config.deployconfig['schema_name']
tablename=config.deployconfig['master_table']
SQL_Query = pd.read_sql_query("Select * from [%s].[%s] where actual_dischargedate is NULL"%(schemaname,tablename), engine)
   
testlos_df = pd.DataFrame(SQL_Query)   

#load_testdata=config.deployconfig['load_test']
#testlos_df=pd.read_csv(load_testdata)
eid=testlos_df['eid']
vdate=testlos_df['vdate']
#discharged=testlos_df['discharged']
testlos_df=testlos_df.drop(['eid','vdate','actual_dischargedate','actual_lengthofstay',
                            'predicted_lengthofstay','predicted_dischargedate'],axis=1)

dummy_rcount=pd.get_dummies(testlos_df['rcount'])
dummy_gender=pd.get_dummies(testlos_df['gender'])
dummy_facid=pd.get_dummies(testlos_df['facid'])
testlos_df=pd.concat([testlos_df,dummy_rcount,dummy_gender,dummy_facid],axis=1)

rcount=testlos_df['rcount']
gender=testlos_df['gender']
facid=testlos_df['facid']

testlos_df=testlos_df.drop(['rcount','gender','facid'],axis=1)

#Calling model
model_dir=config.deployconfig['load_model']
reg_file=open(model_dir+'\\hosp_los_model.sav','rb')
reg_model=pickle.load(reg_file)
reg_file.close()
p=reg_model.predict(testlos_df)
pred=[math.ceil(x) for x in p]
testlos_df=testlos_df.drop([0,1,2,3,4,5,'M','F','A','B','C','D','E'],axis=1) #dropping dummy columns
testlos_df['eid']=eid
testlos_df['vdate']=vdate
#testlos_df['discharged']=discharged
testlos_df['rcount']=rcount
testlos_df['gender']=gender
testlos_df['facid']=facid
testlos_df['predicted_lengthofstay']=pred
pred_dd=[]
j=0
for index,v in vdate.items():
    #pred_dd=v+timedelta(days= testlos_df[testlos_df.loc['vdate']==v,'predicted_lengthofstay'])
    pd=v+timedelta(days=pred[j])
    pred_dd.append(pd)
    j+=1
testlos_df['predicted_dischargedate']=pred_dd
#testlos_df['actual_lengthofstay']=""
#testlos_df['actual_dischargedate']=""
output_csv=config.deployconfig['create_csv']
testlos_df.to_csv(output_csv) #Output written into a CSV file

#Loading Output into database
conn=pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                    'Server=%s;'
                    'Database=%s;'
                    'Trusted_Connection=yes;'%(server,database))

cursor = conn.cursor()
i=0
while(i<len(pred)):
    for index, row in testlos_df.iterrows():
    #try:
        #((row.to_frame()).T).to_sql(name='PredictedData',schema =schemaname, con=engine,if_exists='append',index=False)        
        #print("Updation starts!")
        #vdate=vdate.apply(lambda x: x.strftime("%Y-%m-%d"))
        cursor.execute('''UPDATE %s.%s.%s SET predicted_lengthofstay=%d,predicted_dischargedate='%s' WHERE eid=%d AND vdate='%s' 
                         '''%(database,schemaname,tablename,pred[i],pred_dd[i].strftime("%Y-%m-%d"), row['eid'],row['vdate'].strftime("%Y-%m-%d"))) 
        
        conn.commit()
        i+=1
        #print(i,row['eid'],row['vdate'].strftime("%Y-%m-%d"))
print("Dataset Updated!")   
    #except:
        #print("Duplicate data found!")

