#Data Preprocessing

import pandas as pd
import config 
import urllib
from sqlalchemy import create_engine

# Function to replace missing values with the mode (categorical variables) or mean (continuous variables)
def fill_NA_mode_mean(data):
    for name in objects:
        data.loc[data[name].isnull(),name] = data[name].mode().iloc[0]
    for name in nums:
        data.loc[data[name].isnull(), name] = data[name].mean()
    return data
def standardize(data):
    num_cols = ["hematocrit", "neutrophils", "sodium", "glucose", "bloodureanitro", "creatinine", "bmi", "pulse", "respiration"]
    for x in num_cols:
        data[x]=(data[x]-data[x].mean())/data[x].std()
    return data

server=config.dataprepconfig['server_name']
database=config.dataprepconfig['database_name']
uat="DRIVER={SQL Server Native Client 11.0} ;SERVER=%s;DATABASE=%s;Trusted_Connection=yes;"%(server,database)
params = urllib.parse.quote_plus(uat)
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
schemaname=config.dataprepconfig['schema_name']
tablename=config.dataprepconfig['master_table']
SQL_Query = pd.read_sql_query("Select * from [%s].[%s] where actual_dischargedate is NOT NULL"%(schemaname,tablename), engine)
   
data = pd.DataFrame(SQL_Query) 
data=data.drop(columns=['predicted_lengthofstay','predicted_dischargedate'])  
      
#load_data=config.dataprepconfig['load_csv']
#data=pd.read_csv(load_data)


colnames=data.columns

# Then, get the names of the variables that actually have missing values. Assumption: no NA in eid, lengthofstay, or dates.
#var = [x for x in colnames if x not in ["eid", "actual_lengthofstay", "vdate", 
#                                        "actual_dischargedate","predicted_lengthofstay","predicted_dischargedate"]]
var_with_NA =  data.columns[data.isna().any()].tolist()

method = None
if not var_with_NA:
    print("No missing values.")
    print("You can move to scaling.")
    missing = False
else:
    print("Variables containing missing values are:")
    print(var_with_NA)
    print("Applying one of the methods below to fill missing values:")
    missing = True
    method = "missing"

#filling missing value

if method == "missing":
    print("Filling missing values with mode and mean")
    # Get the variables types (categortical vs. continuous)
    objects = []
    nums = []
    for x in var_with_NA:
        if data.dtypes[x] == "object":
            objects.append(x)
        else:
            nums.append(x)
    data=fill_NA_mode_mean(data)
    
#Step 2 : scaling
data=standardize(data)
data["predicted_lengthofstay"]=""
data["predicted_dischargedate"]=""
print("Preprocessing complete!")
output_csv=config.dataprepconfig['create_csv']
data.to_csv(output_csv,index=False)
#data.to_csv("F:\\Projects\Predicting_hospital_stay\Code\Data_Acquisition_and_Understanding\data_preprocessed.csv")
