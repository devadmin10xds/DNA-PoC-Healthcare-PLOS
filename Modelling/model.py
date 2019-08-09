#Predicting length of hospital stay by patients

import pandas as pd
import math
import config
import pickle
from sklearn import ensemble
from sklearn.metrics import mean_squared_error

#Training dataset
load_data=config.modelconfig['load_train']
los_df=pd.read_csv(load_data) 
los_df=los_df.drop(['eid','vdate','actual_dischargedate',
                    'predicted_lengthofstay','predicted_dischargedate'],axis=1) #Dropping irrelevant columns
dummy_rcount=pd.get_dummies(los_df['rcount']) #Creating dummy variables for columns with string datatype
dummy_gender=pd.get_dummies(los_df['gender'])
dummy_facid=pd.get_dummies(los_df['facid'])
los_df=pd.concat([los_df,dummy_rcount,dummy_gender,dummy_facid],axis=1)
los_df=los_df.drop(['rcount','gender','facid'],axis=1)
y=los_df['actual_lengthofstay']
X=los_df.drop('actual_lengthofstay',axis=1)

#Train-test split of dataset
from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

#Modelling and testing model
params = {'n_estimators': 500, 'max_depth': 4, 'min_samples_split': 2, 'learning_rate': 0.01, 'loss': 'ls'}
reg = ensemble.GradientBoostingRegressor(**params)

reg.fit(X_train, y_train)
y_pred=reg.predict(X_test)
y_pred_ceil=[math.ceil(x) for x in y_pred]

#Saving model
load_model=config.modelconfig['save_model']
filename = 'hosp_los_model.sav'
pickle.dump(reg, open(load_model+'\\hosp_los_model.sav', 'wb'))
print("Model saved!")

mse = mean_squared_error(y_test, y_pred_ceil)
print("MSE: %.4f" % mse)



