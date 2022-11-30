
import pandas as pd
import numpy as np
import re
import functions
import pymysql
import sqlalchemy as alch
from dotenv import load_dotenv
import os
import warnings
warnings.filterwarnings("ignore")
import pycountry_convert as pc
from sklearn.linear_model import LinearRegression
from sklearn.neighbors import KNeighborsRegressor

from sklearn.ensemble import GradientBoostingRegressor

from sklearn.svm import SVR
from sklearn.model_selection import train_test_split
from sklearn import metrics

#-----------------------------------------------------------------------------------------------------------

# Loading Dataframes.

df = pd.read_csv("Data\wind-share-energy.csv")
df1 = pd.read_csv("Data\wind-generation.csv")
df2 = pd.read_csv("Data\Test IRENE.csv")
df3 = pd.read_csv("Data\solar-share-energy.csv")
df4 = pd.read_csv("Data\solar-energy-consumption-by-region.csv")
df5= pd.read_csv("Data\solar-energy-consumption.csv")
df6 = pd.read_csv("Data\share-electricity-wind.csv")
df7= pd.read_csv("Data\share-electricity-solar.csv")
df8 = pd.read_csv("Data\share-electricity-renewables.csv")
df9 = pd.read_csv("Data/share-electricity-hydro.csv")
df10 = pd.read_csv("Data/renewable-share-energy.csv")
df11 = pd.read_csv("Data\per-capita-renewables.csv")
df12 = pd.read_csv("Data\modern-renewable-prod.csv")
df13 = pd.read_csv("Data\modern-renewable-energy-consumption.csv")
df15 = pd.read_csv("Data\hydro-share-energy.csv")
df16 = pd.read_csv("Data\hydropower-consumption.csv")
df17 = pd.read_csv("Data/biofuel-production.csv")
df18 = pd.read_csv("Data/annual-percentage-change-solar.csv")
df19 = pd.read_csv("Data/annual-percentage-change-renewables.csv")
df20 = pd.read_csv("Data/annual-change-solar.csv")
df21 = pd.read_csv("Data/annual-change-renewables.csv")
dfGDP = pd.read_csv("Data/GDPpercapita.csv")
#-----------------------------------------------------------------------------------------------------------
# Creating a list of Dataframes to drop a specific column.

df_list=[df,df6,df7,df8,df9,df10,df11,df12,df13,df15,df17]
df_list=functions.drop_code(df_list)
#-----------------------------------------------------------------------------------------------------------
# Merging Dataframes.

df_main=functions.specific_merge(df3,df_list)


list_growth=[df19,df20,df21]
df_growth_list=functions.drop_code(list_growth)
df_growth=functions.specific_merge(df18,list_growth)
#-----------------------------------------------------------------------------------------------------------
# Cleaning.

df_main=functions.change_main_values(df_main)

df_growth['Entity']=df_growth['Entity'].str.strip()



df_growth=functions.change_growth_values(df_growth)

wrl_renew = df_main.loc[df_main['Entity'] == 'World']
wrl_renew =wrl_renew.reindex(['Entity', 'Code','Year','Renewables (% sub energy)',
'Electricity from other renewables including bioenergy (TWh)'],axis=1)

wrl_renew = wrl_renew.rename(columns={'Entity':'ENTITY','Code':'ISO','Renewables (% sub energy)':'RENEWABLE ENERGY %',
'Electricity from other renewables including bioenergy (TWh)':'RENEWABLE ENERGY TWh','Year':'YEAR'})
#-----------------------------------------------------------------------------------------------------------
# Filtering to keep the last 30 years of the Dataframes.

df_main.drop(df_main[(df_main['Year'] <1990)].index,axis=0,inplace=True)

df_growth.drop(df_growth[(df_growth['Year'] <1990)].index,axis=0,inplace=True)
#-----------------------------------------------------------------------------------------------------------
# Renaming and reindexing.

df_main = df_main.rename(columns={"Entity": "COUNTRY", "Code": "ISO","Year": "YEAR"
,"Solar (% sub energy)": "SOLAR ENERGY %","Wind (% sub energy)": "WIND ENERGY %",
"Wind (% electricity)": "WIND ELECTRICITY %","Solar (% electricity)": "SOLAR ELECTRICITY %",
"Renewables (% electricity)": "RENEWABLE ENERGY %.","Hydro (% electricity)": "HYDRO ELECTRICITY %",
"Renewables (% sub energy)": "RENEWABLE ENERGY %","Renewables per capita (kWh - equivalent)": "RENEWABLE PER CAPITA (KWh)",
"Electricity from wind (TWh)": "WIND ENERGY (TWh)","Electricity from hydro (TWh)": "HYDRO ENERGY (TWh)",
"Electricity from solar (TWh)": "SOLAR ENERGY (TWh)","Wind Generation - TWh": "WIND GENERATION (TWh)",
"Solar Generation - TWh": "SOLAR GENERATION (TWh)","Electricity from other renewables including bioenergy (TWh)": "OTHERS RENEWABLE ENERGIES (TWh)"
,"Hydro Generation - TWh": "HYDRO GENERATION (TWh)","Hydro (% sub energy)": "HYDRO ENERGY %"})
df_main.drop(['SOLAR GENERATION (TWh)','WIND GENERATION (TWh)','HYDRO GENERATION (TWh)','WIND ELECTRICITY %',
 "SOLAR ELECTRICITY %","HYDRO ELECTRICITY %","RENEWABLE ENERGY %."],axis=1,inplace=True)
df_growth = df_growth.rename(columns={"Entity": "ENTITY", "Code": "ISO","Year": "YEAR","Solar (% growth)": "SOLAR GROWTH %",
"Renewables (% growth)": "RENEWABLE GROWTH %","Solar (TWh growth - equivalent)": "SOLAR GROWTH TWh","Renewables (TWh growth - equivalent)": "RENEWABLE GROWTH TWh"})
df_growth = df_growth.reindex(['ENTITY','ISO','YEAR','SOLAR GROWTH %','SOLAR GROWTH TWh','RENEWABLE GROWTH TWh',
'RENEWABLE GROWTH %'], axis=1)

wrl = df_main.loc[df_main['COUNTRY'] == 'World']
wrl=wrl.reset_index(drop=True)
#-----------------------------------------------------------------------------------------------------------
# Assigning Continents by countries.


cou_list=['Africa', 'Asia Pacific', 'Eastern Africa','CIS', 'European Union (27)','Europe',
 'Middle Africa', 'Middle East' ,'Non-OECD', 'OECD', 'Western Africa','World']
for i in cou_list:
    df_main.drop(df_main[(df_main['COUNTRY'] == i)].index,axis=0,inplace=True)

df_main['CONTINENT']=df_main.apply(functions.convert,axis=1)
continents = {
    'NA': 'North America',
    'SA': 'South America', 
    'AS': 'Asia',
    'OC': 'Australia',
    'AF': 'Africa',
    'EU': 'Europe'
}
df_main['CONTINENT']=df_main['CONTINENT'].map(continents)
df_main['CONTINENT']=df_main.CONTINENT.str.upper()
df_main = df_main.reindex(['COUNTRY','ISO','CONTINENT','YEAR',"SOLAR ENERGY (TWh)",
'SOLAR ENERGY %',"WIND ENERGY (TWh)",'WIND ENERGY %',"HYDRO ENERGY (TWh)",'HYDRO ENERGY %',
'RENEWABLE ENERGY %','RENEWABLE PER CAPITA (KWh)','OTHERS RENEWABLE ENERGIES (TWh)'], axis=1)
#-----------------------------------------------------------------------------------------------------------
#Adding GDP per countries.



dfGDP=dfGDP[['COUNTRY','GDP PER CAPITA(2020)']]
df_main=functions.merge(df_main,dfGDP,'COUNTRY')
wrl.drop([
        'RENEWABLE PER CAPITA (KWh)', 'WIND ENERGY (TWh)',
       'HYDRO ENERGY (TWh)', 'SOLAR ENERGY (TWh)',
       'OTHERS RENEWABLE ENERGIES (TWh)', 'Geo Biomass Other - TWh',
       'Biofuels Production - TWh - Total'],axis=1,inplace=True)
#-----------------------------------------------------------------------------------------------------------
# Cleaning a new Dataframe.

df_growth['ENTITY']=df_growth['ENTITY'].str.strip()
df_growth['ENTITY'] = df_growth['ENTITY'].str.replace('\(BP\)','')
def convert(row):
    cn_code= pc.country_name_to_country_alpha2(row.ENTITY,cn_name_format='default')
    conti_code=pc.country_alpha2_to_continent_code(cn_code)
    return conti_code

df_growth = df_growth.reindex(['ENTITY', 'ISO', 'YEAR', 
       'RENEWABLE GROWTH TWh', 'RENEWABLE GROWTH %'],axis=1)

df_growth = df_growth.rename(columns={"ENTITY": "COUNTRY"})
df_growth['COUNTRY']=df_growth['COUNTRY'].str.strip()
cou_list_g=['Africa','OECD ','Non-OECD ','Other Asia Pacific','Other Africa', 'Other Caribbean', 'Other Europe',
       'Other Middle East', 'Other Northern Africa',
       'Other South America', 'Other South and Central America','Eastern Africa ','Western Africa',
       'Other Southern Africa','Western Africa','Other Asia Pacific ','Other Northern Africa ',
       'Western Africa (BP)','Africa ','South and Central America', 'Upper-middle-income countries',
       'South America','Northern Africa','Central America','CIS','CIS ','Caribbean','Other CIS',
       'Oceania','North America','Lower-middle-income countries','High-income countries', 'Asia',
       'Asia Pacific','Asia Pacific ', 'Eastern Africa', 'European Union (27)','Europe', 'Middle Africa', 
       'Middle East' ,'Non-OECD', 'OECD', 'Western Africa','World']
for i in cou_list_g:
    df_growth.drop(df_growth[(df_growth['COUNTRY'] == i)].index,axis=0,inplace=True)
df_growth['CONTINENT']=df_growth.apply(functions.convert,axis=1)
continents = {
    'NA': 'North America',
    'SA': 'South America', 
    'AS': 'Asia',
    'OC': 'Australia',
    'AF': 'Africa',
    'EU': 'Europe'
}
df_growth['CONTINENT']=df_growth['CONTINENT'].map(continents)
df_growth['CONTINENT']=df_growth.CONTINENT.str.upper()
#-----------------------------------------------------------------------------------------------------------
#Saving Dataframes.

df_growth.to_csv('growth.csv', index = False, encoding='utf-8')
df_main.to_csv('main.csv', index = False, encoding='utf-8')
wrl_renew.to_csv('wrl_renew.csv', index = False, encoding='utf-8')

wrl= pd.read_csv("wrl_renew.csv")
main= pd.read_csv("main.csv")
growth= pd.read_csv("growth.csv")

#-----------------------------------------------------------------------------------------------------------
# Predictions.

wrl_test=wrl[['YEAR','RENEWABLE ENERGY %']]
models = {
    "lr": LinearRegression(),    
    "knn": KNeighborsRegressor(),
    "grad": GradientBoostingRegressor(),
    "svr": SVR(),
    
}

wrl_test=wrl_test.drop(wrl_test[(wrl_test['YEAR'] <2010)].index,axis=0)
y = wrl_test["RENEWABLE ENERGY %"]
X=wrl_test.drop(['RENEWABLE ENERGY %'],axis=1)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.30)

for name, model in models.items():
        model.fit(X_train, y_train)

for name, model in models.items():
    y_pred = model.predict(X_test)
    print(f"------{name}------")
    print('RMSE - ', np.sqrt(metrics.mean_squared_error(y_test, y_pred)))
    print('R2 - ', metrics.r2_score(y_test, y_pred))

lr=LinearRegression()
lr.fit(X_train, y_train)
y_pred = lr.predict(X_test)

lr.predict(X_test)
print(f"------lr------")
print('MAE - ', metrics.mean_absolute_error(y_test, y_pred))
print('MSE - ', metrics.mean_squared_error(y_test, y_pred))
print('RMSE - ', np.sqrt(metrics.mean_squared_error(y_test, y_pred)))
print('R2 - ', metrics.r2_score(y_test, y_pred))



list_=[x for x in range(2021,2121)] 
X_f = pd.DataFrame(list_)

X_f.columns=['YEAR']
y_f = lr.predict(X_f)
#-----------------------------------------------------------------------------------------------------------
# Calculating how much we need to growth to be sutainable in the same time predicted.

X_f['RENEWABLE ENERGY %']=y_f
X_f['RENEWABLE ENERGY %']=X_f['RENEWABLE ENERGY %'].apply(lambda x: x*1.1)
X_f['NECESSARY RENEWABLE ENERGY %']=X_f['RENEWABLE ENERGY %'].apply(lambda x: x*1.61)


df_pred=pd.concat([wrl_test, X_f], axis=0)

df_pred.to_csv('pred.csv', index = False, encoding='utf-8')
#-----------------------------------------------------------------------------------------------------------

# Sending Data to SQL

load_dotenv()
password= os.getenv("password")



dbName = "final_project"

connectionData=f"mysql+pymysql://root:{password}@localhost/{dbName}"

engine = alch.create_engine(connectionData)
engine

df_main.to_sql("renewable", if_exists="replace", index=False,con=engine)
df_growth.to_sql("growth", if_exists="replace",index=False, con=engine)
wrl_renew.to_sql("world", if_exists="replace",index=False, con=engine)
df_pred.to_sql("predictions", if_exists="replace", index=False,con=engine)



