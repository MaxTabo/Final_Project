
import pandas as pd
import re
import functions
import pymysql
import sqlalchemy as alch
from dotenv import load_dotenv
import os
import warnings
warnings.filterwarnings("ignore")

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


df_list=[df,df6,df7,df8,df9,df10,df11,df12,df13,df15,df17]
df_list=functions.drop_code(df_list)

df_main=functions.specific_merge(df3,df_list)


list_growth=[df19,df20,df21]
df_growth_list=functions.drop_code(list_growth)
df_growth=functions.specific_merge(df18,list_growth)


df_main=functions.change_main_values(df_main)

df_growth['Entity']=df_growth['Entity'].str.strip()

df_growth['Entity'] = df_growth['Entity'].str.replace('\(BP\)','')
df_growth['Entity'] = df_growth['Entity'].str.replace('Other Southern Africa','South Africa')
df_growth['Entity'] = df_growth['Entity'].str.replace('Other South America','South America')
df_growth['Entity'] = df_growth['Entity'].str.replace('Other South and Central America','South and Central America')
df_growth['Entity'] = df_growth['Entity'].str.replace('Other Northern Africa','Northern Africa')
df_growth['Entity'] = df_growth['Entity'].str.replace('Other Middle East','Middle East')
df_growth['Entity'] = df_growth['Entity'].str.replace('Other Europe','Europe')
df_growth['Entity'] = df_growth['Entity'].str.replace('Other Caribbean','Caribbean')
df_growth['Entity'] = df_growth['Entity'].str.replace('Other Africa','Africa')
df_growth['Entity'] = df_growth['Entity'].str.replace('Other Asia Pacific','Asia Pacific')

df_growth=functions.change_growth_values(df_growth)

df_main.drop(df_main[(df_main['Year'] <1990)].index,axis=0,inplace=True)

df_growth.drop(df_growth[(df_growth['Year'] <1990)].index,axis=0,inplace=True)

df_main = df_main.rename(columns={"Entity": "ENTITY", "Code": "ISO","Year": "YEAR","Solar (% sub energy)": "SOLAR ENERGY %","Wind (% sub energy)": "WIND ENERGY %","Wind (% electricity)": "WIND ELECTRICITY %","Solar (% electricity)": "SOLAR ELECTRICITY %","Renewables (% electricity)": "RENEWABLE ELECTRICITY %","Hydro (% electricity)": "HYDRO ELECTRICITY %","Renewables (% sub energy)": "RENEWABLE ENERGY %","Renewables per capita (kWh - equivalent)": "RENEWABLE PER CAPITA (KWh)","Electricity from wind (TWh)": "WIND ELECTRICITY (TWh)","Electricity from hydro (TWh)": "HYDRO ELECTRICITY (TWh)","Electricity from solar (TWh)": "SOLAR ELECTRICITY (TWh)","Wind Generation - TWh": "WIND GENERATION (TWh)","Solar Generation - TWh": "SOLAR GENERATION (TWh)","Electricity from other renewables including bioenergy (TWh)": "OTHERS RENEWABLE ENERGIES (TWh)","Hydro Generation - TWh": "HYDRO GENERATION (TWh)","Hydro (% sub energy)": "HYDRO ENERGY %"})
df_main.drop(['SOLAR GENERATION (TWh)','WIND GENERATION (TWh)','HYDRO GENERATION (TWh)'],axis=1,inplace=True)
df_growth = df_growth.rename(columns={"Entity": "ENTITY", "Code": "ISO","Year": "YEAR","Solar (% growth)": "SOLAR GROWTH %","Renewables (% growth)": "RENEWABLE GROWTH %","Solar (TWh growth - equivalent)": "SOLAR GROWTH TWh","Renewables (TWh growth - equivalent)": "RENEWABLE GROWTH TWh"})
df_growth = df_growth.reindex(['ENTITY','ISO','YEAR','SOLAR GROWTH %','SOLAR GROWTH TWh','RENEWABLE GROWTH TWh','RENEWABLE GROWTH %'], axis=1)


cou_list=['Africa', 'Asia Pacific', 'Eastern Africa', 'European Union (27)','Europe', 'Middle Africa', 'Middle East' ,'Non-OECD', 'OECD', 'Western Africa','World']
for i in cou_list:
    df_main.drop(df_main[(df_main['ENTITY'] == i)].index,axis=0,inplace=True)

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
df_main = df_main.reindex(['ENTITY','ISO','CONTINENT','YEAR','SOLAR ELECTRICITY (TWh)','SOLAR ENERGY %','WIND ELECTRICITY (TWh)','WIND ENERGY %','WIND ELECTRICITY %','HYDRO ELECTRICITY (TWh)','HYDRO ENERGY %','HYDRO ELECTRICITY %','RENEWABLE ENERGY %','RENEWABLE ELECTRICITY %','RENEWABLE PER CAPITA (KWh)','OTHERS RENEWABLE ENERGIES (TWh)'], axis=1)



load_dotenv()
password= os.getenv("password")



dbName = "final_project"

connectionData=f"mysql+pymysql://root:{password}@localhost/{dbName}"

engine = alch.create_engine(connectionData)
engine

df_main.to_sql("renewable", if_exists="replace", index=False,con=engine)

df_growth.to_sql("growth", if_exists="replace",index=False, con=engine)




