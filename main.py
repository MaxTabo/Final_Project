import requests
from bs4 import BeautifulSoup
import pandas as pd
import requests
import re
import functions

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


df_main=functions.change_w_values(df_main)

functions.change_main_values(df_main)

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








