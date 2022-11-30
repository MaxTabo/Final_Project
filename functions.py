import pandas as pd
import pycountry_convert as pc

#-----------------------------------------------------------------------------------------------------------
# Merging different dataframes.

def merge(df1,df2,columns):
    df3= pd.merge(df1, df2,  how='left', left_on=columns, right_on =columns)
    
    return df3
def specific_merge(df1,list_):
    for i in list_:
        df1=merge(df1,i,['Entity','Year'])
    return df1
#-----------------------------------------------------------------------------------------------------------
# Dropping a specific column from all Dataframes.
    
def drop_code(list_):
    list2=[]
    for df in list_:
        df.drop('Code',axis=1,inplace=True)
        list2.append(df)
    return list2
#-----------------------------------------------------------------------------------------------------------
# Changing ISO values.

def change_value(df,column1,name1,column2,name_2):
    df.loc[df[column1] == name1, column2] = name_2
    return df



def change_main_values(df_main):
    df_main=change_value(df_main,'Entity','World','Code','WRL')
    df_main = df_main[df_main.Entity != 'CIS']
    return df_main


def change_growth_values(df_main):
    df_main=change_value(df_main,'Entity','World','Code','WRL')    
    df_main = df_main[df_main.Entity != 'CIS']
    df_main = df_main[df_main.Entity != 'Other CIS']


    return df_main
#-----------------------------------------------------------------------------------------------------------
# Creating a continent column to filter countries.

def convert(row):
    cn_code= pc.country_name_to_country_alpha2(row.COUNTRY,cn_name_format='default')
    conti_code=pc.country_alpha2_to_continent_code(cn_code)
    return conti_code
