import pandas as pd
import pycountry_convert as pc

def merge(df1,df2,columns):
    df3= pd.merge(df1, df2,  how='left', left_on=columns, right_on =columns)
    
    return df3
def specific_merge(df1,list_):
    for i in list_:
        df1=merge(df1,i,['Entity','Year'])
    return df1
    
def drop_code(list_):
    list2=[]
    for df in list_:
        df.drop('Code',axis=1,inplace=True)
        list2.append(df)
    return list2


def change_value(df,column1,name1,column2,name_2):
    df.loc[df[column1] == name1, column2] = name_2
    return df



def change_main_values(df_main):
    df_main=change_value(df_main,'Entity','World','Code','WRL')
    df_main=change_value(df_main,'Entity','Africa','Code','AFR')
    df_main=change_value(df_main,'Entity','Asia Pacific','Code','ASP')
    df_main=change_value(df_main,'Entity','Eastern Africa','Code','EAF')
    df_main=change_value(df_main,'Entity','European Union (27)','Code','EUU')
    df_main=change_value(df_main,'Entity','Middle Africa','Code','MAF')
    df_main=change_value(df_main,'Entity','Middle Africa','Code','MAF')
    df_main=change_value(df_main,'Entity','Europe','Code','EUR')
    df_main=change_value(df_main,'Entity','Western Africa','Code','WAF')
    df_main=change_value(df_main,'Entity','OECD','Code','OECD')
    df_main=change_value(df_main,'Entity','Middle East','Code','MDLE')
    df_main=change_value(df_main,'Entity','Non-OECD','Code','NOECD')
    df_main=change_value(df_main,'Entity','OECD','Code','OECD')
    df_main = df_main[df_main.Entity != 'CIS']
    return df_main


def change_growth_values(df_main):
    df_main=change_value(df_main,'Entity','Africa','Code','AFR')    
    df_main=change_value(df_main,'Entity','World','Code','WRL')    
    df_main=change_value(df_main,'Entity','Asia Pacific','Code','ASP')
    df_main=change_value(df_main,'Entity','Eastern Africa','Code','EAF')
    df_main=change_value(df_main,'Entity','European Union (27)','Code','EUU')    
    df_main=change_value(df_main,'Entity','Middle Africa','Code','MAF')
    df_main=change_value(df_main,'Entity','Europe','Code','EUR')
    df_main=change_value(df_main,'Entity','Western Africa','Code','WAF')
    df_main=change_value(df_main,'Entity','OECD','Code','OECD')
    df_main=change_value(df_main,'Entity','Middle East','Code','MDLE')
    df_main=change_value(df_main,'Entity','Non-OECD','Code','NOECD')
    df_main=change_value(df_main,'Entity','OECD','Code','OECD')
    df_main=change_value(df_main,'Entity','Asia','Code','ASIA')
    df_main=change_value(df_main,'Entity','Upper-middle-income countries','Code','MIDC')
    df_main=change_value(df_main,'Entity','Asia Pacific','Code','ASIAP')
    df_main=change_value(df_main,'Entity','South and Central America','Code','LAT')
    df_main=change_value(df_main,'Entity','Central America','Code','CENAM')
    df_main=change_value(df_main,'Entity','South America','Code','SA')
    df_main=change_value(df_main,'Entity','High-income countries','Code','HII')
    df_main=change_value(df_main,'Entity','South America','Code','SA')
    df_main=change_value(df_main,'Entity','South Africa','Code','SAF')
    df_main=change_value(df_main,'Entity','Northern Africa','Code','NAF')
    df_main=change_value(df_main,'Entity','Other South and Central America','Code','SCA')
    df_main=change_value(df_main,'Entity','Lower-middle-income countries','Code','LMC')
    df_main=change_value(df_main,'Entity','Caribbean','Code','CARIB')
    df_main=change_value(df_main,'Entity','North America','Code','NAM')
    df_main=change_value(df_main,'Entity','Oceania','Code','OCE')
    df_main = df_main[df_main.Entity != 'CIS']
    df_main = df_main[df_main.Entity != 'Other CIS']


    return df_main


def convert(row):
    cn_code= pc.country_name_to_country_alpha2(row.ENTITY,cn_name_format='default')
    conti_code=pc.country_alpha2_to_continent_code(cn_code)
    return conti_code
