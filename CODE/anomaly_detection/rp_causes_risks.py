# -*- coding: utf-8 -*-
"""
Created on Wed Mar 25 14:36:36 2020

@author: multi
"""
import pandas as pd
from copy import deepcopy
import numpy as np
import time

##To ensure pandas display "normal" numbers rather than scientific notation
pd.set_option('display.float_format', lambda x: '%.2f' % x)

def rp_riskscauses(data1, data2, data3, data4, selectedStart, selectedYear, selectedCountry):
    #Filter risks_top5 to country, start year, end year; same for causes_top5
    data1 = data1[(data1['location_name']==selectedCountry) & (data1['start_year']==int(selectedStart)) & (data1['end_year']==int(selectedYear))]
    data2 = data2[(data2['location_name']==selectedCountry) & (data2['start_year']==int(selectedStart)) & (data2['end_year']==int(selectedYear))]
    #Filter risk_daly_rate and cause daly rate to country
    data3 = data3[data3['location']==selectedCountry]
    data4 = data4[data4['location_name']==selectedCountry]
    #Create search columns for risk_daly_rate
    chosen_columns = ['location_id','location_name','rei_id','rei_name','cause_id','cause_name']
    chosen_columns.append(str(selectedYear)+'_val')
    chosen_columns.append(str(selectedYear)+'_lower')
    chosen_columns.append(str(selectedYear)+'_upper')
    data3 = data3.loc[:,chosen_columns]
    #Create search columns for cause_daly_rate
    chosen_columns = ['location_id','location_name','cause_id','cause_name']
    chosen_columns.append(str(selectedYear)+'_val')
    chosen_columns.append(str(selectedYear)+'_lower')
    chosen_columns.append(str(selectedYear)+'_upper')
    data4 = data4.loc[:,chosen_columns]
    #Obtain list of causes and risks from top 5 lists
    risks = data1.loc[:,'rei_id'].tolist()
    causes = data2.loc[:, 'cause_id'].tolist()
    #Initialize parameters
    columns = ['location_id','location_name','rei_id','rei_name','cause_id', 'cause_name','RF (DALY rate)', 'RF Lower', 'RF Upper', 'HC val', 'HC lower', 'HC upper']
    combined_df = pd.DataFrame(columns = columns)
    #Filter data by cause
    for c in causes:
        #Filter risk_daly_rate
        df = deepcopy(data3)
        cause_level = df['cause_id']==c
        df = df[cause_level]
        df_new = df[df['rei_id'].isin(risks)]
        #Filter cause_daly_rate
        df = deepcopy(data4)
        cause_level = df['cause_id']==c
        df_new2 = df[cause_level]
        #Merge the two dataframes for calculation
        df_new = df_new.reset_index(drop=True).merge(df_new2, on=['location_id','location_name','cause_id', 'cause_name'], how='left')
        df_new.columns=columns
        #Create new resulting dataframe
        combined_df = combined_df.append(df_new) 
    #Perform calculations
    combined_df = combined_df.reset_index(drop=True)
    combined_df['RF(%)'] = combined_df['RF (DALY rate)']/combined_df['HC val']*100
    combined_df['RF(%) Lower'] = combined_df['RF Lower']/combined_df['HC lower']*100
    combined_df['RF(%) Upper'] = combined_df['RF Upper']/combined_df['HC upper']*100
    delete = ['HC val','HC lower', 'HC upper'] #To delete
    for col in delete:
        del combined_df[col] 
    #insert location_id, location_name, start_year, end_year
    combined_df.insert(2, column = 'start_year', value = selectedStart)
    combined_df.insert(3, column = 'end_year', value = selectedYear)
    return combined_df

#Find the relationship between risks and causes for current health issues
path = 'risks_top5.csv'
data1 = pd.read_csv(path)
path2 = 'causes_top5.csv'
data2 = pd.read_csv(path2)
path3 = 'risk_daly_rate.csv'
data3 = pd.read_csv(path3)
path4 = 'cause_daly_rate.csv'
data4 = pd.read_csv(path4)
countries = data4.location_name.unique().tolist()
#Run for the following start_year-end_year combinations (minimum 10 years of data)
start_year_list = list(range(2003,2010))
end_year_list = list(range(2010, 2018))
year_comb = []
for start_year in start_year_list:
    for end_year in end_year_list:
        if end_year-start_year<9:
            continue
        year_comb.append((start_year, end_year))

#Initialize dataframes
df1_total_columns = ['location_id','location_name','start_year','end_year','rei_id','rei_name','cause_id', 'cause_name','RF (DALY rate)', 'RF Lower', 'RF Upper', 'RF(%)', 'RF(%) Lower', 'RF(%) Upper']
df1_total = pd.DataFrame(columns=df1_total_columns)
for country in countries:
    for selectedStart, selectedYear in year_comb:
        start_time = time.time()
        selectedStart = str(selectedStart)
        selectedYear = str(selectedYear)
        result_df1 = rp_riskscauses(data1, data2, data3, data4, selectedStart, selectedYear, country)
        df1_total = df1_total.append(result_df1)
        end_time = time.time()
        print(country, end_time-start_time)
        
df1_total.to_csv(r'top5riskcauses_rp.csv', index=False)


'''
#User parameters
selectedStart = '1990'
selectedYear = '2003'
selectedCountry = 'United States'


rp_current = rp_riskscauses(data1, data2, data3, data4, selectedStart, selectedYear, selectedCountry)

#Find the relationship between risks and causes for escalating health issues
path = 'top5es_riskscauses.csv'
data1 = pd.read_csv(path)
rp_es = rp_riskscauses(data1, data2,selectedYear, selectedCountry)
rp_es.to_csv('top5es_riskcauses_rp.csv', index=False)

#Find the relationship between risks and causes for slow-improving health issues
path = 'top5si_riskscauses.csv'
data1 = pd.read_csv(path)
rp_si = rp_riskscauses(data1, data2,selectedYear, selectedCountry)
rp_si.to_csv('top5si_riskcauses_rp.csv', index=False)
'''
    
