# -*- coding: utf-8 -*-
"""
Created on Thu Apr 16 14:14:06 2020

@author: multi
"""

from keyfunctions import anomalytrend, top5, graphsampler, merge, cast
import pandas as pd
import time

##To ensure pandas display "normal" numbers rather than scientific notation
pd.set_option('display.float_format', lambda x: '%.5f' % x)

#Run for the following start_year-end_year combinations (minimum 10 years of data)
start_year_list = list(range(2003,2010)) #Years are set to be restricted to toy dataset
end_year_list = list(range(2010, 2018))
year_comb = []
for start_year in start_year_list:
    for end_year in end_year_list:
        if end_year-start_year<9:
            continue
        year_comb.append((start_year, end_year))
        
##To find Top 5 Escalating and Slow Improving Risks (Anomaly trend detection algorithm)
#Read data
path1 = 'risks_causes_leafnodes.csv'
data1 = pd.read_csv(path1)
path2 = 'risk_daly_rate.csv'
data2 = pd.read_csv(path2)
path3 = 'kmeans_pca_labels.csv'
data3 = pd.read_csv(path3)
path4 = 'cause_daly_rate.csv'
data4 = pd.read_csv(path4)
countries = data4.location_name.unique().tolist()
cause_level = data2['cause_name']=='All causes' #All causes
data2 = data2[cause_level]
key = 'rei_name'
key_id = 'rei_id'
key_list = data1['Risk'].tolist()
#Initialize dataframes
df1_total_columns = ['location_id','location_name', 'start_year', 'end_year', 'category', 'rank', key_id, key,'endyear_peer_average', 'endyear_x peer_average', '3-Year avg rateofchange','pa_rateofchange']
df2_total_columns = ['selected_countryid', 'selected_country','start_year','end_year', key_id, key, 'location_id', 'location_name']
df1_total = pd.DataFrame(columns=df1_total_columns)
df2_total = pd.DataFrame(columns=df2_total_columns)
for country in countries:
    for selectedStart, selectedYear in year_comb:
        start_time = time.time()
        selectedStart = str(selectedStart)
        selectedYear = str(selectedYear)
        try:
            result_df1, result_df2 = anomalytrend(data1, data2, data3, selectedStart, selectedYear, country, key, key_list, key_id)
        except:
            continue
        df1_total = df1_total.append(result_df1)
        df2_total = df2_total.append(result_df2)
        end_time = time.time()
        print(country, end_time-start_time)
df1_total.to_csv(r'risks_anomaly_top5.csv', index=False)
df2_total.to_csv(r'risks_anomaly_peer.csv', index=False)

##To find Top 5 Current Risks
path = 'risks_causes_leafnodes.csv'
data1 = pd.read_csv(path)
path2 = 'risk_daly_rate.csv'
data2 = pd.read_csv(path2)
#Obtain 67 leaf node risk-dimensions
risk_dimensions = data1['Risk'].unique()
cause_level = data2['cause_name']=='All causes' #All causes
data = data2[cause_level]
data = data[data['rei_name'].isin(risk_dimensions)] 
key = 'rei_name'
key_id = 'rei_id'
#Run iterations
df1_total_columns = ['location_id','location_name', 'start_year', 'end_year', 'category', 'rank', key_id, key]
df1_total = pd.DataFrame(columns=df1_total_columns)
for country in countries:
    for selectedStart, selectedYear in year_comb:
        start_time = time.time()
        selectedStart = str(selectedStart)
        selectedYear = str(selectedYear)
        top5risks = top5(data, selectedStart, selectedYear, country, key_id, key)
        end_time = time.time()
        print(country, end_time-start_time)
        df1_total = df1_total.append(top5risks)
df1_total.to_csv('top5risks.csv', index=False)


##To find similar peers (for line chart) for top 5 current risks
#Read data
path1 = 'top5risks.csv'
data1 = pd.read_csv(path1)
path2 = 'risk_daly_rate.csv'
data2 = pd.read_csv(path2)
path3 = 'kmeans_pca_labels.csv'
data3 = pd.read_csv(path3)
#Run for all countries for all year combinations(risks)
cause_level = data2['cause_name']=='All causes' #All causes
data2 = data2[cause_level]
key = 'rei_name'
key_id = 'rei_id'
#Initialize dataframes
df1_total_columns = ['location_id','location_name', 'start_year', 'end_year', 'category', 'rank', key_id, key,'endyear_peer_average', 'endyear_x peer_average', '3-Year avg rateofchange','pa_rateofchange']
df2_total_columns = ['selected_countryid', 'selected_country','start_year','end_year', key_id, key, 'location_id', 'location_name']
df1_total = pd.DataFrame(columns=df1_total_columns)
df2_total = pd.DataFrame(columns=df2_total_columns)
for country in countries:
    for selectedStart, selectedYear in year_comb:
        start_time = time.time()
        selectedStart = str(selectedStart)
        selectedYear = str(selectedYear)
        result_df1, result_df2 = graphsampler(data1, data2, data3, selectedStart, selectedYear, country, key, key_id)
        df1_total = df1_total.append(result_df1)
        df2_total = df2_total.append(result_df2)
        end_time = time.time()
        print(country, end_time-start_time)
df1_total.to_csv('risks_current_top5.csv', index=False)
df2_total.to_csv('risks_current_peers.csv',index=False)

##Merge top5 results for anomaly and current
path1 = 'risks_current_top5.csv'
data1 = pd.read_csv(path1)
path2 = 'risks_anomaly_top5.csv'
data2 = pd.read_csv(path2)
key = 'rei_name'
key_id = 'rei_id'
df_columns = ['location_id','location_name', 'start_year', 'end_year', 'category', 'rank', key_id, key,'endyear_peer_average', 'endyear_x peer_average', '3-Year avg rateofchange','pa_rateofchange']
result_df = merge(data1, data2, key_id, key, df_columns)
result_df.to_csv('risks_top5.csv', index=False)

##Merge peers results for anomaly and current
path1 = 'risks_current_peers.csv'
data1 = pd.read_csv(path1)
path2 = 'risks_anomaly_peer.csv'
data2 = pd.read_csv(path2)
key = 'rei_name'
key_id = 'rei_id'
df_columns = ['selected_countryid', 'selected_country', 'start_year','end_year', key_id, key, 'location_id', 'location_name']
result_df = merge(data1, data2, key_id, key, df_columns)
result_df.to_csv('risks_peers.csv', index=False)

##Cast raw risk data file for visualization
#Casting the risk_daly_rate.csv and abridging it
key = 'rei_name'
key_id = 'rei_id'
path = 'risk_daly_rate.csv'
data= pd.read_csv(path)
cause_level = data['cause_name']=='All causes' #All causes
data = data[cause_level]
#Create search columns
chosen_columns = ['location_id','location_name', key_id, key]
years_selected = list(range(2003,2018))
for year in years_selected:
    chosen_columns.append(str(year)+'_val')
    chosen_columns.append(str(year)+'_lower')
    chosen_columns.append(str(year)+'_upper')
data = data.loc[:,chosen_columns]
result_df = cast(data, key_id, key)
result_df.to_csv('risk_daly_rate_abridged(cast).csv', index=False)
