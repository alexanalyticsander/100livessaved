# -*- coding: utf-8 -*-
"""
Created on Sat Mar 28 16:47:51 2020
@author: multi
"""
import pandas as pd
from copy import deepcopy

##When selecting by country for causes
##Running clustering functions
#Read data
path1 = 'risks_causes_leafnodes.csv'
data1 = pd.read_csv(path1)
path2 = 'cause_daly_rate.csv'
data2 = pd.read_csv(path2)
path3 = 'kmeans_pca_labels.csv'
data3 = pd.read_csv(path3)


def ratechange(data, selectedYear):
    chosen_columns = ['location_name','cause_name']
    years_selected = list(range(int(selectedStart),int(selectedYear)+1))
    for year in years_selected:
        chosen_columns.append(str(year)+'_val')
    df = deepcopy(data)
    df= df.loc[:,chosen_columns]
    df['3-Year Average Rate of Change'] = ((df[str(selectedYear)+'_val']/df[str(int(selectedYear)-1)+'_val']-1)+(df[str(int(selectedYear)-1)+'_val']/df[str(int(selectedYear)-2)+'_val']-1))/2 * 100
    df = df.sort_values(by ='3-Year Average Rate of Change', ascending = False).reset_index(drop=True)
    df = df.loc[df['3-Year Average Rate of Change'] >= 2,:]
    return df

def graphsampler_val(data1, data2, data3, selectedStart, selectedYear, country, key):
    #Iterate through data1 to get key_list
    key_list = data1[key].tolist()
    #Create search columns
    chosen_columns = ['location_name', key, str(selectedStart)+'_val']
    years_selected = list(range(int(selectedStart)+1,int(selectedYear)+1))
    for year in years_selected:
        chosen_columns.append(str(year)+'_val')
    data2 = data2.loc[:,chosen_columns]
    #Filter data by clusters
    cluster_label = int(data3.loc[data3['location']==country,selectedStart].values)
    countries_data = data3.loc[data3[selectedStart]==cluster_label]
    countries = countries_data.loc[:,'location'].tolist()
    data2 = data2[data2['location_name'].isin(countries)]
    #Set parameters for n nearest neighbours and min samples
    if len(countries)<25:
        n_neighbours = 5
    elif len(countries) >= 50:
        n_neighbours = 9
    else:
        n_neighbours = 7
    #Create dataframe to append results
    combined_df = pd.DataFrame(columns=chosen_columns)
    #Intialize columns
    selstartcolumn = str(selectedStart)+'_val'
    #Run iterations
    for k in key_list:
        df = deepcopy(data2)
        df = df[df[key] == k]
        selstart_value = df.loc[df['location_name']==country, selstartcolumn].values
        #Compute Manhattan distance and find the nearest neighbours (n_neighbours in cluster)
        df['comp_value'] = abs(df[selstartcolumn] - selstart_value)
        df = df.sort_values('comp_value').reset_index(drop=True)
        index_list = list(range(0,n_neighbours))
        df_new = df.iloc[index_list,:]
        del df_new['comp_value']
        #Append results
        combined_df = combined_df.append(df_new)
    combined_df = combined_df.reset_index(drop=True)   
    return combined_df

###Pick countries
#Country 1 (2003 to 2017)
selectedDim = 'location_name'
dim_value = 'United States'
selectedStart = '2003'
selectedYear = '2017'
key = 'cause_name'
key_list = data1['Causes'].tolist()
threshold = 100
data2 = data2[data2['cause_name'].isin(key_list)]
data2 = data2.loc[data2['location_name'] == dim_value, :]
data2 = data2.loc[data2[selectedYear + '_val'] >threshold, :]
results_us = ratechange(data2, selectedYear)
results_us.to_csv(r'rateofchange_us20032017_rater.csv', index=False)
#Find similar peers in cluster for graph
data2 = pd.read_csv(path2)
results_us = graphsampler_val(results_us, data2, data3, selectedStart, selectedYear, dim_value, key)
results_us.to_csv(r'rateofchange_us20032017.csv', index=False)
#Country 2 (2004 to 2016)
selectedDim = 'location_name'
dim_value = 'Indonesia'
selectedStart = '2004'
selectedYear = '2016'
key = 'cause_name'
key_list = data1['Causes'].tolist()
threshold = 100
data2 = data2[data2['cause_name'].isin(key_list)]
data2 = data2.loc[data2['location_name'] == dim_value, :]
data2 = data2.loc[data2[selectedYear + '_val'] >threshold, :]
results_indonesia = ratechange(data2, selectedYear)
results_indonesia.to_csv(r'rateofchange_indonesia20042016_rater.csv', index=False)
#Find similar peers in cluster for graph
data2 = pd.read_csv(path2)
results_indonesia = graphsampler_val(results_indonesia, data2, data3, selectedStart, selectedYear, dim_value, key)
results_indonesia.to_csv(r'rateofchange_indonesia20042016.csv', index=False)

#Country 3 (2005 to 2017)
selectedDim = 'location_name'
dim_value = 'Zimbabwe'
selectedStart = '2005'
selectedYear = '2017'
key = 'cause_name'
key_list = data1['Causes'].tolist()
threshold = 100
data2 = data2[data2['cause_name'].isin(key_list)]
data2 = data2.loc[data2['location_name'] == dim_value, :]
data2 = data2.loc[data2[selectedYear + '_val'] >threshold, :]
results_zimbabwe = ratechange(data2, selectedYear)
results_zimbabwe.to_csv(r'rateofchange_zimbabwe2005to2017_rater.csv', index=False)
#Find similar peers in cluster for graph
data2 = pd.read_csv(path2)
results_zimbabwe = graphsampler_val(results_zimbabwe, data2, data3, selectedStart, selectedYear, dim_value, key)
results_zimbabwe.to_csv(r'rateofchange_zimbabwe20052017.csv', index=False)