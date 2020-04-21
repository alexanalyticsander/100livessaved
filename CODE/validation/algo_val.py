# -*- coding: utf-8 -*-
"""
Created on Thu Apr 16 15:58:44 2020
@author: multi
"""
import pandas as pd
from copy import deepcopy
from keyfunctions import dbscan

def anomalytrend_val(data1, data2, data3, selectedStart, selectedYear, country, key, key_list, key_id):
    #Remove nan in key_list
    key_list = [x for x in key_list if str(x) != 'nan']
    #Create search columns
    final_columns = ['location_id','location_name', key_id, key]
    chosen_columns = ['location_id','location_name', key_id, key]
    years_selected = list(range(int(selectedStart),int(selectedYear)+1))
    dimensions = []
    for year in years_selected:
        final_columns.append(str(year))
        chosen_columns.append(str(year)+'_val')
        dimensions.append(str(year)+'_val')
    data2 = data2.loc[:,chosen_columns]
    #Filter data by clusters
    cluster_label = int(data3.loc[data3['location']==country,selectedStart].values)
    countries_data = data3.loc[data3[selectedStart]==cluster_label]
    countries = countries_data.loc[:,'location'].tolist()
    data2 = data2[data2['location_name'].isin(countries)]
    #Set parameters for n nearest neighbours and min samples
    if len(countries)<25:
        n_neighbours = 5
        min_samples = 3
    elif len(countries) >= 50:
        n_neighbours = 9
        min_samples=6
    else:
        n_neighbours = 7
        min_samples = 4
    #Threshold for causes to be tested
    threshold = 100
    selstartcolumn = str(selectedStart)+'_val'
    selendcolumn = str(selectedYear)+'_val'
    #Iterate through key_list
    combined_df = pd.DataFrame(columns=final_columns)
    combined_df.insert(2, column = 'start_year', value = selectedStart)
    combined_df.insert(3, column = 'end_year', value = selectedYear)
    for k in key_list:
        df = deepcopy(data2)
        df = df[df[key] == k]
        selstart_value = df.loc[df['location_name']==country, selstartcolumn].values
        selend_value = df.loc[df['location_name']==country, selendcolumn].values
        if selend_value <= threshold: #Suppress low priority issues
            continue
        #Compute Manhattan distance and find the nearest neighbours (n_neighbours in cluster)
        df['comp_value'] = abs(df[selstartcolumn] - selstart_value)
        df = df.sort_values('comp_value').reset_index(drop=True)
        df_int = df['comp_value']
        #Filter away scenarios where neighbours are far from baseline values
        if df_int.iloc[int(n_neighbours/2)] >= 0.5*selstart_value:
            continue
        index_list = list(range(0,n_neighbours))
        df_new = df.iloc[index_list,:]
        del df_new['comp_value']
        df_new = df_new.sort_values(selendcolumn).reset_index(drop=True)
        selend_index = df_new[df_new['location_name']==country].index[0]
        if selend_index <=min_samples-1: #Suppress 3rd scenario issues
            continue
        #Store results
        clustering_labels = {}
        country_label = pd.DataFrame(df_new.loc[:,'location_name']).reset_index(drop=True)
        ep = 1
        count = 'continue'
        while True: #Run algorithm
            label, dbscan_text = dbscan(df_new.loc[:,dimensions], eps = ep, min_samples = min_samples, n_jobs=None)
            clustering_labels[dbscan_text] = label
            
            if all(label==0): #when criteria met, decrease epsilon by 1
                while True:
                    ep-=1
                    label, dbscan_text = dbscan(df_new.loc[:,dimensions], eps = ep, min_samples = min_samples, n_jobs=None)
                    clustering_labels[dbscan_text] = label
                    if any(label!=0): #break when convergence criteria met
                        count = 'break'
                        break
            if count == 'break':
                break
            ep +=50
        try:
            assigned_label = country_label.join(pd.DataFrame(clustering_labels['DBSCAN epsilon '+str(ep)]))
        except:
            continue  #Continue when no noise elements noted
        assigned_label.columns = ['location_name', 'result']
        result = assigned_label.loc[assigned_label['location_name'] == country, 'result'].values
        if (result == -1) and assigned_label.result.unique().shape[0] != 1:
            df_new.columns = final_columns
            df_new.insert(2, column = 'start_year', value = selectedStart)
            df_new.insert(3, column = 'end_year', value = selectedYear)
            combined_df = combined_df.append(df_new)
    combined_df = combined_df.reset_index(drop=True)
    if combined_df.empty:
        return

    return combined_df

##To ensure pandas display "normal" numbers rather than scientific notation
pd.set_option('display.float_format', lambda x: '%.5f' % x)

##Data Validation
#Read data
path1 = 'risks_causes_leafnodes.csv'
data1 = pd.read_csv(path1)
path2 = 'cause_daly_rate.csv'
data2 = pd.read_csv(path2)
path3 = 'kmeans_pca_labels.csv'
data3 = pd.read_csv(path3)
##Country1 - 2003 to 2017
country = 'United States'
selectedStart = '2003'
selectedYear = '2017'
key = 'cause_name'
key_id = 'cause_id'
key_list = data1['Causes'].tolist()
###Run trend detection algorithm
result_us = anomalytrend_val(data1, data2, data3, selectedStart, selectedYear, country, key, key_list, key_id)
result_us.to_csv(r'algo_val_us_20032017.csv', index=False)
##Country2 - 2004 to 2016
country = 'Indonesia'
selectedStart = '2004'
selectedYear = '2016'
key = 'cause_name'
key_id = 'cause_id'
key_list = data1['Causes'].tolist()
###Run trend detection algorithm
result_indonesia = anomalytrend_val(data1, data2, data3, selectedStart, selectedYear, country, key, key_list, key_id)
result_indonesia.to_csv(r'algo_val_indonesia_20042016.csv', index=False)
##Country3 - 2005 to 2017
country = 'Zimbabwe'
selectedStart = '2005'
selectedYear = '2017'
key = 'cause_name'
key_id = 'cause_id'
key_list = data1['Causes'].tolist()
###Run trend detection algorithm
result_zimbabwe = anomalytrend_val(data1, data2, data3, selectedStart, selectedYear, country, key, key_list, key_id)
result_zimbabwe.to_csv(r'algo_val_zimbabwe_20052017.csv', index=False)