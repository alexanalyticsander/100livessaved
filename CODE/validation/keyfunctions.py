# -*- coding: utf-8 -*-
"""
Created on Wed Apr 15 22:23:04 2020
@author: multi
"""

import pandas as pd
from sklearn.cluster import DBSCAN
from copy import deepcopy
import numpy as np

def dbscan(data, eps, min_samples, n_jobs):
    model = DBSCAN(eps=eps, min_samples = min_samples, n_jobs = n_jobs).fit(data)
    label = model.labels_
    dbscan_text = f'DBSCAN epsilon {eps}'
    return label, dbscan_text

def peer_average(result_df, key,country, selectedStart, selectedYear, n_neighbours):
    #Calculate peer_average
    df = result_df[result_df['location_name']!=country]
    result = df.loc[:,selectedYear].groupby(np.arange(len(df))//(n_neighbours-1)).mean()
    results = np.repeat(result,n_neighbours)
    results = results.reset_index(drop=True)
    results = results.rename('endyear_peer_average')
    result_df = result_df.merge(results, left_index=True, right_index=True)
    #Calculate x peer_average
    result_df['endyear_x peer_average'] = result_df[selectedYear]/result_df['endyear_peer_average']
    #Calculate 3 year average rate change
    df = result_df
    df['3-Year avg rateofchange'] = ((df[str(selectedYear)]/df[str(int(selectedYear)-1)]-1)+(df[str(int(selectedYear)-1)]/df[str(int(selectedYear)-2)]-1))/2 * 100
    #Determine if issue is escalating or slow-improving
    result_df = deepcopy(df)
    df = result_df[result_df['location_name']!=country]
    result = df.loc[:,'3-Year avg rateofchange'].groupby(np.arange(len(df))//(n_neighbours-1)).mean()
    results = np.repeat(result,n_neighbours)
    results = results.reset_index(drop=True)
    results = results.rename('pa_rateofchange')
    result_df = result_df.merge(results, left_index=True, right_index=True)
    #Determine category of issues
    df = deepcopy(result_df)
    df.loc[(df['pa_rateofchange']<=0.1) & (df['location_name'] == country),'category'] = 'Slow Improving'
    df.loc[(df['pa_rateofchange']>0.1) & (df['location_name'] == country),'category'] = 'Escalating'
    return df

def format_file_anomaly(result_df, key, key_id, selectedYear, country):
    df = deepcopy(result_df)
    df_columns = ['location_id','location_name', 'start_year', 'end_year', 'category', key_id, key,'endyear_peer_average', 'endyear_x peer_average', '3-Year avg rateofchange','pa_rateofchange']
    df = df.loc[:,df_columns]
    #File 1 - ranking of issues
    file1 = df.loc[(df['location_name']==country) & (df['category']=='Escalating')]
    file1 = file1.sort_values(by ='endyear_x peer_average', ascending = False).reset_index(drop=True)
    file1.insert(5, column = 'rank', value = file1.index+1)
    try:
        file1 = file1.iloc[0:5,:] #Filter to top 5 issues for 'Escalating' category
    except:
        file1 = file1
    file2 = df.loc[(df['location_name']==country) & (df['category']=='Slow Improving')]
    file2 = file2.sort_values(by ='endyear_x peer_average', ascending = False).reset_index(drop=True)
    file2.insert(5, column = 'rank', value = file2.index+1)
    try:
        file2 = file2.iloc[0:5,:] #Filter to top 5 issues for 'Slow-improving' category
    except:
        file2 = file2
    combined_df = pd.DataFrame(columns=df_columns)
    combined_df.insert(5, column = 'rank', value = '0')
    combined_df = combined_df.append(file1)
    combined_df = combined_df.append(file2)
    key_list = combined_df[key].tolist()
    #File 2 - peer countries
    file3_columns = ['start_year','end_year', key_id, key, 'location_id', 'location_name']
    file3 = df.loc[:,file3_columns]
    file3.insert(0, column = 'selected_country', value = country)
    country_id = int(df.loc[df['location_name']==country,'location_id'].values[0])
    file3.insert(0, column = 'selected_countryid', value = country_id)
    file3 = file3[file3[key].isin(key_list)]
    return combined_df, file3

def anomalytrend(data1, data2, data3, selectedStart, selectedYear, country, key, key_list, key_id):
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
        #Suppress the scenario where country is declining faster/increasing slowly relative to other countries
        if selend_index <=min_samples-1: 
            continue
        #Store results
        clustering_labels = {}
        country_label = pd.DataFrame(df_new.loc[:,'location_name']).reset_index(drop=True)
        ep = 1
        count = 'continue'
        while True: #Run algorithm
            label, dbscan_text = dbscan(df_new.loc[:,dimensions], eps = ep, min_samples = min_samples, n_jobs = None)
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
            continue #Continue when no noise elements noted
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
    result_df = peer_average(combined_df, key, country, selectedStart, selectedYear, n_neighbours)
    df1, df2 = format_file_anomaly(result_df, key, key_id, selectedYear, country)
    return df1, df2

def top5(data, selectedStart, selectedYear, selectedCountry, key_id, key):
    #Filter data by key
    key_level = data['location_name']==selectedCountry
    data = data[key_level]
    #Create search columns
    chosen_columns = ['location_id','location_name', key_id, key]
    chosen_columns.append(str(selectedYear)+'_val')
    #Filter daata by chosen columns
    df = data.loc[:,chosen_columns]
    #Find top 5
    top5 = df.nlargest(5,str(selectedYear)+'_val').reset_index(drop=True)
    top5.insert(2, column = 'start_year', value = selectedStart)
    top5.insert(3, column = 'end_year', value = selectedYear)
    top5.insert(4, column = 'category', value = 'current')
    top5.insert(5, column = 'rank', value = top5.index+1)
    del top5[selectedYear+'_val']
    return top5

def format_file_top5(result_df, key, key_id, selectedYear, country):
    df = deepcopy(result_df)
    df_columns = ['location_id','location_name', 'start_year', 'end_year', 'category', key_id, key,'endyear_peer_average', 'endyear_x peer_average', '3-Year avg rateofchange','pa_rateofchange']
    df = df.loc[:,df_columns]
    
    #File 1 - ranking of issues
    file1 = df.loc[df['location_name']==country].reset_index(drop=True)
    file1.insert(5, column = 'rank', value = file1.index+1)
    key_list = file1[key].tolist()
    
    #File 2 - peer countries
    file2_columns = ['start_year','end_year', key_id, key, 'location_id', 'location_name']
    file2 = df.loc[:,file2_columns]
    file2.insert(0, column = 'selected_country', value = country)
    country_id = int(df.loc[df['location_name']==country,'location_id'].values[0])
    file2.insert(0, column = 'selected_countryid', value = country_id)
    file2 = file2[file2[key].isin(key_list)]
    return file1, file2

def peer_average_top5(result_df, key,country, selectedStart, selectedYear, n_neighbours):
    #Calculate peer_average
    df = result_df[result_df['location_name']!=country]
    result = df.loc[:,selectedYear].groupby(np.arange(len(df))//(n_neighbours-1)).mean()
    results = np.repeat(result,n_neighbours)
    results = results.reset_index(drop=True)
    results = results.rename('endyear_peer_average')
    result_df = result_df.merge(results, left_index=True, right_index=True)
    #Calculate x peer_average
    result_df['endyear_x peer_average'] = result_df[selectedYear]/result_df['endyear_peer_average']
    #Calculate 3 year average rate change
    df = result_df
    df['3-Year avg rateofchange'] = ((df[str(selectedYear)]/df[str(int(selectedYear)-1)]-1)+(df[str(int(selectedYear)-1)]/df[str(int(selectedYear)-2)]-1))/2 * 100
    #Determine if issue is escalating or slow-improving
    result_df = deepcopy(df)
    df = result_df[result_df['location_name']!=country]
    result = df.loc[:,'3-Year avg rateofchange'].groupby(np.arange(len(df))//(n_neighbours-1)).mean()
    results = np.repeat(result,n_neighbours)
    results = results.reset_index(drop=True)
    results = results.rename('pa_rateofchange')
    result_df = result_df.merge(results, left_index=True, right_index=True)
    #Determine category of issues
    df = deepcopy(result_df)
    df.insert(5, column = 'category', value = 'current')
    return df

def graphsampler(data1, data2, data3, selectedStart, selectedYear, country, key, key_id):
    #Iterate through data1 to get key_list
    data1 = data1.loc[(data1['start_year']==int(selectedStart))&(data1['end_year']==int(selectedYear)) & (data1['location_name']==country)]
    key_list = data1[key].tolist()
    #Create search columns
    final_columns = ['location_id','location_name', key_id, key]
    chosen_columns = ['location_id','location_name', key_id, key, str(selectedStart)+'_val']
    years_selected = list(range(int(selectedYear)-3,int(selectedYear)+1))
    for year in years_selected:
        final_columns.append(str(year))
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
    combined_df = pd.DataFrame(columns=final_columns)
    combined_df.insert(2, column = 'start_year', value = selectedStart)
    combined_df.insert(3, column = 'end_year', value = selectedYear)
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
        del df_new[selstartcolumn]
        #Append results
        df_new.columns = final_columns
        df_new.insert(2, column = 'start_year', value = selectedStart)
        df_new.insert(3, column = 'end_year', value = selectedYear)
        combined_df = combined_df.append(df_new)
    combined_df = combined_df.reset_index(drop=True)   
    #Calculate peer average
    result_df = peer_average_top5(combined_df, key, country, selectedStart, selectedYear, n_neighbours)
    df1, df2 = format_file_top5(result_df, key, key_id, selectedYear, country)
    return df1, df2

def merge(data1, data2, key_id, key, df_columns):
    combined_df = pd.DataFrame(columns=df_columns)
    combined_df = combined_df.append(data1)
    combined_df = combined_df.append(data2)
    #Sort dataframe
    combined_df.sort_values(['location_name', 'start_year', 'end_year'], ascending=[True, True, True])
    return combined_df

def cast(data, key_id, key):
    #Create search columns
    final_columns = ['location_id','location_name', key_id, key]
    chosen_columns = ['location_id','location_name', key_id, key]
    chosen_columns_lower = ['location_id','location_name', key_id, key]
    chosen_columns_upper = ['location_id','location_name', key_id, key]
    years_selected = list(range(2003,2018))
    for year in years_selected:
        final_columns.append(str(year))
        chosen_columns.append(str(year)+'_val')
        chosen_columns_lower.append(str(year)+'_lower')
        chosen_columns_upper.append(str(year)+'_upper')
    combined_df = pd.DataFrame(columns=final_columns)
    
    #Filter data
    data_val = data.loc[:,chosen_columns]
    data_lower = data.loc[:,chosen_columns_lower]
    data_upper = data.loc[:,chosen_columns_upper]
    
    #Append data
    data_val.columns = final_columns
    data_lower.columns = final_columns
    data_upper.columns = final_columns
    
    combined_df.insert(4, column = 'estimate', value = 'val')
    data_val.insert(4, column = 'estimate', value = 'val')
    data_lower.insert(4, column = 'estimate', value = 'lower')
    data_upper.insert(4, column = 'estimate', value = 'upper')
    
    combined_df = combined_df.append(data_val)
    combined_df = combined_df.append(data_lower)
    combined_df = combined_df.append(data_upper)
    
    #Sort dataframe
    combined_df.sort_values(['location_name', key], ascending=[True, True])
    return combined_df