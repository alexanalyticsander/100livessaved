# -*- coding: utf-8 -*-
"""
Created on Tue Mar 17 11:18:43 2020

@author: multi
"""

import numpy as np
import pandas as pd
from sklearn.neighbors import KNeighborsClassifier
import sklearn.preprocessing as skpp
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn import metrics
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

##To ensure pandas display "normal" numbers rather than scientific notation
pd.set_option('display.float_format', lambda x: '%.5f' % x)

##Running clustering functions
#Read data
path = 'risks_causes_leafnodes.csv'
data1 = pd.read_csv(path)
path2 = 'data.csv'
data2 = pd.read_csv(path2)
#Obtain 67 leaf node risk-dimensions
riskdimensions = data1['Risk'].dropna()
riskdimensions = np.append(riskdimensions, 'Unsafe sex')
riskdimensions = np.append(riskdimensions, 'Occupational injuries')
cause_level = data2['cause_level']==0 #All causes
data = data2[cause_level]
data = data[data['risk'].isin(riskdimensions)]
#obtain years included in data
cols = list(data.columns)
yearcols = []
for i in range(len(cols)):
    if cols[i].isdigit() and int(cols[i]) in list(range(1990,2018)):
        yearcols.append(cols[i])
    else:
        pass

clustering_labels = {}
clustering_scores = {}
distances = {}
variance_ratio = pd.DataFrame()

for year in yearcols:
    #Choose clustering dimensions (key = dimension, value = year)
    key = "risk"
    value = str(year)
    df = data.loc[:,['location', key, value]]
    #Cast data such that clustering dimensions become column headers
    fixed_vars = df.columns.difference([key, value])
    tibble = pd.DataFrame(columns=fixed_vars)
    new_vars = df[key].unique()
    new_vars.sort()
    for v in new_vars:
        df_v = df[df[key]==v]
        del df_v[key]
        df_v = df_v.rename(columns = {value:v})
        tibble = tibble.merge(df_v, on=list(fixed_vars),how="right")
    dim_data = tibble
    #Normalize data
    pre_norm_data = dim_data.loc[:,riskdimensions]
    countries = dim_data.loc[:,'location']
    normalized_data = skpp.normalize(pre_norm_data, norm='l2')
    new_data = pd.DataFrame(normalized_data, columns=riskdimensions)
    new_data = new_data.join(countries,how="inner")
    normalize_data = new_data

    cols = list(normalize_data.columns)
    cols = [cols[-1]] + cols[:-1]
    normalize_data = normalize_data[cols]

    '''Dimension Reduction'''
    '''Principal Component Analysis'''
    threshold = 5 #Only keep components that explain >= to 5% of the variance
    ##Principal Component Analysis with normalized data
    print("\nyear",year,": Principal Component Analysis (Normalized data)")

    pre_pca_data = normalize_data.loc[:,riskdimensions]
    n_components =8
    model = PCA(n_components).fit(pre_pca_data)
    initial_variance_ratio = model.explained_variance_ratio_
    initial_variance = model.explained_variance_
    #New model after deciding how many components to keep
    n_components_tokeep = sum(initial_variance_ratio >= (threshold/100))
    variance_ratio[year] = initial_variance_ratio[0:4]
    model2 = PCA(n_components_tokeep).fit(pre_pca_data)
    new_variance_ratio = model2.explained_variance_ratio_
    new_variance = model2.explained_variance_
    #Transform data
    transformed_data = PCA(n_components_tokeep).fit_transform(pre_pca_data)
    newdim_columns = []
    for i in range(1,n_components_tokeep+1):
        newdim_columns.append('PC'+str(i))    
    key_table = pd.DataFrame(countries)
    data_table = pd.DataFrame(transformed_data, columns=newdim_columns)
    new_table = key_table.merge(data_table,left_index=True, right_index=True)
    new_datatable = new_table

    #calculate loadings
    loadings = model2.components_.T * np.sqrt(model2.explained_variance_)
    key_table = pd.DataFrame(riskdimensions)
    data_table = pd.DataFrame(loadings, columns=newdim_columns)
    new_table = key_table.merge(data_table,left_index=True, right_index=True)
    loadings_table = new_table

    #print results
    print(f"Initial variance and its ratio ({n_components} components): {initial_variance},{initial_variance_ratio}")
    print(f"Number of components that meet threshold {threshold}%: {n_components_tokeep}")
    print(f"New variance and its ratio ({n_components_tokeep} components): {new_variance}, {new_variance_ratio}")
    #return transformed data
    pca_data = new_datatable
    pca_loadings = loadings_table

    '''Clustering'''
    #metrics used:
    #Silhouette Coefficient (SC): higher score relates to model with better defined clusters
    #Calinski-Harabsz Index(CHI): higher score relates to model with better defined clusters
    #Davies-Boudlin Index(DBI): lower score relates to model with better partitions
    '''K-Means Clustering'''
    dimensions = []
    for i in range(1,n_components_tokeep+1):
        dimensions.append('PC'+str(i))        

    x_index = 0
    y_index = 1
    pre_km_data = pca_data.loc[:,dimensions]
    country_label = pd.DataFrame(countries)
    country_label.columns = ['location']

    model = KMeans(n_clusters=5).fit(pre_km_data)
    dist = model.transform(pre_km_data)
    dist = pd.DataFrame(dist)

    label = model.labels_
    clustering_labels[str(year)] = label
    
    for row in range(len(dist)):
        dist.loc[row,'distance'] = dist.loc[row, label[row]]
    distances[str(year)] = dist['distance']
    counts_table = pd.value_counts(label).to_frame().reset_index()

    try:
        sc_score = metrics.silhouette_score(pre_km_data, label, metric='euclidean')
    except:
        sc_score = 999
    try:
        chi_score = metrics.calinski_harabasz_score(pre_km_data, label)
    except:
        chi_score = 999
    try:
        db_score = metrics.davies_bouldin_score(pre_km_data, label)
    except:
        db_score = 999
    scores = [sc_score, chi_score, db_score]
    clustering_scores[str(year)] = scores
    
    assigned_label = country_label.copy()
    assigned_label['label'] = label
    data_kmeans = pca_data.merge(assigned_label, on='location',how="right")
    
    #fig = plt.figure()
    #ax = fig.add_subplot(111, projection='3d')
    #ax.set_xlabel('PC1')
    #ax.set_ylabel('PC2')
    #ax.set_zlabel('PC3', labelpad=1)
    #ax.zaxis.label.set_rotation(90)
    #index=0
    #clusters_list=assigned_label.columns[1:]
    #x_axis = dimensions[0]
    #y_axis = dimensions[1]
    #z_axis = dimensions[2]
    #fig.text(0.5, 0.04, x_axis, ha='center', va='center')
    #fig.text(0.06, 0.5, y_axis, ha='center', va='center', rotation='vertical')
    #ax.scatter(data_kmeans[x_axis], data_kmeans[y_axis], data_kmeans[z_axis], c=data_kmeans[clusters_list[index]])
    #plt.show()

distance_table = country_label.assign(**distances)
kmeans_pca_label = country_label.assign(**clustering_labels)
kmeans_pca_scores = pd.DataFrame(clustering_scores.items(), columns=['model', 'scores'])
kmeans_pca_scores[['Silhouette Coefficient', 'Calinski-Harabsz Index', 'Davies-Boudlin Index']] = pd.DataFrame(kmeans_pca_scores.scores.values.tolist(), index= kmeans_pca_scores.index)

variance_ratio.to_csv('pca_variance_ratios.csv', index=False)
distance_table.to_csv('node_centroid_distance.csv', index=False)
kmeans_pca_label.to_csv('kmeans_pca_labels.csv', index=False) 
kmeans_pca_scores.to_csv('kmeans_pca_scores.csv', index=False)

