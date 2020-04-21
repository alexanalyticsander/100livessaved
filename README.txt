# [LivSaver](https://github.gatech.edu/dmaestas3/LivSaver)
## Description
LivSaver is a novel system for visualizing data-driven public health priorities. The dataset used is obtained from the [Global Burden of Disease Study](http://www.healthdata.org/gbd/data). LivSaver applies a segmentation algorithm to segment countries into clusters with similar health risk profiles, and an anomaly detection algorithm to detect anomalous health trends in a selected comparison group. The top anomalous health trends are then visualized via a constructed dashboard in [Tableau](https://www.tableau.com/).

### Authors
C. Elauria, N. Fritter, D. Maestas, M. Ng, A. Song

## Requirements

### To run the algorithms (for the visualization) [optional]
* Alteryx (2019.4)
* Python 3.7 with:
    * pandas 0.24.2
    * sklearn 1.16.4
    * numpy 0.21.2

### To run LivSaver Visualization
* Tableau or Tableau Reader (2020.1)

## Installation
Install [Tableau Reader](https://www.tableau.com/products/reader). After installation, open `code/LivSaver_v1.0.twbx` (or get it from the [Dropbox](https://www.dropbox.com/s/aqjs9wur7nfnqgn/LivSaver_v1.0.twbx?dl=0)) from the root of this repository using Tableau Reader. 

## Execution
We have provided a completely packaged workbook, allowing the end-users to visualize the final product without being forced to run the algorithms. To re-generate the data, refer to the data generation section. To execute LivSaver, simply open the file as shown above in installation.

### To run the algorithms (to prepare data for the visualization) [optional]
#### Raw Data Download (if not using the toy dataset `code/anomaly_detection/data.csv` provided) 
We've provided the steps required to download the raw data from the Global Burden of Disease Study website. 
1. Download CSV from http://ghdx.healthdata.org/gbd-results-tool with the following parameters:
   Base: Single,
   Location: select only countries and territories,
   Year: from 2003 to 2017,
   Context: Risk,
   Age: All Ages,
   Metric: Rate,
   Measure: DALYs (Disability Adjusted Life Years),
   Sex: Both,
   Cause: All causes,
   Risk: select all
2. Download Codebook zip file from http://ghdx.healthdata.org/sites/default/files/ihme_query_tool/IHME_GBD_2017_CODEBOOK.zip
and extract `IHME_GBD_2017_REI_HIERARCHY_Y2018M11D18.XLSX` and `IHME_GBD_2017_CAUSE_HIERARCHY_Y2018M11D18.XLSX`
3. Load the datasets into Alteryx Workflow file named `code/gather/ExtractionFlow.yxmd`
   1. Input Data (3): Dataset CSV from 1.
   2. Input Data (6): IHME_GBD_2017_REI_HIERARCHY_Y2018M11D18.XLSX
   3. Input Data (8): IHME_GBD_2017_CAUSE_HIERARCHY_Y2018M11D18.XLSX
4. Set the "to be saved" location for output file and save file name as `data.csv`

#### Running Segmentation Algorithm (using toy dataset)
The segmentation algorithm first extracts risk values from data.csv. Then, k-means clustering is performed on principal components of normalized data.
1. Make sure that `data.csv` for data collection and `code/anomaly_detection/risks_causes_leafnodes.csv` are available in the same filepath as `code/anomaly_detection/clustering.py.`
2. Run `code/anomaly_detection/clustering.py` to obtain the following final file output (to ignore any other .csv outputs):
   ```
   kmeans_pca_labels.csv
   ```

#### Running Anomaly Detection 
The anomaly detection takes the clusters from the previous section and runs anomaly detection algorithm over the diseases, injuries and risk factors for each country. It then determines the top 5 current, escalating and slow improving issues.
1. Run `code/anomaly_detection/cause.py` to obtain the following files: 
   ```
   causes_top5.csv, causes_peers.csv, cause_daly_rate_abridged(cast).csv. 
   ```
   (Any other .csv output can be safely ignored)
2.  Run `code/anomaly_detection/risk.py` to obtain the following files: 
    ```
    risks_top5.csv, risks_peers.csv, risk_daly_rate_abridged(cast).csv.
    ```
   (Any other .csv output can be safely ignored)   
3. Run `code/anomaly_detection/rp_causes_risks.py` to obtain the following file: `top5riskcauses_rp.csv`

#### Transforming the data for Tableau Visualization [optional]
*Note that this data has been pre-generated and exists in the `code/LivSaver_v1.0.twbx` file.
The steps below transforms the output files from the algorithms to be in a format suitable for visualization in Tableau.
1. Load the output files from the algorithms into the corresponding input streams (input stream number shown in brackets) in Alteryx Workflow file named `code/visualization/Transformation.yxmd`
   1. Input Data (4): risks_top5.csv
   3. Input Data (5): causes_top5.csv
   4. Input Data (8): top5riskcauses_rp.csv
   5. Input Data (14): risks_peers.csv
   6. Input Data (22): risk_daly_rate_abridged(cast).csv
   7. Input Data (16): causes_peers.csv
   8. Input Data (23): cause_daly_rate_abridged(cast).csv
   9. Input Data (27): cause_daly_rate_abridged(cast).csv
   10. Input Data (26): risk_daly_rate_abridged(cast).csv
2. Set Save As locations for all output files and obtain:
   ```
   RiskCause_Peers_DALY.csv, RiskCause_Top5_Rel.csv, risk_daly_rate(cast)_lower.csv, risk_daly_rate(cast)_val.csv, risk_daly_rate(cast)_upper.csv, cause_daly_rate(cast)_lower.csv, cause_daly_rate(cast)_val.csv, cause_daly_rate(cast)_upper.csv
   ```
3. Edit connection of data sources in Tableau to replace datasets.

#### Validation of Anomaly Detection Algorithm
LivSaver's anomaly detection algorithm is validated by generating the ratechange results and LivSaver results, and visually inspects its results. The following code files requires cause_daly_rate.csv, kmeans_pca_labels.csv and risks_causes_leafnodes.csv, which can be obtained from code/anomaly_detection/data.csv. 
1. Run `code/validation/ratechange_val.py` to obtain the following files: 
   ```
   rateofchange_us20032017.csv, rateofchange_indonesia20042016.csv, rateofchange_zimbabwe20052017.csv. 
   ``` 
   (Any other .csv output can be safely ignored)     
2. Run `code/validation/algo_val.py` to obtain the following files: 
   ```
   algo_val_us_20032017.csv, algo_val_indonesia_20042016.csv, algo_val_zimbabwe_20052017.csv
   ```
   (Any other .csv output can be safely ignored) 
3. Import these data files into Tableau for visual inspection of the graphs by independent coders
