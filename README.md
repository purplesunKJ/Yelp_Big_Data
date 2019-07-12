# Yelp_Big_Data

In this project, an end-to-end big data processing pipeline using the Hadoop Framework has been presented. There are 5 fundamental elements in the processing pipeline which included Data Ingestion, Data Storage, Data Processing, Data Querying and Data Visualization. Our data architecture is catered to the dataset retrieved from Yelp, a local search service powered by crowd sourced review forum run by an American Multinational Corporation headquartered in San Francisco, California (https://www.yelp.com/). 

The dataset consists of Yelp reviews and check-in (both live and historical data), in both CSV and JSON format and to accommodate both live and historical data, we adopted the Lambda Architecture, which consists of both Batch Processing Layer (for historical dataset) and Speed Processing Layer (for live streaming of incoming data). Data visualization is done using Tableau (https://www.tableau.com/) connected to the Cloudera Impala.
