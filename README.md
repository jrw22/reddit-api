# Sentiment Analysis of Financial Firms Using Reddit 
This project is an analysis of a select number of financial firms using Reddit comments posted across a number of subrredits. An end-to-end pipeline has been developed to extract, process and analyse this data. 

## Prerequisites
* Python 3.12 (older versions will probably work)
* Dependencies listed in `requirements.txt`

## Usage
 
Use the 'python main.py' command to run the pipeline. A summary of the function of each file is provided below.

* main.py – the main script that orchestrates the pipeline by calling methods from the other
files.
* api.py – handles the API connection to Reddit.
* get_data.py – handles the data extraction from the Reddit API, including what data is
extracted.
* analytics.py – conducts the machine learning for sentiment analysis, summarisation and
topic modelling.
* database.py – handles connections to the sqlite database and the reading/writing of/to
tables.
