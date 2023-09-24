import requests
import zipfile
import os
import pandas as pd

class ADEILoader:
    
    def __init__(self):
        self.dfs = {}
        self.time_series_dfs = {}

    @staticmethod
    def _load_csv(file_path):
        """Load a CSV file into a pandas DataFrame."""
        return pd.read_csv(file_path)
    
    @staticmethod
    def _load_zip(zip_path):
        """Extract ZIP and load CSV files into pandas DataFrames."""
        dfs = {}
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall('extracted_files')
        csv_files = [f for f in os.listdir('extracted_files') if f.endswith('.csv')]
        for csv_file in csv_files:
            file_path = os.path.join('extracted_files', csv_file)
            dfs[csv_file] = ADEILoader._load_csv(file_path)
        return dfs
    
    def download_and_process(self, url):
        """Download data from the provided URL and process it."""
        response = requests.get(url)
        file_name = 'downloaded_data_temp'
        with open(file_name, 'wb') as f:
            f.write(response.content)
        
        # Check if the file is a ZIP archive or a single CSV
        if zipfile.is_zipfile(file_name):
            self.dfs = self._load_zip(file_name)
        else:
            self.dfs = {file_name: self._load_csv(file_name)}
        
        # Splitting CSV files with multiple columns into several data frames
        for csv_file, df in self.dfs.items():
            timestamp_column = df.columns[0]  # Assuming the first column is always the timestamp
            for column in df.columns[1:]:  # Skip the timestamp column
                #key = f'{csv_file}_{column}'
                key = f'{column}'
                self.time_series_dfs[key] = df[[timestamp_column, column]]
        
        return self.time_series_dfs
    
    def get_time_series(self, key):
        """Retrieve a specific time series DataFrame based on the key."""
        return self.time_series_dfs.get(key, None)


def ADEIData(url):
    loader = ADEILoader()
    return loader.download_and_process(url)
