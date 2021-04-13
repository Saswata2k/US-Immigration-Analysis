class Utils:
    def __init__(self, data_immigration=None, data_temp=None, data_us_dem=None, data_airport=None):
        self.data_immigration = data_immigration
        self.data_temp = data_temp
        self.data_us_dem = data_us_dem
        self.data_airport = data_airport

    @staticmethod
    def process_immigration_data(self):
        """
            Pre-Process US immigration dataframe and return
        """
        total_records = self.count()
        print(f'Total records in immigration dataframe: {total_records:,}')

        # From EDA we found our certain columns has 80+% missing data points and hence we drop them
        drop_cols = ['occup', 'entdepu', 'insnum']
        df = self.data_immigration.drop(*drop_cols)

        # drop rows where all elements are missing
        df.dropna(how='all', inplace=True)

        new_total_records = df.count()

        print(f'Total records after cleaning immigration data: {new_total_records:,}')
        return df

    def process_temp_data(self):
        """
            Process global temperatures dataset, handle duplicate values for specific columns
        """
        # drop rows with missing average temperature
        df = self.data_temp.dropna(subset=['AverageTemperature'])

        # drop duplicate rows
        df.drop_duplicates(subset=['dt', 'City', 'Country'], inplace=True)
        return df

    def process_us_demographic_data(self):
        """
            Clean and preprocess the US demographics dataset
        """
        # drop rows with missing values
        subset_cols = [
            'Male Population',
            'Female Population',
            'Number of Veterans',
            'Foreign-born',
            'Average Household Size'
        ]
        df_us_dem = self.data_us_dem.dropna(subset=subset_cols)

        # drop duplicate columns
        df_us_dem.dropDuplicates(subset=['City', 'State', 'State Code', 'Race'], inplace=True)

        rows_dropped_with_duplicates = self.data_us_dem.count() - df_us_dem.count()
        print(f"Total no. of Rows removed after preprocessing : {rows_dropped_with_duplicates}")

        return df_us_dem

    # As we saw from the EDA , Iata_code column has 80% + null values, hence we will be dropping that column
    # drop rows with missing values
    def process_air_traffic_data(self):
        """
            Clean the US demographics dataset
        """
        # drop rows with missing values
        drop_cols = ["iata_code"]
        subset_cols = ['gps_code', 'local_code']
        df_airport = self.data_airport
        data_airport = self.data_airport.drop_duplicates(subset=subset_cols, inplace=True)
        data_airport.drop(labels=drop_cols, axis=1, inplace=True)
        print(f"Total number of rows removed after processing data : {df_airport.shape[0] - data_airport.shape[0]}")
        return data_airport
