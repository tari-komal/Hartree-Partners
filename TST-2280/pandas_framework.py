import pandas as pd
import logging

class DataProcessor:
    """
    A class for processing datasets using the Pandas framework.

    Attributes:
        dataset1_path (str): File path of the first dataset.
        dataset2_path (str): File path of the second dataset.
        result_file (str): File path to save the processed result.
        logger (logging.Logger): Logger instance for logging messages.
    """

    def __init__(self, dataset1_path, dataset2_path, result_file):
        """
        Initialize the DataProcessor instance.

        Args:
            dataset1_path (str): File path of the first dataset.
            dataset2_path (str): File path of the second dataset.
            result_file (str): File path to save the processed result.
        """
        self.dataset1_path = dataset1_path
        self.dataset2_path = dataset2_path
        self.result_file = result_file
        self.logger = self._setup_logger()

    def _setup_logger(self):
        """
        Set up the logger configuration.

        Returns:
            logging.Logger: Logger instance.
        """
        logger = logging.getLogger('DataProcessor')
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
        return logger

    def load_datasets(self):
        """
        Load the datasets from the specified file paths.

        Raises:
            Exception: If an error occurs while loading the datasets.
        """
        try:
            self.logger.info("Loading datasets...")
            self.df1 = pd.read_csv(self.dataset1_path)
            self.df2 = pd.read_csv(self.dataset2_path)
            self.logger.info("Datasets loaded successfully.")
        except Exception as e:
            self.logger.error(f"Error loading datasets: {str(e)}")

    def drop_invoice_id(self):
        """
        Drop the 'invoice_id' column from the first dataset.

        Raises:
            Exception: If an error occurs while dropping the column.
        """
        try:
            self.logger.info("Dropping 'invoice_id' column...")
            self.df1 = self.df1.drop('invoice_id', axis=1)
            self.logger.info("'invoice_id' column dropped successfully.")
        except Exception as e:
            self.logger.error(f"Error dropping 'invoice_id' column: {str(e)}")

    def apply_data_transformation(self):
        """
        Apply data transformation to the first dataset.

        Raises:
            Exception: If an error occurs while applying the transformation.
        """
        try:
            self.logger.info("Applying data transformation...")
            arap_df = self.df1[self.df1['status'] == 'ARAP']
            accr_df = self.df1[self.df1['status'] == 'ACCR']

            arap_df_group = arap_df.groupby(['legal_entity', 'counter_party']).agg({'value': 'sum', 'rating': 'max'}).rename(columns={'value': 'arap_value'})
            accr_df_group = accr_df.groupby(['legal_entity', 'counter_party']).agg({'value': 'sum', 'rating': 'max'}).rename(columns={'value': 'accr_value'})

            self.df_merged = pd.merge(arap_df_group, accr_df_group, on=['legal_entity', 'counter_party'], how='outer')
            self.df_merged = self.df_merged.rename(columns={'rating_x': 'max_rating_x', 'arap_value_x': 'arap_value', 'arap_value_y': 'arap_value', 'rating_y': 'max_rating_y'})
            self.df_merged = self.df_merged.reset_index()
            self.logger.info("Data transformation applied successfully.")
        except Exception as e:
            self.logger.error(f"Error applying data transformation: {str(e)}")

    def merge_with_dataset2(self):
        """
        Merge the transformed dataset with the second dataset.

        Raises:
            Exception: If an error occurs while merging the datasets.
        """
        try:
            self.logger.info("Merging with 'dataset2'...")
            self.df_final = self.df_merged.merge(self.df2, on=['counter_party'])
            self.df_final['ratings'] = self.df_final[['max_rating_x', 'max_rating_y']].max(axis=1)
            self.df_final.fillna(0, inplace=True)
            self.df_final_1 = self.df_final.drop(['max_rating_x', 'max_rating_y'], axis=1)
            self.df_final_1 = self.df_final_1[['legal_entity', 'counter_party', 'tier', 'ratings', 'arap_value', 'accr_value']]
            self.logger.info("'dataset1' merged with 'dataset2' successfully.")
        except Exception as e:
            self.logger.error(f"Error merging with 'dataset2': {str(e)}")

    def save_result(self):
        """
        Save the final processed result to a file.

        Raises:
            Exception: If an error occurs while saving the result.
        """
        try:
            self.logger.info("Saving result to file...")
            self.df_final_1.to_csv(self.result_file, index=False)
            self.logger.info("Result saved successfully.")
        except Exception as e:
            self.logger.error(f"Error saving result: {str(e)}")

    def process_data(self):
        """
        Process the datasets by executing the defined steps.
        """
        self.load_datasets()
        self.drop_invoice_id()
        self.apply_data_transformation()
        self.merge_with_dataset2()
        self.save_result()


# Usage
dataset1_path = 'dataset1.csv'
dataset2_path = 'dataset2.csv'
result_file = 'result_pandas.csv'

data_processor = DataProcessor(dataset1_path, dataset2_path, result_file)
data_processor.process_data()
