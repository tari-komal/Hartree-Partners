import apache_beam as beam
import apache_beam.runners.interactive.interactive_beam as ib
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
import logging

class DataProcessor:
    """
    A class for processing datasets using the Apache Beam framework.

    Args:
        dataset1_file (str): File path of the first dataset.
        dataset2_file (str): File path of the second dataset.
        result_file (str): File path to save the processed result.
    """

    def __init__(self, dataset1_file, dataset2_file, result_file):
        self.dataset1_file = dataset1_file
        self.dataset2_file = dataset2_file
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

    def load_dataframes(self):
        """
        Load the datasets as Beam dataframes.

        Raises:
            Exception: If an error occurs while loading the datasets.
        """
        try:
            self.logger.info("Start task using Apache-beam Framework")
            self.pipeline1 = beam.Pipeline(InteractiveRunner())
            self.pipeline2 = beam.Pipeline(InteractiveRunner())
            self.beam_df1 = self.pipeline1 | 'Read CSV' >> beam.dataframe.io.read_csv(self.dataset1_file)
            self.beam_df2 = self.pipeline2 | 'Read CSV' >> beam.dataframe.io.read_csv(self.dataset2_file)
            self.dataframe_1 = ib.collect(self.beam_df1)
            self.df2 = ib.collect(self.beam_df2)
            self.logger.info("Dataframes loaded successfully.")
        except Exception as e:
            self.logger.error(f"Error loading dataframes: {str(e)}")

    def drop_invoice_id(self):
        """
        Drop the 'invoice_id' column from the first dataset.

        Raises:
            Exception: If an error occurs while dropping the column.
        """
        try:
            self.logger.info("Dropping 'invoice_id' column...")
            self.df = self.dataframe_1.drop('invoice_id', axis=1)
            self.logger.info("'invoice_id' column dropped successfully.")
        except Exception as e:
            self.logger.error(f"Error dropping 'invoice_id' column: {str(e)}")

    def apply_data_transformation(self):
        """
        Apply data transformation to the datasets.

        Raises:
            Exception: If an error occurs while applying the transformation.
        """
        try:
            self.logger.info("Applying data transformation...")
            arap_df = self.df.query("status == 'ARAP'")
            accr_df = self.df.query("status == 'ACCR'")
            arap_df_group = arap_df.pivot_table(values=['value', 'rating'], index=['legal_entity', 'counter_party'], aggfunc={'value': 'sum', 'rating': 'max'}).rename(columns={'value': 'arap_value', 'rating': 'rating_x'})
            accr_df_group = accr_df.pivot_table(values=['value', 'rating'], index=['legal_entity', 'counter_party'], aggfunc={'value': 'sum', 'rating': 'max'}).rename(columns={'value': 'accr_value', 'rating': 'rating_y'})
            df_merged = arap_df_group.join(accr_df_group, on=['legal_entity', 'counter_party'], how='outer').reset_index()

            df_final = df_merged.join(self.df2.set_index('counter_party'), on='counter_party')
            df_final['ratings'] = df_final[['rating_x', 'rating_y']].max(axis=1)
            df_final.fillna(0, inplace=True)
            self.df_final_1 = df_final.drop(['rating_x', 'rating_y'], axis=1)
            self.df_final_1 = self.df_final_1[['legal_entity', 'counter_party', 'tier', 'ratings', 'arap_value', 'accr_value']]
            self.logger.info("Data transformation applied successfully.")
        except Exception as e:
            self.logger.error(f"Error applying data transformation: {str(e)}")

    def save_result(self):
        """
        Save the final processed result to a file.

        Raises:
            Exception: If an error occurs while saving the result.
        """
        try:
            self.logger.info("Final dataframe output:\n" + str(self.df_final_1))
            self.df_final_1.to_csv(self.result_file, index=False)
            self.logger.info("Result saved successfully.")
        except Exception as e:
            self.logger.error(f"Error saving result: {str(e)}")

    def process_data(self):
        """
        Process the datasets by executing the defined steps.
        """
        self.load_dataframes()
        self.drop_invoice_id()
        self.apply_data_transformation()
        self.save_result()


# Usage
dataset1_file = 'dataset1.csv'
dataset2_file = 'dataset2.csv'
result_file = 'result_apache_beam.csv'

data_processor = DataProcessor(dataset1_file, dataset2_file, result_file)
data_processor.process_data()
