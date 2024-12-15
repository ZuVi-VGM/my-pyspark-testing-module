# SYSTEM IMPORTS
import argparse
# THIRD-PART IMPORT
from pyspark.sql import SparkSession
# LOCAL IMPORTS
from modules.spark_job import SparkJob


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-d',
                        type=int,
                        required=False,
                        dest='data',
                        help='Costante utile per il test')

    args = parser.parse_args()

    appName = 'MyApp - Data: ' + str(args.data)

    # Controllo che siano presenti tutti i parametri obbligatori
    script_parameters = vars(args)
    params = {p_name: p_value for p_name, p_value in script_parameters.items()}

    # Creazione spark session
    spark_session = SparkSession.builder \
        .appName(appName) \
        .master('local[*]') \
        .getOrCreate()

    job = SparkJob(spark_session, params)
    job.run()
