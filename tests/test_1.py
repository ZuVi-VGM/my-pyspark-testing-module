import pytest
import pydeequ
from unittest.mock import patch
from pyspark.sql import SparkSession
from modules.spark_job import SparkJob
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult


# Test per verificare che la colonna 'data' sia valorizzata correttamente
@pytest.fixture
def spark_fixture(request):
    spark = SparkSession.builder.config("spark.jars.packages", pydeequ.deequ_maven_coord)\
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)\
        .appName("Testing PySpark Example").master('local[*]').getOrCreate()

    # Add finalizer to stop Spark session after the test completes
    def cleanup():
        # Perform the shutdown callback and stop the Spark session
        spark.sparkContext._gateway.shutdown_callback_server()
        spark.stop()

    request.addfinalizer(cleanup)

    yield spark


def test_spark_job_run(spark_fixture):
    # Mock degli argomenti di linea di comando
    test_args = ["your_script.py", "-d", "42"]  # Passiamo il valore '42' per il parametro 'data'
    # Patch di sys.argv per simularli
    with patch("sys.argv", test_args):

        # Setup per la sessione Spark e l'oggetto SparkJob
        # app_name = "MyApp - Data: 42"
        spark_session = spark_fixture
        params = {'data': 42}

        job = SparkJob(spark_session, params)

        # Eseguiamo il metodo run (questo dovrebbe modificare il DataFrame)
        df = job.run()

        check = Check(spark_session, CheckLevel.Warning, "Review Check")

        checkResult = VerificationSuite(spark_session) \
            .onData(df) \
            .addCheck(check
                      .hasMin("data", lambda x: x == 42)
                      .hasMax("data", lambda x: x == 42))\
            .run()

        # Verifica il risultato
        assert checkResult.status == 'Success', f"Validation failed: {checkResult}"

        checkResult_df = VerificationResult.checkResultsAsDataFrame(spark_session, checkResult)
        checkResult_df.show()
