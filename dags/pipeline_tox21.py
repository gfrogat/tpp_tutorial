import datetime as dt

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    "owner": "tpp",
    "depends_on_past": False,
    "start_date": dt.datetime(2019, 8, 1),
    "email": ["tpp@ml.jku.at"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=30),
}

dag_config = Variable.get("tox21_config.json", deserialize_json=True)

with DAG(
    "pipeline_tox21",
    default_args=default_args,
    schedule_interval=None,
    params=dag_config,
) as dag:

    # Download Tox21
    download_tox21_command = """
    TOX21_DIR={{ params.tox21_data_root }} \
        bash {{ params.tpp_tutorial_home }}/scripts/download_tox21.sh
    """
    download_tox21 = BashOperator(
        task_id="download_tox21", bash_command=download_tox21_command
    )

    # Unzip Tox21
    unzip_tox21_command = """
    pushd {{ params.tox21_data_root }} && \
        (rm -rf *.sdf || true) && \
        unzip "*.zip" && \
        popd
    """
    unzip_tox21 = BashOperator(task_id="unzip_tox21", bash_command=unzip_tox21_command)

    # Parse Tox21 SDF files using PySpark
    parse_tox21_app = "{{ params.tpp_tutorial_home }}/jobs/parse_tox21.py"
    parse_tox21_app_args = [
        "--input-dir",
        "{{ params.tox21_data_root }}",
        "--output-dir",
        "{{ params.tox21_data_root }}",
    ]
    parse_tox21 = SparkSubmitOperator(
        task_id="parse_tox21",
        application=parse_tox21_app,
        application_args=parse_tox21_app_args,
        conf={
            "spark.pyspark.python": "{{ params.conda_prefix }}/bin/python",
            "spark.pyspark.driver.python": "{{ params.conda_prefix }}/bin/python",
        },
    )

    # Process Tox21 Parquet files using PySpark
    process_tox21_app = "{{ params.tpp_tutorial_home }}/jobs/process_tox21.py"
    process_tox21_app_args = [
        "--input-dir",
        "{{ params.tox21_data_root }}",
        "--output-dir",
        "{{ params.tox21_data_root }}",
    ]
    process_tox21 = SparkSubmitOperator(
        task_id="process_tox21",
        application=process_tox21_app,
        application_args=process_tox21_app_args,
        conf={
            "spark.pyspark.python": "{{ params.conda_prefix }}/bin/python",
            "spark.pyspark.driver.python": "{{ params.conda_prefix }}/bin/python",
        },
    )

    # Compute Tox Fingerprints (Tox Features used for Tox21 Challenge)
    compute_tox_features_app = (
        "{{ params.tpp_python_home }}/jobs/compute_descriptors.py"
    )
    compute_tox_features_args = [
        "--input",
        "{{ params.tox21_data_root }}/{{ params.input_path }}",
        "--output",
        "{{ params.tox21_data_root }}/{{ params.output_path }}",
        "--feature-type",
        "{{ params.feature_type }}",
        "--num-partitions",
        "{{ params.num_partitions }}",
    ]
    compute_tox_features = SparkSubmitOperator(
        task_id="compute_tox_features",
        application=compute_tox_features_app,
        application_args=compute_tox_features_args,
        conf={
            "spark.pyspark.python": "{{ params.conda_prefix }}/bin/python",
            "spark.pyspark.driver.python": "{{ params.conda_prefix }}/bin/python",
        },
        params={
            "input_path": "flattened_data.parquet",
            "output_path": "features_tox.parquet",
            "feature_type": "tox",
            "num_partitions": 10,
        },
    )

download_tox21 >> unzip_tox21 >> parse_tox21
parse_tox21 >> process_tox21 >> compute_tox_features


if __name__ == "__main__":
    dag.cli()
