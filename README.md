# TPP Tutorial

## PySpark

Download [SDKMan!](https://sdkman.io/) and install Java 8, Scala and Spark.

```bash
sdk install java 8.0.202-amzn   # optionally update to latest version
sdk install scala 2.11.12
sdk install spark 2.4.3
```

## Python Libraries

Setup a local conda or virtualenv environment and install Apache Airflow and PySpark

```bash
conda create -n "tpp_tutorial" python=3.7
conda activate tpp_tutorial
conda install -c rdkit -c mordred-descriptor mordred
conda install jupyterlab
pip install -r requirements.txt  # it's not available in conda
```

```bash
git clone ssh://git@git.bioinf.jku.at:5792/tpp/tpp_python.git
pushd tpp_python && pip install . && popd
```

## Spark

Navigate to `$SPARK_HOME` and update `$SPARK_HOME/conf/spark-env.sh` with these entries:

```bash
PYSPARK_DRIVER_PYTHON=${CONDA_PREFIX}/bin/python
PYSPARK_PYTHON=${CONDA_PREFIX}/bin/python

SPARK_WORKER_INSTANCES=2
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=2g
```

Now navigate to `$SPARK_HOME/sbin` and start a Spark cluster in local mode:

```bash
./start-master.sh
./start-slave.sh spark://$(hostname):7077
```

The Spark cluster should now be running. Navigate to [http://localhost:8080](http://localhost:8080)

## Airflow

After installation open multiple terminal session (e.g. via tmux), initialize the Airflow SQLite database and start the Airflow Scheduler and Webserver.

```bash
airflow initdb
# in separate session
airflow scheduler
# in separate session
airflow webserver --port 8000
```

This will start a local Airflow environment on your machine.

Open [http://localhost:8000](http://localhost:8000) in your browser and get familiar with the interface.

### Variables

Airflow supports variable configurations. We can create a new variable under `Admin > Variables` and then clicking `Create`.
Navigate to the file [tox21_config.json](./configs/tox21_configs.json) update it with your local settings and copy its contents to the new variable.

As soon as we have the variables set we can setup a task graph, also called DAG.

### DAGS

We will use the file [pipeline_tox21.py](./dags/pipeline_tox21.py). Symlink the local `dags` folder to `$AIRFLOW_HOME/dags` folder (usually this would be `~/airflow/dags`).

```bash
ln -s $(pwd)/dags ~/airflow/dags
```

The graph should now show up in the Webserver UI.
