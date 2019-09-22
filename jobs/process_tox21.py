import argparse
import logging
from pathlib import Path

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

assay_id_schema = T.StructType(
    [
        T.StructField("assay_id", T.StringType(), False),
        T.StructField("global_id", T.IntegerType(), False),
    ]
)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Parse Tox21 SDF Files")
    parser.add_argument(
        "--input-dir",
        required=True,
        type=Path,
        metavar="PATH",
        dest="input_dir_path",
        help="Path to folder with SDF files",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        metavar="PATH",
        dest="output_dir_path",
        help="Path where results should be written in `parquet` format",
    )

    args = parser.parse_args()

    try:
        # If we use PySpark this will already have been setup for us
        spark = (
            SparkSession.builder.appName(parser.prog)
            .config("spark.sql.execution.arrow.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )

        assays = spark.read.parquet(
            (args.input_dir_path / "assays.parquet").expanduser().as_posix()
        )
        compounds = spark.read.parquet(
            (args.input_dir_path / "compounds.parquet").expanduser().as_posix()
        )

        # Get mol_id -> inchikey mapping
        compound_ids = compounds.select("mol_id", "inchikey").dropDuplicates()
        compound_ids.write.parquet(
            (args.output_dir_path / "compound_ids.parquet").expanduser().as_posix()
        )

        # Index assay_id (String --> Integer)
        assay_ids = (
            assays.select("assay_id")
            .distinct()
            .rdd.zipWithIndex()
            .map(lambda x: (*x[0], x[1]))
            .toDF(assay_id_schema)
        )
        assay_ids.write.parquet(
            (args.output_dir_path / "assay_ids.parquet").expanduser().as_posix()
        )

        # Reindex Assays (String --> Integer)
        assays_reindexed = assays.alias("a").join(
            assay_ids.alias("ai"), F.col("a.assay_id") == F.col("ai.assay_id")
        )

        # Merge Compounds and Assays
        merged_data = compounds.alias("c").join(
            assays_reindexed.alias("a"), F.col("c.mol_id") == F.col("a.mol_id")
        )

        # Flatten labels and mol_files
        flattened_data = merged_data.groupBy("inchikey").agg(
            F.collect_set("mol_file").alias("mol_file"),
            F.map_from_entries(
                F.sort_array(
                    F.collect_set(F.struct(F.col("global_id"), F.col("activity")))
                )
            ).alias("labels"),
        )

        flattened_data.write.parquet(
            (args.output_dir_path / "flattened_data.parquet").expanduser().as_posix()
        )
    except Exception as e:
        logging.exception(e)
        raise SystemExit(
            "Spark Job encountered a problem. Check the logs for more information"
        )
    finally:
        spark.stop()
