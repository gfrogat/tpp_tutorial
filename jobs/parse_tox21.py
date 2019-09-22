import argparse
import logging
from pathlib import Path

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Row, SparkSession
from rdkit import Chem

compounds_schema = T.StructType(
    [
        T.StructField("mol_id", T.StringType(), nullable=False),
        T.StructField("inchikey", T.StringType(), nullable=False),
        T.StructField("mol_file", T.StringType(), nullable=False),
        T.StructField("fold", T.IntegerType(), nullable=False),
    ]
)

assay_schema = T.StructType(
    [
        T.StructField("mol_id", T.StringType(), nullable=False),
        T.StructField("assay_id", T.StringType(), nullable=False),
        T.StructField("activity", T.IntegerType(), nullable=True),
    ]
)

fold_map = {
    "tox21_10k_data_all.sdf": 0,  # train
    "tox21_10k_challenge_test.sdf": 1,  # test
    "tox21_10k_challenge_score.sdf": 2,  # score
}


def parse_tox21_compounds(sdf_path):
    res = []
    suppl = Chem.SDMolSupplier(sdf_path.as_posix())

    for mol in suppl:
        if mol is not None:
            prop_names = mol.GetPropNames()

            ################################################################
            # Make sure that `Compound ID` == `Sample ID` for test set items
            if "Sample ID" in prop_names:
                sample_id = mol.GetProp("Sample ID")
                compound_id = mol.GetProp("Compound ID")

                if sample_id[:-3] != compound_id:
                    continue
            ################################################################

            if "Compound ID" in prop_names:
                tox21_id = mol.GetProp("Compound ID")
            else:
                tox21_id = mol.GetProp("DSSTox_CID")

            inchikey = Chem.MolToInchiKey(mol)
            mol_block = Chem.MolToMolBlock(mol)
            fold_id = fold_map[sdf_path.name]

            res.append(
                Row(
                    mol_id=tox21_id, inchikey=inchikey, mol_file=mol_block, fold=fold_id
                )
            )

    return res


def parse_tox21_assays(sdf_path):
    res = []
    suppl = Chem.SDMolSupplier(sdf_path.as_posix())

    for mol in suppl:
        if mol is not None:
            prop_names = mol.GetPropNames()

            if "Compound ID" in prop_names:
                tox21_id = mol.GetProp("Compound ID")
            else:
                tox21_id = mol.GetProp("DSSTox_CID")

            for prop_name in prop_names:
                if prop_name.startswith("NR-") or prop_name.startswith("SR-"):
                    assay_id = prop_name
                    activity = int(mol.GetProp(prop_name))

                    res.append(
                        Row(mol_id=tox21_id, assay_id=assay_id, activity=activity)
                    )

    return res


def melt_df(wide_df, schema):
    tall_df = sc.emptyRDD().toDF(schema=schema)

    for column_name in wide_df.columns:
        if column_name != "Sample ID":
            tall_df_subset = wide_df.select(
                F.col("Sample ID").alias("mol_id"),
                F.lit(column_name).alias("assay_id"),
                F.col(column_name).alias("activity"),
            )
            tall_df = tall_df.union(tall_df_subset)

    tall_df = tall_df.dropna()
    return tall_df


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

        sc = spark.sparkContext

        sdf_files = list(args.input_dir_path.expanduser().glob("*.sdf"))
        sdf_files = sc.parallelize(sdf_files)

        compounds = sdf_files.flatMap(parse_tox21_compounds).toDF(
            schema=compounds_schema
        )
        assays = sdf_files.flatMap(parse_tox21_assays).toDF(schema=assay_schema)

        csv_files = list(args.input_dir_path.expanduser().glob("*.txt"))
        assert len(csv_files) == 1

        assays_test_wide = spark.read.csv(
            csv_files[0].as_posix(),
            sep="\t",
            header=True,
            inferSchema=True,
            nullValue="x",
        )
        assays_test = melt_df(assays_test_wide, schema=assay_schema)
        assays = assays.union(assays_test)

        compounds.write.parquet(
            (args.output_dir_path / "compounds.parquet").expanduser().as_posix()
        )
        assays.write.parquet(
            (args.output_dir_path / "assays.parquet").expanduser().as_posix()
        )
    except Exception as e:
        logging.exception(e)
    finally:
        spark.stop()
