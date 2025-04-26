import os
from typing import Literal, cast

import click
from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession

TABLE_PATH = os.path.join(*os.path.split(__file__)[:-1], "demo-table")


def get_spark_session_with_delta() -> SparkSession:
    builder = cast(SparkSession.Builder, SparkSession.builder)
    builder = (
        builder.appName("MyApp")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark


def initialize_delta_table(spark: SparkSession) -> None:
    if DeltaTable.isDeltaTable(spark, TABLE_PATH):
        return

    df = spark.createDataFrame([(1,)], schema="id INT")
    df.write.format("delta").mode("overwrite").save(TABLE_PATH)

    spark.sql(
        f"ALTER TABLE delta.`{TABLE_PATH}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
    )


def updates(
    spark: SparkSession, update_type: Literal["update", "delete", "overwrite"]
) -> None:
    # Placeholder for the updates entrypoint logic
    print("Running updates entrypoint")


def stream(spark: SparkSession) -> None:
    # Placeholder for the stream entrypoint logic
    print("Running stream entrypoint")


@click.command()
@click.option(
    "--entrypoint",
    default="updates",
    help="Entrypoint to run.",
    type=click.Choice(["updates", "stream"]),
)
@click.option(
    "--update-type",
    default="update",
    help="How to update the table value.",
    type=click.Choice(["update", "delete", "overwrite"]),
)
def main(
    entrypoint: Literal["updates", "stream"],
    update_type: Literal["update", "delete", "overwrite"],
) -> None:
    spark = get_spark_session_with_delta()
    initialize_delta_table(spark)

    match entrypoint:
        case "updates":
            updates(spark=spark, update_type=update_type)
        case "stream":
            stream(spark)
        case _:
            raise ValueError(f"Unknown entrypoint: {entrypoint}")


if __name__ == "__main__":
    main()
