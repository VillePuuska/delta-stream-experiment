import os
from typing import Literal, cast

import click
from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.errors.exceptions.captured import StreamingQueryException
from pyspark.sql import SparkSession

TABLE_PATH = os.path.join(*os.path.split(__file__)[:-1], "demo-table")


def get_spark_session_with_delta() -> SparkSession:
    builder = cast(SparkSession.Builder, SparkSession.builder)
    builder = (
        builder.master("local[8]")
        .appName("delta-stream-experiment")
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

    dt = DeltaTable.create(spark)
    dt.tableName("demo_table").addColumn("id", "int").addColumn("val", "int").location(
        TABLE_PATH
    ).property("delta.enableChangeDataFeed", "true").execute()

    df = spark.createDataFrame([(1, 1), (2, 999)], schema="id INT, val INT")
    df.write.format("delta").mode("overwrite").save(TABLE_PATH)


def updates(
    spark: SparkSession, update_type: Literal["update", "delete", "overwrite"]
) -> None:
    for i in range(2, 5):
        print(f"Update type {update_type}, value {i}")
        match update_type:
            case "update":
                spark.sql(f"UPDATE delta.`{TABLE_PATH}` SET val = {i} WHERE id = 1")
            case "delete":
                spark.sql(f"DELETE FROM delta.`{TABLE_PATH}` WHERE id = 1")
                spark.createDataFrame([(1, i)], schema="id INT, val INT").write.format(
                    "delta"
                ).mode("append").save(TABLE_PATH)
            case "overwrite":
                spark.createDataFrame(
                    [(1, i), (2, 999)], schema="id INT, val INT"
                ).write.format("delta").mode("overwrite").save(TABLE_PATH)
            case _:
                raise ValueError(f"Unknown update type: {update_type}")


def stream(
    spark: SparkSession,
    stream_type: Literal[
        "default", "ignoreDeletes", "ignoreChanges", "skipChangeCommits", "cdf"
    ],
) -> None:
    stream_df_builder = spark.readStream.format("delta").option("startingVersion", 0)
    match stream_type:
        case "default":
            pass
        case "ignoreDeletes":
            stream_df_builder = stream_df_builder.option("ignoreDeletes", "true")
        case "ignoreChanges":
            stream_df_builder = stream_df_builder.option("ignoreChanges", "true")
        case "skipChangeCommits":
            stream_df_builder = stream_df_builder.option("skipChangeCommits", "true")
        case "cdf":
            stream_df_builder = stream_df_builder.option("readChangeFeed", "true")
        case _:
            raise ValueError(f"Unknown stream type: {stream_type}")

    stream_df = stream_df_builder.load(TABLE_PATH)

    query = (
        stream_df.writeStream.format("console")
        .outputMode("append")
        .trigger(availableNow=True)
        .option("truncate", "false")
        .start()
    )
    try:
        query.awaitTermination(120)
    except StreamingQueryException:
        pass
    print(query.exception())


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
@click.option(
    "--stream-type",
    default="default",
    help="What option to use for streaming.",
    type=click.Choice(
        ["default", "ignoreDeletes", "ignoreChanges", "skipChangeCommits", "cdf"]
    ),
)
def main(
    entrypoint: Literal["updates", "stream"],
    update_type: Literal["update", "delete", "overwrite"],
    stream_type: Literal[
        "default", "ignoreDeletes", "ignoreChanges", "skipChangeCommits", "cdf"
    ],
) -> None:
    spark = get_spark_session_with_delta()
    initialize_delta_table(spark)

    match entrypoint:
        case "updates":
            updates(spark=spark, update_type=update_type)
        case "stream":
            stream(spark=spark, stream_type=stream_type)
        case _:
            raise ValueError(f"Unknown entrypoint: {entrypoint}")


if __name__ == "__main__":
    main()
