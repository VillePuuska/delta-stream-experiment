import os
import time
from typing import Literal, cast

import click
from delta import DeltaTable, configure_spark_with_delta_pip
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

    df = spark.createDataFrame([(1,)], schema="id INT")
    df.write.format("delta").mode("overwrite").save(TABLE_PATH)

    spark.sql(
        f"ALTER TABLE delta.`{TABLE_PATH}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
    )


def updates(
    spark: SparkSession, update_type: Literal["update", "delete", "overwrite"]
) -> None:
    for i in range(2, 16):
        print(i)
        match update_type:
            case "update":
                spark.sql(f"UPDATE delta.`{TABLE_PATH}` SET id = {i}")
            case "delete":
                spark.sql(f"DELETE FROM delta.`{TABLE_PATH}`")
                spark.createDataFrame([(i,)], schema="id INT").write.format(
                    "delta"
                ).mode("append").save(TABLE_PATH)
            case "overwrite":
                spark.createDataFrame([(i,)], schema="id INT").write.format(
                    "delta"
                ).mode("overwrite").save(TABLE_PATH)
            case _:
                raise ValueError(f"Unknown update type: {update_type}")
        print("Done")


def stream(
    spark: SparkSession,
    stream_type: Literal[
        "default", "ignoreDeletes", "ignoreChanges", "skipChangeCommits", "cdf"
    ],
    starting_version: Literal["0", "omit"],
) -> None:
    stream_df_builder = spark.readStream.format("delta")
    if starting_version != "omit":
        stream_df_builder = stream_df_builder.option(
            "startingVersion", starting_version
        )
    match stream_type:
        case "default":
            stream_df = stream_df_builder.load(TABLE_PATH)
        case "ignoreDeletes":
            stream_df = stream_df_builder.option("ignoreDeletes", "true").load(
                TABLE_PATH
            )
        case "ignoreChanges":
            stream_df = stream_df_builder.option("ignoreChanges", "true").load(
                TABLE_PATH
            )
        case "skipChangeCommits":
            stream_df = stream_df_builder.option("skipChangeCommits", "true").load(
                TABLE_PATH
            )
        case "cdf":
            stream_df = stream_df_builder.option("readChangeFeed", "true").load(
                TABLE_PATH
            )
        case _:
            raise ValueError(f"Unknown stream type: {stream_type}")

    query = (
        stream_df.writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .start()
    )
    try:
        for _ in range(120):
            if not query.isActive:
                break
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    query.stop()
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
@click.option(
    "--starting-version",
    default="omit",
    help="Starting version for streaming.",
    type=click.Choice(["0", "omit"]),
)
def main(
    entrypoint: Literal["updates", "stream"],
    update_type: Literal["update", "delete", "overwrite"],
    stream_type: Literal[
        "default", "ignoreDeletes", "ignoreChanges", "skipChangeCommits", "cdf"
    ],
    starting_version: Literal["0", "omit"],
) -> None:
    spark = get_spark_session_with_delta()
    initialize_delta_table(spark)

    match entrypoint:
        case "updates":
            updates(spark=spark, update_type=update_type)
        case "stream":
            stream(
                spark=spark, stream_type=stream_type, starting_version=starting_version
            )
        case _:
            raise ValueError(f"Unknown entrypoint: {entrypoint}")


if __name__ == "__main__":
    main()
