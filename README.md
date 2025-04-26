# Spark & Delta Lake stream experiment

Simple concrete example of how the different options for dealing with deletes and updates in a Delta Lake table behave when reading a Delta table with Spark Structured Streaming.

The example scenario first creates the following table:
```
+---+---+
|id |val|
+---+---+
|1  |1  |
|2  |999|
+---+---+
```

Then, depending on the `update-type` choice, the `val` for `id = 1` is updated. First to 2, then 3, and finally to 4. The update is done using
- `UPDATE`,
- `DELETE` then `APPEND`,
- `OVERWRITE`.

*See the results section Option: readChangeFeed to understand the process if you're confused.*

The table is also created as both non-partitioned and partitioned by `id`. The results are different depending on partitioning.

For reading the resulting Delta Table as a stream, starting from the initial version of the Delta table, there are 5 different options to choose from:
- no options, i.e. read the Delta table "raw",
- `ignoreDeletes`,
- `ignoreChanges`,
- `skipChangeCommits`,
- `readChangeFeed`.

# Notes from the results

- `ignoreDeletes` only really works when deletes happen along partitions.
- `ignoreChanges` leads to duplicated data whenever rows are deleted or modified in files that contain rows that are not changed, but at least it emits all new data in all cases.
- `skipChangeCommits` does not emit new data as a result of `UPDATE` or `OVERWRITE`. This option works well if there are only `DELETE`s and `APPEND`s.
- CDF shows overwrites as deletes and inserts.

# Results: non-partitioned table

## Option: no option

All three update types result in the stream failing as you'd expect:
```
[DELTA_SOURCE_TABLE_IGNORE_CHANGES] Detected a data update ...
```
After all, a streaming source needs to be append-only.

## Option: ignoreDeletes

All three update types again result in the stream failing with the same exception as above. This is due to the delete not being along a partitioning column.

## Option: ignoreChanges

When table data is modified with `UPDATE`:
```
+---+---+
|id |val|
+---+---+
|1  |2  |
|2  |999|
|1  |3  |
|2  |999|
|1  |4  |
|2  |999|
|1  |1  |
|2  |999|
+---+---+
```

When table data is modified with `DELETE` then `APPEND`:
```
+---+---+
|id |val|
+---+---+
|2  |999|
|1  |1  |
|2  |999|
|1  |2  |
|1  |3  |
|1  |4  |
+---+---+
```

When table data is modified with `OVERWRITE`:
```
+---+---+
|id |val|
+---+---+
|1  |1  |
|2  |999|
|1  |2  |
|2  |999|
|1  |3  |
|2  |999|
|1  |4  |
|2  |999|
+---+---+
```

## Option: skipChangeCommits

When table data is modified with `UPDATE`:
```
+---+---+
|id |val|
+---+---+
|1  |1  |
|2  |999|
+---+---+
```

When table data is modified with `DELETE` then `APPEND`:
```
+---+---+
|id |val|
+---+---+
|1  |1  |
|2  |999|
|1  |2  |
|1  |3  |
|1  |4  |
+---+---+
```

When table data is modified with `OVERWRITE`:
```
+---+---+
|id |val|
+---+---+
|1  |1  |
|2  |999|
+---+---+
```

## Option: readChangeFeed

When table data is modified with `UPDATE`:
```
+---+---+----------------+---------------+-----------------------+
|id |val|_change_type    |_commit_version|_commit_timestamp      |
+---+---+----------------+---------------+-----------------------+
|1  |1  |update_preimage |2              |2025-04-26 18:45:07.331|
|1  |2  |update_postimage|2              |2025-04-26 18:45:07.331|
|1  |2  |update_preimage |3              |2025-04-26 18:45:08.458|
|1  |3  |update_postimage|3              |2025-04-26 18:45:08.458|
|1  |3  |update_preimage |4              |2025-04-26 18:45:09.673|
|1  |4  |update_postimage|4              |2025-04-26 18:45:09.673|
|1  |1  |insert          |1              |2025-04-26 18:45:05.432|
|2  |999|insert          |1              |2025-04-26 18:45:05.432|
+---+---+----------------+---------------+-----------------------+
```

When table data is modified with `DELETE` then `APPEND`:
```
+---+---+------------+---------------+-----------------------+
|id |val|_change_type|_commit_version|_commit_timestamp      |
+---+---+------------+---------------+-----------------------+
|1  |1  |delete      |2              |2025-04-26 18:49:13.556|
|1  |2  |delete      |4              |2025-04-26 18:49:17.16 |
|1  |3  |delete      |6              |2025-04-26 18:49:20.332|
|1  |1  |insert      |1              |2025-04-26 18:49:11.288|
|2  |999|insert      |1              |2025-04-26 18:49:11.288|
|1  |4  |insert      |7              |2025-04-26 18:49:21.454|
|1  |2  |insert      |3              |2025-04-26 18:49:14.905|
|1  |3  |insert      |5              |2025-04-26 18:49:18.41 |
+---+---+------------+---------------+-----------------------+
```

When table data is modified with `OVERWRITE`:
```
+---+---+------------+---------------+-----------------------+
|id |val|_change_type|_commit_version|_commit_timestamp      |
+---+---+------------+---------------+-----------------------+
|1  |4  |insert      |4              |2025-04-26 18:51:28.67 |
|2  |999|insert      |4              |2025-04-26 18:51:28.67 |
|1  |1  |insert      |1              |2025-04-26 18:51:23.374|
|2  |999|insert      |1              |2025-04-26 18:51:23.374|
|1  |3  |insert      |3              |2025-04-26 18:51:26.751|
|2  |999|insert      |3              |2025-04-26 18:51:26.751|
|1  |2  |insert      |2              |2025-04-26 18:51:25.005|
|2  |999|insert      |2              |2025-04-26 18:51:25.005|
|1  |3  |delete      |4              |2025-04-26 18:51:28.67 |
|2  |999|delete      |4              |2025-04-26 18:51:28.67 |
|1  |2  |delete      |3              |2025-04-26 18:51:26.751|
|2  |999|delete      |3              |2025-04-26 18:51:26.751|
|1  |1  |delete      |2              |2025-04-26 18:51:25.005|
|2  |999|delete      |2              |2025-04-26 18:51:25.005|
+---+---+------------+---------------+-----------------------+
```

# Results: partitioned by id

## Option: no option

All three update types result in the stream failing.

## Option: ignoreDeletes

When table data is modified with `UPDATE`:
```
[DELTA_SOURCE_TABLE_IGNORE_CHANGES] Detected a data update ...
```

When table data is modified with `DELETE` then `APPEND`:
```
+---+---+
|id |val|
+---+---+
|1  |1  |
|1  |2  |
|1  |3  |
|1  |4  |
|2  |999|
+---+---+
```

When table data is modified with `OVERWRITE`:
```
[DELTA_SOURCE_TABLE_IGNORE_CHANGES] Detected a data update ...
```

## Option: ignoreChanges

When table data is modified with `UPDATE`:
```
+---+---+
|id |val|
+---+---+
|1  |2  |
|1  |3  |
|1  |4  |
|1  |1  |
|2  |999|
+---+---+
```

When table data is modified with `DELETE` then `APPEND`:
```
+---+---+
|id |val|
+---+---+
|1  |1  |
|1  |2  |
|1  |3  |
|1  |4  |
|2  |999|
+---+---+
```

When table data is modified with `OVERWRITE`:
```
+---+---+
|id |val|
+---+---+
|1  |1  |
|1  |2  |
|1  |3  |
|1  |4  |
|2  |999|
|2  |999|
|2  |999|
|2  |999|
+---+---+
```

## Option: skipChangeCommits

When table data is modified with `UPDATE`:
```
+---+---+
|id |val|
+---+---+
|1  |1  |
|2  |999|
+---+---+
```

When table data is modified with `DELETE` then `APPEND`:
```
+---+---+
|id |val|
+---+---+
|1  |1  |
|1  |2  |
|1  |3  |
|1  |4  |
|2  |999|
+---+---+
```

When table data is modified with `OVERWRITE`:
```
+---+---+
|id |val|
+---+---+
|1  |1  |
|2  |999|
+---+---+
```

## Option: readChangeFeed

When table data is modified with `UPDATE`:
```
+---+---+----------------+---------------+-----------------------+
|id |val|_change_type    |_commit_version|_commit_timestamp      |
+---+---+----------------+---------------+-----------------------+
|1  |1  |update_preimage |2              |2025-04-26 18:53:56.397|
|1  |2  |update_postimage|2              |2025-04-26 18:53:56.397|
|1  |2  |update_preimage |3              |2025-04-26 18:53:57.899|
|1  |3  |update_postimage|3              |2025-04-26 18:53:57.899|
|1  |3  |update_preimage |4              |2025-04-26 18:53:59.642|
|1  |4  |update_postimage|4              |2025-04-26 18:53:59.642|
|1  |1  |insert          |1              |2025-04-26 18:53:54.679|
|2  |999|insert          |1              |2025-04-26 18:53:54.679|
+---+---+----------------+---------------+-----------------------+
```

When table data is modified with `DELETE` then `APPEND`:
```
+---+---+------------+---------------+-----------------------+
|id |val|_change_type|_commit_version|_commit_timestamp      |
+---+---+------------+---------------+-----------------------+
|2  |999|insert      |1              |2025-04-26 18:56:16.656|
|1  |1  |insert      |1              |2025-04-26 18:56:16.656|
|1  |2  |insert      |3              |2025-04-26 18:56:20.34 |
|1  |4  |insert      |7              |2025-04-26 18:56:26.359|
|1  |3  |insert      |5              |2025-04-26 18:56:23.536|
|1  |3  |delete      |6              |2025-04-26 18:56:25.145|
|1  |1  |delete      |2              |2025-04-26 18:56:18.589|
|1  |2  |delete      |4              |2025-04-26 18:56:22.195|
+---+---+------------+---------------+-----------------------+
```

When table data is modified with `OVERWRITE`:
```
+---+---+------------+---------------+-----------------------+
|id |val|_change_type|_commit_version|_commit_timestamp      |
+---+---+------------+---------------+-----------------------+
|1  |3  |insert      |3              |2025-04-26 18:59:06.889|
|2  |999|insert      |4              |2025-04-26 18:59:09.013|
|2  |999|insert      |3              |2025-04-26 18:59:06.889|
|2  |999|insert      |1              |2025-04-26 18:59:01.651|
|1  |2  |insert      |2              |2025-04-26 18:59:04.233|
|1  |4  |insert      |4              |2025-04-26 18:59:09.013|
|1  |1  |insert      |1              |2025-04-26 18:59:01.651|
|2  |999|insert      |2              |2025-04-26 18:59:04.233|
|1  |1  |delete      |2              |2025-04-26 18:59:04.233|
|2  |999|delete      |4              |2025-04-26 18:59:09.013|
|1  |2  |delete      |3              |2025-04-26 18:59:06.889|
|2  |999|delete      |2              |2025-04-26 18:59:04.233|
|2  |999|delete      |3              |2025-04-26 18:59:06.889|
|1  |3  |delete      |4              |2025-04-26 18:59:09.013|
+---+---+------------+---------------+-----------------------+
```

# Running the example yourself

- Install `uv`,
- `uv run experiment.py --help` to see the CLI options,
- `uv run experiment.py --entrypoint updates` to create the table (options: `--update-type`, `--partitioned`),
- `uv run experiment.py --entrypoint stream` to read the stream (option: `--stream-type`).
