# Databricks notebook source

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
print(catalog)

spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

# DBTITLE 1,Define pipelines
# Enter Pipeline IDs
 
pipeline_id_rdl=''
pipeline_id_pdl=''
pipeline_id_rdl_corr=''
pipeline_id_pdl_corr=''

# COMMAND ----------

# DBTITLE 1,Clear table
spark.sql(f"delete from {catalog}.audit_schema.event_logs_raw where origin.pipeline_id in ('{pipeline_id_rdl}','{pipeline_id_pdl}','{pipeline_id_rdl_corr}','{pipeline_id_pdl_corr}') and log_date >= '2023-10-01' ")

# COMMAND ----------

# DBTITLE 1,Insert RDL Info
spark.sql(f"insert into table {catalog}.audit_schema.event_logs_raw select *, cast(timestamp as date) as log_date from `event_log`('{pipeline_id_rdl}') where cast(timestamp as date) >= cast('2023-10-01' as date);")

# COMMAND ----------

# DBTITLE 1,Insert PDL Info
spark.sql(f"insert into table {catalog}.audit_schema.event_logs_raw select *, cast(timestamp as date) as log_date from `event_log`('{pipeline_id_pdl}') where cast(timestamp as date) >= cast('2023-10-01' as date);")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Run duration on table basis

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with pipeline_runtime as
# MAGIC (select 
# MAGIC     origin.pipeline_name as pipeline_name,
# MAGIC     origin.pipeline_id as pipeline_id,
# MAGIC     origin.flow_id as stream_table_id,
# MAGIC     MAX(origin.flow_name) as stream_table_name, 
# MAGIC     MIN(timestamp) as start_time,
# MAGIC     MAX(timestamp) as end_time  --Max time where latest update happened, need to run immediately after stg and PDL tables are loaded, or false output
# MAGIC  from ${catalog}.audit_schema.event_logs_raw  
# MAGIC     where log_date = '2023-11-09'
# MAGIC     and lower(message) like '%master%'
# MAGIC     and origin.flow_id is not null  
# MAGIC     group by 
# MAGIC     origin.pipeline_name,
# MAGIC     origin.pipeline_id,
# MAGIC     origin.flow_id
# MAGIC     )
# MAGIC
# MAGIC SELECT 
# MAGIC     pipeline_name,
# MAGIC     pipeline_id, 
# MAGIC     --stream_table_id,
# MAGIC     stream_table_name,
# MAGIC     start_time, 
# MAGIC     end_time, 
# MAGIC     datediff(SECOND, start_time, end_time) as run_duration_seconds
# MAGIC     --datediff(millisecond, start_time, end_time) as run_duration_milliseconds
# MAGIC     from pipeline_runtime

# COMMAND ----------

# MAGIC %md
# MAGIC ##View DLT tables record counts load

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   CAST(timestamp as date) as log_date,
# MAGIC   timestamp,
# MAGIC   id,
# MAGIC   sequence.data_plane_id.seq_no as sequence_no,
# MAGIC   origin.pipeline_id as pipeline_id,
# MAGIC   origin.pipeline_name as pipeline_name,
# MAGIC   message,
# MAGIC   maturity_level,
# MAGIC   error,
# MAGIC   from_json(
# MAGIC         details :flow_progress :metrics,
# MAGIC         schema_of_json(
# MAGIC           "{'num_output_rows': 'bigint','backlog_bytes':'bigint','num_upserted_rows': 'bigint','num_deleted_rows':'bigint'}"
# MAGIC         )
# MAGIC       ).num_output_rows as num_output_rows,
# MAGIC   from_json(
# MAGIC         details :flow_progress :metrics,
# MAGIC         schema_of_json(
# MAGIC           "{'num_output_rows': 'bigint','backlog_bytes':'bigint','num_upserted_rows': 'bigint','num_deleted_rows':'bigint'}"
# MAGIC         )
# MAGIC       ).backlog_bytes as backlog_bytes,
# MAGIC   from_json(
# MAGIC         details :flow_progress :metrics,
# MAGIC         schema_of_json(
# MAGIC           "{'num_output_rows': 'bigint','backlog_bytes':'bigint','num_upserted_rows': 'bigint','num_deleted_rows':'bigint'}"
# MAGIC         )
# MAGIC       ).num_upserted_rows as num_upserted_rows,
# MAGIC   from_json(
# MAGIC         details :flow_progress :metrics,
# MAGIC         schema_of_json(
# MAGIC           "{'num_output_rows': 'bigint','backlog_bytes':'bigint','num_upserted_rows': 'bigint','num_deleted_rows':'bigint'}"
# MAGIC         )
# MAGIC       ).num_deleted_rows as num_deleted_rows
# MAGIC    from ${catalog}.audit_schema.event_logs_raw
# MAGIC    where log_date = '2023-11-09'
# MAGIC    and event_type = 'flow_progress'
# MAGIC    and message like '%Completed%'
# MAGIC    and message like '%master%'
# MAGIC    order by timestamp
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC #DQ Dashboard
# MAGIC ###(Querying the event log)

# COMMAND ----------

# DBTITLE 1,DQ Check - Fields failing counts
# MAGIC %sql
# MAGIC SELECT
# MAGIC   row_expectations.dataset as dataset,
# MAGIC   row_expectations.name as expectation,
# MAGIC   SUM(row_expectations.passed_records) as passing_records,
# MAGIC   SUM(row_expectations.failed_records) as failing_records,
# MAGIC   (passing_records*100)/(passing_records+failing_records) as pass_perc,
# MAGIC   (failing_records*100)/(passing_records+failing_records) as fail_perc
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       explode(
# MAGIC         from_json(
# MAGIC           details :flow_progress :data_quality :expectations,
# MAGIC           "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>"
# MAGIC         )
# MAGIC       ) row_expectations
# MAGIC     FROM
# MAGIC       ${catalog}.audit_schema.event_logs_raw
# MAGIC     WHERE 
# MAGIC       --cast(timestamp as date) = '2023-10-17' AND -- To filter specific pipeline run date
# MAGIC       event_type = 'flow_progress'
# MAGIC       AND origin.update_id in (SELECT origin.update_id FROM ${catalog}.audit_schema.event_logs_raw WHERE event_type = 'create_update' AND lower(origin.pipeline_name) like '%master-pdl%' ORDER BY timestamp DESC LIMIT 1)
# MAGIC   ) 
# MAGIC GROUP BY
# MAGIC   row_expectations.dataset,
# MAGIC   row_expectations.name

# COMMAND ----------

# DBTITLE 1,DQ Check - Percentage Summary Part 01
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW summary AS
# MAGIC SELECT
# MAGIC   row_expectations.dataset as dataset,
# MAGIC   SUM(row_expectations.passed_records) as passing_records,
# MAGIC   SUM(row_expectations.failed_records) as failing_records,
# MAGIC   (passing_records*100)/(passing_records+failing_records) as pass_perc,
# MAGIC   (failing_records*100)/(passing_records+failing_records) as fail_perc
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       explode(
# MAGIC         from_json(
# MAGIC           details :flow_progress :data_quality :expectations,
# MAGIC           "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>"
# MAGIC         )
# MAGIC       ) row_expectations
# MAGIC     FROM
# MAGIC       ${catalog}.audit_schema.event_logs_raw
# MAGIC     WHERE
# MAGIC       --cast(timestamp as date) = '2023-10-17' AND -- To filter specific pipeline run date
# MAGIC       event_type = 'flow_progress'
# MAGIC       AND origin.update_id in (SELECT origin.update_id FROM ${catalog}.audit_schema.event_logs_raw WHERE event_type = 'create_update' AND lower(origin.pipeline_name) like '%master-pdl%' ORDER BY timestamp DESC LIMIT 1)
# MAGIC   ) 
# MAGIC GROUP BY
# MAGIC   row_expectations.dataset
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,DQ Check - Percentage Summary Part 02
# MAGIC %sql
# MAGIC SELECT *
# MAGIC     FROM summary UNPIVOT INCLUDE NULLS
# MAGIC     (summary FOR result IN (passing_records AS `Pass Records Count`,
# MAGIC                            failing_records AS `Fail Records Count`));

# COMMAND ----------


