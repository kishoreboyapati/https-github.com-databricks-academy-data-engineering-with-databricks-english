# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

def print_sql(self, rows, sql):
    displayHTML(f"""<body><textarea style="width:100%" rows={rows}> \n{sql.strip()}</textarea></body>""")
    
DBAcademyHelper.monkey_patch(print_sql)

# COMMAND ----------

def generate_daily_patient_avg(self):
    sql = f"SELECT * FROM {DA.db_name}.daily_patient_avg"
    self.print_sql(3, sql)

DBAcademyHelper.monkey_patch(generate_daily_patient_avg)

# COMMAND ----------

def generate_visualization_query(self):
    sql = f"""
SELECT flow_name, timestamp, int(details:flow_progress:metrics:num_output_rows) num_output_rows
FROM {DA.db_name}.dlt_metrics
ORDER BY timestamp DESC;"""
    
    self.print_sql(5, sql)

DBAcademyHelper.monkey_patch(generate_visualization_query)

# COMMAND ----------

generate_register_dlt_event_metrics_sql_string = ""

def generate_register_dlt_event_metrics_sql(self):
    global generate_register_dlt_event_metrics_sql_string
    
    generate_register_dlt_event_metrics_sql_string = f"""
CREATE TABLE IF NOT EXISTS {DA.db_name}.dlt_events
LOCATION '{DA.paths.working_dir}/storage/system/events';

CREATE VIEW IF NOT EXISTS {DA.db_name}.dlt_success AS
SELECT * FROM {DA.db_name}.dlt_events
WHERE details:flow_progress:metrics IS NOT NULL;

CREATE VIEW IF NOT EXISTS {DA.db_name}.dlt_metrics AS
SELECT timestamp, origin.flow_name, details 
FROM {DA.db_name}.dlt_success
ORDER BY timestamp DESC;""".strip()
    
    self.print_sql(13, generate_register_dlt_event_metrics_sql_string)
    
DBAcademyHelper.monkey_patch(generate_register_dlt_event_metrics_sql)

# COMMAND ----------

def print_pipeline_config(self):
    path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    path = "/".join(path.split("/")[:-1]) + "/DE 12.2.2L - DLT Task"

    displayHTML(f"""<table>
    <tr><td style="white-space:nowrap">Pipeline Name:</td><td><b>Cap-12-{DA.username}</b></td></tr>
    <tr><td style="white-space:nowrap">Source:</td><td><b>{DA.paths.working_dir}/source/tracker</b></td></tr>
    <tr><td style="white-space:nowrap">Target:</td><td><b>{DA.db_name}</b></td></tr>
    <tr><td style="white-space:nowrap">Storage Location:</td><td><b>{DA.paths.working_dir}/storage</b></td></tr>
    <tr><td style="white-space:nowrap" style="white-space:nowrap">Notebook Path:</td><td><b>{path}</b></td></tr>
    </table>""")

DBAcademyHelper.monkey_patch(print_pipeline_config)

# COMMAND ----------

def print_job_config(self):
    displayHTML(f"""<table>
    <tr><td style="white-space:nowrap">Job Name:</td><td><b>Cap-12-{DA.username}</b></td></tr>
    </table>""")
    
DBAcademyHelper.monkey_patch(print_job_config)

# COMMAND ----------

DA = DBAcademyHelper()
DA.cleanup()
DA.init()
DA.data_factory = DltDataFactory()
DA.conclude_setup()

