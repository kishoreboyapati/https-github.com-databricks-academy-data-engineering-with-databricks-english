# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

DA = DBAcademyHelper()
DA.cleanup()
DA.init()
install_eltwss_datasets(reinstall=False)
load_eltwss_external_tables()
DA.conclude_setup()

