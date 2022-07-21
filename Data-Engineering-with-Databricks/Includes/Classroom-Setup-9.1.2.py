# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

DA = DBAcademyHelper(lesson="jobs_demo_91")
DA.cleanup()
DA.init()
DA.data_factory = DltDataFactory()
DA.conclude_setup()

