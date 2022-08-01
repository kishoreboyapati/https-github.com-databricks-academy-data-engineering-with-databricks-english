# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

DA = DBAcademyHelper()            # Create the DA object
DA.init(create_db=False)          # Initialize the helper, but don't create a DB
DA.install_datasets(verbose=True) # Install and validate the datasets with verbose output including copyrights
DA.conclude_setup()               # Conclude the setup by printing the DA object's final state

