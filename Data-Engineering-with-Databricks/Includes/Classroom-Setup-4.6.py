# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

DA = DBAcademyHelper()
DA.cleanup()
DA.init()
install_eltwss_datasets(reinstall=False)

print()
create_eltwss_users_update()
    
DA.conclude_setup()

