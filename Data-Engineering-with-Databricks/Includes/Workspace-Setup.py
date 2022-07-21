# Databricks notebook source
# MAGIC %run ./_utility-methods

# COMMAND ----------

DA = DBAcademyHelper()     # Create the DA object with the specified lesson
DA.cleanup(validate=False) # Remove the existing database and files
DA.init(create_db=False)   # True is the default
DA.install_datasets()      # Install (if necissary) and validate the datasets
DA.conclude_setup()        # Conclude the setup by printing the DA object's final state

# COMMAND ----------

# DA.enumerate_remote_datasets()

# COMMAND ----------

# Ensures that all users can create databases on the current catalog 
# for cases wherein the user/student is not an admin.
DA.update_user_specific_grants()

