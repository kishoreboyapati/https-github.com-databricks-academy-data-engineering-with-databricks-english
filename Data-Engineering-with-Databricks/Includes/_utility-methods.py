# Databricks notebook source
# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems \
# MAGIC git+https://github.com/databricks-academy/dbacademy-rest \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

# The following attributes are externalized to make them easy
# for content developers to update with every new course.

_course_code = "dewd"
_naming_params = {"course": _course_code}
_course_name = "data-engineering-with-databricks"
_data_source_name = _course_name
_data_source_version = "v02"

_min_time = "5 min"  # The minimum amount of time to install the datasets (e.g. from Oregon)
_max_time = "15 min" # The maximum amount of time to install the datasets (e.g. from India)

# Set to true only if this course uses streaming APIs which 
# in turn creates a checkpoints path for all checkpoint files.
_enable_streaming_support = True 

# COMMAND ----------

class Paths():
    def __init__(self, working_dir, clean_lesson):
        global _enable_streaming_support
        
        self.working_dir = working_dir

        # The location of the user's database - presumes all courses have at least one DB.
        # The location of the database varies if we have a database per lesson or one database per course
        if clean_lesson: self.user_db = f"{working_dir}/{clean_lesson}.db"
        else:            self.user_db = f"{working_dir}/database.db"

        # When working with streams, it helps to put all checkpoints in their
        # own directory relative the the previously defined working_dir
        if _enable_streaming_support:
            self.checkpoints = f"{working_dir}/_checkpoints"    
            
    @staticmethod
    def exists(path):
        """
        Returns true if the specified path exists else false.
        """
        try: return len(dbutils.fs.ls(path)) >= 0
        except Exception:return False

    def print(self, padding="  "):
        """
        Prints all the pathes attached to this instance of Paths
        """
        max_key_len = 0
        for key in self.__dict__: 
            max_key_len = len(key) if len(key) > max_key_len else max_key_len
        for key in self.__dict__:
            label = f"{padding}self.paths.{key}:"
            print(label.ljust(max_key_len+13) + self.__dict__[key])
        
    def __repr__(self):
        return self.__dict__.__repr__().replace(", ", ",\n").replace("{","").replace("}","").replace("'","")


# COMMAND ----------

class DBAcademyHelper():
    def __init__(self, lesson=None, asynchronous=True):
        import re, time
        from dbacademy import dbgems 
        from dbacademy.dbrest import DBAcademyRestClient

        self.start = int(time.time())
        
        # Intialize from our global variables defined at the top of the notebook
        global _course_code, _course_name, _data_source_name, _data_source_version, _naming_params, _remote_files
        self.course_code = _course_code
        self.course_name = _course_name
        self.remote_files = _remote_files
        self.naming_params = _naming_params
        self.data_source_name = _data_source_name
        self.data_source_version = _data_source_version
        
        self.client = DBAcademyRestClient()

        # Are we running under test? If so we can "optimize" for parallel execution 
        # without affecting the student's runtime-experience. As in the student can
        # use one working directory and one database, but under test, we can use many
        is_smoke_test = (spark.conf.get("dbacademy.smoke-test", "false").lower() == "true")
        
        if lesson is None and asynchronous and is_smoke_test:
            # The developer did not define a lesson, we can run asynchronous, and this 
            # is a smoke test so we can define a lesson here for the sake of testing
            lesson = str(abs(hash(dbgems.get_notebook_path())) % 10000)
            
        self.lesson = None if lesson is None else lesson.lower()
        
        # Define username using the hive function (cleaner than notebooks API)
        self.username = spark.sql("SELECT current_user()").first()[0]

        # Create the database name prefix according to curriculum standards. This
        # is the value by which all databases in this course should start with.
        # Besides creating this lesson's database name, this value is used almost
        # exclusively in the Rest notebook.
        da_name, da_hash = self.get_username_hash()
        self.db_name_prefix = f"da-{da_name}@{da_hash}-{self.course_code}"      # Composite all the values to create the "dirty" database name
        self.db_name_prefix = re.sub("[^a-zA-Z0-9]", "_", self.db_name_prefix)  # Replace all special characters with underscores (not digit or alpha)
        while "__" in self.db_name_prefix: 
            self.db_name_prefix = self.db_name_prefix.replace("__", "_")        # Replace all double underscores with single underscores
        
        # This is the common super-directory for each lesson, removal of which 
        # is designed to ensure that all assets created by students is removed.
        # As such, it is not attached to the path object so as to hide it from 
        # students. Used almost exclusively in the Rest notebook.
        working_dir_root = f"dbfs:/mnt/dbacademy-users/{self.username}/{self.course_name}"

        if self.lesson is None:
            self.clean_lesson = None
            working_dir = working_dir_root         # No lesson, working dir is same as root
            self.paths = Paths(working_dir, None)  # Create the "visible" path
            self.hidden = Paths(working_dir, None) # Create the "hidden" path
            self.db_name = self.db_name_prefix     # No lesson, database name is the same as prefix
        else:
            working_dir = f"{working_dir_root}/{self.lesson}"                     # Working directory now includes the lesson name
            self.clean_lesson = re.sub("[^a-zA-Z0-9]", "_", self.lesson.lower())  # Replace all special characters with underscores
            self.paths = Paths(working_dir, self.clean_lesson)                    # Create the "visible" path
            self.hidden = Paths(working_dir, self.clean_lesson)                   # Create the "hidden" path
            self.db_name = f"{self.db_name_prefix}_{self.clean_lesson}"           # Database name includes the lesson name

        # Register the working directory root to the hidden set of paths
        self.hidden.working_dir_root = working_dir_root

        # This is where the datasets will be downloaded to and should be treated as read-only for all pratical purposes
        self.paths.datasets = f"dbfs:/mnt/dbacademy-datasets/{self.data_source_name}/{self.data_source_version}"
        
        # This is the location in our Azure data repository of the datasets for this lesson
        self.data_source_uri = f"wasbs://courseware@dbacademy.blob.core.windows.net/{self.data_source_name}/{self.data_source_version}"
    
    def get_username_hash(self):
        """
        Utility method to split the user's email address, dropping the domain, and then creating a hash based on the full email address and course_code. The primary usage of this function is in creating the user's database, but is also used in creating SQL Endpoints, DLT Piplines, etc - any place we need a short, student-specific name.
        """
        da_name = self.username.split("@")[0]                                   # Split the username, dropping the domain
        da_hash = abs(hash(f"{self.username}-{self.course_code}")) % 10000      # Create a has from the full username and course code
        return da_name, da_hash

    @staticmethod
    def monkey_patch(function_ref, delete=True):
        """
        This function "monkey patches" the specified function to the DBAcademyHelper class. While not 100% necissary, this pattern does allow each function to be defined in it's own cell which makes authoring notebooks a little bit easier.
        """
        import inspect
        
        signature = inspect.signature(function_ref)
        assert "self" in signature.parameters, f"""Missing the required parameter "self" in the function "{function_ref.__name__}()" """
        
        setattr(DBAcademyHelper, function_ref.__name__, function_ref)
        if delete: del function_ref        


# COMMAND ----------

def init(self, create_db=True):
    """
    This function aims to setup the invironment enabling the constructor to provide initialization of attributes only and thus not modifying the environment upon initialization.
    """

    spark.catalog.clearCache() # Clear the any cached values from previus lessons
    self.create_db = create_db # Flag to indicate if we are creating the database or not

    if create_db:
        print(f"\nCreating the database \"{self.db_name}\"")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.db_name} LOCATION '{self.paths.user_db}'")
        spark.sql(f"USE {self.db_name}")

DBAcademyHelper.monkey_patch(init)

# COMMAND ----------

def cleanup(self, validate=True):
    """
    Cleans up the user environment by stopping any active streams, dropping the database created by the call to init() and removing the user's lesson-specific working directory and any assets created in that directory.
    """

    for stream in spark.streams.active:
        print(f"Stopping the stream \"{stream.name}\"")
        stream.stop()
        try: stream.awaitTermination()
        except: pass # Bury any exceptions

    if spark.sql(f"SHOW DATABASES").filter(f"databaseName == '{self.db_name}'").count() == 1:
        print(f"Dropping the database \"{self.db_name}\"")
        spark.sql(f"DROP DATABASE {self.db_name} CASCADE")

    if self.paths.exists(self.paths.working_dir):
        print(f"Removing the working directory \"{self.paths.working_dir}\"")
        dbutils.fs.rm(self.paths.working_dir, True)

    if validate:
        print()
        self.validate_datasets(fail_fast=False)
        
DBAcademyHelper.monkey_patch(cleanup)

# COMMAND ----------

def conclude_setup(self):
    """
    Concludes the setup of DBAcademyHelper by advertising to the student the new state of the environment such as predefined path variables, databases and tables created on behalf of the student and the total setup time. Additionally, all path attributes are pushed to the Spark context for reference in SQL statements.
    """

    import time

    # Inject the user's database name
    # Add custom attributes to the SQL context here.
    spark.conf.set("da.db_name", self.db_name)
    spark.conf.set("DA.db_name", self.db_name)
    
    # Automatically add all path attributes to the SQL context as well.
    for key in self.paths.__dict__:
        spark.conf.set(f"da.paths.{key.lower()}", self.paths.__dict__[key])
        spark.conf.set(f"DA.paths.{key.lower()}", self.paths.__dict__[key])

    print("\nPredefined Paths:")
    self.paths.print()

    if self.create_db:
        print(f"\nPredefined tables in {self.db_name}:")
        tables = spark.sql(f"SHOW TABLES IN {self.db_name}").filter("isTemporary == false").select("tableName").collect()
        if len(tables) == 0: print("  -none-")
        for row in tables: print(f"  {row[0]}")

    print(f"\nSetup completed in {int(time.time())-self.start} seconds")

DBAcademyHelper.monkey_patch(conclude_setup)

# COMMAND ----------

def block_until_stream_is_ready(self, query, min_batches=2):
    """
    A utility method used in streaming notebooks to block until the stream has processed n batches. This method serves one main purpose in two different usescase
    
    The purpose is to block the current command until the state of the stream is ready and thus allowing the next command to execute against the properly initialized stream.
    
    The first use case is in jobs where where the stream is started in one cell but execution of subsequent cells start prematurely.
    
    The second use case is to slow down students who likewise attempt to execute subsequent cells before the stream is in a valid state either by invoking subsequent cells directly or by execute the Run-All Command
        
    Note: it is best to show the students this code the first time so that they understand what it is doing and why, but from that point forward, just call it via the DA object.
    """
    import time
    while len(query.recentProgress) < min_batches:
        time.sleep(5) # Give it a couple of seconds

    print(f"The stream has processed {len(query.recentProgress)} batchs")
    
DBAcademyHelper.monkey_patch(block_until_stream_is_ready)

# COMMAND ----------

def install_datasets(self, reinstall=False, repairing=False):
    """
    Install the datasets used by this course to DBFS.
    
    This ensures that data and compute are in the same region which subsequently mitigates performance issues when the storage and compute are, for example, on opposite sides of the world.
    """
    import time

    global _min_time, _max_time
    min_time = _min_time
    max_time = _max_time

    if not repairing: print(f"\nThe source for this dataset is\n{self.data_source_uri}/\n")

    if not repairing: print(f"Your dataset directory is\n{self.paths.datasets}\n")
    existing = self.paths.exists(self.paths.datasets)

    if not reinstall and existing:
        print(f"Skipping install of existing dataset.")
        print()
        self.validate_datasets(fail_fast=False)
        return 

    # Remove old versions of the previously installed datasets
    if existing:
        print(f"Removing previously installed datasets from\n{self.paths.datasets}")
        dbutils.fs.rm(self.paths.datasets, True)

    print(f"""\nInstalling the datasets to {self.paths.datasets}""")

    print(f"""\nNOTE: The datasets that we are installing are located in Washington, USA - depending on the
          region that your workspace is in, this operation can take as little as {min_time} and 
          upwards to {max_time}, but this is a one-time operation.""")

    files = dbutils.fs.ls(self.data_source_uri)
    what = "dataset" if len(files) == 1 else "datasets"
    print(f"\nInstalling {len(files)} {what}: ")
    
    install_start = int(time.time())
    for f in files:
        start = int(time.time())
        print(f"Copying /{f.name[:-1]}", end="...")

        dbutils.fs.cp(f"{self.data_source_uri}/{f.name}", f"{self.paths.datasets}/{f.name}", True)
        print(f"({int(time.time())-start} seconds)")

    print()
    self.validate_datasets(fail_fast=True)
    print(f"""\nThe install of the datasets completed successfully in {int(time.time())-install_start} seconds.""")  

DBAcademyHelper.monkey_patch(install_datasets)

# COMMAND ----------

def list_r(self, path, prefix=None, results=None):
    """
    Utility method used by the dataset validation, this method performs a recursive list of the specified path and returns the sorted list of paths.
    """
    if prefix is None: prefix = path
    if results is None: results = list()
    
    try: files = dbutils.fs.ls(path)
    except: files = []
    
    for file in files:
        data = file.path[len(prefix):]
        results.append(data)
        if file.isDir(): 
            self.list_r(file.path, prefix, results)
        
    results.sort()
    return results

DBAcademyHelper.monkey_patch(list_r)

# COMMAND ----------

def enumerate_remote_datasets(self):
    """
    Development function used to enumerate the remote datasets for use in validate_datasets()
    """
    files = self.list_r(self.data_source_uri)
    files = "_remote_files = " + str(files).replace("'", "\"")
    
    displayHTML(f"""
        <p>Copy the following output and paste it in its entirety into cell of the _utility-functions notebook.</p>
        <textarea rows="10" style="width:100%">{files}</textarea>
    """)

DBAcademyHelper.monkey_patch(enumerate_remote_datasets)

# COMMAND ----------

def enumerate_local_datasets(self):
    """
    Development function used to enumerate the local datasets for use in validate_datasets()
    """
    files = self.list_r(self.paths.datasets)
    files = "_remote_files = " + str(files).replace("'", "\"")
    
    displayHTML(f"""
        <p>Copy the following output and paste it in its entirety into cell of the _utility-functions notebook.</p>
        <textarea rows="10" style="width:100%">{files}</textarea>
    """)

DBAcademyHelper.monkey_patch(enumerate_local_datasets)

# COMMAND ----------

def do_validate(self):
    """
    Utility method to compare local datasets to the registered list of remote files.
    """
    
    local_files = self.list_r(self.paths.datasets)
    
    for file in local_files:
        if file not in self.remote_files:
            print(f"\n  - Found extra file: {file}")
            print(f"  - This problem can be fixed by reinstalling the datasets")
            return False

    for file in self.remote_files:
        if file not in local_files:
            print(f"\n  - Missing file: {file}")
            print(f"  - This problem can be fixed by reinstalling the datasets")
            return False
        
    return True
    

def validate_datasets(self, fail_fast:bool):
    """
    Validates the "install" of the datasets by recursively listing all files in the remote data repository as well as the local data repository, validating that each file exists but DOES NOT validate file size or checksum.
    """
    import time
    start = int(time.time())
    print(f"Validating the local copy of the datsets", end="...")
    
    if not self.do_validate():
        if fail_fast:
            raise Exception("Validation failed - see previous messages for more information.")
        else:
            print("\nAttempting to repair local dataset...\n")
            self.install_datasets(reinstall=True, repairing=True)
    
    print(f"({int(time.time())-start} seconds)")

DBAcademyHelper.monkey_patch(do_validate)
DBAcademyHelper.monkey_patch(validate_datasets)

# COMMAND ----------

def update_user_specific_grants(self):
    from dbacademy import dbgems
    from dbacademy.dbrest import DBAcademyRestClient
    
    job_name = f"DA-{DA.course_name}-Configure-Permissions"
    DA.client.jobs().delete_by_name(job_name, success_only=False)

    notebook_path = f"{dbgems.get_notebook_dir()}/Configure-Permissions"

    params = {
        "name": job_name,
        "tags": {
            "dbacademy.course": DA.course_name,
            "dbacademy.source": DA.course_name
        },
        "email_notifications": {},
        "timeout_seconds": 7200,
        "max_concurrent_runs": 1,
        "format": "MULTI_TASK",
        "tasks": [
            {
                "task_key": "Configure-Permissions",
                "description": "Configure all users's permissions for user-specific databases.",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "base_parameters": []
                },
                "new_cluster": {
                    "num_workers": "0",
                    "spark_conf": {
                        "spark.master": "local[*]",
                        "spark.databricks.acl.dfAclsEnabled": "true",
                        "spark.databricks.repl.allowedLanguages": "sql,python",
                        "spark.databricks.cluster.profile": "serverless"             
                    },
                    "runtime_engine": "STANDARD",
                    "spark_env_vars": {
                        "WSFS_ENABLE_WRITE_SUPPORT": "true"
                    },
                },
            },
        ],
    }
    cluster_params = params.get("tasks")[0].get("new_cluster")
    cluster_params["spark_version"] = DA.client.clusters().get_current_spark_version()
    
    if DA.client.clusters().get_current_instance_pool_id() is not None:
        cluster_params["instance_pool_id"] = DA.client.clusters().get_current_instance_pool_id()
    else:
        cluster_params["node_type_id"] = DA.client.clusters().get_current_node_type_id()
               
    create_response = DA.client.jobs().create(params)
    job_id = create_response.get("job_id")

    run_response = DA.client.jobs().run_now(job_id)
    run_id = run_response.get("run_id")

    final_response = DA.client.runs().wait_for(run_id)
    
    final_state = final_response.get("state").get("result_state")
    assert final_state == "SUCCESS", f"Expected the final state to be SUCCESS, found {final_state}"
    
    DA.client.jobs().delete_by_name(job_name, success_only=False)
    
    print()
    print("Update completed successfully.")

DBAcademyHelper.monkey_patch(update_user_specific_grants)

# COMMAND ----------

# def install_source_dataset(source_uri, reinstall, subdir):
#     target_dir = f"{DA.working_dir_prefix}/source/{subdir}"

# #     if reinstall and DA.paths.exists(target_dir):
# #         print(f"Removing existing dataset at {target_dir}")
# #         dbutils.fs.rm(target_dir, True)
    
#     if DA.paths.exists(target_dir):
#         print(f"Skipping install to \"{target_dir}\", dataset already exists")
#     else:
#         print(f"Installing datasets to \"{target_dir}\"")
#         dbutils.fs.cp(source_uri, target_dir, True)
        
#     return target_dir

# COMMAND ----------

# def install_dtavod_datasets(reinstall):
#     source_uri = "wasbs://courseware@dbacademy.blob.core.windows.net/databases_tables_and_views_on_databricks/v02"
#     DA.paths.datasets = install_source_dataset(source_uri, reinstall, "dtavod")

#     copy_source_dataset(f"{DA.paths.datasets}/flights/departuredelays.csv", 
#                         f"{DA.paths.working_dir}/flight_delays",
#                         format="csv", name="flight_delays")

# COMMAND ----------

# def install_eltwss_datasets(reinstall):
#     source_uri = "wasbs://courseware@dbacademy.blob.core.windows.net/elt-with-spark-sql/v02/small-datasets"
#     DA.paths.datasets = install_source_dataset(source_uri, reinstall, "eltwss")

# COMMAND ----------

# def clone_source_table(table_name, source_path, source_name=None):
#     import time
#     start = int(time.time())

#     source_name = table_name if source_name is None else source_name
#     print(f"Cloning the {table_name} table from {source_path}/{source_name}", end="...")
    
#     spark.sql(f"""
#         CREATE OR REPLACE TABLE {table_name}
#         SHALLOW CLONE delta.`{source_path}/{source_name}`
#         """)

#     total = spark.read.table(table_name).count()
#     print(f"({int(time.time())-start} seconds / {total:,} records)")
    
# def load_eltwss_tables():
#     clone_source_table("events", f"{DA.paths.datasets}/delta")
#     clone_source_table("sales", f"{DA.paths.datasets}/delta")
#     clone_source_table("users", f"{DA.paths.datasets}/delta")
#     clone_source_table("transactions", f"{DA.paths.datasets}/delta")    

# COMMAND ----------

# def copy_source_dataset(src_path, dst_path, format, name):
#     import time
#     start = int(time.time())
#     print(f"Creating the {name} dataset", end="...")
    
#     dbutils.fs.cp(src_path, dst_path, True)

#     total = spark.read.format(format).load(dst_path).count()
#     print(f"({int(time.time())-start} seconds / {total:,} records)")
    
# def load_eltwss_external_tables():
#     copy_source_dataset(f"{DA.paths.datasets}/raw/sales-csv", 
#                         f"{DA.paths.working_dir}/sales-csv", "csv", "sales-csv")

#     import time
#     start = int(time.time())
#     print(f"Creating the users table", end="...")

#     # REFACTORING - Making lesson-specific copy
#     dbutils.fs.cp(f"{DA.paths.datasets}/raw/users-historical", 
#                   f"{DA.paths.working_dir}/users-historical", True)

#     # https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
#     # Tom Deleted this
# #     (spark.read
# #           .format("parquet")
# #           .load(f"{DA.paths.working_dir}/users-historical")
# #           .repartition(1)
# #           .write
# #           .format("org.apache.spark.sql.jdbc")
# #           .option("url", f"jdbc:sqlite:/{DA.username}_ecommerce.db")
# #           .option("dbtable", "users") # The table name in sqllight
# #           .mode("overwrite")
# #           .save())
#     # Tom Replaced with this
#     #############################
#     # NOTE TO JACOB - this is a multi-node bug fix
#     import pandas as pd
#     import sqlite3
    
#     # Create Pandas Dataframe, it seems maybe spark does not like something about writing to dbfs, 
#     # Pandas does not like it either, so write to / then move
#     df = pd.read_parquet(path = "/dbfs/user/tom.hanlon@databricks.com/dbacademy/dewd/4.2/users-historical")
#     # Connect to sqlite and create cursor
#     conn = sqlite3.connect(f"/{DA.username}_ecommerce.db") 
#     c = conn.cursor()
 
#     # Create table in sqlite
#     c.execute('CREATE TABLE IF NOT EXISTS users (user_id string, user_first_touch_timestamp decimal(20,0), email string)')
#     conn.commit()
#     # Write or replace to table in sqlite
#     df.to_sql('users', conn, if_exists='replace', index = False)
#     import shutil
#     import os
#     # move file, delete if already present
#     if os.path.exists(f"/dbfs/{DA.username}_ecommerce.db"):
#       os.remove(f"/dbfs/{DA.username}_ecommerce.db")
#     # move file into dbfs
#     shutil.move(f"/{DA.username}_ecommerce.db", "/dbfs")
# #################
# # END TOM SWITCH
# #################
#     total = spark.read.parquet(f"{DA.paths.working_dir}/users-historical").count()
#     print(f"({int(time.time())-start} seconds / {total:,} records)")

# COMMAND ----------

# # lesson: Writing delta 
# def create_eltwss_users_update():
#     import time
#     start = int(time.time())
#     print(f"Creating the users_dirty table", end="...")

#     # REFACTORING - Making lesson-specific copy
#     dbutils.fs.cp(f"{DA.paths.datasets}/raw/users-30m", 
#                   f"{DA.paths.working_dir}/users-30m", True)
    
#     spark.sql(f"""
#         CREATE OR REPLACE TABLE users_dirty AS
#         SELECT *, current_timestamp() updated 
#         FROM parquet.`{DA.paths.working_dir}/users-30m`
#     """)
    
#     spark.sql("INSERT INTO users_dirty VALUES (NULL, NULL, NULL, NULL), (NULL, NULL, NULL, NULL), (NULL, NULL, NULL, NULL)")
    
#     total = spark.read.table("users_dirty").count()
#     print(f"({int(time.time())-start} seconds / {total:,} records)")

# COMMAND ----------

class DltDataFactory:
    def __init__(self):
        self.source = f"/mnt/training/healthcare/tracker/streaming"
        self.userdir = f"{DA.paths.working_dir}/source/tracker"
        try:
            self.curr_mo = 1 + int(max([x[1].split(".")[0] for x in dbutils.fs.ls(self.userdir)]))
        except:
            self.curr_mo = 1
    
    def load(self, continuous=False):
        if self.curr_mo > 12:
            print("Data source exhausted\n")
        elif continuous == True:
            while self.curr_mo <= 12:
                curr_file = f"{self.curr_mo:02}.json"
                target_dir = f"{self.userdir}/{curr_file}"
                print(f"Loading the file {curr_file} to the {target_dir}")
                dbutils.fs.cp(f"{self.source}/{curr_file}", target_dir)
                self.curr_mo += 1
        else:
            curr_file = f"{str(self.curr_mo).zfill(2)}.json"
            target_dir = f"{self.userdir}/{curr_file}"
            print(f"Loading the file {curr_file} to the {target_dir}")

            dbutils.fs.cp(f"{self.source}/{curr_file}", target_dir)
            self.curr_mo += 1

# COMMAND ----------

_remote_files = ["/ecommerce-clickstream-data/", "/ecommerce-clickstream-data/README.md", "/ecommerce-clickstream-data/small-datasets/", "/ecommerce-clickstream-data/small-datasets/delta/", "/ecommerce-clickstream-data/small-datasets/delta/clickpaths/", "/ecommerce-clickstream-data/small-datasets/delta/clickpaths/_delta_log/", "/ecommerce-clickstream-data/small-datasets/delta/clickpaths/_delta_log/.s3-optimization-0", "/ecommerce-clickstream-data/small-datasets/delta/clickpaths/_delta_log/.s3-optimization-1", "/ecommerce-clickstream-data/small-datasets/delta/clickpaths/_delta_log/.s3-optimization-2", "/ecommerce-clickstream-data/small-datasets/delta/clickpaths/_delta_log/00000000000000000000.crc", "/ecommerce-clickstream-data/small-datasets/delta/clickpaths/_delta_log/00000000000000000000.json", "/ecommerce-clickstream-data/small-datasets/delta/clickpaths/_delta_log/00000000000000000001.crc", "/ecommerce-clickstream-data/small-datasets/delta/clickpaths/_delta_log/00000000000000000001.json", "/ecommerce-clickstream-data/small-datasets/delta/clickpaths/part-00000-0337a7e6-2db7-4ac9-b087-8710b1b87e65-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/delta/clickpaths/part-00000-70d28e19-29d1-4630-83f7-263f381dd4b2-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/delta/clickpaths/part-00000-93b217c6-0f9e-4afe-83f1-118ef1460534-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/delta/events/", "/ecommerce-clickstream-data/small-datasets/delta/events/_delta_log/", "/ecommerce-clickstream-data/small-datasets/delta/events/_delta_log/.s3-optimization-0", "/ecommerce-clickstream-data/small-datasets/delta/events/_delta_log/.s3-optimization-1", "/ecommerce-clickstream-data/small-datasets/delta/events/_delta_log/.s3-optimization-2", "/ecommerce-clickstream-data/small-datasets/delta/events/_delta_log/00000000000000000000.crc", "/ecommerce-clickstream-data/small-datasets/delta/events/_delta_log/00000000000000000000.json", "/ecommerce-clickstream-data/small-datasets/delta/events/_delta_log/00000000000000000001.crc", "/ecommerce-clickstream-data/small-datasets/delta/events/_delta_log/00000000000000000001.json", "/ecommerce-clickstream-data/small-datasets/delta/events/part-00000-6841f2e7-d92f-406b-abbf-f9110aec7e14-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/delta/events/part-00000-c93338ab-b7de-413a-97bc-2ef21fc42096-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/delta/events/part-00000-f7a71761-b4e6-4209-a86b-c3b3ef379c0f-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/delta/events_hist/", "/ecommerce-clickstream-data/small-datasets/delta/events_hist/_delta_log/", "/ecommerce-clickstream-data/small-datasets/delta/events_hist/_delta_log/.s3-optimization-0", "/ecommerce-clickstream-data/small-datasets/delta/events_hist/_delta_log/.s3-optimization-1", "/ecommerce-clickstream-data/small-datasets/delta/events_hist/_delta_log/.s3-optimization-2", "/ecommerce-clickstream-data/small-datasets/delta/events_hist/_delta_log/00000000000000000000.crc", "/ecommerce-clickstream-data/small-datasets/delta/events_hist/_delta_log/00000000000000000000.json", "/ecommerce-clickstream-data/small-datasets/delta/events_hist/_delta_log/00000000000000000001.crc", "/ecommerce-clickstream-data/small-datasets/delta/events_hist/_delta_log/00000000000000000001.json", "/ecommerce-clickstream-data/small-datasets/delta/events_hist/part-00000-98bd00c6-5f72-48bb-a1df-890e719630f3-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/delta/events_hist/part-00000-9d919c90-1ba1-495b-a6a6-a873c6168335-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/delta/events_hist/part-00000-e0531e7b-f3b7-49c5-95a0-72e5ee5a3937-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/delta/events_raw/", "/ecommerce-clickstream-data/small-datasets/delta/events_raw/_delta_log/", "/ecommerce-clickstream-data/small-datasets/delta/events_raw/_delta_log/.s3-optimization-0", "/ecommerce-clickstream-data/small-datasets/delta/events_raw/_delta_log/.s3-optimization-1", "/ecommerce-clickstream-data/small-datasets/delta/events_raw/_delta_log/.s3-optimization-2", "/ecommerce-clickstream-data/small-datasets/delta/events_raw/_delta_log/00000000000000000000.crc", "/ecommerce-clickstream-data/small-datasets/delta/events_raw/_delta_log/00000000000000000000.json", "/ecommerce-clickstream-data/small-datasets/delta/events_raw/_delta_log/00000000000000000001.crc", "/ecommerce-clickstream-data/small-datasets/delta/events_raw/_delta_log/00000000000000000001.json", "/ecommerce-clickstream-data/small-datasets/delta/events_raw/part-00000-59dc39c3-23d2-4b58-a6b8-82455f9674f0-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/delta/events_update/", "/ecommerce-clickstream-data/small-datasets/delta/events_update/_delta_log/", "/ecommerce-clickstream-data/small-datasets/delta/events_update/_delta_log/.s3-optimization-0", "/ecommerce-clickstream-data/small-datasets/delta/events_update/_delta_log/.s3-optimization-1", "/ecommerce-clickstream-data/small-datasets/delta/events_update/_delta_log/.s3-optimization-2", "/ecommerce-clickstream-data/small-datasets/delta/events_update/_delta_log/00000000000000000000.crc", "/ecommerce-clickstream-data/small-datasets/delta/events_update/_delta_log/00000000000000000000.json", "/ecommerce-clickstream-data/small-datasets/delta/events_update/_delta_log/00000000000000000001.crc", "/ecommerce-clickstream-data/small-datasets/delta/events_update/_delta_log/00000000000000000001.json", "/ecommerce-clickstream-data/small-datasets/delta/events_update/part-00000-70b394f0-2afc-47c3-b491-2761cd8bfea8-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/delta/events_update/part-00000-743be31f-a839-469b-a62e-4d335c751281-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/delta/events_update/part-00000-f39f7494-b5d1-4c2b-8a46-d32de434f03e-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/delta/item_lookup/", "/ecommerce-clickstream-data/small-datasets/delta/item_lookup/_delta_log/", "/ecommerce-clickstream-data/small-datasets/delta/item_lookup/_delta_log/.s3-optimization-0", "/ecommerce-clickstream-data/small-datasets/delta/item_lookup/_delta_log/.s3-optimization-1", "/ecommerce-clickstream-data/small-datasets/delta/item_lookup/_delta_log/.s3-optimization-2", "/ecommerce-clickstream-data/small-datasets/delta/item_lookup/_delta_log/00000000000000000000.crc", "/ecommerce-clickstream-data/small-datasets/delta/item_lookup/_delta_log/00000000000000000000.json", "/ecommerce-clickstream-data/small-datasets/delta/item_lookup/part-00000-7fdc003c-7137-40a6-85e2-47bdbce39c6d-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/delta/item_lookup/part-00001-f426f81c-02dd-4ce5-aab8-bf55f32073f7-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/delta/item_lookup/part-00002-a31427cf-d560-4f21-acfd-84674c71a819-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/delta/item_lookup/part-00003-8ed5ca81-8d5b-49dd-8abf-15e4af10d85a-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/delta/sales/", "/ecommerce-clickstream-data/small-datasets/delta/sales/_delta_log/", "/ecommerce-clickstream-data/small-datasets/delta/sales/_delta_log/.s3-optimization-0", "/ecommerce-clickstream-data/small-datasets/delta/sales/_delta_log/.s3-optimization-1", "/ecommerce-clickstream-data/small-datasets/delta/sales/_delta_log/.s3-optimization-2", "/ecommerce-clickstream-data/small-datasets/delta/sales/_delta_log/00000000000000000000.crc", "/ecommerce-clickstream-data/small-datasets/delta/sales/_delta_log/00000000000000000000.json", "/ecommerce-clickstream-data/small-datasets/delta/sales/_delta_log/00000000000000000001.crc", "/ecommerce-clickstream-data/small-datasets/delta/sales/_delta_log/00000000000000000001.json", "/ecommerce-clickstream-data/small-datasets/delta/sales/part-00000-23d9c9bc-3585-4996-9894-d3f3891a6937-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/delta/sales/part-00000-952ddf1f-aa41-4bfa-959c-1d84feb6bd95-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/delta/sales/part-00000-eb64a6d8-3b60-4257-9488-4863895ef390-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/delta/sales_hist/", "/ecommerce-clickstream-data/small-datasets/delta/sales_hist/_delta_log/", "/ecommerce-clickstream-data/small-datasets/delta/sales_hist/_delta_log/.s3-optimization-0", "/ecommerce-clickstream-data/small-datasets/delta/sales_hist/_delta_log/.s3-optimization-1", "/ecommerce-clickstream-data/small-datasets/delta/sales_hist/_delta_log/.s3-optimization-2", "/ecommerce-clickstream-data/small-datasets/delta/sales_hist/_delta_log/00000000000000000000.crc", "/ecommerce-clickstream-data/small-datasets/delta/sales_hist/_delta_log/00000000000000000000.json", "/ecommerce-clickstream-data/small-datasets/delta/sales_hist/_delta_log/00000000000000000001.crc", "/ecommerce-clickstream-data/small-datasets/delta/sales_hist/_delta_log/00000000000000000001.json", "/ecommerce-clickstream-data/small-datasets/delta/sales_hist/part-00000-271c7ee7-fd1b-4c42-b55a-3c3e9af1d7ec-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/delta/sales_hist/part-00000-3ecfcbb1-f6f5-4297-a3d5-41f83e1dbfbc-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/delta/sales_hist/part-00000-dd20e0ac-54f4-40e4-b36c-6b93a7c7510a-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/delta/transactions/", "/ecommerce-clickstream-data/small-datasets/delta/transactions/_delta_log/", "/ecommerce-clickstream-data/small-datasets/delta/transactions/_delta_log/.s3-optimization-0", "/ecommerce-clickstream-data/small-datasets/delta/transactions/_delta_log/.s3-optimization-1", "/ecommerce-clickstream-data/small-datasets/delta/transactions/_delta_log/.s3-optimization-2", "/ecommerce-clickstream-data/small-datasets/delta/transactions/_delta_log/00000000000000000000.crc", "/ecommerce-clickstream-data/small-datasets/delta/transactions/_delta_log/00000000000000000000.json", "/ecommerce-clickstream-data/small-datasets/delta/transactions/_delta_log/00000000000000000001.crc", "/ecommerce-clickstream-data/small-datasets/delta/transactions/_delta_log/00000000000000000001.json", "/ecommerce-clickstream-data/small-datasets/delta/transactions/part-00000-94144360-0635-4a96-b128-7c44125367e4-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/delta/users/", "/ecommerce-clickstream-data/small-datasets/delta/users/_delta_log/", "/ecommerce-clickstream-data/small-datasets/delta/users/_delta_log/.s3-optimization-0", "/ecommerce-clickstream-data/small-datasets/delta/users/_delta_log/.s3-optimization-1", "/ecommerce-clickstream-data/small-datasets/delta/users/_delta_log/.s3-optimization-2", "/ecommerce-clickstream-data/small-datasets/delta/users/_delta_log/00000000000000000000.crc", "/ecommerce-clickstream-data/small-datasets/delta/users/_delta_log/00000000000000000000.json", "/ecommerce-clickstream-data/small-datasets/delta/users/_delta_log/00000000000000000001.crc", "/ecommerce-clickstream-data/small-datasets/delta/users/_delta_log/00000000000000000001.json", "/ecommerce-clickstream-data/small-datasets/delta/users/part-00000-120d779b-e65e-47ec-a6fc-78e3c3414714-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/delta/users_hist/", "/ecommerce-clickstream-data/small-datasets/delta/users_hist/_delta_log/", "/ecommerce-clickstream-data/small-datasets/delta/users_hist/_delta_log/.s3-optimization-0", "/ecommerce-clickstream-data/small-datasets/delta/users_hist/_delta_log/.s3-optimization-1", "/ecommerce-clickstream-data/small-datasets/delta/users_hist/_delta_log/.s3-optimization-2", "/ecommerce-clickstream-data/small-datasets/delta/users_hist/_delta_log/00000000000000000000.crc", "/ecommerce-clickstream-data/small-datasets/delta/users_hist/_delta_log/00000000000000000000.json", "/ecommerce-clickstream-data/small-datasets/delta/users_hist/_delta_log/00000000000000000001.crc", "/ecommerce-clickstream-data/small-datasets/delta/users_hist/_delta_log/00000000000000000001.json", "/ecommerce-clickstream-data/small-datasets/delta/users_hist/part-00000-39b04022-8452-4cd1-a36f-de4ff26db24d-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/delta/users_update/", "/ecommerce-clickstream-data/small-datasets/delta/users_update/_delta_log/", "/ecommerce-clickstream-data/small-datasets/delta/users_update/_delta_log/.s3-optimization-0", "/ecommerce-clickstream-data/small-datasets/delta/users_update/_delta_log/.s3-optimization-1", "/ecommerce-clickstream-data/small-datasets/delta/users_update/_delta_log/.s3-optimization-2", "/ecommerce-clickstream-data/small-datasets/delta/users_update/_delta_log/00000000000000000000.crc", "/ecommerce-clickstream-data/small-datasets/delta/users_update/_delta_log/00000000000000000000.json", "/ecommerce-clickstream-data/small-datasets/delta/users_update/_delta_log/00000000000000000001.crc", "/ecommerce-clickstream-data/small-datasets/delta/users_update/_delta_log/00000000000000000001.json", "/ecommerce-clickstream-data/small-datasets/delta/users_update/part-00000-cd8a3013-62e4-4876-837b-8bb161a3111d-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/", "/ecommerce-clickstream-data/small-datasets/raw/events-historical/", "/ecommerce-clickstream-data/small-datasets/raw/events-historical/_SUCCESS", "/ecommerce-clickstream-data/small-datasets/raw/events-historical/_committed_2791280217547372541", "/ecommerce-clickstream-data/small-datasets/raw/events-historical/_committed_5148781236890632344", "/ecommerce-clickstream-data/small-datasets/raw/events-historical/_committed_7228901775923393996", "/ecommerce-clickstream-data/small-datasets/raw/events-historical/_committed_7345950505771384516", "/ecommerce-clickstream-data/small-datasets/raw/events-historical/_committed_860462176816979339", "/ecommerce-clickstream-data/small-datasets/raw/events-historical/_committed_vacuum7162366710152874219", "/ecommerce-clickstream-data/small-datasets/raw/events-historical/_started_5148781236890632344", "/ecommerce-clickstream-data/small-datasets/raw/events-historical/_started_7228901775923393996", "/ecommerce-clickstream-data/small-datasets/raw/events-historical/_started_7345950505771384516", "/ecommerce-clickstream-data/small-datasets/raw/events-historical/_started_860462176816979339", "/ecommerce-clickstream-data/small-datasets/raw/events-historical/part-00000-tid-7228901775923393996-ef636a8a-296c-44e6-93d5-b6b52cb67adb-10-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/events-historical/part-00000-tid-7345950505771384516-67ae00f3-c8fe-4471-bc58-99042a3f0679-5895-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/events-historical/part-00001-tid-7228901775923393996-ef636a8a-296c-44e6-93d5-b6b52cb67adb-11-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/events-historical/part-00001-tid-7345950505771384516-67ae00f3-c8fe-4471-bc58-99042a3f0679-5896-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/events-historical/part-00002-tid-7228901775923393996-ef636a8a-296c-44e6-93d5-b6b52cb67adb-12-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/events-historical/part-00002-tid-7345950505771384516-67ae00f3-c8fe-4471-bc58-99042a3f0679-5897-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/events-historical/part-00003-tid-7228901775923393996-ef636a8a-296c-44e6-93d5-b6b52cb67adb-13-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/events-historical/part-00003-tid-7345950505771384516-67ae00f3-c8fe-4471-bc58-99042a3f0679-5898-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/events-kafka/", "/ecommerce-clickstream-data/small-datasets/raw/events-kafka/000.json", "/ecommerce-clickstream-data/small-datasets/raw/events-kafka/001.json", "/ecommerce-clickstream-data/small-datasets/raw/events-kafka/002.json", "/ecommerce-clickstream-data/small-datasets/raw/events-kafka/003.json", "/ecommerce-clickstream-data/small-datasets/raw/events-kafka/004.json", "/ecommerce-clickstream-data/small-datasets/raw/events-kafka/005.json", "/ecommerce-clickstream-data/small-datasets/raw/events-kafka/006.json", "/ecommerce-clickstream-data/small-datasets/raw/events-kafka/007.json", "/ecommerce-clickstream-data/small-datasets/raw/events-kafka/008.json", "/ecommerce-clickstream-data/small-datasets/raw/events-kafka/009.json", "/ecommerce-clickstream-data/small-datasets/raw/events-kafka/010.json", "/ecommerce-clickstream-data/small-datasets/raw/item-lookup/", "/ecommerce-clickstream-data/small-datasets/raw/item-lookup/_SUCCESS", "/ecommerce-clickstream-data/small-datasets/raw/item-lookup/_committed_2654461418592852096", "/ecommerce-clickstream-data/small-datasets/raw/item-lookup/_started_2654461418592852096", "/ecommerce-clickstream-data/small-datasets/raw/item-lookup/part-00000-tid-2654461418592852096-b933f2da-5dea-4d2d-b0ef-c9bac0905ab0-7615-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/item-lookup/part-00001-tid-2654461418592852096-b933f2da-5dea-4d2d-b0ef-c9bac0905ab0-7616-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/item-lookup/part-00002-tid-2654461418592852096-b933f2da-5dea-4d2d-b0ef-c9bac0905ab0-7617-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/item-lookup/part-00003-tid-2654461418592852096-b933f2da-5dea-4d2d-b0ef-c9bac0905ab0-7618-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/sales-30m/", "/ecommerce-clickstream-data/small-datasets/raw/sales-30m/_SUCCESS", "/ecommerce-clickstream-data/small-datasets/raw/sales-30m/_committed_4198604805198165031", "/ecommerce-clickstream-data/small-datasets/raw/sales-30m/_committed_5364580745385244803", "/ecommerce-clickstream-data/small-datasets/raw/sales-30m/_committed_5484765126949525607", "/ecommerce-clickstream-data/small-datasets/raw/sales-30m/_committed_6144999417711249384", "/ecommerce-clickstream-data/small-datasets/raw/sales-30m/_committed_9074289989443701117", "/ecommerce-clickstream-data/small-datasets/raw/sales-30m/_committed_vacuum7490578173802527130", "/ecommerce-clickstream-data/small-datasets/raw/sales-30m/_started_4198604805198165031", "/ecommerce-clickstream-data/small-datasets/raw/sales-30m/_started_5364580745385244803", "/ecommerce-clickstream-data/small-datasets/raw/sales-30m/_started_5484765126949525607", "/ecommerce-clickstream-data/small-datasets/raw/sales-30m/_started_9074289989443701117", "/ecommerce-clickstream-data/small-datasets/raw/sales-30m/part-00000-tid-5364580745385244803-e6225119-ab40-4518-af4a-6f7a43fa54ce-16-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/sales-30m/part-00000-tid-9074289989443701117-f9b615ab-8207-4281-8383-d6be3dc9f910-5900-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/sales-30m/part-00001-tid-5364580745385244803-e6225119-ab40-4518-af4a-6f7a43fa54ce-17-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/sales-30m/part-00001-tid-9074289989443701117-f9b615ab-8207-4281-8383-d6be3dc9f910-5901-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/sales-30m/part-00002-tid-5364580745385244803-e6225119-ab40-4518-af4a-6f7a43fa54ce-18-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/sales-30m/part-00002-tid-9074289989443701117-f9b615ab-8207-4281-8383-d6be3dc9f910-5902-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/sales-30m/part-00003-tid-5364580745385244803-e6225119-ab40-4518-af4a-6f7a43fa54ce-19-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/sales-30m/part-00003-tid-9074289989443701117-f9b615ab-8207-4281-8383-d6be3dc9f910-5903-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/sales-csv/", "/ecommerce-clickstream-data/small-datasets/raw/sales-csv/000.csv", "/ecommerce-clickstream-data/small-datasets/raw/sales-csv/001.csv", "/ecommerce-clickstream-data/small-datasets/raw/sales-csv/002.csv", "/ecommerce-clickstream-data/small-datasets/raw/sales-csv/003.csv", "/ecommerce-clickstream-data/small-datasets/raw/sales-historical/", "/ecommerce-clickstream-data/small-datasets/raw/sales-historical/_SUCCESS", "/ecommerce-clickstream-data/small-datasets/raw/sales-historical/_committed_115710843736190548", "/ecommerce-clickstream-data/small-datasets/raw/sales-historical/_committed_1668519538481149696", "/ecommerce-clickstream-data/small-datasets/raw/sales-historical/_committed_4575620701854220343", "/ecommerce-clickstream-data/small-datasets/raw/sales-historical/_committed_646693144485676518", "/ecommerce-clickstream-data/small-datasets/raw/sales-historical/_committed_6786832248242696253", "/ecommerce-clickstream-data/small-datasets/raw/sales-historical/_committed_vacuum6912201994486791117", "/ecommerce-clickstream-data/small-datasets/raw/sales-historical/_started_115710843736190548", "/ecommerce-clickstream-data/small-datasets/raw/sales-historical/_started_4575620701854220343", "/ecommerce-clickstream-data/small-datasets/raw/sales-historical/_started_646693144485676518", "/ecommerce-clickstream-data/small-datasets/raw/sales-historical/_started_6786832248242696253", "/ecommerce-clickstream-data/small-datasets/raw/sales-historical/part-00000-tid-115710843736190548-19f23c58-6d33-488c-97f1-0cf6f5638a33-5905-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/sales-historical/part-00000-tid-6786832248242696253-ddac04b5-56f2-4641-81c0-2ca457594ddb-22-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/sales-historical/part-00001-tid-115710843736190548-19f23c58-6d33-488c-97f1-0cf6f5638a33-5906-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/sales-historical/part-00001-tid-6786832248242696253-ddac04b5-56f2-4641-81c0-2ca457594ddb-23-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/sales-historical/part-00002-tid-115710843736190548-19f23c58-6d33-488c-97f1-0cf6f5638a33-5907-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/sales-historical/part-00002-tid-6786832248242696253-ddac04b5-56f2-4641-81c0-2ca457594ddb-24-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/sales-historical/part-00003-tid-115710843736190548-19f23c58-6d33-488c-97f1-0cf6f5638a33-5908-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/sales-historical/part-00003-tid-6786832248242696253-ddac04b5-56f2-4641-81c0-2ca457594ddb-25-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/users-30m/", "/ecommerce-clickstream-data/small-datasets/raw/users-30m/_SUCCESS", "/ecommerce-clickstream-data/small-datasets/raw/users-30m/_committed_1642804274388629440", "/ecommerce-clickstream-data/small-datasets/raw/users-30m/_started_1642804274388629440", "/ecommerce-clickstream-data/small-datasets/raw/users-30m/part-00000-tid-1642804274388629440-4ef42c1a-d5e2-4534-a526-620366f00b43-7583-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/users-30m/part-00001-tid-1642804274388629440-4ef42c1a-d5e2-4534-a526-620366f00b43-7584-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/users-30m/part-00002-tid-1642804274388629440-4ef42c1a-d5e2-4534-a526-620366f00b43-7585-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/users-30m/part-00003-tid-1642804274388629440-4ef42c1a-d5e2-4534-a526-620366f00b43-7586-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/users-historical/", "/ecommerce-clickstream-data/small-datasets/raw/users-historical/_SUCCESS", "/ecommerce-clickstream-data/small-datasets/raw/users-historical/_committed_531959640415905750", "/ecommerce-clickstream-data/small-datasets/raw/users-historical/_started_531959640415905750", "/ecommerce-clickstream-data/small-datasets/raw/users-historical/part-00000-tid-531959640415905750-948b4f2d-2d35-46e3-97eb-e6d85d2bf872-7571-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/users-historical/part-00001-tid-531959640415905750-948b4f2d-2d35-46e3-97eb-e6d85d2bf872-7572-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/users-historical/part-00002-tid-531959640415905750-948b4f2d-2d35-46e3-97eb-e6d85d2bf872-7573-1-c000.snappy.parquet", "/ecommerce-clickstream-data/small-datasets/raw/users-historical/part-00003-tid-531959640415905750-948b4f2d-2d35-46e3-97eb-e6d85d2bf872-7574-1-c000.snappy.parquet", "/flights/", "/flights/README.md", "/flights/airport-codes-na.txt", "/flights/departuredelays.csv", "/health-tracker/", "/health-tracker/agg_data.csv", "/health-tracker/classic_data_2020_h1.snappy.parquet", "/health-tracker/health_profile_data.snappy.parquet", "/health-tracker/health_tracker_data_2020_1.json", "/health-tracker/health_tracker_data_2020_2.json", "/health-tracker/health_tracker_data_2020_2_late.json", "/health-tracker/health_tracker_data_2020_3.json", "/health-tracker/health_tracker_data_2020_4.json", "/health-tracker/health_tracker_data_2020_5.json", "/health-tracker/user-profile-data.json", "/health-tracker/user_data.csv", "/health-tracker/user_profile_data.snappy.parquet", "/nyctaxi-with-zipcodes/", "/nyctaxi-with-zipcodes/subsampled/", "/nyctaxi-with-zipcodes/subsampled/_delta_log/", "/nyctaxi-with-zipcodes/subsampled/_delta_log/.s3-optimization-0", "/nyctaxi-with-zipcodes/subsampled/_delta_log/.s3-optimization-1", "/nyctaxi-with-zipcodes/subsampled/_delta_log/.s3-optimization-2", "/nyctaxi-with-zipcodes/subsampled/_delta_log/00000000000000000000.crc", "/nyctaxi-with-zipcodes/subsampled/_delta_log/00000000000000000000.json", "/nyctaxi-with-zipcodes/subsampled/nyc-zips-dataset-readme.txt", "/nyctaxi-with-zipcodes/subsampled/part-00000-80b68cae-ce6a-41cf-87cd-2573d91b4c07-c000.snappy.parquet", "/nyctaxi-with-zipcodes/subsampled/part-00001-c883942d-366f-478a-be3b-f13fd4bee0ab-c000.snappy.parquet", "/nyctaxi-with-zipcodes/subsampled/part-00002-bbf9fd81-4b3a-46f3-943e-841b48ae743e-c000.snappy.parquet", "/nyctaxi-with-zipcodes/subsampled/part-00003-3d80435e-15f8-4154-92c7-515307e41c1b-c000.snappy.parquet", "/nyctaxi-with-zipcodes/subsampled/part-00004-0b996b45-a3ff-4339-afeb-8fc691770056-c000.snappy.parquet", "/nyctaxi-with-zipcodes/subsampled/part-00005-ec9ab51b-23a3-4333-8d42-1730df56bfb6-c000.snappy.parquet", "/retail-org/", "/retail-org/README.md", "/retail-org/active_promotions/", "/retail-org/active_promotions/active_promotions.parquet", "/retail-org/company_employees/", "/retail-org/company_employees/company_employees.csv", "/retail-org/customers/", "/retail-org/customers/customers.csv", "/retail-org/loyalty_segments/", "/retail-org/loyalty_segments/loyalty_segment.csv", "/retail-org/products/", "/retail-org/products/products.csv", "/retail-org/promotions/", "/retail-org/promotions/promotions.csv", "/retail-org/purchase_orders/", "/retail-org/purchase_orders/purchase_orders.xml", "/retail-org/sales_orders/", "/retail-org/sales_orders/_SUCCESS", "/retail-org/sales_orders/_committed_1771549084454148016", "/retail-org/sales_orders/_started_1771549084454148016", "/retail-org/sales_orders/part-00000-tid-1771549084454148016-e2275afd-a5bb-40ed-b044-1774c0fdab2b-105592-1-c000.json", "/retail-org/sales_stream/", "/retail-org/sales_stream/sales_stream.json/", "/retail-org/sales_stream/sales_stream.json/12-00.json", "/retail-org/sales_stream/sales_stream.json/12-05.json", "/retail-org/sales_stream/sales_stream.json/12-10.json", "/retail-org/sales_stream/sales_stream.json/12-15.json", "/retail-org/sales_stream/sales_stream.json/12-25.json", "/retail-org/sales_stream/sales_stream.json/12-35.json", "/retail-org/sales_stream/sales_stream.json/12-40.json", "/retail-org/sales_stream/sales_stream.json/12-45.json", "/retail-org/sales_stream/sales_stream.json/13-00.json", "/retail-org/sales_stream/sales_stream.json/13-15.json", "/retail-org/sales_stream/sales_stream.json/13-20.json", "/retail-org/sales_stream/sales_stream.json/13-25.json", "/retail-org/sales_stream/sales_stream.json/13-30.json", "/retail-org/sales_stream/sales_stream.json/13-35.json", "/retail-org/sales_stream/sales_stream.json/13-40.json", "/retail-org/sales_stream/sales_stream.json/13-50.json", "/retail-org/sales_stream/sales_stream.json/13-55.json", "/retail-org/sales_stream/sales_stream.json/14-00.json", "/retail-org/sales_stream/sales_stream.json/14-05.json", "/retail-org/sales_stream/sales_stream.json/14-15.json", "/retail-org/sales_stream/sales_stream.json/14-25.json", "/retail-org/sales_stream/sales_stream.json/14-30.json", "/retail-org/sales_stream/sales_stream.json/14-35.json", "/retail-org/sales_stream/sales_stream.json/14-40.json", "/retail-org/sales_stream/sales_stream.json/14-45.json", "/retail-org/sales_stream/sales_stream.json/14-55.json", "/retail-org/sales_stream/sales_stream.json/15-00.json", "/retail-org/sales_stream/sales_stream.json/15-05.json", "/retail-org/sales_stream/sales_stream.json/15-10.json", "/retail-org/sales_stream/sales_stream.json/15-25.json", "/retail-org/sales_stream/sales_stream.json/15-30.json", "/retail-org/sales_stream/sales_stream.json/15-35.json", "/retail-org/sales_stream/sales_stream.json/15-45.json", "/retail-org/sales_stream/sales_stream.json/15-55.json", "/retail-org/sales_stream/sales_stream.json/16-00.json", "/retail-org/sales_stream/sales_stream.json/16-05.json", "/retail-org/sales_stream/sales_stream.json/16-10.json", "/retail-org/sales_stream/sales_stream.json/16-15.json", "/retail-org/sales_stream/sales_stream.json/16-20.json", "/retail-org/sales_stream/sales_stream.json/16-25.json", "/retail-org/sales_stream/sales_stream.json/16-30.json", "/retail-org/sales_stream/sales_stream.json/16-35.json", "/retail-org/sales_stream/sales_stream.json/16-40.json", "/retail-org/sales_stream/sales_stream.json/16-50.json", "/retail-org/sales_stream/sales_stream.json/16-55.json", "/retail-org/solutions/", "/retail-org/solutions/bronze/", "/retail-org/solutions/bronze/active_promotions/", "/retail-org/solutions/bronze/active_promotions/_delta_log/", "/retail-org/solutions/bronze/active_promotions/_delta_log/.s3-optimization-0", "/retail-org/solutions/bronze/active_promotions/_delta_log/.s3-optimization-1", "/retail-org/solutions/bronze/active_promotions/_delta_log/.s3-optimization-2", "/retail-org/solutions/bronze/active_promotions/_delta_log/00000000000000000000.crc", "/retail-org/solutions/bronze/active_promotions/_delta_log/00000000000000000000.json", "/retail-org/solutions/bronze/active_promotions/part-00000-66841a9d-7450-4b16-909b-094f28f245eb-c000.snappy.parquet", "/retail-org/solutions/bronze/customers/", "/retail-org/solutions/bronze/customers/_delta_log/", "/retail-org/solutions/bronze/customers/_delta_log/.s3-optimization-0", "/retail-org/solutions/bronze/customers/_delta_log/.s3-optimization-1", "/retail-org/solutions/bronze/customers/_delta_log/.s3-optimization-2", "/retail-org/solutions/bronze/customers/_delta_log/00000000000000000000.crc", "/retail-org/solutions/bronze/customers/_delta_log/00000000000000000000.json", "/retail-org/solutions/bronze/customers/part-00000-db8f8abe-4cfd-4c0d-bd99-792bdc32004a-c000.snappy.parquet", "/retail-org/solutions/bronze/products/", "/retail-org/solutions/bronze/products/_delta_log/", "/retail-org/solutions/bronze/products/_delta_log/.s3-optimization-0", "/retail-org/solutions/bronze/products/_delta_log/.s3-optimization-1", "/retail-org/solutions/bronze/products/_delta_log/.s3-optimization-2", "/retail-org/solutions/bronze/products/_delta_log/00000000000000000000.crc", "/retail-org/solutions/bronze/products/_delta_log/00000000000000000000.json", "/retail-org/solutions/bronze/products/_delta_log/00000000000000000001.crc", "/retail-org/solutions/bronze/products/_delta_log/00000000000000000001.json", "/retail-org/solutions/bronze/products/_delta_log/00000000000000000002.crc", "/retail-org/solutions/bronze/products/_delta_log/00000000000000000002.json", "/retail-org/solutions/bronze/products/_delta_log/00000000000000000003.crc", "/retail-org/solutions/bronze/products/_delta_log/00000000000000000003.json", "/retail-org/solutions/bronze/products/_delta_log/00000000000000000004.crc", "/retail-org/solutions/bronze/products/_delta_log/00000000000000000004.json", "/retail-org/solutions/bronze/products/part-00000-77826561-f91b-4ecf-87f1-ea8698344641-c000.snappy.parquet", "/retail-org/solutions/bronze/products/part-00000-822480e0-924b-4bce-8049-6c2885a52cfa-c000.snappy.parquet", "/retail-org/solutions/bronze/products/part-00000-b873b452-1219-45b7-9806-33d71b0bdb91-c000.snappy.parquet", "/retail-org/solutions/bronze/products/part-00000-d5bb465a-0823-489f-b707-b10c9ec0b40d-c000.snappy.parquet", "/retail-org/solutions/bronze/products/part-00000-f7432c2c-68fc-4c38-93c2-b3634169d1b2-c000.snappy.parquet", "/retail-org/solutions/bronze/purchase_orders/", "/retail-org/solutions/bronze/purchase_orders/_delta_log/", "/retail-org/solutions/bronze/purchase_orders/_delta_log/.s3-optimization-0", "/retail-org/solutions/bronze/purchase_orders/_delta_log/.s3-optimization-1", "/retail-org/solutions/bronze/purchase_orders/_delta_log/.s3-optimization-2", "/retail-org/solutions/bronze/purchase_orders/_delta_log/00000000000000000000.crc", "/retail-org/solutions/bronze/purchase_orders/_delta_log/00000000000000000000.json", "/retail-org/solutions/bronze/purchase_orders/part-00000-33a8a060-5811-4e95-bbc1-244c3c4b38e6-c000.snappy.parquet", "/retail-org/solutions/bronze/sales_orders/", "/retail-org/solutions/bronze/sales_orders/_delta_log/", "/retail-org/solutions/bronze/sales_orders/_delta_log/.s3-optimization-0", "/retail-org/solutions/bronze/sales_orders/_delta_log/.s3-optimization-1", "/retail-org/solutions/bronze/sales_orders/_delta_log/.s3-optimization-2", "/retail-org/solutions/bronze/sales_orders/_delta_log/00000000000000000000.crc", "/retail-org/solutions/bronze/sales_orders/_delta_log/00000000000000000000.json", "/retail-org/solutions/bronze/sales_orders/part-00000-814353fb-139e-4395-a8e4-58654bf8e591-c000.snappy.parquet", "/retail-org/solutions/bronze/suppliers/", "/retail-org/solutions/bronze/suppliers/_delta_log/", "/retail-org/solutions/bronze/suppliers/_delta_log/.s3-optimization-0", "/retail-org/solutions/bronze/suppliers/_delta_log/.s3-optimization-1", "/retail-org/solutions/bronze/suppliers/_delta_log/.s3-optimization-2", "/retail-org/solutions/bronze/suppliers/_delta_log/00000000000000000000.crc", "/retail-org/solutions/bronze/suppliers/_delta_log/00000000000000000000.json", "/retail-org/solutions/bronze/suppliers/part-00000-d143db6a-a976-4ad8-a23d-c927b26f0234-c000.snappy.parquet", "/retail-org/solutions/gold/", "/retail-org/solutions/gold/sales/", "/retail-org/solutions/gold/sales/_delta_log/", "/retail-org/solutions/gold/sales/_delta_log/.s3-optimization-0", "/retail-org/solutions/gold/sales/_delta_log/.s3-optimization-1", "/retail-org/solutions/gold/sales/_delta_log/.s3-optimization-2", "/retail-org/solutions/gold/sales/_delta_log/00000000000000000000.crc", "/retail-org/solutions/gold/sales/_delta_log/00000000000000000000.json", "/retail-org/solutions/gold/sales/part-00000-0defb1b8-fa7f-4fdb-a3ae-2c89ee9bb1bb-c000.snappy.parquet", "/retail-org/solutions/sales_stream/", "/retail-org/solutions/sales_stream/12-05.json", "/retail-org/solutions/silver/", "/retail-org/solutions/silver/_SUCCESS", "/retail-org/solutions/silver/_committed_6853939106257238539", "/retail-org/solutions/silver/_started_6853939106257238539", "/retail-org/solutions/silver/goods_receipt/", "/retail-org/solutions/silver/goods_receipt/_delta_log/", "/retail-org/solutions/silver/goods_receipt/_delta_log/.s3-optimization-0", "/retail-org/solutions/silver/goods_receipt/_delta_log/.s3-optimization-1", "/retail-org/solutions/silver/goods_receipt/_delta_log/.s3-optimization-2", "/retail-org/solutions/silver/goods_receipt/_delta_log/00000000000000000000.crc", "/retail-org/solutions/silver/goods_receipt/_delta_log/00000000000000000000.json", "/retail-org/solutions/silver/goods_receipt/part-00000-9c53d70f-1e9c-4a8c-a122-2dfcc52c251d-c000.snappy.parquet", "/retail-org/solutions/silver/part-00000-tid-6853939106257238539-8da37d6d-79cd-4a86-8aca-cd904c0bca38-12-1-c000.snappy.parquet", "/retail-org/solutions/silver/promo_prices/", "/retail-org/solutions/silver/promo_prices/_delta_log/", "/retail-org/solutions/silver/promo_prices/_delta_log/.s3-optimization-0", "/retail-org/solutions/silver/promo_prices/_delta_log/.s3-optimization-1", "/retail-org/solutions/silver/promo_prices/_delta_log/.s3-optimization-2", "/retail-org/solutions/silver/promo_prices/_delta_log/00000000000000000000.crc", "/retail-org/solutions/silver/promo_prices/_delta_log/00000000000000000000.json", "/retail-org/solutions/silver/promo_prices/part-00000-5fa6c8e3-9eee-428f-ab1b-12d15b445985-c000.snappy.parquet", "/retail-org/solutions/silver/purchase_orders.delta/", "/retail-org/solutions/silver/purchase_orders.delta/_delta_log/", "/retail-org/solutions/silver/purchase_orders.delta/_delta_log/.s3-optimization-0", "/retail-org/solutions/silver/purchase_orders.delta/_delta_log/.s3-optimization-1", "/retail-org/solutions/silver/purchase_orders.delta/_delta_log/.s3-optimization-2", "/retail-org/solutions/silver/purchase_orders.delta/_delta_log/00000000000000000000.crc", "/retail-org/solutions/silver/purchase_orders.delta/_delta_log/00000000000000000000.json", "/retail-org/solutions/silver/purchase_orders.delta/part-00000-f1da016e-a0d6-461e-b01f-8580c5a0a1a0-c000.snappy.parquet", "/retail-org/solutions/silver/purchase_orders_cleansed.delta/", "/retail-org/solutions/silver/purchase_orders_cleansed.delta/_delta_log/", "/retail-org/solutions/silver/purchase_orders_cleansed.delta/_delta_log/.s3-optimization-0", "/retail-org/solutions/silver/purchase_orders_cleansed.delta/_delta_log/.s3-optimization-1", "/retail-org/solutions/silver/purchase_orders_cleansed.delta/_delta_log/.s3-optimization-2", "/retail-org/solutions/silver/purchase_orders_cleansed.delta/_delta_log/00000000000000000000.crc", "/retail-org/solutions/silver/purchase_orders_cleansed.delta/_delta_log/00000000000000000000.json", "/retail-org/solutions/silver/purchase_orders_cleansed.delta/part-00000-5110efaf-a2b7-41fe-a1c0-63f8e90ba478-c000.snappy.parquet", "/retail-org/solutions/silver/purchase_orders_cleansed/", "/retail-org/solutions/silver/purchase_orders_cleansed/_delta_log/", "/retail-org/solutions/silver/purchase_orders_cleansed/_delta_log/.s3-optimization-0", "/retail-org/solutions/silver/purchase_orders_cleansed/_delta_log/.s3-optimization-1", "/retail-org/solutions/silver/purchase_orders_cleansed/_delta_log/.s3-optimization-2", "/retail-org/solutions/silver/purchase_orders_cleansed/_delta_log/00000000000000000000.crc", "/retail-org/solutions/silver/purchase_orders_cleansed/_delta_log/00000000000000000000.json", "/retail-org/solutions/silver/purchase_orders_cleansed/part-00000-06d5cc45-8388-41cd-b6a9-173c0a4bd498-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/", "/retail-org/solutions/silver/sales_orders/_delta_log/", "/retail-org/solutions/silver/sales_orders/_delta_log/.s3-optimization-0", "/retail-org/solutions/silver/sales_orders/_delta_log/.s3-optimization-1", "/retail-org/solutions/silver/sales_orders/_delta_log/.s3-optimization-2", "/retail-org/solutions/silver/sales_orders/_delta_log/00000000000000000000.crc", "/retail-org/solutions/silver/sales_orders/_delta_log/00000000000000000000.json", "/retail-org/solutions/silver/sales_orders/part-00000-596a0749-4657-4616-aaea-8a8a19de9fcd-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00001-529c1abd-0305-4172-b10c-9061ed5a71f8-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00002-ae45432b-d996-4539-a145-eff1d3ad9198-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00003-dda2dd4a-b133-42de-853a-87e8ab900ccc-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00004-1d981219-2a07-43d9-b3fa-bad542e4e38b-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00005-388e576e-5602-4140-9088-91a49ce9f01a-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00006-a1b22ccf-f3eb-42d9-a129-6501247b1af6-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00007-1a080f20-7116-414e-8299-80964795b127-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00008-5f064136-28c0-456c-8edc-4deb65a963bb-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00009-ac67b427-8320-4e5c-9657-0d27e3fb62f4-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00010-82a1b215-3279-4a21-9efc-13f470cbbdf7-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00011-3816d202-0c28-441a-83cc-501c2b9f6565-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00012-08ea3589-41f6-43b3-9e80-47fbe9b4bee8-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00013-942dab8c-12f2-43c2-8ef0-d081afe5a7d0-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00014-13189bef-0c8d-46f5-8110-ebac7b216f20-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00015-9ecc5e4b-99c7-409c-9e47-bd413e3567c0-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00016-a5ea7ae7-57e7-445e-8cd2-ad2e6dccbafd-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00017-042702b2-1b69-4afc-aeec-08ed39167975-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00018-4106d1db-d457-4ab8-a40a-a31231ccc6d3-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00019-c6bea7fa-1885-4ae0-86fe-b1447f9d4342-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00020-abfab4ef-4d41-47cc-b3cc-5013c32fdc0b-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00021-c0fce417-7e09-407c-ba48-3d9684e3b445-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00022-b5a45e49-b1d9-4ac5-8ef9-a86edf1c80a3-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00023-4a7b4460-593f-40d1-b56d-98f4d5cb216f-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00024-0b0fef9b-298f-49b6-8df6-1587904e9090-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00025-05522774-6a67-4800-8305-f3c2901e1f94-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00026-cf76dc2e-4a9b-4d21-9885-ddbe78de5011-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00027-3b9765e0-5439-4cfd-9378-cc15581080a2-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00028-f51230d4-a48d-4067-a8e1-c6408791c6d8-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00029-dccfacd2-9434-4791-8dbc-115189796bcf-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00030-d3ceae78-5963-4adf-a806-2a41a6c49de3-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00031-0300b5b3-6cd0-4c6a-85d4-41f6d1bec4d0-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00032-c85553e1-c05e-408b-b150-4b8231c5de67-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00033-d24100b2-09ae-42dc-b3ee-f26bd2f3f297-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00034-b38a0aec-dccf-4856-8703-5f814566d38c-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00035-7de5e9a2-e0f3-4941-9066-9ae9667851d3-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00036-376b199f-f275-42a9-9589-804a33232969-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00037-a689bab9-9894-45e8-bcd3-8ff57e2be0b2-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00038-aaac4e61-428d-45f9-96f1-5b82a4f44266-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00039-9a769317-15a7-4e17-a331-13dccff84222-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00040-ce2829e0-8016-44e3-97e7-026936bcf9af-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00041-3462a3a4-00d0-4064-ad61-93861a58be04-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00042-52a9b435-9e87-4805-abd5-b91be3e89504-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00043-892b294d-d45d-47f6-8854-63ea0c62c27c-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00044-681426d4-8de3-4713-9a6d-754d67a1454c-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00045-2c815267-7072-421b-9548-3fb4f7469988-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00046-5734907c-5f63-476a-b641-b12157ab89ee-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00047-d84eb521-05c8-4f5f-a33c-2e17d4abb864-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00048-f54b7228-1a40-4b57-8c03-540a87d3929a-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00049-1d56903f-3781-4ba4-b5d4-32358ff3d6b3-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00050-1dd01a83-c650-4d72-a152-86067175c816-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00051-fec877c5-de96-4be5-9417-953a654791a2-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00052-acce140a-255e-472a-92d9-1925fd338f24-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00053-8a18315e-f975-4212-be91-0cfb35fef8bd-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00054-e7887fa8-0197-4a83-89d3-4563575da378-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00055-eab718a5-5693-41d7-8dd4-6a3d07ffaecf-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00056-7c7a6c9a-4708-4cec-ad63-c6df2ff148c4-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00057-4c99d5c3-5ee9-40a7-849d-83213d8399ee-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00058-5c6605df-f583-43a5-8786-f132a875a229-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00059-54793fd3-d383-47f3-9810-b3ccdb7d4e0b-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00060-bbd56a19-e540-45bd-b7c9-07c0afe25784-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00061-1e87e4ad-7b85-436b-94c1-bc5b57011176-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00062-2e268fe7-6a78-4b3c-8457-2c6091da1fe9-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00063-b3b0fcc0-2d6c-46ae-8746-a100ea9d6d30-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00064-ea2f9a18-f32d-474d-b6ac-81bb87b719cf-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00065-ceb35584-3ec5-4d74-b281-faeb637e115f-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00066-86ae0f9e-a104-43b6-8edc-a2b296fc30bf-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00067-cb482d49-9d02-4cc0-890b-5254885cca5d-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00068-8468940a-b064-4365-8d0e-180f7a65c74e-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00069-ca218a11-028e-4811-9e57-101afcc32730-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00070-0ebbf85e-1960-4b57-ac81-4164b04fc70d-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00071-96639b25-1685-485d-acf8-f7e486d85db2-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00072-5f39948f-7e67-4aac-8328-8ecdab1aef98-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00073-2ee6c3e8-938b-4d0d-9080-d9c165d7ff5c-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00074-ae490ddd-1500-4643-a5c6-db515ba5f3e7-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00075-4b30d47e-6c27-45c9-ab2a-7087a72c0bb8-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00076-f052fbdd-2ff8-442e-9fc8-225a7bc0ff93-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00077-41944abb-f25f-4900-9ed6-e949428eed02-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00078-87f78167-71e8-44a2-9172-da294fe87b7a-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00079-96b7ba71-713c-4184-98ca-985fcc8bfdc9-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00080-dbe26f07-3d5a-42eb-b563-1698ccb39d52-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00081-267ae14f-3970-4f64-9fe0-c27ff6599808-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00082-940e4a16-e5d7-4fb8-b1e2-4fc21d17beb0-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00083-3984a1b4-46fb-40e4-bd24-3dee10fb7cdc-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00084-04ce11ae-e480-4e9d-95dd-ea2870001740-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00085-068796b6-4c07-43b2-8e3f-8f18cec96f2e-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00086-dac2fe00-c19a-4b4d-9088-f322c3b8f270-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00087-f8b391e4-4829-427a-8359-8ddd65bc517a-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00088-d4f705ec-fe04-4d5c-9638-772acf6dbe80-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00089-508bd7bb-4a98-4d8c-b07c-c2cfa22f6315-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00090-7c248042-6386-40d6-bd36-66b4fb61c484-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00091-0afb71f9-8bff-452c-9b35-cd1f2879ffc9-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00092-666e5106-1630-44af-9e69-f4136b6e9a08-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00093-1fc02b05-9dbf-416e-b6ed-193e68542425-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00094-ed71aef6-0755-459f-9362-fd394ad71f16-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00095-bc225cae-d496-418c-b60b-d56b4a94a9ff-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00096-944e0a29-44b7-4cfb-b3f1-1a2ef80c6756-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00097-6a0ccd8a-d191-4219-a0de-8ccc8ea92c72-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00098-260ecea8-e9c1-491c-994e-33e3f18666dc-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00099-46f979f6-02f9-4348-bcea-032837803e77-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00100-58869468-a9b9-4e4c-bdf2-7fb542e59fed-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00101-de84a8b4-13a8-4c26-8188-0ddadc8ce355-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00102-90b670cf-5f92-4da9-bd48-f4b2de1fce37-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00103-c698c39d-445e-48ff-8e8d-5999d3b5a7e7-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00104-2dd233a5-59c0-4a3a-99d5-dadbadad0bba-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00105-1011cfb1-d679-4648-905d-6656ab3ae93e-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00106-98710a22-27fe-4bfe-abf0-8e1d516ae8f5-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00107-df054751-939d-4720-bbd1-b81183fa6f6d-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00108-a5adbf6f-ae0f-4c9c-b947-1b1fc3f79b41-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00109-0ecc4304-a91d-4a5d-9049-d0e25f1f97b2-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00110-62cad0fa-b4c4-43a5-96b8-bc0bda9d4719-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00111-ef3602e1-6985-41bc-aa13-d9fbd9c23471-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00112-313b86d3-69b5-4ff2-a458-960a44e99828-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00113-659d37fb-4fb1-4c54-9f28-0f5827d7c49b-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00114-61f90bcb-a2ba-4e4b-bc95-f1d15b783bda-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00115-5ff9c2a6-801d-4169-9e1f-c79840904ba7-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00116-36801dc4-a520-436b-8a7f-320d5633c6df-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00117-9dc9275e-862d-4754-b50c-37e7b49b5567-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00118-ea3d7e09-4610-424a-834d-93ca82dc0826-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00119-9990d1b9-eba6-46a1-9af6-df910c3e125e-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00120-d8abfe41-4c29-4a2d-b48f-a5ff8c290450-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00121-29d7d30c-2f05-4f25-905b-3b55431b729f-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00122-1849d995-be86-4463-8e56-34812f53507a-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00123-f7d742d4-3aba-468d-980f-94b91bf78758-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00124-42c7bd70-4ac0-4c9c-be16-f71c9619e704-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00125-c5830a70-9813-4bee-816f-e9215b9e3012-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00126-a6160da4-1fe9-4db9-b993-40d4c6fb05c6-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00127-97d5ae11-ae7f-4b3c-aa51-665ec4117f6e-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00128-ab7db55a-4ac1-456d-82bf-d68c4b90ed90-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00129-0794bbc5-1133-4724-8fba-e354a39a76db-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00130-ff24c572-85f4-4088-b9de-f5ac8731cd38-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00131-f18d7633-69f1-41d1-8887-6b9cd2928cf0-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00132-572dc12f-fc6c-4fbc-b6c9-c0ee429a5e8e-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00133-8580548a-88e0-440e-bc7a-7a0cfc0f3b13-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00134-6bcd86d3-279b-4b81-921d-a616e6fcd061-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00135-4136b1ab-8089-480d-b109-1b1c97a78321-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00136-aeadac01-de0b-4025-b674-72fc42788c76-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00137-2c66d31f-d394-4838-86ed-c8f39e6deb39-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00138-b6ffa75e-a708-40e3-b8b0-a85441b4d067-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00139-a46a0fab-bbfe-49e3-af76-fd262873d83e-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00140-22597dae-37b7-4c66-adf1-b96b0eb17c66-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00141-058c330d-f3b8-438b-aaeb-44cca9526699-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00142-e7dd5d94-7f89-4358-b3cc-17ce6cc37d46-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00143-465752aa-483c-48be-b0e7-e08642524250-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00144-b4e368e2-dba6-4c29-89e6-ab82d0b2e9a0-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00145-55f4c4b0-2e94-4629-b3e1-dea7e2d6d729-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00146-fd1bb570-9e85-4752-97fc-3653d0ab9ded-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00147-f9092020-2f1a-4be6-859d-570b7f9158d6-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00148-a6c9b928-bce8-4d77-994d-c0c50cdd0a29-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00149-54bb1210-878c-4f7d-b1b7-3acb0c887af8-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00150-362094b0-3009-4e73-b145-2ef52994fd04-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00151-266aa304-0e33-4ecb-8669-29a9942bc0f9-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00152-857dee25-ac65-4b8a-93cb-eedeaed359b1-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00153-cad54416-3065-4b3f-9efe-a131f0f8fe23-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00154-0865cc09-0355-4a4a-90e6-5687459b5ed5-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00155-65ee50a7-7287-4c1b-87e0-8a0126bd100b-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00156-737c6154-2d17-4abf-80f3-e1ddb3f9fe9a-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00157-35432185-0701-4a92-a27e-39c92ef100b5-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00158-ec4d00ef-925b-4d8c-8677-22fa6cea688d-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00159-d71a2b25-f22a-4c29-b2d5-426701643a9d-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00160-defcae83-3520-4537-8c9e-c5dd80c872dc-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00161-1a7687fb-1e5a-4eaa-ae63-b8c122fa77f6-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00162-84dd7735-a137-4592-bbae-fb94793de695-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00163-edbf5892-cc69-4ad4-8d7c-7d2f0dc86bdd-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00164-aa72504d-dc80-43e2-910f-cdb7df717300-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00165-b8587eb3-9ab3-4b7d-8a56-8e49b1979bd8-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00166-bf069bfc-4b5f-4603-9b35-dbfe50f229f8-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00167-bf99945f-93c1-4ae5-901f-3a1e8f7ff972-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00168-27e0c8a5-84d0-4f16-acd4-98fdbb9b7d21-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00169-c0c52cca-4625-4e78-b658-f4d36842bb56-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00170-4161a58a-43bc-446a-95af-188e922d4ce7-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00171-ff98c8a7-499f-44f9-9237-510c558214aa-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00172-2d3ca012-3d7c-4077-84cc-f3173d42dad3-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00173-b5dd3a11-87dc-4f87-909b-85fb5101a8fe-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00174-7fb1d5d2-242b-4b53-8a6a-30e03ef1aad7-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00175-0211bf74-60ea-4355-b20c-e20e760a2573-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00176-3488dd3e-86d5-408d-a920-3f3149b3120c-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00177-b1729cd1-f15b-43da-a723-18c81218a3b0-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00178-d9a36e58-29f0-42ee-ba2f-1583223ba968-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00179-c45112a5-8628-434d-a2b8-57609d2fdb03-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00180-4a2b5785-e3ac-4216-af4c-646af5cd1c53-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00181-7e5c8f4e-7cfa-41e0-89ed-66553e22da2c-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00182-e9532c6e-7279-4285-8629-f88b1bfc07f8-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00183-8b2186e5-5886-4db6-bec0-37431c0b9954-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00184-73d5bbd9-e79d-4bf1-bc01-d5cf449079ea-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00185-c51ec718-06f9-41d9-8039-25895f6f71e8-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00186-ad248c22-d5f9-4af7-89ed-baffab0b2996-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00187-c712d96c-f84f-49b9-8281-fe7351682b11-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00188-367db1ff-4935-475d-bf49-259c14d95fb0-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00189-1e8aef86-aba2-469a-af8b-32c9ad65adbe-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00190-6f7a252c-4791-4e65-9dd7-19cfaddfaf54-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00191-6285a9e0-f32d-4835-8a99-fd19c4a614db-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00192-f1ce3d1d-19b3-4c11-87bf-2b07c08ae579-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00193-170caa6e-f26b-4a37-a29d-1714e3561343-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00194-3099336f-fcaa-46d0-9f16-2ae5edc0316d-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00195-63b870f7-96c2-4371-9dca-e7bc5d978777-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00196-4005bb53-95ce-4942-b2d0-51195d17d79b-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00197-6dd08883-e343-4d7f-a615-67d5ec5452d0-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00198-d8ddcdad-8f7d-47c2-9539-69eea426f391-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders/part-00199-b9022048-ddcd-4b5e-84af-615af135656c-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/", "/retail-org/solutions/silver/sales_orders_3/_delta_log/", "/retail-org/solutions/silver/sales_orders_3/_delta_log/.s3-optimization-0", "/retail-org/solutions/silver/sales_orders_3/_delta_log/.s3-optimization-1", "/retail-org/solutions/silver/sales_orders_3/_delta_log/.s3-optimization-2", "/retail-org/solutions/silver/sales_orders_3/_delta_log/00000000000000000000.crc", "/retail-org/solutions/silver/sales_orders_3/_delta_log/00000000000000000000.json", "/retail-org/solutions/silver/sales_orders_3/part-00000-c3324580-732a-492f-9391-006e87f546c2-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00001-1a8afdaa-ad03-4746-a5e9-ebb59da58727-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00002-558e343e-b6da-45b4-9bb2-86d6b2dfd77d-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00003-2ea6e2d4-0c32-41f9-965b-bfb3ee1e32d7-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00004-d338a476-9a52-41f7-b62a-de2a7f99ea24-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00005-54b82d2f-6f77-4bb5-8799-8d809c9592e7-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00006-2028d56d-3272-493a-992f-334dfcd25f7a-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00007-261a8910-aa31-42e3-a3c0-830e2a854404-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00008-e03ba0f9-1a2d-4f1d-953f-21d68da8153b-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00009-979a4d2f-4ff7-4fcc-8eed-8207c66c52e6-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00010-8ff9b50c-b6be-4918-be7d-fb3b6ed05e29-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00011-aacd2b26-8a48-4323-9088-8bcd9f53934d-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00012-afb698d3-8579-4bd2-965c-773ed573994a-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00013-966d155c-694e-4f1d-a5b1-da752fcdf40b-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00014-bc276f64-0e56-4776-8aa0-a87a233c79aa-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00015-88f423df-d7a0-4081-9e47-880a62360b28-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00016-ce78e3cb-c162-47a8-994c-b2ab47604b9c-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00017-806345fc-c234-4cdd-aba7-bcd9cd81d495-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00018-b1d455a6-c154-4fbe-ba8d-7b6bf5fa912c-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00019-73e9894c-c0aa-4f3d-b7fb-7c71077713f4-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00020-865ceae5-5f37-4ecf-bc75-ba86a8f7bebb-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00021-16b5c15b-b8b4-4478-bf57-136f61a9e2e9-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00022-266499df-871e-4975-8c14-2feb281ffff8-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00023-9077dce0-c14e-40f3-8139-e6c2b7c3e799-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00024-ac4bf056-3a2c-4d23-84c8-fb49ffbe8dbd-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00025-db11f5f0-4709-4aaf-adb4-810fa60e40f6-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00026-125ee2c7-53b8-4343-8f35-954aef604c8f-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00027-0de99332-81e6-438a-93b9-1abc1adefdc8-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00028-9984af8a-5236-4f4c-9936-3f5011215b88-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00029-13921bee-684e-4d37-a90a-12e2a96e000d-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00030-34da4dd0-a501-413b-a09d-4be59c1066f1-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00031-78431ede-f942-4db2-a828-61521f82255e-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00032-029d36dd-69eb-4c33-9ea8-a63e8be27ae0-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00033-bca5a922-109f-49c0-9318-b6ff73e6497a-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00034-167f4e8e-5f79-4afe-a134-f30c1ad3acbf-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00035-8b5924a1-70e2-4f57-888c-caf3110706c2-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00036-c91351f6-4387-438c-a515-0403e181b074-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00037-51619262-b65b-4cd1-b5cd-d2420ccf2860-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00038-d1b1f3c2-12a7-4812-b120-86ec509947a1-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00039-51322af7-d41c-490a-9d7b-3a7dcf091661-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00040-8056ec66-df98-472f-94da-dc2f98a5bce6-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00041-b0249419-1ed8-42bf-b194-d403cb64605e-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00042-a833840b-ab47-48ef-8ca4-9154a2adcb87-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00043-08da2b23-b44e-4eb5-9d6e-34c53bfa37ac-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00044-47129183-8feb-4cfc-8922-790878d19954-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00045-5702274f-6103-4724-bbf0-93a9fd3eea11-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00046-aa6b921c-e75b-4e38-ab7d-5c2d797032cc-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00047-73ba72c4-1e96-4e6b-8f54-569d6cad8050-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00048-47256b6d-257c-4a2f-ac33-dad001315dfa-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00049-2fb2d8e8-45a2-4cfb-b7d1-a5d35d6adede-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00050-0098e0e7-3ebb-4a52-a72d-2795a1235806-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00051-7846548d-8537-4f51-8dce-e981a4e090ec-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00052-9ee3934a-3d01-4422-85d7-9895fa9a4b89-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00053-95bf9b60-ae0c-4425-b034-fc4550fdb1a0-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00054-91deee9a-bc9f-4d7c-8192-117dbdad3c9f-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00055-9d9345b7-6b74-4d31-b2d3-acc58b0637a3-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00056-315b3632-b162-4d97-b462-facbf5a2946b-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00057-6ed358a4-ccdd-484e-b0ac-dfd03e90a402-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00058-3b73533e-e781-4913-a481-ab09fed46714-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00059-67dd375d-36c8-4362-bd5e-05fa5b825934-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00060-9c25ea09-89a0-4fd1-8715-871afa62dbf3-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00061-bd65e15a-4005-481b-8779-be19d6b72c09-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00062-f39e3f8a-4dfe-49b2-a299-e00dcc3da911-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00063-d85bb470-7e26-459c-a2ab-bcaa9d25a88e-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00064-7044c512-4019-4f3b-a60d-414b6511e81d-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00065-bf34dcf5-f614-48e3-8787-e157de1a14f2-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00066-bb7b9dc7-cc2e-412d-8538-47d1faa23508-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00067-dabe04c4-cbdb-4f4b-9f93-5d9f32207b3c-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00068-03573d12-8081-4e1d-970a-0c66df2bf905-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00069-d49fad70-eb03-4684-893a-6d3bd14453f5-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00070-6e0ecfc1-9c2b-4585-b7b0-5b12f63e9fce-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00071-4d967064-fc9b-4c97-9c77-0ccc737a02b5-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00072-27bc2843-8388-4933-b5ab-2f8ad06e8a2f-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00073-144b9c6c-7e3b-485c-a27e-ca6936c1b19e-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00074-f4cf83cd-1593-49bc-9dd5-a50037980118-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00075-2d4806a1-e8a9-46ff-9e90-8fa7eb309faf-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00076-159eb87d-8907-47cb-9b59-e1eb635300a5-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00077-1c22c4b5-8a04-49b3-b4e7-cbbddbf7f13f-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00078-4892189d-a4bf-4017-98e2-6a20a61c8a84-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00079-8f9e214f-7402-4f23-a3bb-5b2343540670-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00080-92caeaf2-561b-466d-873f-b3b21ef94a63-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00081-71753f7a-758d-489f-b17e-b0376639dceb-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00082-067db036-e1a9-40e5-b02a-42d0ea7e347e-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00083-e243ce16-a7b2-45be-a516-5da45dcbe2ff-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00084-959b74b5-a45a-4f81-b580-f39debc23687-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00085-9e02e94d-e8c2-4109-baed-b6513381053d-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00086-a28917f0-80d4-4856-8cc1-f419473def34-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00087-e8ab8ffd-5147-42c2-9f29-3f70996db7ec-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00088-b1322fba-0506-45d8-9d02-a8e288775a6f-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00089-f57b06fc-6c97-4686-b3fe-eec2adc7afac-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00090-82690dc7-8220-47ca-a2d7-5178144f2252-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00091-350d90de-9cd8-4e3f-a360-a547538c7255-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00092-e9c079e0-d979-4d7f-ada2-a9b180f08665-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00093-49d627b7-dc0d-4a06-9e47-6c016d233b99-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00094-ac0ebfda-e138-4ac1-aa1b-aa1676af1e3a-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00095-6c777694-59f0-4242-bdd2-8cc3fd48bdbd-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00096-30788be9-3b27-41d3-899c-c40c56e0e050-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00097-1b2ee33c-2dde-4918-9c3f-6acc33f17f57-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00098-a6fbd06d-b52a-49e4-bc9c-0a3de2f3efd9-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00099-28d73d67-62a3-43fd-ad49-6b3d3ae73a5e-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00100-d55bb6fc-628b-412e-9529-7adfd5e70ccb-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00101-175ab1c3-52ad-4897-a79d-bf8b7c0f3c0e-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00102-e9116588-6655-4831-a15d-f76feae01cb1-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00103-1ddc595e-69a1-47a3-a563-7652a7befb32-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00104-c15af9e1-59e9-4795-a054-38ddbce43bc4-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00105-070a4c09-babd-4153-aaa3-f4341796deba-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00106-044fb464-7d2d-490f-9919-89e69fc10b6c-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00107-b437f3d6-9ae3-4574-b197-0b525ca114fe-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00108-42f4e69e-34e1-43d8-a8ad-3ed91ec5ad3c-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00109-4abed381-e950-4dd1-9cd5-ad78176738d2-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00110-757fdcc3-3dd9-401c-9546-59a53084f7d6-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00111-c5579631-f52a-4170-9a7d-9000bec8e727-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00112-465ed257-819d-4362-83be-aa96644c5287-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00113-f3e1235c-8441-40ee-82af-6025d0839ca2-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00114-37313691-9fb3-40c2-848d-a5655f82f831-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00115-9cc5d976-b431-4dc5-b212-0c518e6bb9be-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00116-f3afca5a-fb38-43be-9dfc-6983ba255e55-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00117-426ac214-f2b8-40b7-ab33-3cd4f97d987d-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00118-5838772a-737b-404f-be42-ce21ff4139b9-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00119-9a285b3b-bcfa-409a-b79f-b731f15b53b1-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00120-6ae75ecf-de21-4908-857d-50f87750a8c8-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00121-983c4199-e747-42cf-8189-85bb450a2954-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00122-2aa25dd7-0f4e-492b-a6a1-aae063151103-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00123-9d6d8d5e-4ef5-40a1-af76-40dac08c5871-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00124-cdea225f-81de-4472-a23d-1fe22d53485d-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00125-ccf17e70-612f-427a-ba18-c80b4c449ba2-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00126-ef47cc73-b46d-4927-82ce-077efb7ddff7-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00127-6d30bab4-2047-43ce-a4f8-8323a42f11f8-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00128-f2c31b04-e102-4d07-9c43-4de9d77acca2-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00129-0f5766b2-7ef8-44ca-acdd-c6deb7af3b87-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00130-97f97b86-6a5e-48fc-bdf1-be19625252f5-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00131-30d19a40-41a9-4566-a1a6-858a03cdfae4-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00132-ffa1bfe6-6d4f-4d6c-a3b0-b6549cfc68ad-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00133-4853417c-9f7b-4231-aa7a-79d246768198-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00134-a22432ba-59ce-4c24-a40e-bfdd2bf988fa-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00135-aa5bed65-c3ff-4e2d-8663-2bd54965c56e-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00136-1069dfc5-7190-4ec1-bbe8-3a87506583fc-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00137-55bcafbe-7327-44de-a207-5093d6a40780-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00138-b50badfa-a775-4924-b33e-9e8be1059402-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00139-9e4be8da-2cae-49e5-810e-633d8af61063-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00140-dd782fcf-ace8-4b0a-8171-3b106b91bbb7-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00141-21808489-cba7-4568-adad-06ebe7c18164-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00142-577dff09-ff1d-4e3b-9481-64f6a9bc1c8a-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00143-f41e281d-dc61-4192-82ec-eeeff7778cb3-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00144-19835963-f8ed-43d3-a661-f83c0a3ba760-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00145-1515da5a-8e7c-4335-b658-d2bea3e712dc-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00146-2ff33ed7-c826-4d49-bb64-7e305d529551-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00147-c0cb2041-5d1d-4d03-a1db-8ffad9cb8f60-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00148-c1e4d7e4-c38b-4c2a-b167-500671de0306-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00149-a620ea2f-d2b0-4f29-ae52-ba29c1e29a54-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00150-830da1f4-cd7a-4907-8295-6116e9a14301-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00151-97e976fa-b92a-460f-bdd6-6b5f9500e0a0-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00152-8eaaae90-3a8e-4ced-8ad7-a39a535b9879-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00153-21505690-c565-415d-b7a7-19500cf3365a-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00154-d8214a2c-e6d7-4b74-9528-1b27cecda676-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00155-b66c95da-4c99-4198-a868-edf30ca34d0f-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00156-a51999a7-fafc-4499-ad63-7957660ea286-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00157-7347da92-f343-4988-8ab5-3397fb53cd54-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00158-6e0c30ae-e90e-4e4d-abb1-7a78c28a0104-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00159-6ca94da9-8618-4911-a8b0-bb599cae0caf-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00160-7ee577fa-791f-48ac-a565-8fb34f077c4f-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00161-8d4b3423-bb9c-4fc4-a025-86fdc976faac-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00162-0b3c273d-8b3b-4472-8303-d631ed222172-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00163-94a34f35-31e7-4acc-b576-c81587b91fa7-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00164-c8680a10-c48e-44d6-a84f-22170fc0484f-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00165-71ad62c6-7ada-40af-83a8-cb583ba2115c-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00166-3b6602cd-65a1-47f7-a432-2308fcbd9ad9-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00167-7ae529fa-a4de-44d0-8c51-f281aee76b97-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00168-808164ac-4310-43cd-93b8-575f14a765c6-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00169-8939d87b-83ef-488f-91b9-311b7d54d57f-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00170-d6af9db5-6569-4d69-94c4-78f3c9c2ce0c-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00171-59226ce0-a751-4f6c-8640-aecc16ff6ef8-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00172-3cdd868b-f763-44e4-a1a2-38d38ad7a5aa-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00173-836d7e57-8c3a-49b0-a47f-3df51d4293e0-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00174-87087099-40d2-4e80-88c3-2754782c0fde-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00175-b9b35665-8384-4053-8f6e-599725a8b461-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00176-9997d62a-459b-44c8-8408-621ae9973427-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00177-c5c07f43-6d4e-4042-aa1a-93c1b77973e2-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00178-bca28289-ad8b-4bc0-b128-6f7381e4cc53-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00179-9279d2f8-b141-4422-9cab-bfe8c1d0f82b-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00180-98be1e52-b5cf-43f2-8cc8-38c2f2529fa4-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00181-467814eb-b8ca-479b-8995-173fec602e6d-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00182-133f9531-10c7-4d38-b756-72310b7d3dc4-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00183-a8d866e7-5eaa-488a-bc4d-edcabe46c474-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00184-e55cd1d3-7ccf-4a88-984c-e7044446c4a1-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00185-d8e043e4-8fe4-46f9-9606-89bec84618d2-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00186-eeca21f1-265e-4cb3-aa4e-78811c8024c8-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00187-1d38dd7f-b718-4c37-a91d-6beb9ea600bf-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00188-5fdb4a83-7120-45a4-91eb-4e2b47befd72-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00189-ca4aada7-cd00-4773-a1aa-9c0172ca5588-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00190-41067a53-c35f-4550-9550-d56d8009b5cd-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00191-63f4ad6a-2d4c-4f53-a995-fdc7de0de5e5-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00192-2aeda72e-dbde-44d8-ab47-5d67bcf79560-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00193-789cfe99-c6f3-415f-afed-57856f8d6e39-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00194-477d6bc3-72b6-44eb-ae6b-751a759e7461-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00195-9909da6d-0f63-44ef-b58c-b690aff52c30-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00196-90b82c9d-0364-44ed-b147-5d80662b5a61-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00197-56d0e7a6-77d2-4e41-8d34-5cc0f4e5c798-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00198-b7dec7f0-d792-43e6-8843-7cb384d072e5-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_3/part-00199-402c5953-ab35-4f38-a6c2-6a6166769968-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_verified/", "/retail-org/solutions/silver/sales_orders_verified/_delta_log/", "/retail-org/solutions/silver/sales_orders_verified/_delta_log/.s3-optimization-0", "/retail-org/solutions/silver/sales_orders_verified/_delta_log/.s3-optimization-1", "/retail-org/solutions/silver/sales_orders_verified/_delta_log/.s3-optimization-2", "/retail-org/solutions/silver/sales_orders_verified/_delta_log/00000000000000000000.crc", "/retail-org/solutions/silver/sales_orders_verified/_delta_log/00000000000000000000.json", "/retail-org/solutions/silver/sales_orders_verified/part-00000-102d5b73-9be0-4412-9544-a80ca2b6d189-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_verified/part-00001-00d98295-9ec6-4852-87a8-21cdde66740e-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_verified/part-00002-47405cba-2139-49da-bd12-d4b34ff2f258-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_verified/part-00003-63d3c002-8297-4e32-abda-53aaa9e32e83-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_verified/part-00004-9ad89585-58c9-4d3d-adb3-942ed50d0094-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_verified/part-00005-ba292f18-42d8-4a2c-b2d6-24b7c7ffe0c7-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_verified/part-00006-924cce28-746d-418d-a593-c8d2df6eedf2-c000.snappy.parquet", "/retail-org/solutions/silver/sales_orders_verified/part-00007-0a1671cf-5b74-467d-8bca-7860819a5376-c000.snappy.parquet", "/retail-org/solutions/silver/suppliers/", "/retail-org/solutions/silver/suppliers/_delta_log/", "/retail-org/solutions/silver/suppliers/_delta_log/.s3-optimization-0", "/retail-org/solutions/silver/suppliers/_delta_log/.s3-optimization-1", "/retail-org/solutions/silver/suppliers/_delta_log/.s3-optimization-2", "/retail-org/solutions/silver/suppliers/_delta_log/00000000000000000000.crc", "/retail-org/solutions/silver/suppliers/_delta_log/00000000000000000000.json", "/retail-org/solutions/silver/suppliers/part-00000-7502d2de-29a5-4960-b7c9-ac26fcfa20e8-c000.snappy.parquet", "/retail-org/suppliers/", "/retail-org/suppliers/suppliers.csv", "/weather/", "/weather/data/", "/weather/data/_SUCCESS", "/weather/data/_committed_6522815070831625438", "/weather/data/_started_6522815070831625438", "/weather/data/part-00000-tid-6522815070831625438-4358ade9-a419-4a61-a2ea-9f64c9c3c0fc-3504-c000.snappy.parquet", "/weather/data/part-00001-tid-6522815070831625438-4358ade9-a419-4a61-a2ea-9f64c9c3c0fc-3505-c000.snappy.parquet", "/weather/data/part-00002-tid-6522815070831625438-4358ade9-a419-4a61-a2ea-9f64c9c3c0fc-3506-c000.snappy.parquet", "/weather/data/part-00003-tid-6522815070831625438-4358ade9-a419-4a61-a2ea-9f64c9c3c0fc-3507-c000.snappy.parquet"]

