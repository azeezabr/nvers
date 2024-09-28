# Databricks notebook source
import requests
from datetime import datetime, timedelta
import time
from azure.storage.blob import ContainerClient
from azure.storage.blob import BlobServiceClient

# COMMAND ----------


databricks_instance = dbutils.secrets.get(scope="nvers", key="Workspace")
token = dbutils.secrets.get(scope="nvers", key="token")
existing_cluster_id = dbutils.secrets.get(scope="nvers", key="cluster")
user_dir = dbutils.secrets.get(scope="nvers", key="usr_dir")
storage_name = dbutils.secrets.get('nvers','storage_name')
container_name = dbutils.secrets.get('nvers','container_name')
SID = dbutils.secrets.get('nvers','SID')


adls_path = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net"
process_store_path = f"{adls_path}/process_store"
job_control_table_name = 'job_control_table'
backfill_control_table_name = 'backfill_control_table'
job_control_table_path = f"{process_store_path}/{job_control_table_name}"
backfill_control_table_path = f"{process_store_path}/{backfill_control_table_name}"


headers = {
    "Authorization": f"Bearer {token}"
}

# COMMAND ----------


'''

sql = f"""
    INSERT INTO delta.`{job_control_table_path}`
    (continue_flag, last_updated)
    VALUES 
    ('Y', current_timestamp());
"""
spark.sql(sql)

'''



# COMMAND ----------

def check_file_exists(year, month, day):

    base_url = f"https://{storage_name}.blob.core.windows.net/{container_name}"
    directory_path = f"bronze/timeseries-daily/year={year}/month={str(month).zfill(1)}/day={str(day).zfill(1)}"
    full_url = f"{base_url}/{directory_path}?{SID}"

    try:

        account_url = f"https://{storage_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url, credential=SID)
        #blob_service_client = get_blob_service_client_sas(SID)
        container_client = blob_service_client.get_container_client(container_name)
        blobs = list(container_client.list_blobs(name_starts_with=directory_path))
            
        if len(blobs) > 0:
            print(f"Files found for {year}-{month}-{day}")
            return True
        else:
            print(f"No files found for {year}-{month}-{day}")
            return False

    except Exception as e:
        print(f"Failed to check file existence for {year}-{month}-{day}: {str(e)}")
        return False

# COMMAND ----------

def check_continue_flag():
    df = spark.read.format("delta").load(job_control_table_path)
    flag = df.where("continue_flag = 'Y'").count()
    return flag > 0

# COMMAND ----------

#df = spark.read.format("delta").load(job_control_table_path)

#display(df)

# COMMAND ----------

def log_job_details(job_id, run_id, processed_date, start_time, end_time, status, cluster_id, notebook_path, result_message):
    spark.sql(f"""
        INSERT INTO delta.`{backfill_control_table_path}`
        (job_id, run_id, processed_date, start_time, end_time, status, cluster_id, notebook_path, result_message, last_updated)
        VALUES ('{job_id}', '{run_id}', '{processed_date}', '{start_time}', '{end_time}', '{status}', '{cluster_id}', '{notebook_path}', '{result_message}', current_timestamp())
    """)

# COMMAND ----------

def create_backfill_job(year, month, day,user_dir,existing_cluster_id):
    job_name = f"Backfill Job {year}-{str(month).zfill(1)}-{str(day).zfill(1)}"
    businessdate = f"{year}-{str(month).zfill(2)}-{str(day).zfill(2)}" 
    job_json = {
        "name": job_name,
        "tasks": [

        

            {
                "task_key": "silver_stock_price_daily",
                "notebook_task": {
                    "notebook_path": f"{user_dir}/nvers/src/processing/002-silver/silver_stock_price_daily",
                    "base_parameters": {
                        "year": year,
                        "month": month,
                        "day": day
                    }
                },
                "existing_cluster_id": f"{existing_cluster_id}",

                 
            },


            {
                "task_key": "gold_fact_DividendPayout",
                "notebook_task": {
                    "notebook_path": f"{user_dir}/nvers/src/processing/003-gold/gold_fact_FactDividendPayout",
                    "base_parameters": {
                        "year": year,
                        "month": month,
                        "day": day
                    }
                },
                "existing_cluster_id": f"{existing_cluster_id}",

                "depends_on": [
                    {"task_key": "silver_stock_price_daily"}
                ]
            },

            {
                "task_key": "gold_fact_StockPriceDaily",
                "notebook_task": {
                    "notebook_path": f"{user_dir}/nvers/src/processing/003-gold/gold_fact_FactStockPriceDaily",
                    "base_parameters": {
                        "year": year,
                        "month": month,
                        "day": day
                    }
                },
                "existing_cluster_id": f"{existing_cluster_id}",

                "depends_on": [
                    {"task_key": "silver_stock_price_daily"}
                    
                ]
            },
            # Add more tasks later
        ]
    }

    response = requests.post(f"{databricks_instance}/api/2.1/jobs/create", headers=headers, json=job_json)

    if response.status_code == 200:
        job_id = response.json().get("job_id")
        print(f"Job created successfully for {year}-{month}-{day} with Job ID: {job_id}")
        return job_id
    else:
        print(f"Failed to create job: {response.text}")
        return None


# COMMAND ----------

def run_job(job_id):
    run_url = f"{databricks_instance}/api/2.1/jobs/run-now"
    response = requests.post(run_url, headers=headers, json={"job_id": job_id})
    if response.status_code == 200:
        run_id = response.json().get("run_id")
        print(f"Job {job_id} triggered successfully with Run ID: {run_id}")
        return run_id
    else:
        print(f"Failed to run job {job_id}: {response.text}")
        return None



# COMMAND ----------

def check_job_status(run_id):
    
    status_url = f"{databricks_instance}/api/2.1/jobs/runs/get"
    while True:
        response = requests.get(status_url, headers=headers, params={"run_id": run_id})
        if response.status_code == 200:
            state = response.json().get("state")
            life_cycle_state = state.get("life_cycle_state")
            result_state = state.get("result_state")


            if life_cycle_state == "TERMINATED":
                if result_state == "SUCCESS":
                    print(f"Job Run {run_id} completed successfully.")
                    return True
                else:
                    print(f"Job Run {run_id} failed or was cancelled.")
                    return False
            elif life_cycle_state in ["PENDING", "RUNNING"]:
                print(f"Job Run {run_id} is still running...")
                time.sleep(30)  # Wait for 30 seconds before checking again
            else:
                print(f"Unexpected job state: {state}")
                return False
        else:
            print(f"Failed to get status for job run {run_id}: {response.text}")
            return False

# COMMAND ----------

def main():
    start_date = datetime(2024, 6, 1)
    end_date = datetime(2024, 1, 1)

    current_date = start_date

    while current_date >= end_date:
        year = current_date.year
        month = current_date.month
        day = current_date.day

        if not check_continue_flag():
            print("Job control flag set to 'N'. Stopping the backfill process.")
            break

        if check_file_exists(year, month, day):
            job_id = create_backfill_job(year, month, day,user_dir,existing_cluster_id)
            if job_id:
                run_id = run_job(job_id)
                if run_id:
                    start_time = datetime.now()

                    job_success = check_job_status(run_id)

                    end_time = datetime.now()
                    status = "SUCCESS" if job_success else "FAILED"
                    try:
                        log_job_details(job_id, run_id, current_date.date(), start_time, end_time, status, existing_cluster_id, "REDACTED", "")
                        print(f"Job details logged successfully for job_id: {job_id}, run_id: {run_id}")
                    except Exception as e:
                        print(f"Failed to log job details for job_id: {job_id}, run_id: {run_id}. Error: {str(e)}")
                    
                    if not job_success:
                        print(f"Job for {year}-{month}-{day} failed. Exiting...")
                        break
        else:
            print(f"No data available for {year}-{month}-{day}. Skipping...")

        current_date -= timedelta(days=1)

# Run the main process
if __name__ == "__main__":
    main()

# COMMAND ----------


