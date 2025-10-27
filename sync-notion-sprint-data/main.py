# main.py - 完整最終確認版

import os
import json
import requests
import re
import functions_framework
from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timedelta
import copy

# ==============================================================================
# 1. 全域配置讀取 (Global Configuration)
# ==============================================================================
BQ_PROJECT_ID = os.environ.get("BQ_PROJECT_ID")
BQ_DATASET_ID = os.environ.get("BQ_DATASET_ID")
BQ_ALL_TASKS_TABLE_ID = os.environ.get("BQ_ALL_TASKS_TABLE_ID")
BQ_COMPLETED_TASKS_TABLE_ID = os.environ.get("BQ_COMPLETED_TASKS_TABLE_ID")
NOTION_API_VERSION = os.environ.get("NOTION_API_VERSION", "2022-06-28")

# 全域變數，將在主函式中動態設定
NOTION_SPRINT_DATABASE_ID = None
NOTION_TASK_DATABASE_ID = None
NOTION_PROJECT_DATABASE_ID = None

# ==============================================================================
# 2. 輔助函式 (Helper Functions)
# ==============================================================================

def initialize_bigquery_client():
    try:
        # use project id to create a bigquery client  
        # client is like a controler that can manipulate the dataset and tables in foospace-data
        client = bigquery.Client(project=BQ_PROJECT_ID)
        print(f"BigQuery client initialized for project: {BQ_PROJECT_ID}")
        return client
    except Exception as e:
        print(f"Error initializing BigQuery client: {e}")
        return None

def upload_dataframe_to_bigquery(df, table_id, client, sprint_name=None, department=None):
    if df.empty:
        print(f"DataFrame for {table_id} is empty. No data to upload.")
        return
    full_table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_id}"
    print(f"\nAttempting to upload data to BigQuery table: {full_table_id}")
    try:
        delete_query = None
        if sprint_name and department:
            if table_id == BQ_ALL_TASKS_TABLE_ID:
                delete_query = f"DELETE FROM `{full_table_id}` WHERE sprint = '{sprint_name}' AND Department = '{department}'"
            elif table_id == BQ_COMPLETED_TASKS_TABLE_ID:
                delete_query = f"DELETE FROM `{full_table_id}` WHERE completed_sprint = '{sprint_name}' AND Department = '{department}'"
        
        if delete_query:
            print(f"Executing targeted delete for sprint '{sprint_name}' and department '{department}'.")
            query_job = client.query(delete_query)
            query_job.result()
            print("Delete completed.")
        
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
        job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
        job.result()
        print(f"Successfully uploaded {job.output_rows} rows to {full_table_id}.")
    except Exception as e:
        print(f"Error uploading data to BigQuery table {full_table_id}: {e}")
        raise

def get_page_title(page_id, notion_token):
    if not notion_token: return f"Page Not Found ({page_id})"
    url = f"https://api.notion.com/v1/pages/{page_id}"
    headers = {"Authorization": f"Bearer {notion_token}", "Notion-Version": NOTION_API_VERSION}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        page_data = response.json()
        properties = page_data.get("properties", {})
        for prop_value in properties.values():
            if prop_value.get("type") == "title":
                return "".join(t.get("plain_text", "") for t in prop_value["title"])
        return f"Unnamed Page (ID: {page_id})"
    except Exception as e:
        print(f"Error fetching page title for ID {page_id}: {e}")
        return f"Error Fetching Page ({page_id})"

def _query_notion_database(database_id, notion_token, query_payload={}):
    
    # database_id = Notion database ID
    # notion_token = real Notion API token for authentication 
    # query_payload = Notion search condition ex. filter, sort, default set empty dict
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    headers = {"Authorization": f"Bearer {notion_token}", "Notion-Version": NOTION_API_VERSION, "Content-Type": "application/json"}
    # results collected 
    all_results = []
    # Notion API might send through multiple pages, bool to check if continue
    has_more = True
    # split page cursur to tell Notion where is the next pack of data 
    next_cursor = None
    
    # while there is more data 
    while has_more:
        # 
        payload = copy.deepcopy(query_payload)
        if next_cursor: payload["start_cursor"] = next_cursor
        
        # 
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        all_results.extend(data.get("results", []))
        has_more = data.get("has_more")
        next_cursor = data.get("next_cursor")
    return all_results

def get_sprints(notion_token):
    if not NOTION_SPRINT_DATABASE_ID: return []
    
    # check function
    try: return _query_notion_database(NOTION_SPRINT_DATABASE_ID, notion_token)
    except Exception as e:
        print(f"Error fetching sprints: {e}")
        return []

def get_tasks_for_sprint(sprint_id, notion_token):
    if not NOTION_TASK_DATABASE_ID: return []
    query = {"filter": {"property": "Sprint", "relation": {"contains": sprint_id}}}
    try: return _query_notion_database(NOTION_TASK_DATABASE_ID, notion_token, query)
    except Exception as e:
        print(f"Error fetching tasks for sprint {sprint_id}: {e}")
        return []

def get_all_projects_map(notion_token):
    project_map = {}
    if not NOTION_PROJECT_DATABASE_ID: return project_map
    try:
        project_pages = _query_notion_database(NOTION_PROJECT_DATABASE_ID, notion_token)
        for project_page in project_pages:
            project_id = project_page["id"]
            project_title = get_page_title(project_id, notion_token)
            if "Error" not in project_title and "Not Found" not in project_title:
                project_map[project_id] = project_title
    except Exception as e:
        print(f"Error fetching projects: {e}")
    return project_map

def get_subtask_map(task_list_raw):
    subtask_map = {task_page["id"]: False for task_page in task_list_raw}
    for task_page in task_list_raw:
        parent_prop = task_page.get("properties", {}).get("Parent-task", {})
        relation_list = parent_prop.get("relation")
        if relation_list:
            parent_id = relation_list[0]["id"]
            if parent_id in subtask_map:
                subtask_map[parent_id] = True
    return subtask_map

def get_sprint_name_from_properties(sprint_page_properties):
    name_prop = sprint_page_properties.get("Sprint name", {}).get("title", [])
    return name_prop[0].get("plain_text") if name_prop else "Unnamed Sprint"

def get_sprint_number_from_name(sprint_page_properties):
    sprint_name = get_sprint_name_from_properties(sprint_page_properties)
    match = re.search(r'(\d+)', sprint_name)
    return int(match.group(1)) if match else 0

def get_current_sprint(notion_token):
    
    # check function
    all_sprints_raw = get_sprints(notion_token)
    if not all_sprints_raw: return None
    sprints_list_sorted = sorted(all_sprints_raw, key=lambda s: get_sprint_number_from_name(s.get("properties", {})))
    for sprint_page in sprints_list_sorted:
        props = sprint_page.get("properties", {})
        status_prop = props.get("Sprint status", {})
        status_name = None
        if status_prop.get("type") == "status":
            status_name = status_prop.get("status", {}).get("name")
        elif status_prop.get("type") == "select":
            status_name = status_prop.get("select", {}).get("name")
        if status_name == "Current":
            date_prop = props.get("Dates", {})
            return {
                "id": sprint_page["id"], "name": get_sprint_name_from_properties(props),
                "start_date": date_prop.get("date", {}).get("start"),
                "end_date": date_prop.get("date", {}).get("end")
            }
    return None

# ==============================================================================
# 3. 資料處理函式 (Data Processing Functions)
# ==============================================================================

def extract_task_data(task_page, project_name_map, department_from_input, completed_statuses, notion_token):
    properties = task_page.get("properties", {})
    
    task_name_prop = properties.get("Task name", {}).get("title", [])
    task_name = task_name_prop[0].get("plain_text") if task_name_prop else "Unnamed Task"

    task_id_prop = properties.get("Task ID", {})
    task_id_display = "N/A"
    if task_id_prop.get("type") == "unique_id":
        unique_id_data = task_id_prop.get("unique_id", {})
        prefix, number = unique_id_data.get("prefix", ""), unique_id_data.get("number")
        if number is not None: task_id_display = f"{prefix}{number}"
    
    parent_task = None
    parent_relation = properties.get("Parent-task", {}).get("relation", [])
    if parent_relation:
        parent_id = parent_relation[0]["id"]
        if parent_id: parent_task = get_page_title(parent_id, notion_token)

    assignee_prop = properties.get("Assignee", {}).get("people", [])
    assignee_identifier = assignee_prop[0].get("name", "Unassigned") if assignee_prop else "Unassigned"
    
    estimates_prop = properties.get("Estimates", {}).get("select", {})
    story_point = 0
    if estimates_prop:
        try: story_point = int(estimates_prop.get("name", 0))
        except (ValueError, TypeError): pass

    project_relation = properties.get("Project", {}).get("relation", [])
    project_name = "No Project"
    if project_relation:
        project_id = project_relation[0]["id"]
        project_name = project_name_map.get(project_id, "Unknown Project")
        
    status_prop = properties.get("Status", {})
    status_name = "Unknown"
    if status_prop.get("type") == "status":
        status_name = status_prop.get("status", {}).get("name", "Unknown")
    elif status_prop.get("type") == "select":
        status_name = status_prop.get("select", {}).get("name", "Unknown")

    return {"id": task_page["id"], "task_id_display": task_id_display, "task_name": task_name,
            "parent_task": parent_task, "assignee_identifier": assignee_identifier,
            "assignee_count": len(assignee_prop),
            "story_point": story_point, "project": project_name, "status": status_name,
            "is_completed": status_name in completed_statuses, "department": department_from_input}

def process_all_tasks(tasks_list_raw, sprint_name, project_name_map, has_subtask_map, department_from_input, sprint_week_start, completed_statuses, notion_token):
    new_records = []
    for task_page in tasks_list_raw:
        task = extract_task_data(task_page, project_name_map, department_from_input, completed_statuses, notion_token)
        
        if task["assignee_count"] > 1:
            print(f"  [DATA QUALITY RULE] Skipping task '{task['task_name']}' ({task['task_id_display']}) because it has {task['assignee_count']} assignees.")
            continue

        new_records.append({
            "id": task["id"], "Task_ID": task["task_id_display"], "task_name": task["task_name"],
            "Parent_task": task["parent_task"], "sprint": sprint_name, "assignee_name": task["assignee_identifier"],
            "estimates": task["story_point"], "Project": task["project"], "Status": task["status"],
            "Department": task["department"], "sprint_week_start_date": sprint_week_start
        })
    if not new_records: return pd.DataFrame()
    df = pd.DataFrame(new_records)
    df['sprint_week_start_date'] = pd.to_datetime(df['sprint_week_start_date'], errors='coerce').dt.date
    return df

def process_complete_tasks(tasks_list_raw, sprint_name, project_name_map, department_from_input, sprint_week_start, completed_statuses, notion_token, bq_client, complete_tasks_table_id):
    print(f"Pre-fetching completed Task_IDs from OTHER sprints (excluding '{sprint_name}')...")
    existing_task_ids_from_other_sprints = set()
    full_table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{complete_tasks_table_id}"
    try:
        query = f"SELECT DISTINCT Task_ID FROM `{full_table_id}` WHERE completed_sprint != '{sprint_name}'"
        query_job = bq_client.query(query)
        for row in query_job.result(): existing_task_ids_from_other_sprints.add(row.Task_ID)
        print(f"Found {len(existing_task_ids_from_other_sprints)} completed Task_IDs from other sprints.")
    except Exception as e:
        print(f"Warning: Could not fetch existing Task_IDs. Uniqueness check may not be complete. Error: {e}")

    tasks_data_cache = {}
    parent_to_children_map = {}
    for task_page in tasks_list_raw:
        task_info = extract_task_data(task_page, project_name_map, department_from_input, completed_statuses, notion_token)
        tasks_data_cache[task_page["id"]] = task_info
        parent_relation = task_page.get("properties", {}).get("Parent-task", {}).get("relation", [])
        if parent_relation:
            parent_id = parent_relation[0]["id"]
            parent_to_children_map.setdefault(parent_id, []).append(task_page["id"])
    
    new_records = []
    for task_id, task in tasks_data_cache.items():
        is_eligible = (
            task["is_completed"] and 
            task["story_point"] != 0 and 
            task["assignee_identifier"] != "Unassigned" and
            task["assignee_count"] <= 1
        )
        if not is_eligible: 
            if task["assignee_count"] > 1:
                 print(f"  [DATA QUALITY RULE] Skipping completed task '{task['task_name']}' ({task['task_id_display']}) because it has {task['assignee_count']} assignees.")
            continue
        
        is_parent = task_id in parent_to_children_map
        should_add = False
        if not is_parent:
            should_add = True
        else:
            children_ids = parent_to_children_map[task_id]
            sum_of_children_points = sum(tasks_data_cache.get(child_id, {}).get("story_point", 0) for child_id in children_ids)
            if sum_of_children_points == 0: should_add = True
        
        if should_add:
            if task["task_id_display"] in existing_task_ids_from_other_sprints:
                print(f"Skipping task {task['task_id_display']} as it was completed in another sprint.")
                continue
            
            new_records.append({
                "Task_ID": task["task_id_display"], "Taskid": task["id"], "completed_sprint": sprint_name,
                "assignee_name": task["assignee_identifier"], "task_name": task["task_name"], "estimates": task["story_point"],
                "Department": task["department"], "sprint_week_start_date": sprint_week_start
            })
    
    if not new_records:
        return pd.DataFrame(columns=["Task_ID", "Taskid", "completed_sprint", "assignee_name", "task_name", "estimates", "Department", "sprint_week_start_date"])

    df = pd.DataFrame(new_records)
    df['sprint_week_start_date'] = pd.to_datetime(df['sprint_week_start_date'], errors='coerce').dt.date
    return df

# ==============================================================================
# 4. 流程協調函式 (Orchestration Function)
# ==============================================================================

def process_and_upload_sprint(sprint_info, bq_client, config):
    sprint_id, sprint_name, notion_token = sprint_info["id"], sprint_info["name"], config["notion_token"]

    original_start_date_str = sprint_info.get("start_date")
    original_end_date_str = sprint_info.get("end_date")

    if not original_start_date_str:
        raise ValueError(f"Sprint '{sprint_name}' is missing a start date.")
    if not original_end_date_str:
        raise ValueError(f"Sprint '{sprint_name}' is missing an end date.")

    try:
        start_date_obj = datetime.fromisoformat(original_start_date_str.split('T')[0]).date()
        end_date_obj = datetime.fromisoformat(original_end_date_str.split('T')[0]).date()
    except ValueError:
        raise ValueError(
            f"Sprint '{sprint_name}' has an invalid date format. Start: {original_start_date_str}, End: {original_end_date_str}"
        )

    if end_date_obj < start_date_obj:
        raise ValueError(f"Sprint '{sprint_name}' end date {end_date_obj} is before start date {start_date_obj}.")

    first_monday_offset = (0 - start_date_obj.weekday()) % 7
    first_monday_in_range = start_date_obj + timedelta(days=first_monday_offset)

    if first_monday_in_range > end_date_obj:
        print(f"Warning: No Monday between {start_date_obj} and {end_date_obj}. Using start date as sprint_week_start.")
        sprint_week_start = start_date_obj.isoformat()
    else:
        sprint_week_start = first_monday_in_range.isoformat()

    print(f"\n--- Processing Sprint: {sprint_name} (ID: {sprint_id}) ---")
    print(f"Original start: {original_start_date_str}, Original end: {original_end_date_str}, Computed sprint_week_start: {sprint_week_start}")

    try:
        project_name_map = get_all_projects_map(notion_token=notion_token)
        tasks_list_raw = get_tasks_for_sprint(sprint_id, notion_token=notion_token)
        print(f"Found {len(tasks_list_raw)} tasks for this sprint.")
        if not tasks_list_raw:
            print("No tasks to process. Skipping.")
            return

        has_subtask_map = get_subtask_map(tasks_list_raw)
        
        all_tasks_df = process_all_tasks(tasks_list_raw, sprint_name, project_name_map, has_subtask_map, config["department"], sprint_week_start, config["completed_statuses"], notion_token)
        complete_tasks_df = process_complete_tasks(tasks_list_raw, sprint_name, project_name_map, config["department"], sprint_week_start, config["completed_statuses"], notion_token, bq_client, BQ_COMPLETED_TASKS_TABLE_ID)

        upload_dataframe_to_bigquery(all_tasks_df, BQ_ALL_TASKS_TABLE_ID, bq_client, sprint_name=sprint_name, department=config["department"])
        upload_dataframe_to_bigquery(complete_tasks_df, BQ_COMPLETED_TASKS_TABLE_ID, bq_client, sprint_name=sprint_name, department=config["department"])
    
    except Exception as e:
        print(f"!!! An error occurred while processing sprint '{sprint_name}': {e}")

# ==============================================================================
# 5. 主觸發函式 (Main Trigger Function)
# ==============================================================================

@functions_framework.http
def notion_bq_sync_trigger(request):
    """
    HTTP Cloud Function. Supports 'current' and 'backfill' modes.
    Uses unified JSON for configs and separate env vars for tokens.
    """
    print("==================================================")
    print("Cloud Function triggered. Starting data sync process.")
    global NOTION_SPRINT_DATABASE_ID, NOTION_TASK_DATABASE_ID, NOTION_PROJECT_DATABASE_ID
    
    # 1. Parse request and load configurations
    try:
        # ex. https://...run.app/?env=dti&mode=current&department=DTI
        
        # parse environment information from input request URL and set to variable 
        selected_env = request.args.get('env')
        if not selected_env: return "Error: 'env' parameter is required.", 400
        selected_env = selected_env.lower()
        
        #parse mode information from input request URL and set to variable, set default as current mode 
        mode = request.args.get('mode', 'current').lower()
        print(f"Environment: '{selected_env}', Mode: '{mode}'")

        config = {}
        
        # get environment variables that are setup in Cloud Run setting
        configs_json = os.environ.get("NOTION_CONFIGS_JSON")
        if not configs_json: return "Error: NOTION_CONFIGS_JSON is not set.", 500
        
        # load the JSON string into a Python dict 
        all_configs = json.loads(configs_json)
        
        # pass in the current env variable as the key of the dict and get the correspond setting value(also a dict)
        current_env_config = all_configs.get(selected_env)
        if not current_env_config: return f"Error: Config for env '{selected_env}' not found.", 400

        # get token variable name from current environment config
        token_var_name = current_env_config.get("TOKEN_VARIABLE_NAME")
        if not token_var_name: return f"Error: TOKEN_VARIABLE_NAME not defined for env '{selected_env}'.", 500
        
        # input the token var name and find the secret real Notion token kept on GCP 
        notion_token = os.environ.get(token_var_name)
        if not notion_token: return f"Error: Token environment variable '{token_var_name}' is not set.", 500
        
        # save real token to config dict 
        config["notion_token"] = notion_token
        # save department to config dict 
        config["department"] = request.args.get('department', 'N/A')
        # save completed statuses to config dict 
        config["completed_statuses"] = current_env_config.get("COMPLETED_STATUSES", [])
        
        # save Notion database ID to variables
        NOTION_SPRINT_DATABASE_ID = current_env_config.get("SPRINT_DB_ID")
        NOTION_TASK_DATABASE_ID = current_env_config.get("TASK_DB_ID")
        NOTION_PROJECT_DATABASE_ID = current_env_config.get("PROJECT_DB_ID")

        if not all([NOTION_SPRINT_DATABASE_ID, NOTION_TASK_DATABASE_ID, NOTION_PROJECT_DATABASE_ID]):
            return "Error: Missing one or more database IDs in config.", 500
            
    except Exception as e:
        return f"Error during configuration setup: {e}", 500

    # 2. Initialize BigQuery client

    # check function
    bq_client = initialize_bigquery_client()
    if bq_client is None: return "BigQuery client initialization failed.", 500

    # 3. Determine which sprints to process based on mode
    sprints_to_process = []
    
    # mode is determined earlier by parse request URL
    if mode == 'current':
        print("Running in 'Current' mode...")
        # check function 
        current_sprint = get_current_sprint(notion_token=config["notion_token"])
        if current_sprint: sprints_to_process.append(current_sprint)
    
    elif mode == 'backfill':
        print("Running in 'Backfill' mode...")
        all_sprints_raw = get_sprints(notion_token=config["notion_token"])
        sprints_list_sorted = sorted(all_sprints_raw, key=lambda s: get_sprint_number_from_name(s.get("properties", {})))
        
        current_sprint_index = -1
        for i, sprint_page in enumerate(sprints_list_sorted):
            props = sprint_page.get("properties", {})
            status_prop = props.get("Sprint status", {})
            status_name = status_prop.get("status", {}).get("name") if status_prop.get("type") == "status" else status_prop.get("select", {}).get("name")
            if status_name == "Current":
                current_sprint_index = i
                break
        
        sprints_to_process_raw = []
        if current_sprint_index != -1:
            sprints_to_process_raw = sprints_list_sorted[:current_sprint_index + 1]
            print(f"Found 'Current' sprint. Will process {len(sprints_to_process_raw)} sprints up to this point.")
        else:
            print("Warning: No 'Current' sprint found to set an endpoint. All sprints will be processed.")
            sprints_to_process_raw = sprints_list_sorted

        for sprint_page in sprints_to_process_raw:
            props = sprint_page.get("properties", {})
            sprints_to_process.append({
                "id": sprint_page["id"],
                "name": get_sprint_name_from_properties(props),
                "start_date": props.get("Dates", {}).get("date", {}).get("start"),
                "end_date": props.get("Dates", {}).get("date", {}).get("end")
            })

    else:
        return f"Error: Invalid mode '{mode}'. Use 'current' or 'backfill'.", 400

    # 4. Loop through and process sprints
    if not sprints_to_process:
        message = "No sprints to process for the selected mode."
        print(message)
        return message, 200

    total_sprints = len(sprints_to_process)
    for i, sprint_info in enumerate(sprints_to_process):
        print(f"\n>>> Processing sprint {i+1} of {total_sprints}...")
        process_and_upload_sprint(sprint_info, bq_client, config)

    final_message = f"Data synchronization completed successfully for {total_sprints} sprint(s) in '{mode}' mode."
    print("\n==================================================")
    print(final_message)
    return final_message, 200
