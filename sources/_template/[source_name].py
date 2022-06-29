import os
import src.bagel as bagel

SYSTEM = '[source_name]'
AUTH_SECRET = 'env_secret'
BASE_URL = 'https://trimedxext.source_name.com/api/v1/'
#TABLE_FILE = "C:\\repos\\python-elt\\sources\\okta\\tables.yaml"
TABLE_FILE = os.path.join("sources",SYSTEM,"tables.yaml")


def main():

  #get list of tables
  tables = bagel.get_tables(TABLE_FILE)
  counter = 0
  for t in tables:
    running_log = {}
    table = t["name"]
    elt_type = t["elt_type"]
    running_log["table_name"] = table
    running_log["elt_type"] = elt_type
    
    #get current timestamp
    current_timestamp = bagel.get_current_timestamp()
    running_log["current_timestamp"] = current_timestamp

    #get last run timestamp
    table_client = bagel.connect_azure_table()
    last_run_timestamp = bagel.get_last_run_timestamp(table_client, SYSTEM, table)
    running_log["last_run_timestamp"] = last_run_timestamp

    #get data
    data = 'some function above in any format'

    #generate file_name
    file_name = bagel.format_json_blob_name(SYSTEM, table)
    running_log["file"] = file_name

    #upload blob 
    container_client = bagel.connect_azure_blob()
    bagel.write_json_to_blob(container_client, file_name, bytes(str(data),'utf-8'))
    counter+=1
    #overwrite last run timestamp
    entity = bagel.write_run_timestamp(table_client, SYSTEM, table, current_timestamp)
    running_log["azure_table_new_entity"] = entity 
    running_log["files_created"] = counter 

    #upload log
    log_file_name = bagel.format_json_blob_name(SYSTEM, table, True)
    bagel.write_json_to_blob(container_client, log_file_name, bytes(str(running_log),'utf-8'))
    print(running_log)

if __name__ == "__main__":
  main()    