
# get_ipython().system('pip install pandas mysql-connector-python')


# ### Solution 1:
# 
# We call different procedures for each task in a sequence inside python and handle errors
# 
# 1. call import data procedures
# 2. call combine data procedure
# 3. call export data procedure
# 
# ### Solution 2:
# 
# We use a single procedure to do the combine and export tasks together in the database and handle the errors with a transaction
# 
# 1. call import data procedures
# 2. call transform and export procedure (transaction handling)
# 

# Import required libraries
# 



import pandas as pd
import mysql.connector
import json
from collections import OrderedDict
import os


# Read config file
# 


def get_config(config_path):
    with open(config_path, 'r') as f:
        config = json.load(f)  
    return config


# Read each dataframe row by row and insert it to the corresponfing table
# using stored procedure
# 


def insert_df_into_table(df, con, curs, stored_procedure_name):
    # iterate over rows
    for _, row in df.iterrows():
        try:
            # turn values to tuple to pass to the stored procedure
            values = tuple(row)
            # call the procedure
            curs.callproc(stored_procedure_name, values)
        except Exception as e:
            print(e)

    # commit changes
    con.commit()


# In this function we iterate over different dataframes and pass the data frame and its stored procedure name to the previous function
# 


def insert_data(data_dict, con, curs):
  # loop on different data frames
  for k, v in data_dict.items():
    # extract procedure name
    stored_procedure_name = v[1]
    # pass it to the function to insert into corresponding table
    insert_df_into_table(v[0], con, curs, stored_procedure_name)


# In this function we call the procedure that is responsible to combine the data
# 



def combine_data(con, curs, config):
    
    try:
        # call the procedure defined in config file for transforming data
        curs.callproc(config['procedures']['transform'])
    except Exception as e:
            print(e)
    # commit the changes
    con.commit()
    
    print("Data combined to data final table")


# In this function we retrieve data from the dataset using stored procedure and write it to the output file based on the defined schema
# 


def get_data_final_and_export(con, curs, config):
    # calling the procedure for exporting the data
   curs.callproc(config['procedures']['export'])
   # storing the result
   query_result = curs.stored_results()
   # get the output directory
   output_dir = config['output_dir']
   # make the output directory if it doesnt exist
   if not os.path.exists(output_dir):
       os.makedirs(output_dir)
   # create the output file

   with open(os.path.join(output_dir, config['output_filename']), 'w') as f:
        # Load schema from config
        schema = config['schema']  
        # iterate over query results
        for item in query_result:
            for row in item.fetchall():
                # get json data
                data = json.loads(row[0])
                # to reorder and map it to the defined schema
                reordered_data = OrderedDict()
                for field in schema:
                    out_key = field["output_field"]
                    k = field["json_key"]
                    reordered_data[out_key] = data.get(k)
                # write it to the file
                f.write(json.dumps(reordered_data, separators=(',', ':')) + '\n')
        
        


# This function get the data from the input directory defined in config file,
# put it in a dictionary (key: name, value: [dataframe, stored_procedure_name]),
# return the dictionary
# 


def get_initial_data(config):
  # get the data
  article_df = pd.read_csv(f'{config["input_data_dir"]}/article.tsv', sep='\t')
  relationship_df = pd.read_csv(f'{config["input_data_dir"]}/relationship.tsv', sep='\t')
  tag_df = pd.read_csv(f'{config["input_data_dir"]}/tag.tsv', sep='\t')
  
  # create data dictionary (key: name, value: [dataframe, stored_procedure_name])
  data = {
  'article': [article_df, config["procedures"]["import_article"]],
  'relationship': [relationship_df, config["procedures"]["import_relationship"]],
  'tag': [ tag_df, config["procedures"]["import_tag"]]
  }

  return data
  


# This is my solution 2 function which calls one procedure to do the combine and export tasks together with transaction handling
# 


def transform_and_export(con, curs, config):
   # call procedure
   curs.callproc(config['procedures']['transform_export'])
   # get the results
   query_result = curs.stored_results()
   # commit changes (since we do insertion to the data_final table)
   con.commit() 
   # get output directory
   output_dir = config['output_dir']
   # if output dir doesnt exist, creates it
   if not os.path.exists(output_dir):
       os.makedirs(output_dir)
   # create the output file
   with open(os.path.join(output_dir, config['output_filename']), 'w') as f:
        # Load schema from config
        schema = config['schema']  
        # iterate over query results
        for item in query_result:
            for row in item.fetchall():
                # get json data
                data = json.loads(row[0])
                # to reorder and map it to the defined schema
                reordered_data = OrderedDict()
                for field in schema:
                    out_key = field["output_field"]
                    k = field["json_key"]
                    reordered_data[out_key] = data.get(k)
                # write it to the file
                f.write(json.dumps(reordered_data, separators=(',', ':')) + '\n')




if __name__ == "__main__":
    # get config file path
    config_file_path = 'config.json'
    # store config parameters  
    config = get_config("config.json")
    # get the data
    data = get_initial_data(config)
    
    # create a connection to the data base using information in the config file
    db_connection = mysql.connector.connect(host= config['host'], 
                                        user= config['user'], 
                                        password= config['password'], 
                                        database= config['database_name'], 
                                        port=config['port'])
    
    
   # create a cursor
    db_cursor = db_connection.cursor()
    # choose the soulution 
    solution = config['solution']
    
    if solution == 1:
         # in this solution we call different procedures responsible for each task in a sequence
        try:
            insert_data(data, db_connection, db_cursor)
            combine_data(db_connection, db_cursor, config)
            get_data_final_and_export(db_connection, db_cursor, config)
        except Exception as e:
                print(e)
        
    else:
        # in this solution we 
        # 1: import the data with the procedure
        # 2: Use another procedure to do the combine and export tasks together in a sequence inside the database
        # and use transaction to handle the errors
        try:
            insert_data(data, db_connection, db_cursor)
            transform_and_export(db_connection, db_cursor, config)
        except Exception as e:
                print(e)
    # closing the connection
    db_connection.close()

