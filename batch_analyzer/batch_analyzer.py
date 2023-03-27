import sys, getopt
import operator
import pandas as pd
import os
import subprocess
import re
import requests
import time
import json
from jproperties import Properties
from presidio_analyzer import AnalyzerEngine, BatchAnalyzerEngine

df_dataset = None
df_preprocessed = None
root_dir = os.path.dirname(os.path.abspath(__file__))
input_file_name = None
configs = Properties()


def main(argv):
    global df_dataset, configs, input_file_name

    with open('pipeline-config.properties', 'rb') as config_file:
        configs.load(config_file)

    #Load dataset from input directory
    input_dir = None
    opts, args = getopt.getopt(argv,"i:",["ifile="])
    for opt, arg in opts:
        if opt in ("-i", "--ifile"):
            input_dir = arg
    df_dataset = pd.read_csv(input_dir, sep=',')

    #Get input dataset name
    input_file_name = input_dir.rsplit('\\', 1)[-1].rsplit('.csv')[0]

    #Begin PIPELINE
    #1) Analyze file and return sensitive/personal columns
    sensitive_columns =  analyzeFile()
    #2) Prepate input file for aninymization
    preprocessed_dir = preprocessFile(input_file_name)
    #3) Anonymize file
    anonymizeFile(sensitive_columns, preprocessed_dir)


def analyzeFile():
    global df_dataset

    df_dict = df_dataset.to_dict(orient="list")
    df_sensitive_columns = pd.DataFrame(columns=['columnName', 'columnType', 'mostFrequentEntity', 'percentage'])
    rows_number = len(df_dataset.index)

    #Format column data types
    column_datatypes = df_dataset.dtypes.replace("object","string").replace("bool","string").replace("category","string").replace("float64","double").replace("int64","int").replace("datetime64","date")
    
    print("\n")
    print("####################################")
    print("Analysis process has been initiated.")
    print("####################################")
    print("\nPlease wait...\n")

    #Implement Batch Analysis
    analyzer = AnalyzerEngine()
    batch_analyzer = BatchAnalyzerEngine(analyzer_engine=analyzer)
    analyzer_results = batch_analyzer.analyze_dict(df_dict, language="en")
    analyzer_results = list(analyzer_results)

    #Find number of entities per column and calculate percentage of most frequent entity
    for column in analyzer_results:
        column_entity_types = {}
        column_recognized_entities = column.recognizer_results
        for entity in column_recognized_entities:
            if(entity):
                entity_type = entity[0].entity_type
                if(entity_type in column_entity_types):
                    column_entity_types[entity_type]+=1
                else:
                    column_entity_types[entity_type]=1
        if(column_entity_types):
            max_key = max(column_entity_types.items(), key=operator.itemgetter(1))[0]
            max_value = max(column_entity_types.values())
            percentage_of_sensitivity = max_value / rows_number
            #Assume personal/sensitive column if percentage>0.5
            if(percentage_of_sensitivity > 0.5):
                #Handle DATE_TIME column type
                if(max_key == "DATE_TIME"):
                    df_sensitive_columns.loc[len(df_sensitive_columns.index)] = [column.key, "date", max_key, percentage_of_sensitivity]
                    continue
                df_sensitive_columns.loc[len(df_sensitive_columns.index)] = [column.key, column_datatypes[column.key], max_key, percentage_of_sensitivity]

    #Print analysis results
    print("Analysis results")
    print("====================================")
    print(df_sensitive_columns)
    return df_sensitive_columns


def preprocessFile(input_file_name):
    global df_dataset, df_preprocessed, root_dir

    #Create folder of preprocessed datasets
    preprocessed_dir = f"{root_dir}\\build\\preprocessed_datasets\\"
    preprocessed_file_name = f"preprocessed_{input_file_name}"
    full_path = preprocessed_dir+preprocessed_file_name+".csv"

    if not os.path.exists(preprocessed_dir):
        os.makedirs(preprocessed_dir)

    #Replace spaces
    df_preprocessed = df_dataset.replace(', ', '-', regex=True).replace(' ', '_', regex=True)
    df_preprocessed.to_csv(full_path, sep=',', index=False, encoding='utf-8')
    return full_path


def anonymizeFile(df_sensitive_columns, preprocessed_dir):
    global configs, root_dir, input_file_name

    print("\n")
    print("########################################")
    print("Anonymization process has been initiated.")
    print("########################################")
    print("\nPlease wait...\n")

    #Run Amnesia 
    # amnesia_installation_path = str(configs.get("AMNESIA_PATH").__getattribute__("data")).replace("\"", "")
    # os.chdir(amnesia_installation_path)
    # exec_command = ["java", "-Xms1024m", "-Xmx4096m", "-Dorg.eclipse.jetty.server.Request.maxFormKeys=1000000", "-Dorg.eclipse.jetty.server.Request.maxFormContentSize=1000000", "-jar", "amnesiaBackEnd-1.0-SNAPSHOT.jar", "--server.port=8181"]
    # subprocess.Popen(exec_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    #Prepare columns to anonymize
    columns_to_anonymize = dict()
    for index, row in df_sensitive_columns.iterrows():
        columns_to_anonymize[row['columnName']] = row['columnType']
    str_columns_to_anonymize = str(columns_to_anonymize).replace("\'", "\"").replace(" ", "")    

    #Get session ID and prepare requests metadata
    response = requests.post('http://localhost:8181/getSession')
    json_response = json.loads(response.text)

    session_id = json_response['Session_Id']
    cookies = {
        'JSESSIONID': f"{session_id}",
    }
    files = {
        'file': open(preprocessed_dir,'rb'),
        'del': (None, ','),
        'datasetType': (None, 'tabular'),
        'columnsType': (None, str_columns_to_anonymize),
    }

    #Load dataset to Amnesia
    response = requests.post('http://localhost:8181/loadData', cookies=cookies, files=files, allow_redirects=True)
    print(response.text)
    if ("Success" not in response.text):
        return 0
    
    #Create hierarchies
    print("\nCreating Hierarchies:")
    print("====================================")

    #Create folder for hierarchies and save them
    hierarchies_dir = f"{root_dir}\\build\\hierarchies\\"
    current_dataset_hierarchies_path = f"{hierarchies_dir}{input_file_name}"
    if not os.path.exists(current_dataset_hierarchies_path):
        os.makedirs(current_dataset_hierarchies_path)

    #Initialize anonymization binding
    bind_dict = dict()

    for column_name in columns_to_anonymize:
        payload = None
        hierarchy_name = f'{column_name}_hier'
        bind_dict[column_name] = hierarchy_name
        column_type = columns_to_anonymize[column_name]
        if(column_type=="date"):
            #Prepare date range hierarchy request
            payload = {
                'hierType': (None,'range'),
                'varType': (None,'date'),
                'attribute': (None, column_name),
                'hierName': (None,hierarchy_name),
                'startYear': (None,'1940'),
                'endYear': (None,'2023'),
                'years': (None,'5'),
                'months': (None,'6'),
                'days': (None,'7'),
                'fanout': (None,'3')
            }
        elif(column_type=="string"):
            #Prepare distinct hierarchy request
            payload = {
                'hierType': (None,'distinct'),
                'varType': (None,'string'),
                'attribute': (None, column_name),
                'hierName': (None,hierarchy_name),
                'sorting': (None,'alphabetical'),
                'fanout': (None,'3')
            }
        elif(column_type=="double"):
            #Prepare double hierarchy request
            endLimit = 100
            step = 5
            payload = {
                'hierType': (None,'range'),
                'varType': (None,'double'),
                'attribute': (None,column_name),
                'hierName': (None, hierarchy_name),
                'startLimit': (None,'1'),
                'endLimit': (None,endLimit),
                'step': (None,step),
                'fanout': (None,'3')
            }
        elif(column_type=="int"):
            #Prepare double hierarchy request
            endLimit = 100
            step = 5
            payload = {
                'hierType': (None,'range'),
                'varType': (None,'int'),
                'attribute': (None,column_name),
                'hierName': (None, hierarchy_name),
                'startLimit': (None,'1'),
                'endLimit': (None,endLimit),
                'step': (None,step),
                'fanout': (None,'3')
            }

        #Send request
        response = requests.post('http://localhost:8181/generateHierarchy', cookies=cookies, files=payload, allow_redirects=True)
        if("Fail" in response.text):
            print(f"Failed to create hierarchy for {column_name}.")
            return 0
        
        #Save hierarchy
        hierarchy_file = f"hier_{column_name}_codes.txt"
        hierarchy_path = f"{current_dataset_hierarchies_path}\\{hierarchy_file}"
        with open(hierarchy_path, 'wb') as f:
            f.write(response.content)

        print(f"Created hierarchy {hierarchy_file}")

        #Load Hierarchy to Amnesia
        payload = {
            'hierarchies': open(hierarchy_path, 'rb'),
        }
        response = requests.post("http://localhost:8181/loadHierarchies", cookies=cookies, files=payload)
        print(response.text)
        time.sleep(2)

    #Binding hierarchies to columns
    print("\nBinding Hierarchies to Columns:")
    print("====================================")

    #Prepare request
    bind_dict_str = str(bind_dict).replace("\'", "\"").replace(" ", "")
    k = int(configs.get("K").__getattribute__("data"))
    payload = {
        'bind': (None, bind_dict_str),
        'k': (None, k)
    }

    #Create folder for bindings and save them
    bindings_dir = f"{root_dir}\\build\\bindings\\{input_file_name}"
    binding_file = f"{input_file_name}_binding.txt"
    binding_file_path = f'{bindings_dir}\\{binding_file}'
    if not os.path.exists(bindings_dir):
        os.makedirs(bindings_dir)

    #Send request and save response
    response = requests.post('http://localhost:8181/anonymization', cookies=cookies, files=payload)
    with open(binding_file_path, 'wb') as f:
        f.write(response.content)
    print(response.text)
    if ("Success" not in response.text):
        return 0

    return 1

if __name__ == "__main__":
    main(sys.argv[1:])