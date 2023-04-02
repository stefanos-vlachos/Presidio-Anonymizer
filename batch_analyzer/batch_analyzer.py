import sys, getopt
import operator
import pandas as pd
import os
import re
import subprocess
import requests
import json
import psutil
import math
from jproperties import Properties
from presidio_analyzer import AnalyzerEngine, BatchAnalyzerEngine


root_dir = os.path.dirname(os.path.abspath(__file__))
input_file_name = None
df_dataset = None
df_preprocessed = None
configs = Properties()


def main(argv):
    #Load properties
    with open('pipeline-config.properties', 'rb') as config_file:
        configs.load(config_file)

    #Load dataset from input directory
    loadDataset(argv)
    #Analyze file
    sensitive_columns =  analyzeFile()
    #Prepare input file for anonymization
    preprocessed_dir = None
    if(sensitive_columns is not None) :
        preprocessed_dir = preprocessFile(sensitive_columns)
    #Start Amnesia
    amnesiaStarted = None
    if(preprocessed_dir):
        amnesiaStarted = initiateAmnesia()
    #Anonymize file
    anonymizationDone = None
    if(amnesiaStarted):
        anonymizationDone = anonymizeFile(sensitive_columns, preprocessed_dir)
    #Stop Amnesia
    if(anonymizationDone):
        stopAmnesia()


def loadDataset(argv):
    global input_file_name, df_dataset, configs

    input_dir = None
    opts, args = getopt.getopt(argv,"i:",["ifile="])
    for opt, arg in opts:
        if opt in ("-i", "--ifile"): 
            input_dir = arg
    df_dataset = pd.read_csv(input_dir, sep=',')
    input_file_name = input_dir.rsplit('\\', 1)[-1].rsplit('.csv')[0]


def analyzeFile():
    global df_dataset

    print("\n")
    print("####################################")
    print("Analysis process has been initiated.")
    print("####################################")
    print("\nPlease wait...\n")

    df_dict = df_dataset.to_dict(orient="list")
    df_sensitive_columns = pd.DataFrame(columns=['columnName', 'columnType', 'mostFrequentEntity', 'percentage'])
    rows_number = len(df_dataset.index)

    #1st level of finding columns type
    column_datatypes = df_dataset.dtypes.replace("object","string").replace("bool","string").replace("category","string").replace("float64","double").replace("int64","int").replace("datetime64","date")

    #Implement Presidio Batch Analysis
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
        #Check if sensitive entities were found
        if(column_entity_types):
            #Find most frequent entity in the current column
            max_key = max(column_entity_types.items(), key=operator.itemgetter(1))[0]
            max_value = max(column_entity_types.values())
            #Assume personal/sensitive column if percentage>0.5
            percentage_of_sensitivity = max_value / rows_number
            if(percentage_of_sensitivity >= 0.5):
                #Handle DATE_TIME column type
                if(max_key == "DATE_TIME"):
                    #Handle as dates only acceptable Amnesia date formats
                    df_dates = pd.DataFrame()
                    df_dates['non_matching'] = df_dataset[column.key].apply(extract_acceptable_dates).apply(pd.Series)
                    non_matching_cells = df_dates['non_matching'].sum()
                    if (non_matching_cells==0):
                        df_sensitive_columns.loc[len(df_sensitive_columns.index)] = [column.key, "date", max_key, percentage_of_sensitivity]
                        continue
                df_sensitive_columns.loc[len(df_sensitive_columns.index)] = [column.key, column_datatypes[column.key], max_key, percentage_of_sensitivity]

    if(not df_sensitive_columns.empty):
        #Print analysis results
        print("Analysis results")
        print("====================================")
        print(df_sensitive_columns)
        return df_sensitive_columns
    
    print("Analysis results")
    print("====================================")
    print("No personal or sensitive fields found in the given dataset.")
    return None


def extract_acceptable_dates(text):
    # Regular expression for ISO 8601 format (YYYY-MM-DD)
    iso_pattern = r"\d{4}-\d{2}-\d{2}"
    # Regular expression for RFC 2822 format (e.g. Tue, 01 Mar 2022 00:00:00 GMT)
    rfc_pattern = r"[A-Za-z]{3},\s\d{2}\s[A-Za-z]{3}\s\d{4}\s\d{2}:\d{2}:\d{2}\s[A-Z]{3}"
    # Regular expression for Unix timestamp format (e.g. 1646064000)
    unix_pattern = r"\d{10}"

    non_matches = re.sub(f"{iso_pattern}|{rfc_pattern}|{unix_pattern}", "", text).strip()
    return (len(non_matches))


def preprocessFile(sensitive_columns):
    global df_dataset, df_preprocessed, root_dir, input_file_name

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


def initiateAmnesia():
    global configs

    #Run Amnesia 
    amnesia_installation_path = str(configs.get("AMNESIA_PATH").__getattribute__("data")).replace("\"", "")
    os.chdir(amnesia_installation_path)
    exec_command = ["java", "-Xms1024m", "-Xmx4096m", "-Dorg.eclipse.jetty.server.Request.maxFormKeys=1000000", "-Dorg.eclipse.jetty.server.Request.maxFormContentSize=1000000", "-jar", "amnesiaBackEnd-1.0-SNAPSHOT.jar", "--server.port=8181"]
    subprocess.Popen(exec_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return True


def anonymizeFile(df_sensitive_columns, preprocessed_dir):
    global configs, root_dir, input_file_name, df_dataset

    df_anonymized = df_dataset.copy()

    print("\n")
    print("########################################")
    print("Anonymization process has been initiated.")
    print("########################################")
    print("\nPlease wait...\n")

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

    ########################################
    #PHASE 1: LOADING DATA TO AMNESIA
    ########################################

    payload = {
        'file': open(preprocessed_dir,'rb'),
        'del': (None, ','),
        'datasetType': (None, 'tabular'),
        'columnsType': (None, str_columns_to_anonymize),
    }

    response = requests.post('http://localhost:8181/loadData', cookies=cookies, files=payload, allow_redirects=True)
    print(response.text)
    if ("Success" not in response.text):
        return 0
    
    ########################################
    #PHASE 2: CREATING HIERARCHIES
    ########################################

    print("\nCreating Hierarchies:")
    print("====================================")

    #Create folder for hierarchies
    hierarchies_dir = f"{root_dir}\\build\\hierarchies\\"
    current_dataset_hierarchies_path = f"{hierarchies_dir}{input_file_name}"
    if not os.path.exists(current_dataset_hierarchies_path):
        os.makedirs(current_dataset_hierarchies_path)

    #Initialize anonymization binding dictionary
    column_hierarchy_bindings = []

    #Get fanout from properties
    fanout = configs.get("FANOUT").__getattribute__("data")
    string_anon_method = configs.get("STRING_ANON_METHOD").__getattribute__("data")

    #Create hierarchy for each sensitive column
    for column_name in columns_to_anonymize:
        hierarchy_name = f'{column_name}_hier'
        column_type = columns_to_anonymize[column_name]

        column_hierarchy_bindings.append((column_name, hierarchy_name))

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
                'fanout': (None,fanout)
            }
        elif(column_type=="string"):
            #Prepare distinct hierarchy request
            if(string_anon_method=="distinct"):
                payload = {
                    'hierType': (None,'distinct'),
                    'varType': (None,'string'),
                    'attribute': (None, column_name),
                    'hierName': (None,hierarchy_name),
                    'sorting': (None,'alphabetical'),
                    'fanout': (None,fanout)
                }
            #Prepare masking hierarchy request
            elif(string_anon_method=="mask"):
                mask_length = configs.get("MASK_LENGTH").__getattribute__("data")
                payload = {
                    'hierType': (None,'mask'),
                    'varType': (None,'string'),
                    'attribute': (None, column_name),
                    'hierName': (None,hierarchy_name),
                    'length': (None,mask_length)
                }
        elif(column_type=="double"):
            #Prepare double hierarchy request
            column_max_value = df_dataset.loc[df_dataset[column_name].idxmax()][column_name]
            endLimit = math.ceil(column_max_value)
            step = int(endLimit/10)
            payload = {
                'hierType': (None,'range'),
                'varType': (None,'double'),
                'attribute': (None,column_name),
                'hierName': (None, hierarchy_name),
                'startLimit': (None,'1'),
                'endLimit': (None,endLimit),
                'step': (None,step),
                'fanout': (None,fanout)
            }
        elif(column_type=="int"):
            #Prepare double hierarchy request
            column_max_value = df_dataset.loc[df_dataset[column_name].idxmax()][column_name]
            endLimit =  column_max_value // 10 if column_max_value % 10 == 0 else column_max_value // 10 + 1
            step = int(endLimit/10)
            payload = {
                'hierType': (None,'range'),
                'varType': (None,'int'),
                'attribute': (None,column_name),
                'hierName': (None, hierarchy_name),
                'startLimit': (None,'1'),
                'endLimit': (None,endLimit),
                'step': (None,step),
                'fanout': (None,fanout)
            }

        #Send request and save hierarchy to file
        response = requests.post('http://localhost:8181/generateHierarchy', cookies=cookies, files=payload, allow_redirects=True)
        if("Fail" in response.text):
            print(f"Failed to create hierarchy for {column_name}.")
            return 0
        hierarchy_file = f"hier_{column_name}_codes.txt"
        hierarchy_path = f"{current_dataset_hierarchies_path}\\{hierarchy_file}"
        with open(hierarchy_path, 'wb') as f:
            f.write(response.content)

        print(f"Created hierarchy {hierarchy_file}")

        ########################################
        #PHASE 3: LOADING HIERARCHIES TO AMNESIA
        ########################################
        payload = {
            'hierarchies': open(hierarchy_path, 'rb'),
        }
        response = requests.post("http://localhost:8181/loadHierarchies", cookies=cookies, files=payload)
        print(response.text)

    ########################################################
    #PHASE 4: BINDING HIERARCHIES TO COLUMNS AND ANONYMIZING
    ########################################################

    print("\nBinding Hierarchies to Columns:")
    print("====================================")

    #Create folder for bindings
    bindings_dir = f"{root_dir}\\build\\bindings"
    if not os.path.exists(bindings_dir):
        os.makedirs(bindings_dir)
    binding_file = f"{input_file_name}_binding.json"
    binding_file_path = f'{bindings_dir}\\{binding_file}'

    #Create folder for anonymization
    anonymization_dir = f"{root_dir}\\build\\anonymizations\\{input_file_name}"
    if not os.path.exists(anonymization_dir):
        os.makedirs(anonymization_dir)

    #Prepare request
    k = int(configs.get("K").__getattribute__("data"))
    binding_results = dict()

    for column, hierarchy in column_hierarchy_bindings:
        bind = str({column: hierarchy}).replace("\'", "\"").replace(" ", "")
        payload = {
            'bind': (None, bind),
            'k': (None, k)
        }
        #Send request and save response
        response = requests.post('http://localhost:8181/anonymization', cookies=cookies, files=payload)
        binding_results[column] = json.loads(response.text)

        if ("Solutions" not in response.text):
            return 0
        
        print(f"Binded succesfully {hierarchy} hierarchy to column {column}.")

        #ΑΝΟΝΥΜΙΖΑΤΙΟΝ
        column_solutions = json.loads(response.text)["Solutions"]
        safe_solutions_levels = dict() 
        for solution in column_solutions:
            result = column_solutions[solution]["result"]
            levels = column_solutions[solution]["levels"]
            if result == "safe":
                safe_solutions_levels[solution] = levels

        selected_solution = min(safe_solutions_levels, key=safe_solutions_levels.get)
        selected_levels = safe_solutions_levels[selected_solution]

        payload = {
            'sol': (None, selected_levels)
        }

        #Send request and save response
        response = requests.post('http://localhost:8181/getSolution', cookies=cookies,  files=payload)
        anonymized_column_file = f"{input_file_name}_{column}_anonymized.csv"
        anonymized_column_path = f'{anonymization_dir}\\{anonymized_column_file}'
        with open(anonymized_column_path, 'wb') as f:
            f.write(response.content)

        #Keep anonymized column
        df_anonymized_column = pd.read_csv(anonymized_column_path)
        df_anonymized[column] = df_anonymized_column[column]

    #Store bindings
    with open(binding_file_path, 'w+') as f:
        f.write(str(binding_results).replace("\'", "\"").replace(" ", ""))

    #Store fully anonymized dataset
    fully_anonymized_file = f"{input_file_name}_anonymized.csv"
    anonymized_file_path = f'{anonymization_dir}\\{fully_anonymized_file}'
    df_anonymized.to_csv(anonymized_file_path, index=False)

    #Print analysis results
    print("\nAnonymization results")
    print("====================================")
    print("Anonymization is complete!!")
    print(f"Check {fully_anonymized_file} in {anonymization_dir}.")

    return True


def stopAmnesia():
    # Port to search for
    port = 8181

    # Find process ID of the process listening on the port
    for proc in psutil.process_iter(['pid', 'name', 'connections']):
        try:
            for conn in proc.info['connections']:
                if conn.laddr.port == port:
                    proc.send_signal(psutil.signal.SIGTERM)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass


if __name__ == "__main__":
    main(sys.argv[1:])