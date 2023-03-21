import sys, getopt
import operator
import pandas as pd
from presidio_analyzer import AnalyzerEngine, BatchAnalyzerEngine

df_dataset = None


def main(argv):
    input_dir = ''
    opts, args = getopt.getopt(argv,"i:",["ifile="])
    for opt, arg in opts:
        if opt in ("-i", "--ifile"):
            input_dir = arg

    sensitive_columns =  analyzeFile(input_dir)
    if not sensitive_columns.empty:
        anonymizeFile(sensitive_columns)



def analyzeFile(input_dir):
    print("\n")
    print("####################################")
    print("Analysis process has been initiated.")
    print("####################################")
    print("\nPlease wait...\n")

    df_dataset = pd.read_csv(input_dir, sep=',')
    df_dict = df_dataset.to_dict(orient="list")
    df_rows = len(df_dataset.index)
    df_sensitive_columns = pd.DataFrame(columns=['columnName', 'mostFrequentEntity', 'percentage'])

    analyzer = AnalyzerEngine()
    batch_analyzer = BatchAnalyzerEngine(analyzer_engine=analyzer)

    analyzer_results = batch_analyzer.analyze_dict(df_dict, language="en")
    analyzer_results = list(analyzer_results)

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
        
        #Calculate percentage of most frequent entity
        if(column_entity_types):
            max_key = max(column_entity_types.items(), key=operator.itemgetter(1))[0]
            max_value = max(column_entity_types.values())
            percentage_of_sensitivity = max_value / df_rows
            if(percentage_of_sensitivity > 0.5):
                df_sensitive_columns.loc[len(df_sensitive_columns.index)] = [column.key, max_key, percentage_of_sensitivity]


    print("Analysis results")
    print("====================================")
    print(df_sensitive_columns)
    return df_sensitive_columns



def anonymizeFile(df_sensitive_columns):
    print("\n")
    print("########################################")
    print("Anonymization process has been initiated.")
    print("########################################")
    print("\nPlease wait...\n")

    return 1


if __name__ == "__main__":
    main(sys.argv[1:])