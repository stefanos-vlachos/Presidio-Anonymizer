import pandas as pd
from presidio_analyzer import AnalyzerEngine, BatchAnalyzerEngine

df = pd.read_csv(r'../resources/generator_output/custom_dataset.csv', sep=',')

#Convert dataframe to dictionary
df_dict = df.to_dict(orient="list")

analyzer = AnalyzerEngine()
batch_analyzer = BatchAnalyzerEngine(analyzer_engine=analyzer)

analyzer_results = batch_analyzer.analyze_dict(df_dict, language="en")
analyzer_results = list(analyzer_results)

output = ""
for column in analyzer_results:
    line = f"Column {column.key}\n"
    column_entity_types = {}
    column_recognized_entities = column.recognizer_results
    for entity in column_recognized_entities:
        if(entity):
            entity_type = entity[0].entity_type
            if(entity_type in column_entity_types):
                column_entity_types[entity_type]+=1
            else:
                column_entity_types[entity_type]=1
    
    for key in column_entity_types:
        line = line + f"\tType of entity: {key}, Number of instances: {column_entity_types[key]}\n"
    
    output += line

print(output)