''' 
@file: export_to_json.py
@author: John3Kim
@desc: A couple of functions that are used to export data as files 
-> A successful result will allow you to export the data as a JSON object 
-> An unsuccessful result will output a text file with issues
'''

import json
import os.path

def build_json_file(new_data:json, file_export_name:str) -> None:
    '''
    Takes a json object and exports the results as a json file. 
    
    Arguments:
        new_data: json -> New data that will be exported as a JSON file 
        file_export_name: str -> The name of the exported file

    Returns: 
        None
    '''
    
    file_export_name = f"{file_export_name}.json"

    if not(os.path.exists(file_export_name)):

        with open(file_export_name,"w",encoding='utf-8') as f:
            json.dump(new_data, f, ensure_ascii=False, indent=4)

    else:

        with open(file_export_name,"r",encoding='utf-8') as f:
            existing_data = json.load(f)
        
        existing_data.append(new_data)
        
        with open(file_export_name,"w",encoding='utf-8') as f:
            json.dump(existing_data, f, ensure_ascii=False, indent=4)

def build_error_text_file(new_data:str, file_export_name:str) -> None:
    '''
    Takes a string and exports the results as a text file. 

    Arguments:
        new_data: json -> New data that will be exported as a str file 
        file_export_name: str -> The name of the exported file

    Returns: 
        None
    '''

    with open(f"{file_export_name}.txt", 'a', encoding='utf-8') as error_file:
        error_file.write(f"{new_data}\n")