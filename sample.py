import re
import pandas as pd
import json
import os
from pyapacheatlas.auth import ServicePrincipalAuthentication
from pyapacheatlas.core import PurviewClient, AtlasEntity
from pprint import *

# Counting GUIDs
guid_counter = -1

def parse_format1(filepath):
    data = []
    with open(filepath, 'r') as file_object:
        # Parse line by line
        line = file_object.readline()
        while line:
            column = line.strip().split('=')[0]
            value = line.strip().split('=')[1]
            # Create a dictionary containing row of data
            row = {
                'ColumnName': column,
                'Value': value,
            }
            data.append(row)
            # Next line
            line = file_object.readline()
    data = pd.DataFrame(data)
    return data

def append_to_dict(columns, values, data):
    for i in range(0, len(columns), 1):
        data.append({
                        'ColumnName': columns[i],
                        'Value': values[i],
                    })
    return data

def parse_format2(filepath):
    data = []
    with open(filepath, 'r') as file_object:
        # Read all lines
        lines = file_object.readlines()
    
    # Loop 3 lines at a time
    for i in range(0, len(lines), 3):
        # Extract column values
        columns = lines[i].strip().split(',')
        # Append 2 rows of data
        data = append_to_dict(columns, lines[i+1].strip().split(','), data)
        data = append_to_dict(columns, lines[i+2].strip().split(','), data)
    data = pd.DataFrame(data)
    return data

def authenticated_client():
    # PyApacheAtlas - Authenticate to Purview Atlas API
    oauth = ServicePrincipalAuthentication(
        tenant_id = os.environ.get('AZURE_TENANT_ID', ''),
        client_id = os.environ.get('AZURE_CLIENT_ID', ''),
        client_secret = os.environ.get('AZURE_CLIENT_SECRET', '')
        )
    atlas_client = PurviewClient(
        account_name = os.environ.get('PURVIEW_CATALOG_NAME', ''),
        authentication = oauth
        )
    return atlas_client

def create_tabular_schema_entity(schema_name):
    global guid_counter
    guid_counter -= 1
    ts = AtlasEntity(
                        name=schema_name,
                        typeName="tabular_schema",
                        qualified_name="rbc://{}".format(schema_name),
                        guid = guid_counter
                    )
    return ts

def create_column_entities(ts, tabular_schema, columns):
    column_entities = []
    global guid_counter

    for column in columns:
        guid_counter -= 1
        column_entity = AtlasEntity(
                                    name=column,
                                    typeName="column",
                                    qualified_name="rbc://{}/{}".format(tabular_schema, column),
                                    guid = guid_counter,
                                    attributes={
                                        "type": "String",
                                        "description": column
                                    },
                                    relationshipAttributes = {
                                        "composeSchema": ts.to_json(minimum=True)
                                    }
                                )
        column_entities.append(column_entity)
    return column_entities

def create_resrouce_set(ts, resource_set_name):
    global guid_counter
    guid_counter -= 1
    rs = AtlasEntity(
        name="{}_set".format(resource_set_name),
        typeName="azure_datalake_gen2_resource_set",
        qualified_name="rbc://{}_set".format(resource_set_name),
        guid = guid_counter,
        relationshipAttributes = {
            "tabular_schema": ts.to_json(minimum=True)
        }
    )
    return rs

def convert_entities_to_json(entities):
    entities_json = []
    for entity in entities:
        entities_json.append(entity.to_json())
    return entities_json

if __name__ == '__main__':
    #############
    # Parse files
    #############
    # File1
    data1 = parse_format1('sample_files/file1.format1')
    print("Parsing Data Format 1:\n")
    print(data1)

    # File2
    data2 = parse_format2('sample_files/file1.format2')
    print("\nParsing Data Format 2:\n")
    print(data2)

    ###################################
    # Purview Atlas API
    ###################################
    # Authenticate
    atlas_client = authenticated_client()

    # Create tabular schema per file
    ts1 = create_tabular_schema_entity("weirdschema1")
    ts2 = create_tabular_schema_entity("weirdschema2")

    # Create Column entities per schema
    col_arr_1 = create_column_entities(ts1, "weirdschema1", data1["ColumnName"].unique())
    col_arr_2 = create_column_entities(ts2, "weirdschema2", data2["ColumnName"].unique())

    # Create Resource Set
    rs1 = create_resrouce_set(ts1, "vendor1")
    rs2 = create_resrouce_set(ts2, "vendor2")

    entities_arr = []
    entities_arr.append(ts1)
    entities_arr.append(ts2)
    entities_arr.extend(col_arr_1)
    entities_arr.extend(col_arr_2)
    entities_arr.append(rs1)
    entities_arr.append(rs2)

    print("\nEntities to upload:\n")
    print(entities_arr)

    entities_arr_json = convert_entities_to_json(entities_arr)

    # Upload entities
    results = atlas_client.upload_entities(
        entities_arr_json
    )

    # Print out results to see the guid assignemnts
    print(json.dumps(results, indent=2))