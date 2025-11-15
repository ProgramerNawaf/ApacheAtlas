from datetime import datetime
import json
import os
import re
import subprocess

import numpy as np
import requests
from classes.AzureDevops import AzureDevops
from classes.Connection import Connection
import pandas as pd
import oracledb
from requests.auth import HTTPBasicAuth




def add_change_records(df, columns, cr_detection, change_type):
    """
    Process dataframe records and add to CR_DETECTION.
    
    Args:
        df: pandas DataFrame to process
        columns: list of column names to extract
        cr_detection: CR_DETECTION dictionary to update
        change_type: key in CR_DETECTION dict (default: 'Change Attribute')
    """
    if df.empty:
        return
    
    records = (
        df[columns]
        .drop_duplicates()
        .replace({np.nan: None})
        .to_dict('records')
    )
    
    if records:
        cr_detection[change_type].extend(records)
        cr_detection['Logs']['total_changes'] += len(records)
        

            

if __name__ == '__main__':

    CR_DETECTION = {
        "Add Table": [],
        "Delete Table": [],
        "Change Table":[],
        "Add Attribute": [],
        "Delete Attribute": [],
        "Change Attribute":[],
        "Logs": {
            "detection_time": datetime.now().isoformat(),
            "total_changes": 0
        },
        "Impacted Pipliens": []
    }
    customMetaDataCatlogDatabase=Connection(option='Destination',databaseType='SQL')
    customMetaDataCatlogDatabase.connect()

    print('testing connection destinationDatabase ',customMetaDataCatlogDatabase.testConnection())

    # customMetaDataCatlogDatabase=Connection(IP='10.0.122.203',port= '1433',databaseName='Nawaf_test',password='123456' ,userName='loadTest',databaseType='SQL')
    # print(customMetaDataCatlogDatabase.connect())
    print('testing connection sourceDatabase ',customMetaDataCatlogDatabase.testConnection())

    query=f'''
               SELECT
                DA.ID as DATASTORE_ID , PP.ID as PIPELINE_ID , P.ID as ATTRIBUTE_ID , t.Connection_ID as CONNECTION_ID , 
                T.Schema_Name as TABLE_SCHEMA,
                T.Table_Name as TABLE_NAME,
                P.Attribute_Name as ATTRIBUTE_NAME,
                P.Attribute_Type as DATA_TYPE,
                CASE WHEN P.IsPK = 1 THEN 'Yes' WHEN P.IsPK = 0 THEN 'No' END AS IS_PK,
                CASE WHEN P.IsFK = 1 THEN 'Yes' WHEN P.IsFK = 0 THEN 'No' END AS IS_FK,
                CASE WHEN P.IsLastOperation_Attribute = 1 THEN 'Yes' END AS IsLastOperation_Attribute,
                CASE WHEN P.IsTimestamp_Attribute = 1 THEN 'Yes' END AS IsTimestamp_Attribute,
                P.Attribute_Format,
                RA.Referenced_Schema,
                RA.Referenced_Tables,
                RA.Referenced_Attributes,
                CASE WHEN P.IsMasked = 1 THEN 'Yes' WHEN P.IsMasked = 0 THEN 'No' END AS IsMasked
                FROM Meta_Agency A
                LEFT JOIN Meta_Dataset D ON A.ID = D.Agency_ID
                LEFT JOIN Meta_Datastore DA ON DA.Dataset_ID = D.ID
                LEFT JOIN Meta_Pipeline PP ON DA.ID = PP.Datastore_ID
                LEFT JOIN Meta_Attribute P ON P.Datastore_ID = DA.ID
                LEFT JOIN Meta_Table T ON T.Datastore_ID = DA.ID
                LEFT JOIN (
                SELECT
                a1.ID AS Attribute_ID,
                STRING_AGG(CASE WHEN t1.Schema_Name IS NOT NULL THEN t1.Schema_Name END, ' | ') AS Referenced_Schema,
                STRING_AGG(CASE WHEN t1.Table_Name IS NOT NULL THEN t1.Table_Name END, ' | ') AS Referenced_Tables,
                STRING_AGG(CASE WHEN a2.Attribute_Name IS NOT NULL THEN a2.Attribute_Name END, ' | ') WITHIN GROUP (ORDER BY a2.Attribute_Name) AS Referenced_Attributes
                FROM Meta_Attribute_Reference ra
                JOIN Meta_Attribute a1 ON ra.Attribute_ID = a1.id
                JOIN Meta_Attribute a2 ON ra.Reference_Attribute_ID = a2.id
                JOIN Meta_Datastore d1 ON d1.ID = a2.Datastore_ID
                JOIN Meta_Table t1 ON d1.ID = t1.Datastore_ID
                GROUP BY a1.ID
                ) RA ON P.ID = RA.Attribute_ID
                WHERE Agency_Short_Name = '{os.getenv('AGENCY')}'

                AND Dataset_Short_Name = '{os.getenv('DATASET')}'
                and DA.Datastore_Name in ('ossys_Tenant' , 'OSUSR_kzv_AssistedFlareType') -- and P.ATTRIBUTE_NAME !='IS_ACTIVE'
                AND DA.Datastore_Zone = 4
                AND DA.IsActive = 1
                AND T.IsActive = 1
                AND P.IsActive = 1 
'''
    cmcTableDF=pd.read_sql(query,customMetaDataCatlogDatabase.getConnection())

    # connectionDF=customMetaDataCatlogDatabase.retriveAConnection(cmcTableDF['CONNECTION_ID'][cmcTableDF.index.min()])
    # connectionDF=pd.read_sql(query,customMetaDataCatlogDatabase.getConnection())
    
    # if(os.getenv('DATASET') == 'IGS'):
   
    
    # else:
    # sourceConnection=Connection(IP='localhost',port= '1521',databaseName='XEPDB1',password='123456' ,userName='loadTest',databaseType='ORCL')
    sourceConnection=Connection(IP='10.0.122.203',port= '1433',databaseName='Nawaf_test',password='123456' ,userName='loadTest',databaseType='SQL')
    print(sourceConnection.connect())
    print('testing connection sourceDatabase ',sourceConnection.testConnection())
    metadataDF=sourceConnection.getMetadataDF()

    print(metadataDF)
    print(cmcTableDF)
    # query=f'''


    #     SELECT * FROM DBO.CMC_Columns WHERE PIPELINE_ID=31675 '''
    # cmcTableDF=pd.read_sql(query,customMetaDataCatlogDatabase.getConnection())

    # print(cmcTableDF)
    # print(metadataDF)
    added_columns = metadataDF.merge(
    cmcTableDF[['TABLE_SCHEMA', 'TABLE_NAME', 'PIPELINE_ID']], 
    on=['TABLE_SCHEMA', 'TABLE_NAME'], 
    how='left', 
    indicator=True
    )
    added_columns = added_columns[added_columns['_merge'] == 'left_only'].drop(columns='_merge')
    # print(added_columns)

    deleted_columns = cmcTableDF.merge(
    metadataDF[['TABLE_SCHEMA', 'TABLE_NAME' ]], 
    on=['TABLE_SCHEMA', 'TABLE_NAME'], 
    how='left', 
    indicator=True
    )
    deleted_columns = deleted_columns[deleted_columns['_merge'] == 'left_only'].drop(columns='_merge')


    added_tables = (
    added_columns[['TABLE_SCHEMA', 'TABLE_NAME' , 'TABLE_OBJECT_ID']]
    .drop_duplicates()
    .to_dict('records')
    )

    CR_DETECTION['Add Table'].extend(added_tables)
    CR_DETECTION['Logs']['total_changes'] += len(added_tables)


    deleted_tables = (
    deleted_columns[['TABLE_SCHEMA', 'TABLE_NAME' , 'PIPELINE_ID' , 'DATASTORE_ID']]
    .drop_duplicates()
    .to_dict('records')
    )
    pipe_ids = (
    deleted_columns['PIPELINE_ID']
    .dropna()
    .tolist()
    )

    CR_DETECTION['Delete Table'].extend(deleted_tables)
    CR_DETECTION['Logs']['total_changes'] += len(deleted_tables)
    CR_DETECTION['Impacted Pipliens'] = list(
        dict.fromkeys(CR_DETECTION['Impacted Pipliens'] + pipe_ids)
    )


    common_table_columns = cmcTableDF.merge(
    metadataDF,
    on=['TABLE_SCHEMA', 'TABLE_NAME', 'ATTRIBUTE_NAME'],
    how='outer',
    indicator=True,
    suffixes=('_CMC', '_SOURCE')
    )
    common_table_columns = common_table_columns[
        ~common_table_columns['PIPELINE_ID'].isin(CR_DETECTION['Impacted Pipliens'])
    ]
    
    deleted_common_columns = common_table_columns[common_table_columns['_merge'] == 'left_only'].copy()
    added_columns = common_table_columns[common_table_columns['_merge'] == 'right_only'].copy()
    common_columns = common_table_columns[common_table_columns['_merge'] == 'both'].copy()


    columns=[
        'TABLE_SCHEMA',
        'COLUMN_ID',
        'TABLE_NAME', 
        'ATTRIBUTE_NAME',
        'DATA_TYPE_SOURCE',
        'IS_PK_SOURCE',
        'IS_FK_SOURCE'
    ]
    add_change_records(added_columns, columns, CR_DETECTION , 'Add Attribute')


    
    deleted_columns=[
        'TABLE_SCHEMA',
        'TABLE_NAME',
        'PIPELINE_ID',
        'ATTRIBUTE_NAME',
        'IS_PK_CMC',
        'IS_FK_CMC'
    ]
    add_change_records(deleted_common_columns, deleted_columns, CR_DETECTION , 'Delete Attribute')

    pipe_ids = (
    deleted_common_columns['PIPELINE_ID']
    .dropna()
    .astype(int)
    .tolist()
    )
    CR_DETECTION['Impacted Pipliens'] = list(
        dict.fromkeys(CR_DETECTION['Impacted Pipliens'] + pipe_ids)
    )

    modified_PK = common_table_columns[
    (common_table_columns['DATA_TYPE_CMC'].notna()) & 
    (common_table_columns['DATA_TYPE_SOURCE'].notna()) & 
    (common_table_columns['IS_PK_CMC'] !=  common_table_columns['IS_PK_SOURCE']  )
        ].copy().assign(CHANGE_TYPE='Primary Key')  
    

    pk_columns=[
           'TABLE_SCHEMA',
            'TABLE_NAME',
            'ATTRIBUTE_NAME',
            'IS_PK_CMC',
            'IS_PK_SOURCE',
            'CHANGE_TYPE'
    ]
    add_change_records(modified_PK, pk_columns, CR_DETECTION , 'Change Attribute')


    modified_datatypes = common_columns[
    (common_columns['DATA_TYPE_CMC'].notna()) & 
    (common_columns['DATA_TYPE_SOURCE'].notna()) & 
    (common_columns['DATA_TYPE_CMC'] != common_columns['DATA_TYPE_SOURCE'])
        ].copy().assign(CHANGE_TYPE='Data Type and Length')
    
    
    datatype_columns = [
    'TABLE_SCHEMA',
    'TABLE_NAME',
    'ATTRIBUTE_NAME',
    'DATA_TYPE_CMC',
    'DATA_TYPE_SOURCE',
    'CHANGE_TYPE'
]
    add_change_records(modified_datatypes, datatype_columns, CR_DETECTION , 'Change Attribute')    
    pipeline_ids = modified_datatypes['PIPELINE_ID'].unique().astype(int).tolist()
    existing_ids = set(CR_DETECTION['Impacted Pipliens'])
    new_ids = set(pipeline_ids)

    ids_to_add = new_ids - existing_ids
    CR_DETECTION['Impacted Pipliens'].extend(ids_to_add)
    
    
    json_payload = json.dumps(CR_DETECTION, ensure_ascii=False, indent=2)
    print(json_payload)
    
    

#     if CR_DETECTION['Logs']['total_changes'] > 0:
        
#         query=f'''
#            INSERT INTO [snapshot_detection] ([dataset_id] , [total_objects_captured])
#            OUTPUT INSERTED.snapshot_id      
#            VALUES ( 1000 , {CR_DETECTION['Logs']['total_changes']}) 

# '''
        
#         result=pd.read_sql(query,customMetaDataCatlogDatabase.getConnection())
#         snapshot_id=int(result['snapshot_id'][0])


#         if CR_DETECTION['Add Table']:

#             for record in added_tables:
#                 query = f'''
#                    SET NOCOUNT ON
#                     INSERT INTO [dbo].[metadata_detection] 
#                     ([snapshot_id], [source_object_id], [source_value], [change_type])
#                     VALUES ({snapshot_id}, {record['TABLE_OBJECT_ID']}, '{record['TABLE_NAME']}', 'Add Table')
#                     SELECT 1 AS A
#                 '''
#                 result=pd.read_sql(query,customMetaDataCatlogDatabase.getConnection())

#         if CR_DETECTION['Delete Table']:

#             for record in deleted_tables:
#                 query = f'''
#                    SET NOCOUNT ON
#                     INSERT INTO [dbo].[metadata_detection] 
#                     ([snapshot_id], [metadata_cmc_id], [change_type])
#                     VALUES ({snapshot_id}, {record['DATASTORE_ID']} , 'Delete Table')
#                     SELECT 1 AS A
#                 '''
#                 result=pd.read_sql(query,customMetaDataCatlogDatabase.getConnection())

#         if CR_DETECTION['Add Attribute']:
#             added_columns['COLUMN_ID'] = pd.to_numeric(added_columns['COLUMN_ID'], errors='raise').astype('int64')
#             for index, row in added_columns.iterrows():
#                 query = f'''
#                     SET NOCOUNT ON
#                     INSERT INTO [dbo].[metadata_detection] 
#                     ([snapshot_id], [source_object_id], [source_value], [change_type])
#                     VALUES ({snapshot_id}, {row['COLUMN_ID']}, '{row['ATTRIBUTE_NAME']}', 'Add Attribute')
#                     SELECT 1 AS A
#                 '''
#                 result=pd.read_sql(query,customMetaDataCatlogDatabase.getConnection())

        
#         if CR_DETECTION['Delete Attribute']:
#             deleted_common_columns['ATTRIBUTE_ID'] = pd.to_numeric(deleted_common_columns['ATTRIBUTE_ID'], errors='raise').astype('int64')
#             for index, row in deleted_common_columns.iterrows():
#                 query = f'''
#                     SET NOCOUNT ON
#                     INSERT INTO [dbo].[metadata_detection] 
#                     ([snapshot_id], [metadata_cmc_id],  [change_type])
#                     VALUES ({snapshot_id}, {row['ATTRIBUTE_ID']}, 'Delete Attribute')
#                     SELECT 1 AS A
#                 '''
#                 result=pd.read_sql(query,customMetaDataCatlogDatabase.getConnection())

#         if CR_DETECTION['Change Attribute']:
#             modified_PK['COLUMN_ID'] = pd.to_numeric(modified_PK['COLUMN_ID'], errors='raise').astype('int64')
#             modified_PK['ATTRIBUTE_ID'] = pd.to_numeric(modified_PK['ATTRIBUTE_ID'], errors='raise').astype('int64')
#             for index, row in modified_PK.iterrows():
#                 query = f'''
#                     SET NOCOUNT ON
#                     INSERT INTO [dbo].[metadata_detection] 
#                     ([snapshot_id], [source_object_id], [metadata_cmc_id] , [source_value], [change_type])
#                     VALUES ({snapshot_id}, {row['COLUMN_ID']},  '{row['ATTRIBUTE_ID']}', '{row['IS_PK_SOURCE']}', '{row['CHANGE_TYPE']}')
#                     SELECT 1 AS A
#                 '''
#                 result=pd.read_sql(query,customMetaDataCatlogDatabase.getConnection())

#             modified_datatypes['COLUMN_ID'] = pd.to_numeric(modified_datatypes['COLUMN_ID'], errors='raise').astype('int64')
#             modified_datatypes['ATTRIBUTE_ID'] = pd.to_numeric(modified_datatypes['ATTRIBUTE_ID'], errors='raise').astype('int64')
#             for index, row in modified_datatypes.iterrows():
#                 query = f'''
#                     SET NOCOUNT ON
#                     INSERT INTO [dbo].[metadata_detection] 
#                     ([snapshot_id], [source_object_id], [metadata_cmc_id] , [source_value], [change_type])
#                     VALUES ({snapshot_id}, {row['COLUMN_ID']},  '{row['ATTRIBUTE_ID']}', '{row['DATA_TYPE_SOURCE']}', '{row['CHANGE_TYPE']}')
#                     SELECT 1 AS A
#                 '''
#                 result=pd.read_sql(query,customMetaDataCatlogDatabase.getConnection())
            

    
#         azureDevops  = AzureDevops(os.getenv('ORGANIZATION'),os.getenv('AREA_PATH'),os.getenv('PAT'),os.getenv('ORGANIZATION'))
#         workItems=azureDevops.getWorkItems(os.getenv('AGENCY') , os.getenv('DATASET'))
#         print(workItems)
#         updateworkItem=azureDevops.updateTicket(TicketID=workItems[0]['id'],TargetColumn=os.getenv('TARGET_COLUMN'),State=None,Tfs_Column=os.getenv('KANBAN_COLUMN'),Comment='Dear DQ Start',Assignee=None )


