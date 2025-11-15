import json
import pprint
from classes.Connection import Connection
import re
import os
from requests.auth import HTTPBasicAuth
import requests
from bs4 import BeautifulSoup
import pandas as pd
import base64
from datetime import datetime


class AzureDevops:

    
    def __init__(self,ORGANIZATION=None,PROJECT=None,PAT=None,organization_url=None):
            self.PROJECT=PROJECT
            self.PAT=PAT
            self.ORGANIZATION=ORGANIZATION
            self.organization_url= ORGANIZATION
            self.session = requests.Session()
            self.session.auth = HTTPBasicAuth('', PAT)
            self.session.verify = False  # Disable SSL verification


    def getWorkItems(self,Agency,Dataset):
        
        work_items_data = []
        try:
            wiql_query = {
            'query': f"""
                SELECT [System.Id], [System.Title], [System.State], [System.BoardColumn] , [System.AreaPath], [System.CreatedBy]
                FROM WorkItems
                WHERE [System.AreaPath] = "{self.PROJECT}"
                AND [System.WorkItemType] = 'Feature'
                AND [System.Title] CONTAINS '{Agency} - {Dataset}'
            """
            }

            response = requests.post(
                    f'{self.organization_url}/_apis/wit/wiql?api-version=6.0',
                    json=wiql_query , auth=HTTPBasicAuth("", self.PAT), verify=False
                )
            # response.raise_for_status()  # Check for HTTP error
            wiql_result = response.json()
            if response.status_code !=200 or not wiql_result['workItems']:
                return []
            work_item_ids = [item['id'] for item in wiql_result['workItems']]
            response = self.session.get(
                    f'{self.organization_url}/_apis/wit/workitems?ids={",".join(map(str, work_item_ids))}&api-version=6.0'
                )
            response.raise_for_status()  # Check for HTTP errors
            work_items = response.json()['value']
            

              
            for item in work_items:
                fields = item['fields'] 
                work_item_data = {
                            'id': item['id'],
                            'title': fields.get('System.Title', ''),
                            'boardColumn': fields.get('System.BoardColumn', ''),
                            'workItemType':fields.get('System.WorkItemType', ''),
                            'areaPath':fields.get('System.AreaPath', ''),
                            'CreatedBy': fields.get('System.CreatedBy', '')
                        }
                            
                work_items_data.append(work_item_data)
               
        except Exception as e:
            print(f"An error occurred: {e}")
            
        return work_items_data
    
    def parse_datetime_string(self,datetime_string):
        formats = [
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%dT%H:%M:%S.%fZ',   
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(datetime_string, fmt)
            except ValueError:
                continue
        raise ValueError(f"Could not parse datetime string: {datetime_string}")


    
    def generateReport(self):
        work_items_data = []
        try:
                wiql_query = {
                    'query': f"""
                        SELECT [System.Id], [System.Title], [System.State], [System.BoardColumn] , [System.CreatedBy]
                        FROM WorkItems
                        WHERE [System.TeamProject] = 'NDB2 - Data Integration'
                         AND [System.WorkItemType] = 'Feature'
                         AND [System.CreatedDate] > '2024-02-10'


                    """
                }
                time = 0
                response = self.session.post(
                    f'{self.organization_url}/_apis/wit/wiql?api-version=6.0',
                    json=wiql_query
                )
                response.raise_for_status()
                wiql_result = response.json()['workItems']
                work_item_ids = [item['id'] for item in wiql_result]
                schema_updates=[]
                response = self.session.get(
                    f'{self.organization_url}/_apis/wit/workitems?ids={",".join(map(str, work_item_ids))}&api-version=6.0'
                )
                time_diff=0
                work_items = response.json()['value']
                november=datetime(2024,10,24)
                for item in work_items:

                    response = self.session.get(
                    f'{self.organization_url}/_apis/wit/workitems/{item["id"]}/updates?&api-version=6.0'
                )

                    updates = response.json()['value']
                    for update in updates:
                        fields=update.get('fields',{})
                        if 'relations' in update and  'System.ChangedDate' in fields and fields['System.ChangedDate'].get('oldValue') is not None  and self.parse_datetime_string(fields['System.ChangedDate'].get('oldValue')) < november:
                           data=update.get('relations' , ['']).get('added',[{'attributes':{'name':''}}])
                           
                           if  'development' in data[0].get('attributes','').get('name',''):
                                time=((self.parse_datetime_string(fields['System.ChangedDate'].get('newValue'))  - self.parse_datetime_string(fields['System.ChangedDate'].get('oldValue'))).total_seconds())/60
                                time_diff+=time
                                schema_updates.append( {
                                'ID': item['id'],
                                'Title': item.get('fields',{}).get('System.Title', ''),
                                'BoardColumn': fields.get('System.BoardColumn', ''),
                                'ChangedDate': fields.get('System.ChangedDate', ''),
                                'ResponseTime':time
                            })
        except requests.RequestException as e:
            print(f"An error occurred: {e}")
        schema_updates.append({'Average Time':str((time_diff/len(schema_updates)))+' Minutes' })
        return schema_updates

                
                    
    def getBeneficiaryTickets(self,condition):
            work_items_data = []
            try:
                wiql_query = {
                    'query': f"""
                        SELECT [System.Id], [System.Title], [System.State], [System.BoardColumn] , [System.CreatedBy]
                        FROM WorkItems
                        WHERE [System.TeamProject] = '{self.PROJECT}'
                        AND [System.BoardColumn] IN {condition}
                        AND [Custom.IssueType] = 'Outdated Tables'
                    """
                } ## AND [System.Id] in (54242)
                response = self.session.post(
                    f'{self.organization_url}/_apis/wit/wiql?api-version=6.0',
                    json=wiql_query
                )
                response.raise_for_status()  # Check for HTTP errors
                wiql_result = response.json()['workItems']
                work_item_ids = [item['id'] for item in wiql_result]
                
                response = self.session.get(
                    f'{self.organization_url}/_apis/wit/workitems?ids={",".join(map(str, work_item_ids))}&api-version=6.0'
                )
                response.raise_for_status()  # Check for HTTP errors
                work_items = response.json()['value']
                
                for item in work_items:
                    fields = item['fields']
                    
                    if 'custom.Query' in fields:
                        soup = BeautifulSoup(fields['custom.Query'], 'html.parser')
                        queryWithoutHTML = soup.get_text(separator='\n').strip()
                        

                        combined_query = queryWithoutHTML.strip()
               
                      
                       
                        if re.search(r'union all', queryWithoutHTML, re.IGNORECASE):
                            
                            split_queries = re.split(r'union all', queryWithoutHTML, flags=re.IGNORECASE)
                        else:
                            # Split by 'select' (case insensitive)
                            split_queries = re.split(r'(?i)select', queryWithoutHTML)
                            split_queries = [f'SELECT {q.strip()}' for q in split_queries if q.strip()] 
                        
                            
                       
                        query = []
                        for i, q in enumerate(split_queries):
                            
                            ndb2query = q.strip()
                            clean_text = re.sub(r'<.*?>', '', ndb2query)
                            clean_text = clean_text.strip()
                            query.append(clean_text)
                        
                        work_item_data = {
                            'id': item['id'],
                            'title': fields.get('System.Title', ''),
                            'state': fields.get('System.State', ''),
                            'boardColumn': fields.get('System.BoardColumn', ''),
                            'CreatedBy': fields.get('System.CreatedBy', ''),
                            'ndbQuery': query,
                            'ndbQueryCombined': combined_query 
                        }
                            
                        work_items_data.append(work_item_data)
                        query = []
            except requests.RequestException as e:
                print(f"An error occurred: {e}")
            
            return work_items_data


    
    
    def getAnnouncedCrDatasetWorkItems(self):
        work_items_data = []
        try:
            wiql_query = {
            'query': f"""
                SELECT [System.Id], [System.Title], [System.State], [System.BoardColumn] , [System.AreaPath], [System.CreatedBy],[System.WorkItemType],
                [custom.agency],[custom.datasetName]
                FROM WorkItems
                WHERE [System.TeamProject] = 'Batch-Based Data Integration'
                AND [System.WorkItemType] in  ('New Dataset' , 'CR Announcement' )
                AND [System.State] in ('Completed & Send Email Notification')      
                AND [State Change Date] >= @Today - 5
                AND [custom.platform]='NDB2'  

            """
            }
            # ,'Data Re-Initialization')
            response = self.session.post(
                    f'{self.organization_url}/_apis/wit/wiql?api-version=6.0',
                    json=wiql_query
                )
            # response.raise_for_status()  # Check for HTTP error
            wiql_result = response.json()
            if response.status_code !=200 or not wiql_result['workItems']:
                return []
            work_item_ids = [item['id'] for item in wiql_result['workItems']]
            response = self.session.get(
                    f'{self.organization_url}/_apis/wit/workitems?ids={",".join(map(str, work_item_ids))}&$expand=all&api-version=6.0'
                )
             

            # response.raise_for_status()  # Check for HTTP errors
            work_items_json = response.json()
            print("work_items_json:", work_items_json)
            work_items = work_items_json.get('value', [])
            if not work_items:
                print("No 'value' key in response or it's empty.")
                return []

            latest_items = {}
            for item in work_items:
                key = (item["fields"].get("custom.agency", "") + item["fields"].get("custom.datasetName", ""))
                if key not in latest_items or item["id"] > latest_items[key]["id"]:
                    latest_items[key] = item

            for item in latest_items.values():
                fields = item['fields']
                version = None       
                if 'relations' in item:
                    
                    for relation in item['relations']:
                        if relation['rel'] == 'AttachedFile' and 'attributes' in relation and 'name' in relation['attributes']:
                            name = relation['attributes']['name']
                            match = re.search(r'_V\d+R\d+', name)
                            if match:
                                version = match.group()[1:]  # Remove leading underscore
                            
                work_item_data = {
                    'id': item['id'],
                    'title': fields.get('System.Title', ''),
                    'state': fields.get('System.State', ''),
                    'areaPath': fields.get('System.AreaPath', ''),
                    'CreatedBy': fields.get('System.CreatedBy', ''),
                    'workItemType': fields.get('System.WorkItemType', ''),
                    'version': version}
                work_items_data.append(work_item_data)
                        
                    
        except Exception as e:
            print(f"An error occurred: {e}")
        
        return work_items_data
    
    

    

    def updateTicket(self,TicketID,TargetColumn,State,Tfs_Column,Comment,Assignee):
        print('step1')     
        url = f'{self.organization_url}/_apis/wit/workitems/{TicketID}?api-version=6.0-preview.3'
        response = self.session.get(url)
        response.raise_for_status()
        work_item=response.json()


        update_document = []

        if Comment is not None:
            update_document.append({
                'op': 'add',
                'path': '/fields/System.History',
                'value': Comment
            })

        if Assignee is not None:
            update_document.append({
                'op': 'add',
                'path': '/fields/System.AssignedTo',
                'value': Assignee
            })

        if State is not None :
            update_document.append( {
                'op': 'add',
                'path': '/fields/System.State',
                'value': State
            })
            
            
        if TargetColumn is not None :
            update_document.append( {
                'op': 'add',
                'path': f'/fields/{Tfs_Column}',
                'value': TargetColumn
            })
        self.update_work_item(TicketID, update_document)

        return 1

    


    def update_work_item(self, TicketID, update_document):
        url = f'{self.organization_url}/_apis/wit/workitems/{TicketID}?api-version=6.0-preview.3'
        headers = {
            'Content-Type': 'application/json-patch+json'
        }
        response = self.session.patch(url, json=update_document, headers=headers)
        response.raise_for_status()  # Raise error for bad status codes
        return response.json()
    

    
    
    def attachXlsxToWorkItem(self ,filePath,workItemID):
        
        upload_url = f'{self.organization_url}/_apis/wit/attachments?fileName={filePath.split("/")[-1]}&api-version=6.0'

        headers = {
            'Content-Type': 'application/octet-stream'  
        }

        with open(filePath, 'rb') as file_data:
            upload_response =  self.session.post(upload_url, headers=headers, data=file_data, auth=HTTPBasicAuth('', self.PAT) , )

        # Check if the upload was successful
        if upload_response.status_code == 201:
            upload_response_json = upload_response.json()
            attachment_url = upload_response_json['url']
            print(f"File uploaded successfully. Attachment URL: {attachment_url}")
            
            
            work_item_url = f'{self.organization_url}/_apis/wit/workItems/{workItemID}?api-version=6.0'
            
            
            json_body = [
                {
                    "op": "add",
                    "path": "/relations/-",
                    "value": {
                        "rel": "AttachedFile",
                        "url": attachment_url,
                        "attributes": {
                            "comment": "Attached XLSX file"
                        }
                    }
                }
            ]
            
            headers = {
                'Content-Type': 'application/json-patch+json'
            }
            
            # Make the PATCH request to attach the file to the work item
            attach_response =  self.session.patch(work_item_url, json=json_body, headers=headers, auth=HTTPBasicAuth('', self.PAT))
            
            if attach_response.status_code == 200:
                print("File attached successfully to the work item.")
            else:
                print(f"Failed to attach file to work item: {attach_response.status_code} - {attach_response.text}")
        else:
            print(f"File upload failed: {upload_response.status_code} - {upload_response.text}")

    def downloadSchemaMetaData(self,Agency_Code,Dataset_Code,Fixed_UPLOAD_DIR , Item,Tfs_Column):
        base_url=self.organization_url+f'/_apis/git/repositories/4faf5f83-203e-4038-a28c-08765320acf5/itemsbatch?api-version=6.0'
        data = {
        "itemDescriptors": [
            {
                "path": f'/{Agency_Code}/{Dataset_Code}',
                "recursionLevel": "OneLevel" 
            }
            ]
        }
        
        response = self.session.post(base_url, headers={'Content-Type': 'application/json'}, data=json.dumps(data), auth=HTTPBasicAuth("", self.PAT))
        
        if response.status_code != 200:
            updateworkItem=self.updateTicket(Item['id'],'Not Ready for Profiling',None,Tfs_Column,(f"Dear <a href='#' data-vss-mention='version:2.0,{Item['CreatedBy']['id']}'>@{Item['CreatedBy']['displayName']}</a>,<br>" )+
                    ("Following error message appears:<br><br><span style=\"color:rgb(155, 0, 0);background-color:rgb(204, 204, 204)\"><b><b style=\"box-sizing:border-box;outline:none;background-color:rgb(204, 204, 204)\">Agency/Dataset folder doesn't exist or naming mismatch.</b></b>  </span><br>"),
                                                                                Item['CreatedBy']['displayName'])
            return '-1'
        
            
            
            
            
        folderPath=response.json()['value'][0][-1]['path']
        data = {
        "itemDescriptors": [
            {
                "path": f'{folderPath}/Schema',
                "recursionLevel": "OneLevel" 
            }
            ]
        }

    
        response = self.session.post(base_url, headers={'Content-Type': 'application/json'}, data=json.dumps(data), auth=HTTPBasicAuth("", self.PAT))
        if response.status_code != 200:
            updateworkItem=self.updateProfileTicket(Item['id'],'Not Ready for Profiling',None,Tfs_Column,(f"Dear <a href='#' data-vss-mention='version:2.0,{Item['CreatedBy']['id']}'>@{Item['CreatedBy']['displayName']}</a>,<br>" )+
                    ("Following error message appears:<br><br><span style=\"color:rgb(155, 0, 0);background-color:rgb(204, 204, 204)\"><b><b style=\"box-sizing:border-box;outline:none;background-color:rgb(204, 204, 204)\">Schema Folder doesn't exist in latest folder.</b></b>  </span><br>"),
                                                                                Item['CreatedBy']['displayName'])
            return '-1'
        
        
        folder_contents = response.json()['value'][0]
        
        url=f'{self.organization_url}/_apis/git/repositories/4faf5f83-203e-4038-a28c-08765320acf5/refs?filter=heads/main&api-version=6.0'
        response = self.session.get(url,headers={'Content-Type': 'application/json'}, data=json.dumps(data),auth=HTTPBasicAuth("", self.PAT))
        
        url=f'{self.organization_url}/_apis/git/repositories/4faf5f83-203e-4038-a28c-08765320acf5/pushes?api-version=6.0'
        data = {
            "refUpdates": [
                {
                    "name": f"refs/heads/main",
                    "oldObjectId": response.json()['value'][0]['objectId']
                }
            ],
            "commits": [
                {
                    "comment": "Create folder Profiling and add a dummy file",
                    "changes": [
                        {
                            "changeType": "add",
                            "item": {
                                "path":folderPath+'/Profiling/dummy.txt'
                            },
                            "newContent": {
                                "content": 'This is a placeholder file for creating the folder.',
                                "contentType": "rawtext"
                            }
                        }
                    ]
                }
            ]
        }

        
        response = self.session.post(url,headers={'Content-Type': 'application/json'}, data=json.dumps(data),auth=HTTPBasicAuth("", self.PAT))
        downloadPath = None

        
        # Loop through 'value' array to check for .xlsx files
        for item in folder_contents:
            # Check if the item path ends with .xlsx
            if f'{folderPath}/Schema/{Agency_Code}_{Dataset_Code}_{folderPath.split("/")[-1]}.' in item['path']:
                downloadPath = item['path']  # Save the path of the .xlsx file
                break  # Break if you only need the first .xlsx file (remove if you want all)
        if downloadPath is None:
            updateworkItem=self.updateProfileTicket(Item['id'],'Not Ready for Profiling',None,Tfs_Column,(f"Dear <a href='#' data-vss-mention='version:2.0,{Item['CreatedBy']['id']}'>@{Item['CreatedBy']['displayName']}</a>,<br>" )+
                    ("Following error message appears:<br><br><span style=\"color:rgb(155, 0, 0);background-color:rgb(204, 204, 204)\"><b><b style=\"box-sizing:border-box;outline:none;background-color:rgb(204, 204, 204)\">The specified schema name is invalid or dosen't exists.</b></b>  </span><br>"),
                                                                                Item['CreatedBy']['displayName'])
            return '-1'
           
        file_name = os.path.basename(downloadPath)
        downloadurl = self.organization_url+f'/_apis/git/repositories/4faf5f83-203e-4038-a28c-08765320acf5/items?path={downloadPath}&download=true&api-version=6.0'
        print(downloadurl ,'url')
        response = self.session.get(downloadurl, headers={'Content-Type': 'application/json'} ,auth=HTTPBasicAuth("", self.PAT))
        
        # print(file_name,'fileName')
        if response.status_code == 200:
            with open(str(Fixed_UPLOAD_DIR /file_name), 'wb') as file:
                file.write(response.content)
            print(f"File downloaded successfully and saved to {str(Fixed_UPLOAD_DIR /file_name)}!")
        
            
        
        return file_name
    
    

    def uploadSheet(self,Agency_Code,Dataset_Code,file_path):
        base_url=self.organization_url+f'/_apis/git/repositories/4faf5f83-203e-4038-a28c-08765320acf5/itemsbatch?api-version=6.0'
        data = {
        "itemDescriptors": [
            {
                "path": f'/{Agency_Code}/{Dataset_Code}',
                "recursionLevel": "OneLevel" 
            }
            ]
        }
        
        response = self.session.post(base_url, headers={'Content-Type': 'application/json'}, data=json.dumps(data), auth=HTTPBasicAuth("", self.PAT))
        
        if response.status_code != 200:
            return '-1'
        folderPath=response.json()['value'][0][-1]['path']
        data = {
        "itemDescriptors": [
            {
                "path": f'{folderPath}/Schema',
                "recursionLevel": "OneLevel" 
            }
            ]
        }

    
        response = self.session.post(base_url, headers={'Content-Type': 'application/json'}, data=json.dumps(data), auth=HTTPBasicAuth("", self.PAT))
        if response.status_code != 200:
            return '-2'
        
        
        folder_contents = response.json()['value'][0]
        version = re.search(r'V\d+R\d+', folderPath).group()
        
        url=f'{self.organization_url}/_apis/git/repositories/4faf5f83-203e-4038-a28c-08765320acf5/refs?filter=heads/main&api-version=6.0'
        response = self.session.get(url,headers={'Content-Type': 'application/json'}, data=json.dumps(data),auth=HTTPBasicAuth("", self.PAT))
        
        
        
        with open(file_path, "rb") as file:
            content = base64.b64encode(file.read()).decode()
        
        url=f'{self.organization_url}/_apis/git/repositories/4faf5f83-203e-4038-a28c-08765320acf5/pushes?api-version=6.0'
        data = {
            "refUpdates": [
                {
                    "name": f"refs/heads/main",
                    "oldObjectId": response.json()['value'][0]['objectId']
                }
            ],
            "commits": [
                {
                    "comment": "Upload Passed Profiling sheet.",
                    "changes": [
                        {
                            "changeType": "add",
                            "item": {
                                "path":f'{folderPath}/Profiling/{Agency_Code}_{Dataset_Code}_Profiling_{version}.xlsx'
                            },
                            "newContent": {
                                "content":content ,
                                "contentType": "base64encoded"
                            }
                        }
                    ]
                }
            ]
        }

        
        response = self.session.post(url,headers={'Content-Type': 'application/json'}, data=json.dumps(data),auth=HTTPBasicAuth("", self.PAT))
        # response.raise_for_status() 
        return response.json()
    
    
    
    
    
    def uploadSchemaMetaData(self,file_path,file_name,Item,Tfs_Column , con):
        url = os.getenv('URL')+':8443/api/metadata'

        files = {
            'filepond': open(file_path, 'rb'), 
        }


        headers = {
            'Access-Control-Allow-Origin': os.getenv('URL')+':80',
            'Access-Control-Allow-Credentials': 'true',
            'Access-Control-Allow-Methods': 'POST'
        }

        try:
            response = requests.post(url, files=files, headers=headers)
            if(response.status_code==400):
                updateworkItem=self.updateProfileTicket(Item['id'],'Not Ready for Profiling',None,Tfs_Column,(f"Dear <a href='#' data-vss-mention='version:2.0,{Item['CreatedBy']['id']}'>@{Item['CreatedBy']['displayName']}</a>,<br>" )+
                    ('Error in schema:<br><br>')+ (f"<span style=\"color:rgb(155, 0, 0);background-color:rgb(204, 204, 204)\"><b><b style=\"box-sizing:border-box;outline:none;background-color:rgb(204, 204, 204)\">{response.json()[0]}</span><br>"),
                                                                                Item['CreatedBy']['displayName'])
                return '-1'
            return '1'
        except Exception as e:
            print(e)
            q=f'''	select Error_Msg from   [CONFIG].[Agency_Schema_Source_Log] where Row_ID=(

        select max(Row_ID) from [CONFIG].[Agency_Schema_Source_Log] ag left join [CONFIG].[Agency_Schema_Source] a on a.Agency_Schema_Source_ID=ag.Agency_Schema_Source_ID
        where a.[File_Name] = '{file_name}'

        )'''
            a=pd.read_sql(q,con)
            updateworkItem=self.updateProfileTicket(Item['id'],'Not Ready for Profiling',None,Tfs_Column,(f"Dear <a href='#' data-vss-mention='version:2.0,{Item['CreatedBy']['id']}'>@{Item['CreatedBy']['displayName']}</a>,<br>" )+
                    ('Error in schema:<br><br>')+ (f"<span style=\"color:rgb(155, 0, 0);background-color:rgb(204, 204, 204)\"><b><b style=\"box-sizing:border-box;outline:none;background-color:rgb(204, 204, 204)\">{ a['Error_Msg'][0]}</span><br>"),
                                                                                Item['CreatedBy']['displayName'])
            return '-1'
    
    


