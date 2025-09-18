#!/usr/bin/env python3
"""
Database to Atlas Synchronization Script
Compares SQL Server database with Atlas entities and synchronizes changes:
- Detects new tables/columns (CREATE in Atlas)
- Detects modified tables/columns (UPDATE in Atlas)
- Detects deleted tables/columns (DELETE from Atlas)
- Provides detailed comparison reports
"""

import requests
import json
import pyodbc
from requests.auth import HTTPBasicAuth
import time
from collections import defaultdict
from datetime import datetime

# Configuration
ATLAS_URL = "http://localhost:21000"
ATLAS_USERNAME = "admin"
ATLAS_PASSWORD = "admin"

SQL_HOST = "localhost"
SQL_PORT = "1433"
SQL_DATABASE = "Mem"
SQL_USERNAME = "nawaaf"
SQL_PASSWORD = "26102610nnN@"

class AtlasDBSync:
    def __init__(self):
        self.atlas_base_url = ATLAS_URL
        self.atlas_auth = HTTPBasicAuth(ATLAS_USERNAME, ATLAS_PASSWORD)
        self.atlas_headers = {"Content-Type": "application/json"}
        
        # SQL Server connection string
        self.sql_conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SQL_HOST},{SQL_PORT};"
            f"DATABASE={SQL_DATABASE};"
            f"UID={SQL_USERNAME};"
            f"PWD={SQL_PASSWORD};"
            f"TrustServerCertificate=yes;"
        )
        
        # Atlas qualified names
        self.instance_qualified_name = f"sqlserver://{SQL_HOST}:{SQL_PORT}/{SQL_DATABASE}@cluster1"
        self.db_qualified_name = f"sqlserver://{SQL_HOST}:{SQL_PORT}/{SQL_DATABASE}.{SQL_DATABASE}@cluster1"
        
        # Sync statistics
        self.sync_stats = {
            'tables_added': 0,
            'tables_modified': 0,
            'tables_deleted': 0,
            'columns_added': 0,
            'columns_modified': 0,
            'columns_deleted': 0,
            'errors': 0
        }
        
        # Storage for comparison data
        self.sql_tables = {}
        self.sql_columns = {}
        self.atlas_tables = {}
        self.atlas_columns = {}
    
    def check_entity_exists(self, type_name, qualified_name):
        """Check if an entity exists by qualified name"""
        
        url = f"{self.atlas_base_url}/api/atlas/v2/entity/uniqueAttribute/type/{type_name}"
        params = {"attr:qualifiedName": qualified_name}
        
        try:
            response = requests.get(url, params=params, headers=self.atlas_headers, auth=self.atlas_auth)
            
            if response.status_code == 200:
                result = response.json()
                entity = result.get('entity', {})
                if entity and entity.get('guid'):
                    return {
                        'exists': True,
                        'guid': entity.get('guid'),
                        'name': entity.get('attributes', {}).get('name', 'Unknown'),
                        'status': entity.get('status', 'Unknown')
                    }
            
            return {'exists': False}
            
        except Exception as e:
            print(f"❌ Error checking {type_name}: {str(e)}")
            return {'exists': False, 'error': str(e)}
    
    def create_instance_entity(self):
        """Create rdbms_instance entity"""
        
        print("🖥️  Creating rdbms_instance entity...")
        
        instance_entity = {
            "entity": {
                "typeName": "rdbms_instance",
                "attributes": {
                    "qualifiedName": self.instance_qualified_name,
                    "name": f"{SQL_DATABASE}_Instance",
                    "description": f"SQL Server Instance for {SQL_DATABASE} database",
                    "owner": SQL_USERNAME,
                    "rdbms_type": "SqlServer",
                    "hostname": SQL_HOST,
                    "port": SQL_PORT,
                    "protocol": "sqlserver"
                },
                "status": "ACTIVE"
            }
        }
        
        url = f"{self.atlas_base_url}/api/atlas/v2/entity"
        
        try:
            response = requests.post(url, data=json.dumps(instance_entity), 
                                   headers=self.atlas_headers, auth=self.atlas_auth)
            
            if response.status_code in [200, 201]:
                result = response.json()
                guid = result.get('entity', {}).get('guid')
                print(f"   ✅ Created instance: {SQL_DATABASE}_Instance")
                print(f"   📋 GUID: {guid}")
                return guid
            else:
                print(f"   ❌ Failed to create instance: {response.text}")
                return None
                
        except Exception as e:
            print(f"   ❌ Error creating instance: {str(e)}")
            return None
    
    def create_database_entity(self):
        """Create rdbms_db entity"""
        
        print("🗄️  Creating rdbms_db entity...")
        
        db_entity = {
            "entity": {
                "typeName": "rdbms_db",
                "attributes": {
                    "qualifiedName": self.db_qualified_name,
                    "name": SQL_DATABASE,
                    "description": f"{SQL_DATABASE} database on SQL Server",
                    "owner": SQL_USERNAME,
                    "instance": {
                        "typeName": "rdbms_instance",
                        "uniqueAttributes": {
                            "qualifiedName": self.instance_qualified_name
                        }
                    }
                },
                "status": "ACTIVE"
            }
        }
        
        url = f"{self.atlas_base_url}/api/atlas/v2/entity"
        
        try:
            response = requests.post(url, data=json.dumps(db_entity), 
                                   headers=self.atlas_headers, auth=self.atlas_auth)
            
            if response.status_code in [200, 201]:
                result = response.json()
                guid = result.get('entity', {}).get('guid')
                print(f"   ✅ Created database: {SQL_DATABASE}")
                print(f"   📋 GUID: {guid}")
                return guid
            else:
                print(f"   ❌ Failed to create database: {response.text}")
                return None
                
        except Exception as e:
            print(f"   ❌ Error creating database: {str(e)}")
            return None
    
    def ensure_prerequisites(self):
        """Ensure rdbms_instance and rdbms_db entities exist"""
        
        print("🔍 CHECKING PREREQUISITES")
        print("-" * 40)
        
        prerequisites_ok = True
        
        # Check rdbms_instance
        print("1️⃣ Checking rdbms_instance entity...")
        instance_check = self.check_entity_exists("rdbms_instance", self.instance_qualified_name)
        
        if instance_check['exists']:
            print(f"   ✅ Instance exists: {instance_check['name']} ({instance_check['guid']})")
        else:
            print(f"   ❌ Instance missing, creating...")
            instance_guid = self.create_instance_entity()
            if not instance_guid:
                prerequisites_ok = False
            else:
                # Small delay to ensure entity is processed
                time.sleep(1)
        
        # Check rdbms_db
        print("2️⃣ Checking rdbms_db entity...")
        db_check = self.check_entity_exists("rdbms_db", self.db_qualified_name)
        
        if db_check['exists']:
            print(f"   ✅ Database exists: {db_check['name']} ({db_check['guid']})")
        else:
            print(f"   ❌ Database missing, creating...")
            db_guid = self.create_database_entity()
            if not db_guid:
                prerequisites_ok = False
            else:
                # Small delay to ensure entity is processed
                time.sleep(2)
        
        if prerequisites_ok:
            print("✅ All prerequisites verified/created successfully!")
        else:
            print("❌ Failed to create required prerequisites")
        
        return prerequisites_ok
    
    def get_sql_server_structure(self):
        """Get complete structure from SQL Server"""
        
        print("🗄️  Reading SQL Server database structure...")
        
        try:
            conn = pyodbc.connect(self.sql_conn_str)
            cursor = conn.cursor()
            
            # Get all tables
            table_query = """
            SELECT 
                t.TABLE_SCHEMA,
                t.TABLE_NAME,
                t.TABLE_TYPE,
                (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS c 
                 WHERE c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME) as COLUMN_COUNT
            FROM INFORMATION_SCHEMA.TABLES t
            WHERE t.TABLE_TYPE = 'BASE TABLE'
            ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
            """
            
            cursor.execute(table_query)
            tables = cursor.fetchall()
            
            for row in tables:
                table_key = f"{row[0]}.{row[1]}"
                self.sql_tables[table_key] = {
                    'schema': row[0],
                    'name': row[1],
                    'type': row[2],
                    'column_count': row[3],
                    'qualified_name': f"sqlserver://{SQL_HOST}:{SQL_PORT}/{SQL_DATABASE}.{SQL_DATABASE}.{row[0]}.{row[1]}@cluster1"
                }
            
            # Get all columns for all tables
            column_query = """
            SELECT 
                c.TABLE_SCHEMA,
                c.TABLE_NAME,
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.NUMERIC_PRECISION,
                c.NUMERIC_SCALE,
                c.IS_NULLABLE,
                c.COLUMN_DEFAULT,
                c.ORDINAL_POSITION,
                CASE 
                    WHEN pk.COLUMN_NAME IS NOT NULL THEN 'YES'
                    ELSE 'NO'
                END as IS_PRIMARY_KEY
            FROM INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN (
                SELECT ku.COLUMN_NAME, ku.TABLE_SCHEMA, ku.TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ) pk ON pk.COLUMN_NAME = c.COLUMN_NAME AND pk.TABLE_SCHEMA = c.TABLE_SCHEMA AND pk.TABLE_NAME = c.TABLE_NAME
            ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
            """
            
            cursor.execute(column_query)
            columns = cursor.fetchall()
            
            for row in columns:
                table_key = f"{row[0]}.{row[1]}"
                column_key = f"{table_key}.{row[2]}"
                
                # Build data type string
                data_type_str = row[3]
                if row[4] and row[4] > 0:
                    data_type_str += f"({row[4]})"
                elif row[5] and row[5] > 0:
                    if row[6] and row[6] > 0:
                        data_type_str += f"({row[5]},{row[6]})"
                    else:
                        data_type_str += f"({row[5]})"
                
                self.sql_columns[column_key] = {
                    'table_schema': row[0],
                    'table_name': row[1],
                    'column_name': row[2],
                    'data_type': row[3],
                    'data_type_full': data_type_str,
                    'max_length': row[4] if row[4] else 0,
                    'precision': row[5] if row[5] else 0,
                    'scale': row[6] if row[6] else 0,
                    'is_nullable': row[7] == 'YES',
                    'default_value': row[8],
                    'ordinal_position': row[9],
                    'is_primary_key': row[10] == 'YES',
                    'qualified_name': f"sqlserver://{SQL_HOST}:{SQL_PORT}/{SQL_DATABASE}.{SQL_DATABASE}.{row[0]}.{row[1]}.{row[2]}@cluster1"
                }
            
            conn.close()
            
            print(f"✅ Found {len(self.sql_tables)} tables and {len(self.sql_columns)} columns in SQL Server")
            return True
            
        except Exception as e:
            print(f"❌ Error reading SQL Server structure: {str(e)}")
            return False
    
    def verify_database_entity_exists(self):
        """Verify that the rdbms_db entity exists in Atlas"""
        
        print("🔍 Verifying database entity exists in Atlas...")
        
        url = f"{self.atlas_base_url}/api/atlas/v2/entity/uniqueAttribute/type/rdbms_db"
        params = {"attr:qualifiedName": self.db_qualified_name}
        
        try:
            response = requests.get(url, params=params, headers=self.atlas_headers, auth=self.atlas_auth)
            
            if response.status_code == 200:
                result = response.json()
                entity = result.get('entity', {})
                if entity:
                    print(f"✅ Database entity found: {entity.get('attributes', {}).get('name', 'Unknown')}")
                    print(f"   GUID: {entity.get('guid')}")
                    print(f"   Qualified Name: {self.db_qualified_name}")
                    return True
                else:
                    print(f"❌ Database entity not found in Atlas")
                    return False
            else:
                print(f"❌ Failed to check database entity: {response.status_code}")
                print(f"   Response: {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Error checking database entity: {str(e)}")
            return False
    
    def create_database_entity_if_missing(self):
        """Create the database entity if it doesn't exist"""
        
        print("🆕 Creating missing database entity...")
        
        db_entity = {
            "entity": {
                "typeName": "rdbms_db",
                "attributes": {
                    "qualifiedName": self.db_qualified_name,
                    "name": SQL_DATABASE,
                    "description": f"{SQL_DATABASE} database on SQL Server",
                    "owner": SQL_USERNAME,
                    "instance": {
                        "typeName": "rdbms_instance",
                        "uniqueAttributes": {
                            "qualifiedName": f"sqlserver://{SQL_HOST}:{SQL_PORT}/{SQL_DATABASE}@cluster1"
                        }
                    }
                },
                "status": "ACTIVE"
            }
        }
        
        url = f"{self.atlas_base_url}/api/atlas/v2/entity"
        
        try:
            response = requests.post(url, data=json.dumps(db_entity), 
                                   headers=self.atlas_headers, auth=self.atlas_auth)
            
            if response.status_code in [200, 201]:
                result = response.json()
                guid = result.get('entity', {}).get('guid')
                print(f"✅ Created database entity: {SQL_DATABASE}")
                print(f"   GUID: {guid}")
                return True
            else:
                print(f"❌ Failed to create database entity: {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Error creating database entity: {str(e)}")
            return False
    
    def get_atlas_structure(self):
        """Get complete structure from Atlas"""
        
        print("🌐 Reading Atlas entities structure...")
        
        try:
            # Get all tables from Atlas
            url = f"{self.atlas_base_url}/api/atlas/v2/search/dsl?query=rdbms_table where db.name = 'Mem'"
            # params = {"query": f'from rdbms_table where db.name = "Mem"' }
            response = requests.get(url, headers=self.atlas_headers, auth=self.atlas_auth)
            
            if response.status_code == 200:
                print(response.json())
                result = response.json()
                tables = result.get('entities', [])
                
                for table in tables:
                    # Get detailed table information
                    table_guid = table.get('guid')
                    table_detail = self.get_entity_detail(table_guid)
                    
                    if table_detail:
                        qualified_name = table_detail.get('attributes', {}).get('qualifiedName', '')
                        name = table_detail.get('attributes', {}).get('name', '')
                        
                        # Extract schema and table name from qualified name
                        # Format: sqlserver://host:port/db.db.schema.table@cluster
                        parts = qualified_name.split('.')
                        if len(parts) >= 4:
                            schema = parts[-2]
                            table_name = parts[-1].split('@')[0]
                            table_key = f"{schema}.{table_name}"
                            
                            self.atlas_tables[table_key] = {
                                'guid': table_guid,
                                'qualified_name': qualified_name,
                                'name': name,
                                'schema': schema,
                                'description': table_detail.get('attributes', {}).get('description', ''),
                                'owner': table_detail.get('attributes', {}).get('owner', '')
                            }
            
            # Get all columns from Atlas
            params = {"query": SQL_DATABASE, "typeName": "rdbms_column", "limit": 5000}
            
            response = requests.get(url, params=params, headers=self.atlas_headers, auth=self.atlas_auth)
            
            if response.status_code == 200:
                result = response.json()
                columns = result.get('entities', [])
                
                for column in columns:
                    # Get detailed column information
                    column_guid = column.get('guid')
                    column_detail = self.get_entity_detail(column_guid)
                    
                    if column_detail:
                        qualified_name = column_detail.get('attributes', {}).get('qualifiedName', '')
                        
                        # Extract schema, table, and column name from qualified name
                        # Format: sqlserver://host:port/db.db.schema.table.column@cluster
                        parts = qualified_name.split('.')
                        if len(parts) >= 5:
                            schema = parts[-3]
                            table_name = parts[-2]
                            column_name = parts[-1].split('@')[0]
                            table_key = f"{schema}.{table_name}"
                            column_key = f"{table_key}.{column_name}"
                            
                            attrs = column_detail.get('attributes', {})
                            self.atlas_columns[column_key] = {
                                'guid': column_guid,
                                'qualified_name': qualified_name,
                                'table_key': table_key,
                                'column_name': column_name,
                                'data_type': attrs.get('data_type', ''),
                                'length': attrs.get('length', 0),
                                'precision': attrs.get('precision', 0),
                                'scale': attrs.get('scale', 0),
                                'is_nullable': attrs.get('is_nullable', True),
                                'is_primary_key': attrs.get('is_primary_key', False),
                                'default_value': attrs.get('default_value'),
                                'ordinal_position': attrs.get('ordinal_position', 0),
                                'description': attrs.get('description', ''),
                                'owner': attrs.get('owner', '')
                            }
            
            print(f"✅ Found {len(self.atlas_tables)} tables and {len(self.atlas_columns)} columns in Atlas")
            return True
            
        except Exception as e:
            print(f"❌ Error reading Atlas structure: {str(e)}")
            return False
    
    def get_entity_detail(self, guid):
        """Get detailed entity information by GUID"""
        
        url = f"{self.atlas_base_url}/api/atlas/v2/entity/guid/{guid}"
        
        try:
            response = requests.get(url, headers=self.atlas_headers, auth=self.atlas_auth)
            if response.status_code == 200:
                result = response.json()
                return result.get('entity', {})
            return None
        except:
            return None
    
    def compare_structures(self):
        """Compare SQL Server and Atlas structures"""
        
        print("\n🔍 Comparing structures...")
        print("-" * 50)
        
        # Compare Tables
        sql_table_keys = set(self.sql_tables.keys())
        atlas_table_keys = set(self.atlas_tables.keys())
        
        tables_to_add = sql_table_keys - atlas_table_keys
        tables_to_delete = atlas_table_keys - sql_table_keys
        tables_common = sql_table_keys & atlas_table_keys
        
        print(f"📊 TABLE COMPARISON:")
        print(f"   🆕 Tables to add: {len(tables_to_add)}")
        print(f"   🗑️  Tables to delete: {len(tables_to_delete)}")
        print(f"   🔄 Tables to check: {len(tables_common)}")
        
        if tables_to_add:
            print(f"   📋 New tables:")
            for table in sorted(tables_to_add):
                print(f"      + {table}")
        
        if tables_to_delete:
            print(f"   📋 Tables to delete:")
            for table in sorted(tables_to_delete):
                print(f"      - {table}")
        
        # Compare Columns
        sql_column_keys = set(self.sql_columns.keys())
        atlas_column_keys = set(self.atlas_columns.keys())
        
        columns_to_add = sql_column_keys - atlas_column_keys
        columns_to_delete = atlas_column_keys - sql_column_keys
        columns_common = sql_column_keys & atlas_column_keys
        
        print(f"\n📋 COLUMN COMPARISON:")
        print(f"   🆕 Columns to add: {len(columns_to_add)}")
        print(f"   🗑️  Columns to delete: {len(columns_to_delete)}")
        print(f"   🔄 Columns to check: {len(columns_common)}")
        
        # Check for modified columns
        columns_modified = []
        for column_key in columns_common:
            sql_col = self.sql_columns[column_key]
            atlas_col = self.atlas_columns[column_key]
            
            differences = []
            
            # Compare data type
            if sql_col['data_type_full'] != atlas_col['data_type']:
                differences.append(f"data_type: '{atlas_col['data_type']}' → '{sql_col['data_type_full']}'")
            
            # Compare nullable
            if sql_col['is_nullable'] != atlas_col['is_nullable']:
                differences.append(f"is_nullable: {atlas_col['is_nullable']} → {sql_col['is_nullable']}")
            
            # Compare primary key
            if sql_col['is_primary_key'] != atlas_col['is_primary_key']:
                differences.append(f"is_primary_key: {atlas_col['is_primary_key']} → {sql_col['is_primary_key']}")
            
            # Compare ordinal position
            if sql_col['ordinal_position'] != atlas_col['ordinal_position']:
                differences.append(f"ordinal_position: {atlas_col['ordinal_position']} → {sql_col['ordinal_position']}")
            
            if differences:
                columns_modified.append({
                    'key': column_key,
                    'differences': differences
                })
        
        print(f"   🔧 Columns modified: {len(columns_modified)}")
        
        if columns_modified:
            print(f"   📋 Modified columns:")
            for mod in columns_modified[:10]:  # Show first 10
                print(f"      ~ {mod['key']}: {', '.join(mod['differences'])}")
            if len(columns_modified) > 10:
                print(f"      ... and {len(columns_modified) - 10} more")
        
        return {
            'tables_to_add': tables_to_add,
            'tables_to_delete': tables_to_delete,
            'columns_to_add': columns_to_add,
            'columns_to_delete': columns_to_delete,
            'columns_modified': columns_modified
        }
    
    def create_table_entity(self, table_key):
        """Create a new table entity in Atlas"""
        
        table_info = self.sql_tables[table_key]
        
        table_entity = {
            "entity": {
                "typeName": "rdbms_table",
                "attributes": {
                    "qualifiedName": table_info['qualified_name'],
                    "name": table_info['name'],
                    "description": f"Table {table_info['name']} in {table_info['schema']} schema ({table_info['column_count']} columns)",
                    "owner": SQL_USERNAME,
                    "db": {
                        "typeName": "rdbms_db",
                        "uniqueAttributes": {
                            "qualifiedName": self.db_qualified_name
                        }
                    }
                },
                "status": "ACTIVE"
            }
        }
        
        url = f"{self.atlas_base_url}/api/atlas/v2/entity"
        
        try:
            response = requests.post(url, data=json.dumps(table_entity), 
                                   headers=self.atlas_headers, auth=self.atlas_auth)
            
            if response.status_code in [200, 201]:
                result = response.json()
                guid = result.get('entity', {}).get('guid')
                print(f"   ✅ Created table: {table_key}")
                self.sync_stats['tables_added'] += 1
                return guid
            else:
                print(f"   ❌ Failed to create table {table_key}: {response.text}")
                self.sync_stats['errors'] += 1
                return None
                
        except Exception as e:
            print(f"   ❌ Error creating table {table_key}: {str(e)}")
            self.sync_stats['errors'] += 1
            return None
    
    def delete_table_entity(self, table_key):
        """Delete a table entity from Atlas"""
        
        atlas_table = self.atlas_tables[table_key]
        guid = atlas_table['guid']
        
        url = f"{self.atlas_base_url}/api/atlas/v2/entity/guid/{guid}"
        
        try:
            response = requests.delete(url, headers=self.atlas_headers, auth=self.atlas_auth)
            
            if response.status_code in [200, 204]:
                print(f"   ✅ Deleted table: {table_key}")
                self.sync_stats['tables_deleted'] += 1
                return True
            else:
                print(f"   ❌ Failed to delete table {table_key}: {response.text}")
                self.sync_stats['errors'] += 1
                return False
                
        except Exception as e:
            print(f"   ❌ Error deleting table {table_key}: {str(e)}")
            self.sync_stats['errors'] += 1
            return False
    
    def create_column_entity(self, column_key):
        """Create a new column entity in Atlas"""
        
        column_info = self.sql_columns[column_key]
        table_key = f"{column_info['table_schema']}.{column_info['table_name']}"
        table_qualified_name = self.sql_tables[table_key]['qualified_name']
        
        column_entity = {
            "typeName": "rdbms_column",
            "attributes": {
                "qualifiedName": column_info['qualified_name'],
                "name": column_info['column_name'],
                "description": f"Column {column_info['column_name']} of type {column_info['data_type_full']}",
                "owner": SQL_USERNAME,
                "data_type": column_info['data_type_full'],
                "length": column_info['max_length'],
                "precision": column_info['precision'],
                "scale": column_info['scale'],
                "is_nullable": column_info['is_nullable'],
                "is_primary_key": column_info['is_primary_key'],
                "default_value": column_info['default_value'],
                "ordinal_position": column_info['ordinal_position'],
                "table": {
                    "typeName": "rdbms_table",
                    "uniqueAttributes": {
                        "qualifiedName": table_qualified_name
                    }
                }
            },
            "status": "ACTIVE"
        }
        
        url = f"{self.atlas_base_url}/api/atlas/v2/entity"
        
        try:
            response = requests.post(url, data=json.dumps({"entity": column_entity}), 
                                   headers=self.atlas_headers, auth=self.atlas_auth)
            
            if response.status_code in [200, 201]:
                print(f"   ✅ Created column: {column_key}")
                self.sync_stats['columns_added'] += 1
                return True
            else:
                print(f"   ❌ Failed to create column {column_key}: {response.text}")
                self.sync_stats['errors'] += 1
                return False
                
        except Exception as e:
            print(f"   ❌ Error creating column {column_key}: {str(e)}")
            self.sync_stats['errors'] += 1
            return False
    
    def update_column_entity(self, column_key, differences):
        """Update a column entity in Atlas"""
        
        atlas_column = self.atlas_columns[column_key]
        sql_column = self.sql_columns[column_key]
        guid = atlas_column['guid']
        
        # Build updated attributes
        updated_attributes = {
            "data_type": sql_column['data_type_full'],
            "length": sql_column['max_length'],
            "precision": sql_column['precision'],
            "scale": sql_column['scale'],
            "is_nullable": sql_column['is_nullable'],
            "is_primary_key": sql_column['is_primary_key'],
            "default_value": sql_column['default_value'],
            "ordinal_position": sql_column['ordinal_position'],
            "description": f"Column {sql_column['column_name']} of type {sql_column['data_type_full']} (updated)"
        }
        
        update_entity = {
            "entity": {
                "guid": guid,
                "typeName": "rdbms_column",
                "attributes": updated_attributes
            }
        }
        
        url = f"{self.atlas_base_url}/api/atlas/v2/entity/guid/{guid}"
        
        try:
            response = requests.put(url, data=json.dumps(update_entity), 
                                  headers=self.atlas_headers, auth=self.atlas_auth)
            
            if response.status_code in [200, 204]:
                print(f"   ✅ Updated column: {column_key} ({', '.join(differences)})")
                self.sync_stats['columns_modified'] += 1
                return True
            else:
                print(f"   ❌ Failed to update column {column_key}: {response.text}")
                self.sync_stats['errors'] += 1
                return False
                
        except Exception as e:
            print(f"   ❌ Error updating column {column_key}: {str(e)}")
            self.sync_stats['errors'] += 1
            return False
    
    def delete_column_entity(self, column_key):
        """Delete a column entity from Atlas"""
        
        atlas_column = self.atlas_columns[column_key]
        guid = atlas_column['guid']
        
        url = f"{self.atlas_base_url}/api/atlas/v2/entity/guid/{guid}"
        
        try:
            response = requests.delete(url, headers=self.atlas_headers, auth=self.atlas_auth)
            
            if response.status_code in [200, 204]:
                print(f"   ✅ Deleted column: {column_key}")
                self.sync_stats['columns_deleted'] += 1
                return True
            else:
                print(f"   ❌ Failed to delete column {column_key}: {response.text}")
                self.sync_stats['errors'] += 1
                return False
                
        except Exception as e:
            print(f"   ❌ Error deleting column {column_key}: {str(e)}")
            self.sync_stats['errors'] += 1
            return False
    
    def sync_changes(self, changes, apply_changes=False):
        """Apply synchronization changes"""
        
        if not apply_changes:
            print("\n📋 SYNCHRONIZATION PREVIEW (DRY RUN)")
            print("=" * 50)
            print("Use apply_changes=True to actually perform these operations")
            return True
        
        print("\n🔄 APPLYING SYNCHRONIZATION CHANGES")
        print("=" * 50)
        
        start_time = time.time()
        
        # 1. Add new tables
        if changes['tables_to_add']:
            print(f"\n🆕 Adding {len(changes['tables_to_add'])} new tables...")
            for table_key in changes['tables_to_add']:
                self.create_table_entity(table_key)
                time.sleep(0.1)  # Avoid overwhelming Atlas
        
        # 2. Add new columns
        if changes['columns_to_add']:
            print(f"\n🆕 Adding {len(changes['columns_to_add'])} new columns...")
            for column_key in changes['columns_to_add']:
                self.create_column_entity(column_key)
                time.sleep(0.05)
        
        # 3. Update modified columns
        if changes['columns_modified']:
            print(f"\n🔧 Updating {len(changes['columns_modified'])} modified columns...")
            for mod in changes['columns_modified']:
                self.update_column_entity(mod['key'], mod['differences'])
                time.sleep(0.05)
        
        # 4. Delete removed columns
        if changes['columns_to_delete']:
            print(f"\n🗑️  Deleting {len(changes['columns_to_delete'])} removed columns...")
            for column_key in changes['columns_to_delete']:
                self.delete_column_entity(column_key)
                time.sleep(0.05)
        
        # 5. Delete removed tables
        if changes['tables_to_delete']:
            print(f"\n🗑️  Deleting {len(changes['tables_to_delete'])} removed tables...")
            for table_key in changes['tables_to_delete']:
                self.delete_table_entity(table_key)
                time.sleep(0.1)
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"\n🎉 SYNCHRONIZATION COMPLETED!")
        print("=" * 40)
        print(f"📊 SYNC STATISTICS:")
        for key, value in self.sync_stats.items():
            if value > 0:
                print(f"   {key.replace('_', ' ').title()}: {value}")
        print(f"   ⏱️  Total time: {duration:.2f} seconds")
        
        return True
    
    def run_sync_analysis(self):
        """Run complete sync analysis"""
        
        print("🔄 DATABASE TO ATLAS SYNCHRONIZATION")
        print("=" * 60)
        print(f"📊 Database: {SQL_DATABASE}")
        print(f"🖥️  SQL Server: {SQL_HOST}:{SQL_PORT}")
        print(f"🌐 Atlas: {ATLAS_URL}")
        print(f"🔗 DB Qualified Name: {self.db_qualified_name}")
        print(f"🕐 Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        # Step 0: Verify database entity exists
        if not self.verify_database_entity_exists():
            print("\n⚠️  Database entity missing. Attempting to create it...")
            if not self.create_database_entity_if_missing():
                print("❌ Cannot proceed without database entity. Please create rdbms_db entity first.")
                return False
            # Wait a moment for entity to be processed
            time.sleep(2)
        
        # Step 1: Read SQL Server structure
        if not self.get_sql_server_structure():
            return False
        
        # Step 2: Read Atlas structure
        if not self.get_atlas_structure():
            return False
        
        # Step 3: Compare structures
        changes = self.compare_structures()
        
        return changes

def main():
    """Main function"""
    
    sync = AtlasDBSync()
    
    while True:
        print("\n" + "=" * 60)
        print("🔄 DATABASE TO ATLAS SYNCHRONIZATION TOOL")
        print("=" * 60)
        print("🔧 Available Operations:")
        print("1. 🔍 Analyze differences (read-only)")
        print("2. 🔄 Preview synchronization (dry run)")
        print("3. ⚡ Apply synchronization (make changes)")
        print("4. 📊 Show sync statistics")
        print("5. ❌ Exit")
        
        choice = input("\nEnter your choice (1-5): ").strip()
        
        if choice == '1':
            changes = sync.run_sync_analysis()
            if changes:
                print("\n✅ Analysis completed. Use option 2 to preview sync or option 3 to apply changes.")
        
        elif choice == '2':
            changes = sync.run_sync_analysis()
            if changes:
                sync.sync_changes(changes, apply_changes=False)
        
        elif choice == '3':
            confirm = input("\n⚠️  This will modify Atlas entities. Are you sure? (y/N): ")
            if confirm.lower() == 'y':
                changes = sync.run_sync_analysis()
                if changes:
                    sync.sync_changes(changes, apply_changes=True)
            else:
                print("❌ Synchronization cancelled")
        
        elif choice == '4':
            print("\n📊 Synchronization Statistics:")
            for key, value in sync.sync_stats.items():
                print(f"   {key.replace('_', ' ').title()}: {value}")
        
        elif choice == '5':
            print("👋 Goodbye!")
            break
        
        else:
            print("❌ Invalid choice. Please enter 1-5.")

if __name__ == "__main__":
    main()