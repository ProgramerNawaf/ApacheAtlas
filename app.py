#!/usr/bin/env python3
"""
Complete Database Discovery Script for Apache Atlas
Discovers and creates entities in proper hierarchy:
1. Database (already exists)
2. Schemas → 3. Tables → 4. Columns

This script handles the complete database structure discovery and Atlas entity creation.
"""

import requests
import json
import pyodbc
from requests.auth import HTTPBasicAuth
import time
from collections import defaultdict

# Configuration
ATLAS_URL = "http://localhost:21000"
ATLAS_USERNAME = "admin"
ATLAS_PASSWORD = "admin"

SQL_HOST = "localhost"
SQL_PORT = "1433"
SQL_DATABASE = "DataQuality"
SQL_USERNAME = "nawaaf"
SQL_PASSWORD = "26102610nnN@"

class CompleteDBDiscovery:
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
        self.db_qualified_name = f"sqlserver://{SQL_HOST}:{SQL_PORT}/{SQL_DATABASE}.{SQL_DATABASE}@cluster1"
        
        # Storage for created entities
        self.created_schemas = {}
        self.created_tables = {}
        self.discovery_stats = {
            'schemas': 0,
            'tables': 0,
            'columns': 0,
            'errors': 0
        }
    
    def test_connections(self):
        """Test both SQL Server and Atlas connections"""
        
        print("🔌 Testing Connections...")
        print("-" * 40)
        
        # Test SQL Server
        try:
            print("🗄️  Testing SQL Server connection...")
            conn = pyodbc.connect(self.sql_conn_str)
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION, DB_NAME()")
            version, db_name = cursor.fetchone()
            print(f"✅ Connected to SQL Server")
            print(f"   Database: {db_name}")
            print(f"   Version: {version[:50]}...")
            conn.close()
            sql_ok = True
        except Exception as e:
            print(f"❌ SQL Server connection failed: {str(e)}")
            sql_ok = False
        
        # Test Atlas
        try:
            print("🌐 Testing Atlas connection...")
            url = f"{self.atlas_base_url}/api/atlas/v2/search/basic?query=DataQuality&typeName=rdbms_db"
            response = requests.get(url, headers=self.atlas_headers, auth=self.atlas_auth)
            
            if response.status_code == 200:
                result = response.json()
                if result.get('entities'):
                    print(f"✅ Connected to Atlas")
                    print(f"   Found {len(result['entities'])} DataQuality database(s)")
                    atlas_ok = True
                else:
                    print("⚠️  Atlas connected but no DataQuality database found")
                    atlas_ok = False
            else:
                print(f"❌ Atlas connection failed: {response.status_code}")
                atlas_ok = False
        except Exception as e:
            print(f"❌ Atlas connection failed: {str(e)}")
            atlas_ok = False
        
        return sql_ok and atlas_ok
    
    def discover_all_schemas(self):
        """Step 1: Discover all schemas in the database"""
        
        print("\n📁 STEP 1: Discovering Schemas...")
        print("-" * 50)
        
        try:
            conn = pyodbc.connect(self.sql_conn_str)
            cursor = conn.cursor()
            
            # Query to get all schemas
            schema_query = """
            SELECT 
                SCHEMA_NAME,
                SCHEMA_OWNER
            FROM INFORMATION_SCHEMA.SCHEMATA
            WHERE SCHEMA_NAME NOT IN ('information_schema', 'sys', 'db_owner', 'db_accessadmin', 
                                     'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 
                                     'db_datareader', 'db_datawriter', 'db_denydatareader', 
                                     'db_denydatawriter')
            ORDER BY SCHEMA_NAME
            """
            
            cursor.execute(schema_query)
            schemas = cursor.fetchall()
            
            schema_list = []
            for row in schemas:
                schema_info = {
                    'name': row[0],
                    'owner': row[1]
                }
                schema_list.append(schema_info)
            
            conn.close()
            
            print(f"✅ Found {len(schema_list)} schemas:")
            for schema in schema_list:
                print(f"   📁 {schema['name']} (owner: {schema['owner']})")
            
            return schema_list
            
        except Exception as e:
            print(f"❌ Error discovering schemas: {str(e)}")
            return []
    
    def discover_all_tables(self):
        """Step 2: Discover all tables grouped by schema"""
        
        print("\n📊 STEP 2: Discovering Tables...")
        print("-" * 50)
        
        try:
            conn = pyodbc.connect(self.sql_conn_str)
            cursor = conn.cursor()
            
            # Query to get all tables with schema information
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
            
            # Group tables by schema
            tables_by_schema = defaultdict(list)
            total_tables = 0
            
            for row in tables:
                table_info = {
                    'schema': row[0],
                    'name': row[1],
                    'type': row[2],
                    'column_count': row[3]
                }
                tables_by_schema[row[0]].append(table_info)
                total_tables += 1
            
            conn.close()
            
            print(f"✅ Found {total_tables} tables across {len(tables_by_schema)} schemas:")
            for schema_name, schema_tables in tables_by_schema.items():
                print(f"   📁 {schema_name}: {len(schema_tables)} tables")
                for table in schema_tables:
                    print(f"      📊 {table['name']} ({table['column_count']} columns)")
            
            return tables_by_schema
            
        except Exception as e:
            print(f"❌ Error discovering tables: {str(e)}")
            return {}
    
    def discover_columns_for_table(self, schema_name, table_name):
        """Step 3: Discover columns for a specific table"""
        
        try:
            conn = pyodbc.connect(self.sql_conn_str)
            cursor = conn.cursor()
            
            column_query = """
            SELECT 
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
            WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
            ORDER BY c.ORDINAL_POSITION
            """
            
            cursor.execute(column_query, schema_name, table_name)
            columns = cursor.fetchall()
            
            column_list = []
            for row in columns:
                column_info = {
                    'name': row[0],
                    'data_type': row[1],
                    'max_length': row[2] if row[2] else 0,
                    'precision': row[3] if row[3] else 0,
                    'scale': row[4] if row[4] else 0,
                    'is_nullable': row[5] == 'YES',
                    'default_value': row[6],
                    'ordinal_position': row[7],
                    'is_primary_key': row[8] == 'YES'
                }
                column_list.append(column_info)
            
            conn.close()
            return column_list
            
        except Exception as e:
            print(f"❌ Error getting columns for {schema_name}.{table_name}: {str(e)}")
            return []
    
    def create_schema_entity(self, schema_info):
        """Create schema entity in Atlas"""
        
        schema_name = schema_info['name']
        schema_qualified_name = f"sqlserver://{SQL_HOST}:{SQL_PORT}/{SQL_DATABASE}.{SQL_DATABASE}.{schema_name}@cluster1"
        
        schema_entity = {
            "entity": {
                "typeName": "rdbms_schema",
                "attributes": {
                    "qualifiedName": schema_qualified_name,
                    "name": schema_name,
                    "description": f"Schema {schema_name} in {SQL_DATABASE} database",
                    "owner": schema_info.get('owner', SQL_USERNAME),
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
            response = requests.post(url, data=json.dumps(schema_entity), 
                                   headers=self.atlas_headers, auth=self.atlas_auth)
            
            if response.status_code in [200, 201]:
                result = response.json()
                guid = result.get('entity', {}).get('guid')
                self.created_schemas[schema_name] = guid
                print(f"   ✅ Created schema: {schema_name}")
                self.discovery_stats['schemas'] += 1
                return guid
            else:
                print(f"   ❌ Failed to create schema {schema_name}: {response.text}")
                self.discovery_stats['errors'] += 1
                return None
                
        except Exception as e:
            print(f"   ❌ Error creating schema {schema_name}: {str(e)}")
            self.discovery_stats['errors'] += 1
            return None
    
    def create_table_entity(self, table_info):
        """Create table entity in Atlas"""
        
        schema_name = table_info['schema']
        table_name = table_info['name']
        
        table_qualified_name = f"sqlserver://{SQL_HOST}:{SQL_PORT}/{SQL_DATABASE}.{SQL_DATABASE}.{schema_name}.{table_name}@cluster1"
        schema_qualified_name = f"sqlserver://{SQL_HOST}:{SQL_PORT}/{SQL_DATABASE}.{SQL_DATABASE}.{schema_name}@cluster1"
        
        # Try with schema reference first
        table_entity = {
            "entity": {
                "typeName": "rdbms_table",
                "attributes": {
                    "qualifiedName": table_qualified_name,
                    "name": table_name,
                    "description": f"Table {table_name} in {schema_name} schema ({table_info['column_count']} columns)",
                    "owner": SQL_USERNAME,
                    "db": {
                        "typeName": "rdbms_db",
                        "uniqueAttributes": {
                            "qualifiedName": self.db_qualified_name
                        }
                    },
                    "schema": {
                        "typeName": "rdbms_schema",
                        "uniqueAttributes": {
                            "qualifiedName": schema_qualified_name
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
                table_key = f"{schema_name}.{table_name}"
                self.created_tables[table_key] = guid
                print(f"      ✅ Created table: {schema_name}.{table_name}")
                self.discovery_stats['tables'] += 1
                return guid
            else:
                # Try without schema reference if it fails
                print(f"      ⚠️  Retrying without schema reference for {schema_name}.{table_name}")
                return self.create_table_entity_fallback(table_info)
                
        except Exception as e:
            print(f"      ❌ Error creating table {schema_name}.{table_name}: {str(e)}")
            self.discovery_stats['errors'] += 1
            return None
    
    def create_table_entity_fallback(self, table_info):
        """Create table entity without schema reference (fallback)"""
        
        schema_name = table_info['schema']
        table_name = table_info['name']
        
        table_qualified_name = f"sqlserver://{SQL_HOST}:{SQL_PORT}/{SQL_DATABASE}.{SQL_DATABASE}.{schema_name}.{table_name}@cluster1"
        
        table_entity = {
            "entity": {
                "typeName": "rdbms_table",
                "attributes": {
                    "qualifiedName": table_qualified_name,
                    "name": table_name,
                    "description": f"Table {table_name} in {schema_name} schema ({table_info['column_count']} columns)",
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
                table_key = f"{schema_name}.{table_name}"
                self.created_tables[table_key] = guid
                print(f"      ✅ Created table (fallback): {schema_name}.{table_name}")
                self.discovery_stats['tables'] += 1
                return guid
            else:
                print(f"      ❌ Failed to create table {schema_name}.{table_name}: {response.text}")
                self.discovery_stats['errors'] += 1
                return None
                
        except Exception as e:
            print(f"      ❌ Error creating table {schema_name}.{table_name}: {str(e)}")
            self.discovery_stats['errors'] += 1
            return None
    
    def create_column_entities(self, schema_name, table_name, columns):
        """Create column entities for a table"""
        
        if not columns:
            print(f"         ⚠️  No columns found for {schema_name}.{table_name}")
            return
        
        table_qualified_name = f"sqlserver://{SQL_HOST}:{SQL_PORT}/{SQL_DATABASE}.{SQL_DATABASE}.{schema_name}.{table_name}@cluster1"
        
        column_entities = []
        for col in columns:
            column_qualified_name = f"{table_qualified_name}.{col['name']}@cluster1"
            
            # Build data type string
            data_type_str = col['data_type']
            if col['max_length'] and col['max_length'] > 0:
                data_type_str += f"({col['max_length']})"
            elif col['precision'] and col['precision'] > 0:
                if col['scale'] and col['scale'] > 0:
                    data_type_str += f"({col['precision']},{col['scale']})"
                else:
                    data_type_str += f"({col['precision']})"
            
            column_entity = {
                "typeName": "rdbms_column",
                "attributes": {
                    "qualifiedName": column_qualified_name,
                    "name": col['name'],
                    "description": f"Column {col['name']} of type {data_type_str} in {schema_name}.{table_name}",
                    "owner": SQL_USERNAME,
                    "data_type": data_type_str,
                    "length": col['max_length'],
                    "precision": col['precision'],
                    "scale": col['scale'],
                    "is_nullable": col['is_nullable'],
                    "is_primary_key": col['is_primary_key'],
                    "default_value": col['default_value'],
                    "ordinal_position": col['ordinal_position'],
                    "table": {
                        "typeName": "rdbms_table",
                        "uniqueAttributes": {
                            "qualifiedName": table_qualified_name
                        }
                    }
                },
                "status": "ACTIVE"
            }
            column_entities.append(column_entity)
        
        # Create columns in bulk
        bulk_entity = {"entities": column_entities}
        url = f"{self.atlas_base_url}/api/atlas/v2/entity/bulk"
        
        try:
            response = requests.post(url, data=json.dumps(bulk_entity), 
                                   headers=self.atlas_headers, auth=self.atlas_auth)
            
            if response.status_code in [200, 201]:
                print(f"         ✅ Created {len(column_entities)} columns for {schema_name}.{table_name}")
                self.discovery_stats['columns'] += len(column_entities)
            else:
                print(f"         ❌ Failed to create columns for {schema_name}.{table_name}: {response.text}")
                self.discovery_stats['errors'] += 1
                
        except Exception as e:
            print(f"         ❌ Error creating columns for {schema_name}.{table_name}: {str(e)}")
            self.discovery_stats['errors'] += 1
    
    def run_complete_discovery(self):
        """Run the complete discovery process: Schemas → Tables → Columns"""
        
        print("🚀 COMPLETE DATABASE DISCOVERY")
        print("=" * 60)
        print(f"📊 Database: {SQL_DATABASE}")
        print(f"🖥️  SQL Server: {SQL_HOST}:{SQL_PORT}")
        print(f"👤 User: {SQL_USERNAME}")
        print(f"🌐 Atlas: {ATLAS_URL}")
        print("=" * 60)
        
        # Test connections
        if not self.test_connections():
            print("❌ Connection test failed. Please check your configuration.")
            return False
        
        start_time = time.time()
        
        # Step 1: Discover and create schemas
        print("\n🔄 Starting complete discovery process...")
        schemas = self.discover_all_schemas()
        
        if not schemas:
            print("❌ No schemas found. Stopping discovery.")
            return False
        
        print(f"\n📁 Creating {len(schemas)} schema entities...")
        for schema in schemas:
            self.create_schema_entity(schema)
        
        if True:
            return '1'
        # Small delay to ensure schema entities are created
        print("   ⏳ Waiting for schema entities to be processed...")
        time.sleep(2)
        
        # Step 2: Discover and create tables
        tables_by_schema = self.discover_all_tables()
        
        if not tables_by_schema:
            print("❌ No tables found. Stopping discovery.")
            return False
        
        print(f"\n📊 Creating table entities...")
        for schema_name, schema_tables in tables_by_schema.items():
            print(f"   📁 Processing schema: {schema_name}")
            for table in schema_tables:
                table_guid = self.create_table_entity(table)
                
                # Step 3: Create columns for this table
                if table_guid:
                    print(f"         🔍 Discovering columns for {schema_name}.{table['name']}...")
                    columns = self.discover_columns_for_table(schema_name, table['name'])
                    if columns:
                        self.create_column_entities(schema_name, table['name'], columns)
                
                # Small delay to avoid overwhelming Atlas
                time.sleep(0.1)
        
        # Summary
        end_time = time.time()
        duration = end_time - start_time
        
        print("\n" + "=" * 60)
        print("🎉 DISCOVERY COMPLETED!")
        print("=" * 60)
        print(f"📊 STATISTICS:")
        print(f"   ✅ Schemas created: {self.discovery_stats['schemas']}")
        print(f"   ✅ Tables created: {self.discovery_stats['tables']}")
        print(f"   ✅ Columns created: {self.discovery_stats['columns']}")
        print(f"   ❌ Errors encountered: {self.discovery_stats['errors']}")
        print(f"   ⏱️  Total time: {duration:.2f} seconds")
        print(f"\n🌐 View your entities at: {ATLAS_URL}")
        print(f"🔍 Search for 'DataQuality' to see all your entities")
        
        return True
    
    def list_created_entities(self):
        """List all entities created during discovery"""
        
        print("\n🔍 ENTITY SUMMARY")
        print("=" * 40)
        
        entity_types = ['rdbms_schema', 'rdbms_table', 'rdbms_column']
        
        for entity_type in entity_types:
            url = f"{self.atlas_base_url}/api/atlas/v2/search/basic"
            params = {"query": "DataQuality", "typeName": entity_type, "limit": 100}
            
            try:
                response = requests.get(url, params=params, headers=self.atlas_headers, auth=self.atlas_auth)
                if response.status_code == 200:
                    result = response.json()
                    entities = result.get('entities', [])
                    
                    if entities:
                        print(f"\n📊 {entity_type.upper().replace('_', ' ')} ({len(entities)}):")
                        for entity in entities[:10]:  # Show first 10
                            print(f"   ✓ {entity.get('displayText')}")
                        if len(entities) > 10:
                            print(f"   ... and {len(entities) - 10} more")
                
            except Exception as e:
                print(f"❌ Error searching {entity_type}: {str(e)}")

def main():
    """Main function"""
    
    discovery = CompleteDBDiscovery()
    
    while True:
        print("\n" + "=" * 60)
        print("🎯 COMPLETE DATABASE DISCOVERY TOOL")
        print("=" * 60)
        print("🔧 Available Operations:")
        print("1. 🚀 Run COMPLETE discovery (Schemas → Tables → Columns)")
        print("2. 🔍 List existing DataQuality entities")
        print("3. 🔌 Test connections only")
        print("4. 📊 Show discovery statistics")
        print("5. ❌ Exit")
        
        choice = input("\nEnter your choice (1-5): ").strip()
        
        if choice == '1':
            confirm = input("\n⚠️  This will discover and create ALL database entities. Continue? (y/N): ")
            if confirm.lower() == 'y':
                discovery.run_complete_discovery()
            else:
                print("❌ Operation cancelled")
                
        elif choice == '2':
            discovery.list_created_entities()
            
        elif choice == '3':
            discovery.test_connections()
            
        elif choice == '4':
            print("\n📊 Discovery Statistics:")
            for key, value in discovery.discovery_stats.items():
                print(f"   {key.capitalize()}: {value}")
            
        elif choice == '5':
            print("👋 Goodbye!")
            break
            
        else:
            print("❌ Invalid choice. Please enter 1-5.")

if __name__ == "__main__":
    main()