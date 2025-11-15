import json
import oracledb
import pandas as pd
import cx_Oracle
import requests
import sqlalchemy
import urllib
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import pyodbc
import os
import socket
import pymysql
import psycopg2
import pymssql
import sys
import re
from urllib.parse import quote_plus
currenttime = datetime.today()

dbname=os.getenv('DB_NAME')
dbpass=os.getenv('DB_PASSWORD')
dbuser=os.getenv('DB_USERNAME')
dbip=os.getenv('DB_IP')
dbport=os.getenv('DB_PORT')
schemaip=os.getenv('SCEHMA_IP')
schemaport=os.getenv('SCEHMA_PORT')

print('in connection class ',dbname,dbpass,dbuser,dbip,dbport,schemaip,schemaport , sys.version)

class Connection:


    def __init__(self):
        # self.tableID=Metadata(tableID)
        self.option=None
        self.connection=None

    def __init__(self,IP=None,port=None,databaseName=None,password=None,description=None,userName=None,TNSNAME=None,databaseType=None,connectionType=None,oracleDBType=None,API=None,option=None,MetaData_UPLOAD_DIR=None,odbcName=None):
        # self.tableID=Metadata(tableID)
        self.option=option
        self.connection=None
        self.IP=IP
        self.port=port
        self.databaseName=databaseName
        self.password=password
        self.description=description
        self.userName=userName
        self.TNSNAME=TNSNAME
        self.databaseType=databaseType
        self.connectionType=connectionType
        self.oracleDBType=oracleDBType
        self.API=API
        self.MetaData_UPLOAD_DIR=MetaData_UPLOAD_DIR
        self.odbcName=odbcName

    
    

    def connect(self ):
        if(self.option=='Destination'):
            params = urllib.parse.quote_plus(
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={dbip};"
                f"DATABASE={dbname};"
                f"UID={dbuser};"
                f"PWD={dbpass};"
            )
                    

            self.connection = sqlalchemy.create_engine(
                f"mssql+pyodbc:///?odbc_connect={params}",
                fast_executemany=True,
                connect_args={'connect_timeout': 10},
                pool_pre_ping=True  # Optional: test connections before using them
            ) 
        elif(self.option=='ndb2'):
            # temp=connect(host = "localhost", port = 21050)
            self.connection=pyodbc.connect(DSN=self.odbcName, autocommit=True, encoding='utf-8')
        elif(self.option=='40'):
            params = urllib.parse.quote_plus("DRIVER={ODBC+Driver+17+for+SQL+Server};"
                                 "SERVER="+'10.9.8.40'+","+'2995'+";"
                                 "DATABASE="+'NICDATA'+";"
                                 "UID="+'sa'+";"
                                 "PWD="+'Nic@estishraf2030')
        

            

            self.connection=sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params),
            execution_options={
                "isolation_level": "AUTOCOMMIT"
            },encoding="utf8",fast_executemany = True,connect_args={'connect_timeout': 10}) 
        else:
            if(self.databaseType=='SQL'):
                msg=0

                if(self.databaseName.strip()==''):
                    msg='You have to provide database name!!'
                    return msg
                elif(';' in self.databaseName):
                    msg='Is it valid that database name contains '';'' ?'
                    return msg

                try:
                    serverAndport=f"{self.IP}','{self.port};ApplicationIntent=ReadOnly"
                    self.connection = pymssql.connect(server=self.IP, port=self.port, user=self.userName,
                                                      password=self.password, database=self.databaseName)
                    
                
        
            
            
                except Exception as e:
                    msg=str(e)
                
                return msg
            elif(self.databaseType=='ORCL'):
                msg=0
                print(self.option
                ,self.IP
                ,self.port
                ,self.databaseName
                ,self.password
                ,self.description
                ,self.userName
                ,self.TNSNAME
                ,self.databaseType
                ,self.connectionType
                ,self.connectionType=='2'
                ,self.oracleDBType
                ,self.MetaData_UPLOAD_DIR)

                try:
                    if(self.connectionType=='2'):
                        self.connection = cx_Oracle.connect(self.userName,str(self.password) , self.TNSNAME, encoding="UTF-8")
                    else:
                        self.connection = oracledb.connect(
                        user=self.userName,
                        password=str(self.password) ,
                        dsn=f"""{self.IP}:{self.port}/{self.databaseName}"""
                        )
                        # self.

            
                except Exception as e:
                    msg=str(e)
                
                return msg
            elif(self.databaseType=='DENODO'):
                msg=0 
                print(self.option
                ,self.IP
                ,self.port
                ,self.databaseName
                ,self.password
                ,self.description
                ,self.userName
                ,self.TNSNAME
                ,self.databaseType
                ,self.connectionType
                ,self.connectionType=='2'
                ,self.oracleDBType
                ,self.MetaData_UPLOAD_DIR) 

                
                try:
              
                    self.connection = psycopg2.connect("user=%s password=%s host=%s dbname=%s port=%s" %\
                        (self.userName, self.password, self.IP, self.databaseName, self.port))
                     
                except Exception as e:
                    msg=str(e)      
                
                return msg
            elif(self.databaseType=='POSTGRESQL'):
                msg=0 
                print(self.option
                ,self.IP
                ,self.port
                ,self.databaseName
                ,self.password
                ,self.description
                ,self.userName
                ,self.TNSNAME
                ,self.databaseType
                ,self.connectionType
                ,self.connectionType=='2'
                ,self.oracleDBType
                ,self.MetaData_UPLOAD_DIR) 
                try:

                    serverAndport=self.IP+':'+self.port

                    self.connection=sqlalchemy.create_engine("postgresql+psycopg2://"""+self.userName+""":"""+self.password+"""@"""+serverAndport+"""/"""+self.databaseName,
                    execution_options={
                        "isolation_level": "AUTOCOMMIT"
                    },encoding="utf8")
        
                except Exception as e:
                    msg=str(e)
                
                return msg
            elif(self.databaseType=='MySQL'):
                print(self.option
                ,self.IP
                ,self.port
                ,self.databaseName
                ,self.password
                ,self.description
                ,self.userName
                ,self.TNSNAME
                ,self.databaseType
                ,self.connectionType
                ,self.connectionType=='2'
                ,self.oracleDBType
                ,self.MetaData_UPLOAD_DIR)
                msg=0

                try:
                    serverAndport=self.IP+':'+self.port
        
                    encoded_password = quote_plus(self.password)

                    self.connection=sqlalchemy.create_engine("""mysql+pymysql://"""+self.userName+""":"""+encoded_password+"""@"""+serverAndport+"""/"""+self.databaseName+"""?charset=utf8""",
                    execution_options={
                        "isolation_level": "AUTOCOMMIT"
                    },encoding="utf8")
                    

  
            
            
                except Exception as e:
                    msg=str(e)
                
                return msg

            elif(self.databaseType=='Impala'):
                
                msg=0

                try:
                    # serverAndport=self.IP+':'+self.port
                    # params = urllib.parse.quote_plus("DRIVER={MySQL ODBC 8.0 Unicode Driver};"
                    #                 "SERVER="+serverAndport+";"
                    #                 "DATABASE="+self.databaseName+";"
                    #                 "UID="+self.userName+";"
                    #                 "PWD="+self.password)
        

                    # self.connection = sqlalchemy.create_engine("apacheimpala:///?Server=127.0.0.1&;Port=21050")
                    self.connection=pyodbc.connect(DSN=self.odbcName, autocommit=True, encoding='utf-8')
                    # self.connection=sqlalchemy.create_engine("impala:///?Server=127.0.0.1&;Port=21050",
                    # execution_options={
                    #     "isolation_level": "AUTOCOMMIT"
                    # },encoding="utf8")
                    


            
            
                except Exception as e:
                    msg=str(e)
                
                return msg
            else:
                msg='Invalid Connection Type!'  
                return msg                
         

    def getConnection(self):
        return self.connection
    
    
    
    def testConnection(self):
        if(self.databaseType=='SQL' or self.databaseType=='MySQL' or  self.databaseType=='POSTGRESQL' or self.databaseType=='DENODO'):
            msg=0
            try:
                df=pd.read_sql('select 1 as a ',self.connection )
                      
            except Exception as e:
                msg=str(e)
            return msg
        elif(self.databaseType=='ORCL'):
            msg=0
            try:
                df=pd.read_sql('''select 1 AS "a" from dual''',self.connection)
            
            except Exception as e:
                msg=str(e)
            return msg
        elif(self.databaseType=='Impala'):
            msg=0
            return msg
        else:
            msg='Invalid Database Type!'
            return msg
    

    def close(self):
        self.connection.close()    

    def telnet(self):
        msg=''
        if(self.connectionType=='2' or self.connectionType=='3' ): # TNS or API
            msg=0
        else:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                msg = sock.connect_ex((self.IP,int(self.port)))

                
            except Exception as e:
                msg = str(e)

            sock.close()
        return msg

    

    def getAllConnections(self):
        query="""
        
  
        
        EXEC	[dbo].[PR_Get_Connection]
        @EncryptionKey	= 'P@ssw0rd@@ndb??123DqteamIsThatComplex'

        """
        a=pd.read_sql(query,self.connection)
        a=a.astype(str)

        return a

    def retriveAConnection(self,Connection_ID):

        query=f"""
        select * from {os.getenv('CONNECTION_TABLE')} where ID = {Connection_ID}

        """
        a=pd.read_sql(query,self.connection)
        

        return a        

 
    
   

    def getMetadataDF(self):
        if(self.databaseType=='SQL'):
            msg=0
            try:
                query='''   SELECT
    OBJECT_ID(cols.TABLE_SCHEMA + '.' + cols.TABLE_NAME) AS TABLE_OBJECT_ID,
    COLUMNPROPERTY(OBJECT_ID(cols.TABLE_SCHEMA + '.' + cols.TABLE_NAME), cols.COLUMN_NAME, 'ColumnId') AS COLUMN_ID,
    TABLE_SCHEMA,
    TABLE_NAME,
    COLUMN_NAME ATTRIBUTE_NAME,
    SCHEMA_NAME(objs.schema_id) BASE_SCHEMA,
    OBJECT_NAME(dpnds.referenced_major_id) BASE_TABLE,
    real_names.name BASE_COLUMN,
    CASE
        WHEN cols.DATA_TYPE IN ('bigint', 'int', 'smallint', 'tinyint', 'bit', 'ntext', 'money', 'smallmoney', 'date', 'datetime', 'smalldatetime', 'uniqueidentifier') THEN DATA_TYPE
        WHEN cols.DATA_TYPE IN ('numeric', 'decimal') THEN DATA_TYPE + ' (' + CONVERT(varchar(50), NUMERIC_PRECISION) + ', ' + CONVERT(varchar(50), NUMERIC_SCALE) + ')'
        WHEN cols.DATA_TYPE IN ('float') THEN DATA_TYPE + ' (' + CONVERT(varchar(50), NUMERIC_PRECISION) + ')'
        WHEN cols.DATA_TYPE IN ('datetime2', 'datetimeoffset', 'time') THEN DATA_TYPE + ' (' + CONVERT(varchar(50), DATETIME_PRECISION) + ')'
        WHEN cols.DATA_TYPE IN ('char', 'varchar', 'text', 'nchar', 'nvarchar', 'binary', 'varbinary', 'image') AND CHARACTER_MAXIMUM_LENGTH != -1 THEN DATA_TYPE + ' (' + CONVERT(varchar(50), CHARACTER_MAXIMUM_LENGTH) + ')'
        WHEN cols.DATA_TYPE IN ('char', 'varchar', 'text', 'nchar', 'nvarchar', 'binary', 'varbinary', 'image') AND CHARACTER_MAXIMUM_LENGTH = -1 THEN DATA_TYPE + ' (max)'
    ELSE cols.DATA_TYPE
    END AS DATA_TYPE,
    -- Check to see if the attribute is in the list of KEYs of this table (Could be composite key)
    IIF(cols.COLUMN_NAME IN 
    (
        SELECT k.column_name
        FROM information_schema.table_constraints t
            JOIN information_schema.key_column_usage k
            ON t.constraint_name = k.constraint_name
        WHERE t.constraint_type='PRIMARY KEY'
            AND t.table_schema=cols.TABLE_SCHEMA
            AND t.table_name=cols.TABLE_NAME
    ), 'Yes', 'No') IS_PK,
    -- FK attributes will depend on the subquery join
    IIF(FK_INFO.FK_SCHEMA IS NOT NULL, 'Yes', 'No') IS_FK,
    IIF(FK_INFO.FK_SCHEMA IS NOT NULL, FK_INFO.Referenced_Schema, '') REFERENCED_SCHEMA,
    IIF(FK_INFO.FK_TABLE IS NOT NULL, FK_INFO.Referenced_Table, '') REFERENCED_TABLE,
    IIF(FK_INFO.FK_COLUMN IS NOT NULL, FK_INFO.Referenced_Column, '') REFERENCED_COLUMN,
    CASE WHEN real_names.name IS NOT NULL AND vws.type_desc IS NOT NULL THEN 'Yes' ELSE 'No' END AS IS_VIEW
FROM INFORMATION_SCHEMA.COLUMNS cols
    -- The following join to get the FK reference
    LEFT JOIN (
        SELECT
            KF.TABLE_SCHEMA FK_SCHEMA,
            KF.TABLE_NAME FK_TABLE,
            KF.COLUMN_NAME FK_COLUMN,
            KP.TABLE_SCHEMA Referenced_Schema,
            KP.TABLE_NAME Referenced_Table,
            KP.COLUMN_NAME Referenced_Column
        FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KF ON RC.CONSTRAINT_NAME = KF.CONSTRAINT_NAME
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KP ON RC.UNIQUE_CONSTRAINT_NAME = KP.CONSTRAINT_NAME
    ) AS FK_INFO
    ON  cols.TABLE_SCHEMA = FK_INFO.FK_SCHEMA 
    AND cols.TABLE_NAME = FK_INFO.FK_TABLE 
    AND cols.COLUMN_NAME = FK_INFO.FK_COLUMN
    -- The following joins to get the view base
    LEFT JOIN sys.views vws ON OBJECT_ID(cols.TABLE_SCHEMA + '.' + cols.TABLE_NAME) = vws.object_id
    LEFT JOIN sys.sql_dependencies dpnds ON OBJECT_ID(cols.TABLE_SCHEMA + '.' + cols.TABLE_NAME) = dpnds.object_id AND cols.ORDINAL_POSITION = dpnds.referenced_minor_id
    LEFT JOIN sys.columns real_names ON dpnds.referenced_major_id = real_names.object_id AND dpnds.referenced_minor_id = real_names.column_id
    LEFT JOIN sys.objects objs ON dpnds.referenced_major_id = objs.object_id
WHERE
    TABLE_SCHEMA NOT IN ('db_accessadmin', 'db_backupoperator', 'db_datareader', 'db_datawriter', 'db_ddladmin', 'db_denydatareader', 'db_denydatawriter', 'db_owner',
                        'db_securityadmin', 'db_ssisadmin', 'db_ssisltduser', 'db_ssisoperator', 'guest', 'managed_backup', 'smart_admin', 'SQLAgentOperatorRole',        
                        'SQLAgentReaderRole', 'SQLAgentUserRole', 'TargetServersRole', 'sys', 'INFORMATION_SCHEMA')
                        AND TABLE_NAME in ('ossys_Tenant' , 'ossys_Tenant1') and TABLE_NAME+''+COLUMN_NAME !='ossys_TenantLastOperation'
                            '''
                # filename= self.description+"_"+self.databaseType +"_"+self.databaseName+"_"+currenttime.strftime('%Y%m%d%H%M%S')+".xlsx"
                df=pd.read_sql(query,self.connection)
                # df.to_excel(str( self.MetaData_UPLOAD_DIR / filename),sheet_name="sheet1",index=False)
                return df

            
            except Exception as e:
                msg=str(e)
            return msg
        elif(self.databaseType=='ORCL'):
            msg=0
            try:
                query=f'''
                       SELECT
            main.OWNER AS TABLE_SCHEMA,
            TABLE_NAME,
            COLUMN_NAME AS ATTRIBUTE_NAME,
            mview.DETAILOBJ_OWNER AS BASE_SCHEMA,
            mview.DETAILOBJ_NAME AS BASE_TABLE,
            mview.DETAILOBJ_COLUMN AS BASE_COLUMN,
            CASE
                WHEN DATA_TYPE = 'NUMBER' AND DATA_PRECISION IS NOT NULL AND DATA_SCALE IS NOT NULL THEN 'NUMBER (' || DATA_PRECISION || ', ' || DATA_SCALE || ')'
                WHEN DATA_TYPE = 'FLOAT' AND DATA_PRECISION IS NOT NULL THEN 'FLOAT ' || '(' || DATA_PRECISION || ')'
                WHEN DATA_TYPE IN ('LONG', 'DATE', 'BINARY_FLOAT', 'BINARY_DOUBLE', 'LONG RAW','ROWID', 'UROWID', 'CLOB', 'NCLOB', 'BLOB', 'BFILE') THEN DATA_TYPE
                WHEN DATA_TYPE IN ('VARCHAR', 'VARCHAR2', 'NVARCHAR2') AND CHAR_USED = 'B' THEN DATA_TYPE || ' (' || CHAR_LENGTH || ' BYTE)'
                WHEN DATA_TYPE IN ('VARCHAR','VARCHAR2', 'NVARCHAR2') AND CHAR_USED = 'C' THEN DATA_TYPE || ' (' || CHAR_LENGTH || ' CHAR)'
                WHEN DATA_TYPE IN ('NCHAR', 'CHAR') THEN DATA_TYPE || ' (' || CHAR_LENGTH || ')'
                WHEN DATA_TYPE LIKE 'TIMESTAMP%' THEN DATA_TYPE
                ELSE DATA_TYPE
            END AS DATA_TYPE,
            CASE (SELECT
                    ALL_CONSTRAINTS.CONSTRAINT_TYPE
                  FROM
                    ALL_CONSTRAINTS, ALL_CONS_COLUMNS
                  WHERE
                    UPPER(ALL_CONSTRAINTS.OWNER) = '{self.userName.upper()}'
                    and ALL_CONSTRAINTS.CONSTRAINT_TYPE = 'P'
                    and ALL_CONSTRAINTS.CONSTRAINT_NAME = ALL_CONS_COLUMNS.CONSTRAINT_NAME
                    and ALL_CONSTRAINTS.OWNER = ALL_CONS_COLUMNS.OWNER
                    and ALL_CONS_COLUMNS.TABLE_NAME = main.TABLE_NAME
                    and ALL_CONS_COLUMNS.COLUMN_NAME = main.COLUMN_NAME
                    AND ROWNUM <= 1)
            WHEN 'P' THEN 'Yes'
            ELSE 'No' END IS_PK,
            CASE (SELECT
                COUNT(c_pk.table_name) count
              FROM
                all_cons_columns a
              JOIN all_constraints c ON a.owner = c.owner
                AND a.constraint_name = c.constraint_name
              JOIN all_constraints c_pk ON c.r_owner = c_pk.owner
                AND c.r_constraint_name = c_pk.constraint_name
              WHERE
                UPPER(a.OWNER) = '{self.userName.upper()}'
                AND c.constraint_type = 'R'
                AND a.TABLE_NAME = COALESCE(mview.DETAILOBJ_NAME, main.TABLE_NAME)
                AND a.COLUMN_NAME = COALESCE(mview.DETAILOBJ_COLUMN, main.COLUMN_NAME))
            WHEN 0 THEN 'No'
            ELSE 'Yes' END IS_FK,
            COALESCE((SELECT
                c.R_OWNER
              FROM
                all_cons_columns a
              JOIN all_constraints c ON a.owner = c.owner
                AND a.constraint_name = c.constraint_name
              JOIN all_constraints c_pk ON c.r_owner = c_pk.owner
                AND c.r_constraint_name = c_pk.constraint_name
              WHERE
                UPPER(a.OWNER) = '{self.userName.upper()}'
                AND c.constraint_type = 'R'
                AND a.TABLE_NAME = COALESCE(mview.DETAILOBJ_NAME, main.TABLE_NAME)
                AND a.COLUMN_NAME = COALESCE(mview.DETAILOBJ_COLUMN, main.COLUMN_NAME)
                AND ROWNUM <= 1), ' ') REFERENCED_SCHEMA,
            COALESCE((SELECT
                c_pk.table_name r_table_name
              FROM
                all_cons_columns a
              JOIN all_constraints c ON a.owner = c.owner
                AND a.constraint_name = c.constraint_name
              JOIN all_constraints c_pk ON c.r_owner = c_pk.owner
                AND c.r_constraint_name = c_pk.constraint_name
              WHERE
                UPPER(a.OWNER) = '{self.userName.upper()}'
                AND c.constraint_type = 'R'
                AND a.TABLE_NAME = COALESCE(mview.DETAILOBJ_NAME, main.TABLE_NAME)
                AND a.COLUMN_NAME = COALESCE(mview.DETAILOBJ_COLUMN, main.COLUMN_NAME)
                AND ROWNUM <= 1), ' ') REFERENCED_TABLE,
            COALESCE((SELECT ucc_p.column_name
                        FROM
                            ALL_CONSTRAINTS uc_r
                        JOIN
                            ALL_CONS_COLUMNS ucc_r ON ucc_r.CONSTRAINT_NAME = uc_r.CONSTRAINT_NAME
                        JOIN
                            ALL_CONSTRAINTS uc_p ON uc_p.CONSTRAINT_NAME = uc_r.R_CONSTRAINT_NAME
                        JOIN
                            ALL_CONS_COLUMNS ucc_p ON ucc_p.CONSTRAINT_NAME = uc_p.CONSTRAINT_NAME AND ucc_p.POSITION = ucc_r.POSITION
                        WHERE
                        UPPER(uc_r.OWNER) = '{self.userName.upper()}'
                        AND uc_r.constraint_type = 'R'
                        AND uc_r.TABLE_NAME = COALESCE(mview.DETAILOBJ_NAME, main.TABLE_NAME)
                        AND ucc_r.COLUMN_NAME = COALESCE(mview.DETAILOBJ_COLUMN, main.COLUMN_NAME)
                        AND ROWNUM <= 1), ' ') REFERENCED_COLUMN,
            CASE
              WHEN objects.object_type IN ('VIEW') THEN 'Yes' ELSE 'No' END AS IS_VIEW
        FROM
            ALL_TAB_COLUMNS main
            LEFT JOIN
            SYS.ALL_MVIEW_KEYS mview ON main.OWNER = mview.OWNER AND main.TABLE_NAME = mview.MVIEW_NAME AND main.COLUMN_NAME = mview.CONTAINER_COLUMN
            LEFT JOIN 
            all_objects objects ON main.OWNER = objects.OWNER AND main.TABLE_NAME = objects.object_name
        WHERE
            UPPER(main.OWNER) = '{self.userName.upper()}' -- Replace with schema name
                    '''
                
                df=pd.read_sql(query,self.connection)
                print('Hey')
                print(df)
                return df
            
            except Exception as e:
                msg=str(e)
            return msg
        # elif(self.databaseType=='MySQL'):
        #     msg=0
        #     try:
        #         query='''
        #             SELECT
		# 					{file_ID} as Agency_Schema_Source_ID,
		# 					'test' as agencyName,
		# 					'dataset' as datasetName,
		# 					' ' as datasetDescription,
		# 					'SQL_SERVER' as databaseType,
		# 					'Weekly' as updateFrequency,
		# 					'1' as schemaVersion,
        #             COLUMNS.TABLE_SCHEMA                                                                                            AS 'schemaName',
        #             COLUMNS.TABLE_NAME                                                                                              AS 'tableName',
		# 			' '																												AS 'tableDescription',
		# 			0																												AS 'isLookup',
        #             COLUMNS.COLUMN_NAME                                                                                             AS 'columnName',
		# 			''																												AS 'columnDescription',
        #             COLUMNS.COLUMN_TYPE                                                                                             AS 'dataType',
        #             IF((COLUMNS.COLUMN_KEY = 'PRI'), 1, 0)																			AS 'isPrimaryKey',
		# 			0																												AS 'isLastOperation',
		# 			0																												AS 'isSyncTimestamp',
		# 			''																												AS 'timestampFormat',
		# 			''																												AS 'sampleValues1',
		# 			''																												AS 'sampleValues2',
		# 			''																												AS 'sampleValues3',
		# 			''																												AS 'sampleValues4',
		# 			''																												AS 'sampleValues5',
        #             IF((FORIEGN_KEYS.REFERENCED_TABLE_SCHEMA IS NOT NULL), FORIEGN_KEYS.REFERENCED_TABLE_SCHEMA, ' ')               AS 'referencedSchema',
        #             IF((FORIEGN_KEYS.REFERENCED_TABLE_NAME IS NOT NULL), FORIEGN_KEYS.REFERENCED_TABLE_NAME, ' ')                   AS 'referencedTable',
        #             IF((FORIEGN_KEYS.REFERENCED_COLUMN_NAME IS NOT NULL), FORIEGN_KEYS.REFERENCED_COLUMN_NAME, ' ')                 AS 'referencedColumn',
        #             0                                                                                                            AS 'isAdditional',
		# 			'NO' AS 'maskingValue',
		# 					1 as 'Inserted_By',
		# 					CURDATE() as 'Insert_Timestamp',
		# 					NULL as 'Updated_By',
		# 					NULL as 'Update_Timestamp'
        #             FROM
        #             INFORMATION_SCHEMA.COLUMNS COLUMNS
        #                 LEFT JOIN
        #             (SELECT
        #                 TABLE_CONSTRAINTS.TABLE_SCHEMA,
        #                 TABLE_CONSTRAINTS.TABLE_NAME,
        #                 COLUMN_USAGE.COLUMN_NAME,
        #                 COLUMN_USAGE.REFERENCED_TABLE_SCHEMA,
        #                 COLUMN_USAGE.REFERENCED_TABLE_NAME,
        #                 COLUMN_USAGE.REFERENCED_COLUMN_NAME
        #                 FROM
        #                     INFORMATION_SCHEMA.TABLE_CONSTRAINTS TABLE_CONSTRAINTS
        #                         INNER JOIN
        #                     INFORMATION_SCHEMA.KEY_COLUMN_USAGE COLUMN_USAGE
        #                         ON
        #                     TABLE_CONSTRAINTS.CONSTRAINT_NAME = COLUMN_USAGE.CONSTRAINT_NAME
        #                 WHERE
        #                     CONSTRAINT_TYPE='FOREIGN KEY') FORIEGN_KEYS
        #                 ON
        #                     FORIEGN_KEYS.TABLE_SCHEMA = COLUMNS.TABLE_SCHEMA AND
        #                     FORIEGN_KEYS.TABLE_NAME = COLUMNS.TABLE_NAME AND
        #                     FORIEGN_KEYS.COLUMN_NAME = COLUMNS.COLUMN_NAME
        #             WHERE
        #                 COLUMNS.TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
        #             ORDER BY
        #                 COLUMNS.TABLE_SCHEMA,
        #                 COLUMNS.TABLE_NAME,
        #                 COLUMNS.COLUMN_NAME;'''.format_map({"file_ID":file_ID})
        #         # filename= self.description+"_"+self.databaseType +"_"+self.databaseName+"_"+currenttime.strftime('%Y%m%d%H%M%S')+".xlsx"
        #         df=pd.read_sql(query,self.connection)
        #         # df.to_excel(str( self.MetaData_UPLOAD_DIR / filename),sheet_name="sheet1",index=False)
        #         return df

        #     except Exception as e:
        #         msg=str(e)
        #     return msg
        else:
            '2'

        return '1'
    
    
    
    
    
    def checkConnectionStatus(self,Connection_ID,con ):
        
        message='Connection Error:'
        # there is no connection added to dataset
        if Connection_ID is None:              
            return message+'Dataset has no connection, Please provide connection details'
        
        
        
        query=f'''  EXEC	[dbo].[PR_Get_Connection]
        @EncryptionKey	= 'P@ssw0rd@@ndb??123Dqteam'
        ,@CONNECTION_ID={Connection_ID}'''
        connection=pd.read_sql(query,con )
        sourceConnection=Connection(IP=connection['IP'][0],port=connection['port'][0],databaseName=connection['DB_Name'][0],password=connection['Password'][0],userName=connection['User_Name'][0],TNSNAME=connection['TNS_Name'][0],databaseType=connection['DB_Type'][0],connectionType=connection['Connection_Type'][0],oracleDBType=connection['Oracle_DB_Type'][0],API=connection['API'][0])
        isConnectionRefused=sourceConnection.telnet()
        
        print('telnet msg',isConnectionRefused)
        if(isConnectionRefused!=0):            
            return isConnectionRefused
        
        connect=sourceConnection.connect()
        print('connect msg',connect)

        if(connect!=0):
            
            if 'login failed' in connect.lower() or 'tcp provider: error code 0x68 (104)%' in connect.lower():
                    message ='login failed'
            elif 'tcp provider: error code 0x2746%' in connect.lower():
                message = message+'No Network'
            elif 'login timeout' in connect.lower():
                message =message+'Login timeout'
            elif 'permission was denied' in connect.lower():
                message =message+'no permission'
            else:
                message =message+'unknown issue'
                        
                                         
            return message
        
        
        if(connection['Connection_Type'][0] != '3'):
            testConnection=sourceConnection.testConnection()
            print('connect msg',testConnection)

            if(testConnection!=0):
                return message
            
        return '1'
    
    
    
    def single_quote(self,word):
        word=str(word).replace("'","''")
        return """'%s'""" % word
