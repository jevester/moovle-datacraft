# import requests
import mysql.connector
import pandas as pd
import numpy as np
from datetime import datetime
import yaml

from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential



class sqlConnection():
    def __init__(self, configSql, sqlUser, sqlPassword):
        self.config = configSql
        self.user = sqlUser
        self.password = sqlPassword
        self.connection = self.openConnection()
        self.cursorSingle = self.get_cursor()
        self.cursorMany = self.get_cursor(fast=True)

    def __str__(self):
        return f"SQLConnectionMySQL(server='{self.config['server']}', database='{self.config['database']}', user='{self.user}')"

    def openConnection(self):
        try:
            conn = mysql.connector.connect(
                host=self.config['server'],
                port=self.config.get('port', 3306),
                user=self.user,
                password=self.password,
                database=self.config['database'],
                auth_plugin='mysql_native_password'
            )
            return conn
        except Exception as e:
            print(f"Fout bij het openen van verbinding: {e}")
            raise

    def get_cursor(self, fast=False):
        cur = self.connection.cursor(buffered=True)
        return cur

    def execSingle(self, query, params=None):
        self.cursorSingle.execute(query, params)
        if query.strip().lower().startswith("select"):
            return self.cursorSingle.fetchall()
        else:
            self.connection.commit()
            return None

    def parseValues(self, df: pd.DataFrame):
        data = []
        for _, row in df.iterrows():
            row_data = []
            for col in df.columns:
                val = row[col]
                if pd.isna(val):
                    row_data.append(None)
                else:
                    row_data.append(val)
            data.append(tuple(row_data))
        return data

    def writeMany(self, df: pd.DataFrame, tableName, schemaName='dbo', merge=True, mergeKeys: list = None):
        if df.empty:
            print("Geen data om te schrijven")
            return

        df = df.where(pd.notna(df), None)
        df[df.select_dtypes(include="bool").columns] = df.select_dtypes(include="bool").astype(int)
        cols = df.columns.to_list()

        if merge:
            if not mergeKeys:
                raise Exception("mergeKeys is required when merge=True")

            placeholders = ", ".join(["%s"] * len(cols))
            updates = ", ".join([f"{col}=VALUES({col})" for col in cols if col not in mergeKeys])
            query = f"INSERT INTO `{schemaName}`.{tableName} ({', '.join(cols)}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates};"
            self.cursorMany.executemany(query, self.parseValues(df))
        else:
            placeholders = ", ".join(["%s"] * len(cols))
            self.execSingle(f"TRUNCATE TABLE `{schemaName}`.{tableName}")
            query = f"INSERT INTO `{schemaName}`.{tableName} ({', '.join(cols)}) VALUES ({placeholders})"
            self.cursorMany.executemany(query, self.parseValues(df))

        self.connection.commit()

        # Optioneel: log de sync
        self.execSingle(f"""
            INSERT INTO `{schemaName}`.syncLog (tableName, date, records, insertedTimestamp)
            VALUES (%s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE records=VALUES(records)
        """, (tableName, datetime.now().date(), len(df)))

    def close(self):
        if self.cursorSingle:
            self.cursorSingle.close()
            self.cursorSingle = None
        if self.cursorMany:
            self.cursorMany.close()
            self.cursorMany = None
        if self.connection:
            self.connection.close()
            self.connection = None

    
    # def checkExistingTable(self, schemaName, tableName):
    #     return self.execSingle(f"""
    #         SELECT TABLE_NAME 
    #         FROM INFORMATION_SCHEMA.TABLES 
    #         WHERE TABLE_SCHEMA = '{schemaName}'
    #         AND TABLE_NAME = '{tableName}'
    #     """)

    def checkExistingDatabase(self, schemaName):
        return self.execSingle(f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{schemaName}' AND TABLE_TYPE = 'BASE TABLE'") if True else None

    def constructDatabase(self, configDatabase, schemaName):
        existingTables = self.checkExistingDatabase(schemaName)
        if existingTables:
            print("Database bestaat al")
            existingTables = [r[0] for r in existingTables]
        else:
            print("Database bestaat nog niet. Maak aan...")
            querySchema = \
                f"CREATE DATABASE IF NOT EXISTS {schemaName}"
            self.execSingle(querySchema)
            existingTables = []

        monitor = configDatabase['tables']['monitor']
        fact = configDatabase['tables']['fact']
        meta = configDatabase['tables']['meta']

        tables = (monitor or []) + (meta or []) + (fact or [])
        tables = [t for t in tables if t['name'] not in existingTables]

        for table in tables:
            constraints = []
            # if table.get('constraints'):
            #     for constraint in table['constraints']:
            #         constraints.append(f"CONSTRAINT {table['name']}_{constraint['name']} {constraint['type']} {'CLUSTERED' if constraint['clustered'] else ''} ({', '.join(constraint['columns'])})")


            self.execSingle(
                f"""CREATE TABLE `{schemaName}`.{table['name']}"""
                f"""({table['columns'].replace('$NAME', table['name'])}{(','+','.join(constraints)) if len(constraints) > 0 else ''})"""
                )
            
            if table.get('indexes'):
                for index in table['indexes']:
                    self.execSingle(f"CREATE INDEX {table['name']}_{index['name']} ON `{schemaName}`.{table['name']} ({', '.join(index['columns'])})")

    # def constructDatabase(self, configDatabase, dbName):



    #     # Loop over alle categorieën
    #     for category in ['monitor', 'fact', 'meta']:
    #         for table in configDatabase['tables'].get(category, []):
    #             tableName = table['name']

    #             existing = self.checkExistingTable(dbName, tableName)
    #             if existing:
    #                 print(f"Tabel {tableName} bestaat al")
    #                 continue

    #             print(f"Aanmaken van tabel {tableName}")

    #             # Converteer kolommen naar MySQL syntax
    #             columns_sql = table['columns']
    #             # columns_sql = columns_sql.replace('[', '').replace(']', '')
    #             # columns_sql = columns_sql.replace('IDENTITY(1,1)', 'AUTO_INCREMENT')
    #             # columns_sql = columns_sql.replace('NVARCHAR', 'VARCHAR')
    #             # columns_sql = columns_sql.replace('DATETIMEOFFSET', 'DATETIME')
    #             # columns_sql = columns_sql.replace('SYSDATETIMEOFFSET()', 'CURRENT_TIMESTAMP')
    #             columns_sql = columns_sql.replace('$NAME', tableName)

    #             print(f"CREATE TABLE `{dbName}`.{tableName} ({columns_sql})")
    #             # exit()
    #             # Maak de tabel
    #             self.execSingle(f"CREATE TABLE `{dbName}`.{tableName} ({columns_sql})")

    #             # Voeg eventueel constraints toe
    #             if table.get('constraints'):
    #                 for constraint in table['constraints']:
    #                     if constraint['type'].upper() == 'PRIMARY KEY':
    #                         cols = [c.replace('[','').replace(']','') for c in constraint['columns']]
    #                         self.execSingle(f"ALTER TABLE `{dbName}`.{tableName} ADD PRIMARY KEY ({', '.join(cols)})")

    #             # Voeg eventueel indexes toe
    #             if table.get('indexes'):
    #                 for index in table['indexes']:
    #                     indexCols = [c.replace('[','').replace(']','') for c in index['columns']]
    #                     self.execSingle(f"CREATE INDEX {tableName}_{index['name']} ON `{dbName}`.{tableName} ({', '.join(indexCols)})")

class secrets():
    def __init__(self, vaultUrl='', local=False, prefix='', credFolder=''):
        self.local = local
        self.prefix = prefix
        self.credFolder = credFolder
        if not local:
            self.client = SecretClient(vault_url=vaultUrl, credential=DefaultAzureCredential())
        else:
            with open(f"{(credFolder+'/') if credFolder else ''}credentials.yaml", 'r') as f:
                self.creds = yaml.safe_load(f)


    def get(self, name):
        n = self.prefix+name
        if self.local:
            try:
                return self.creds[n]
            except:
                raise Exception(f"Secret {n} not found in credentials.yaml")
        else:
            try:
                return self.client.get_secret(n).value
            except:
                raise Exception(f"Secret {n} not found in keyvault")
    

    def set(self, name, value):
        n = self.prefix+name
        if self.local:
            self.creds[n] = value
            with open(f"{(self.credFolder+'/') if self.credFolder else ''}credentials.yaml", 'w') as f:
                yaml.dump(self.creds, f)
        else:
            self.client.set_secret(n, value)