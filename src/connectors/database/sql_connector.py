# src/connectors/database/sql_connector.py

import os
import sqlite3
import pandas as pd
from typing import List, Dict, Optional, Union, Any, Tuple
import sqlalchemy
from urllib.parse import quote_plus


class SQLConnector:
    """
    SQL database connector with support for SQLite, MySQL, PostgreSQL, and SQL Server.
    """
    
    def __init__(self, connection_string: Optional[str] = None, engine_type: str = "sqlite"):
        """
        Initialize the SQL connector.
        
        Args:
            connection_string: Connection string for the database
            engine_type: Database engine type ("sqlite", "mysql", "postgresql", "sqlserver")
        """
        self.connection_string = connection_string
        self.engine_type = engine_type.lower()
        self.connection = None
        self.engine = None
        self.cursor = None
    
    def connect(self, connection_string: Optional[str] = None, 
                engine_type: Optional[str] = None, **kwargs) -> bool:
        """
        Connect to a database.
        
        Args:
            connection_string: Connection string for the database (overrides init value)
            engine_type: Database engine type (overrides init value)
            **kwargs: Additional connection parameters
            
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            if connection_string:
                self.connection_string = connection_string
            if engine_type:
                self.engine_type = engine_type.lower()
                
            # Close existing connection if any
            self.close()
            
            # Connect based on engine type
            if self.engine_type == "sqlite":
                return self._connect_sqlite(**kwargs)
            else:
                return self._connect_sqlalchemy(**kwargs)
                
        except Exception as e:
            print(f"Connection error: {str(e)}")
            return False
    
    def _connect_sqlite(self, in_memory: bool = False, **kwargs) -> bool:
        """Connect to SQLite database"""
        try:
            if in_memory:
                self.connection = sqlite3.connect(":memory:")
            elif self.connection_string:
                self.connection = sqlite3.connect(self.connection_string)
            else:
                raise ValueError("No connection string provided")
                
            self.cursor = self.connection.cursor()
            return True
        except Exception as e:
            print(f"SQLite connection error: {str(e)}")
            return False
    
    def _connect_sqlalchemy(self, **kwargs) -> bool:
        """Connect to database using SQLAlchemy"""
        try:
            # Build connection string if not provided
            if not self.connection_string:
                # Extract connection parameters from kwargs
                host = kwargs.get("host", "localhost")
                port = kwargs.get("port", "")
                database = kwargs.get("database", "")
                username = kwargs.get("username", "")
                password = kwargs.get("password", "")
                
                # Build connection string based on engine type
                if self.engine_type == "mysql":
                    port = port or "3306"
                    self.connection_string = f"mysql+pymysql://{username}:{quote_plus(password)}@{host}:{port}/{database}"
                elif self.engine_type == "postgresql":
                    port = port or "5432"
                    self.connection_string = f"postgresql://{username}:{quote_plus(password)}@{host}:{port}/{database}"
                elif self.engine_type == "sqlserver":
                    port = port or "1433"
                    self.connection_string = f"mssql+pyodbc://{username}:{quote_plus(password)}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
                else:
                    raise ValueError(f"Unsupported engine type: {self.engine_type}")
            
            # Create engine and connection
            self.engine = sqlalchemy.create_engine(self.connection_string)
            self.connection = self.engine.connect()
            
            return True
        except Exception as e:
            print(f"SQLAlchemy connection error: {str(e)}")
            return False
    
    def close(self) -> None:
        """Close the database connection."""
        try:
            if self.cursor:
                self.cursor.close()
                self.cursor = None
                
            if self.connection:
                self.connection.close()
                self.connection = None
                
            if self.engine:
                self.engine.dispose()
                self.engine = None
        except Exception as e:
            print(f"Error closing connection: {str(e)}")
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> Tuple[bool, Any]:
        """
        Execute a SQL query.
        
        Args:
            query: SQL query to execute
            params: Parameters for the query
            
        Returns:
            Tuple[bool, Any]: (Success flag, Result or error message)
        """
        if not self.connection:
            return False, "Not connected to a database"
            
        try:
            if self.engine_type == "sqlite":
                if params:
                    result = self.cursor.execute(query, params)
                else:
                    result = self.cursor.execute(query)
                self.connection.commit()
                return True, result
            else:
                if params:
                    result = self.connection.execute(query, params)
                else:
                    result = self.connection.execute(query)
                return True, result
        except Exception as e:
            return False, str(e)
    
    def fetch_all(self, query: str, params: Optional[tuple] = None) -> Tuple[bool, Union[List[tuple], str]]:
        """
        Execute a query and fetch all results.
        
        Args:
            query: SQL query to execute
            params: Parameters for the query
            
        Returns:
            Tuple[bool, Union[List[tuple], str]]: (Success flag, Results or error message)
        """
        if not self.connection:
            return False, "Not connected to a database"
            
        try:
            if self.engine_type == "sqlite":
                if params:
                    self.cursor.execute(query, params)
                else:
                    self.cursor.execute(query)
                return True, self.cursor.fetchall()
            else:
                if params:
                    result = self.connection.execute(query, params)
                else:
                    result = self.connection.execute(query)
                return True, result.fetchall()
        except Exception as e:
            return False, str(e)
    
    def fetch_as_dataframe(self, query: str, params: Optional[tuple] = None) -> Tuple[bool, Union[pd.DataFrame, str]]:
        """
        Execute a query and fetch results as a pandas DataFrame.
        
        Args:
            query: SQL query to execute
            params: Parameters for the query
            
        Returns:
            Tuple[bool, Union[pd.DataFrame, str]]: (Success flag, DataFrame or error message)
        """
        if not self.connection:
            return False, "Not connected to a database"
            
        try:
            if self.engine_type == "sqlite":
                return True, pd.read_sql_query(query, self.connection, params=params)
            else:
                return True, pd.read_sql_query(query, self.connection, params=params)
        except Exception as e:
            return False, str(e)
    
    def get_tables(self) -> Tuple[bool, Union[List[str], str]]:
        """
        Get a list of tables in the database.
        
        Returns:
            Tuple[bool, Union[List[str], str]]: (Success flag, List of tables or error message)
        """
        if not self.connection:
            return False, "Not connected to a database"
            
        try:
            if self.engine_type == "sqlite":
                query = "SELECT name FROM sqlite_master WHERE type='table';"
                self.cursor.execute(query)
                tables = [table[0] for table in self.cursor.fetchall() if not table[0].startswith('sqlite_')]
                return True, tables
            else:
                # Use SQLAlchemy's introspection
                inspector = sqlalchemy.inspect(self.engine)
                tables = inspector.get_table_names()
                return True, tables
        except Exception as e:
            return False, str(e)
    
    def get_table_schema(self, table_name: str) -> Tuple[bool, Union[List[Dict[str, str]], str]]:
        """
        Get the schema for a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Tuple[bool, Union[List[Dict[str, str]], str]]: (Success flag, Schema info or error message)
        """
        if not self.connection:
            return False, "Not connected to a database"
            
        try:
            if self.engine_type == "sqlite":
                query = f"PRAGMA table_info({table_name});"
                self.cursor.execute(query)
                columns = []
                for row in self.cursor.fetchall():
                    columns.append({
                        "name": row[1],
                        "type": row[2],
                        "notnull": bool(row[3]),
                        "default": row[4],
                        "pk": bool(row[5])
                    })
                return True, columns
            else:
                # Use SQLAlchemy's introspection
                inspector = sqlalchemy.inspect(self.engine)
                columns = []
                for column in inspector.get_columns(table_name):
                    columns.append({
                        "name": column["name"],
                        "type": str(column["type"]),
                        "nullable": column["nullable"],
                        "default": column["default"],
                        "primary_key": column.get("primary_key", False)
                    })
                return True, columns
        except Exception as e:
            return False, str(e)
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, 
                         if_exists: str = 'replace', index: bool = False) -> Tuple[bool, str]:
        """
        Insert a pandas DataFrame into a database table.
        
        Args:
            df: DataFrame to insert
            table_name: Name of the target table
            if_exists: Action if table exists ('fail', 'replace', 'append')
            index: Whether to include the DataFrame's index
            
        Returns:
            Tuple[bool, str]: (Success flag, Message)
        """
        if not self.connection:
            return False, "Not connected to a database"
            
        try:
            if self.engine_type == "sqlite":
                df.to_sql(table_name, self.connection, if_exists=if_exists, index=index)
                return True, f"Successfully inserted {len(df)} rows into {table_name}"
            else:
                df.to_sql(table_name, self.connection, if_exists=if_exists, index=index)
                return True, f"Successfully inserted {len(df)} rows into {table_name}"
        except Exception as e:
            return False, str(e)
    
    def export_query_to_csv(self, query: str, output_path: str, 
                           params: Optional[tuple] = None) -> Tuple[bool, str]:
        """
        Execute a query and export results to a CSV file.
        
        Args:
            query: SQL query to execute
            output_path: Path to save the CSV file
            params: Parameters for the query
            
        Returns:
            Tuple[bool, str]: (Success flag, Message)
        """
        if not self.connection:
            return False, "Not connected to a database"
            
        try:
            success, result = self.fetch_as_dataframe(query, params)
            if not success:
                return False, result
                
            result.to_csv(output_path, index=False)
            return True, f"Successfully exported {len(result)} rows to {output_path}"
        except Exception as e:
            return False, str(e)
    
    def export_query_to_excel(self, query: str, output_path: str, 
                             params: Optional[tuple] = None) -> Tuple[bool, str]:
        """
        Execute a query and export results to an Excel file.
        
        Args:
            query: SQL query to execute
            output_path: Path to save the Excel file
            params: Parameters for the query
            
        Returns:
            Tuple[bool, str]: (Success flag, Message)
        """
        if not self.connection:
            return False, "Not connected to a database"
            
        try:
            success, result = self.fetch_as_dataframe(query, params)
            if not success:
                return False, result
                
            result.to_excel(output_path, index=False)
            return True, f"Successfully exported {len(result)} rows to {output_path}"
        except Exception as e:
            return False, str(e)
    
    def execute_script(self, script_path: str) -> Tuple[bool, str]:
        """
        Execute a SQL script from a file.
        
        Args:
            script_path: Path to the SQL script file
            
        Returns:
            Tuple[bool, str]: (Success flag, Message)
        """
        if not self.connection:
            return False, "Not connected to a database"
            
        try:
            # Read the script file
            with open(script_path, 'r') as file:
                script = file.read()
                
            # Split the script into individual statements
            # This simple approach splits on semicolons, which works for most cases
            statements = [s.strip() for s in script.split(';') if s.strip()]
            
            # Execute each statement
            for statement in statements:
                success, _ = self.execute_query(statement)
                if not success:
                    return False, f"Error executing statement: {statement}"
                    
            return True, f"Successfully executed script from {script_path}"
        except Exception as e:
            return False, str(e)
    
    def create_table_from_dataframe(self, df: pd.DataFrame, table_name: str, 
                                   primary_key: Optional[str] = None) -> Tuple[bool, str]:
        """
        Create a new table from a DataFrame schema.
        
        Args:
            df: DataFrame with the desired schema
            table_name: Name for the new table
            primary_key: Column to use as primary key
            
        Returns:
            Tuple[bool, str]: (Success flag, Message)
        """
        if not self.connection:
            return False, "Not connected to a database"
            
        try:
            # Generate CREATE TABLE statement
            columns = []
            for col_name, dtype in zip(df.columns, df.dtypes):
                col_type = self._map_pandas_type_to_sql(dtype)
                pk_clause = "PRIMARY KEY" if primary_key and col_name == primary_key else ""
                columns.append(f"{col_name} {col_type} {pk_clause}")
                
            create_stmt = f"CREATE TABLE {table_name} (\n  " + ",\n  ".join(columns) + "\n);"
            
            # Execute the CREATE TABLE statement
            success, _ = self.execute_query(create_stmt)
            if not success:
                return False, f"Error creating table: {table_name}"
                
            return True, f"Successfully created table: {table_name}"
        except Exception as e:
            return False, str(e)
    
    def _map_pandas_type_to_sql(self, pandas_type) -> str:
        """Map pandas dtype to SQL data type"""
        type_str = str(pandas_type)
        
        if 'int' in type_str:
            return 'INTEGER'
        elif 'float' in type_str:
            return 'REAL'
        elif 'datetime' in type_str:
            return 'TIMESTAMP'
        elif 'bool' in type_str:
            return 'BOOLEAN'
        else:
            return 'TEXT'
    
    def backup_database(self, backup_path: str) -> Tuple[bool, str]:
        """
        Backup a SQLite database to a file.
        
        Args:
            backup_path: Path to save the backup
            
        Returns:
            Tuple[bool, str]: (Success flag, Message)
        """
        if not self.connection:
            return False, "Not connected to a database"
            
        if self.engine_type != "sqlite":
            return False, "Database backup is only supported for SQLite"
            
        try:
            # Create a backup connection
            backup_conn = sqlite3.connect(backup_path)
            
            # Backup the database
            self.connection.backup(backup_conn)
            
            # Close the backup connection
            backup_conn.close()
            
            return True, f"Successfully backed up database to {backup_path}"
        except Exception as e:
            return False, str(e)
