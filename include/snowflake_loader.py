"""
Snowflake Data Loader

This module provides utilities for loading data into Snowflake tables
with proper error handling and logging.
"""

import logging
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class SnowflakeLoader:
    """Utility class for loading data into Snowflake."""

    def __init__(self, snowflake_hook):
        """
        Initialize the Snowflake loader.

        Args:
            snowflake_hook: Airflow SnowflakeHook instance
        """
        self.hook = snowflake_hook

    @contextmanager
    def get_connection(self):
        """
        Context manager for Snowflake connection.

        Yields:
            Snowflake connection object
        """
        conn = None
        try:
            conn = self.hook.get_conn()
            yield conn
        finally:
            if conn:
                conn.close()
                logger.info("Snowflake connection closed")

    def create_table_if_not_exists(
        self, table_name: str, database: str, schema: str, create_table_sql: str
    ) -> None:
        """
        Create a Snowflake table if it doesn't exist.

        Args:
            table_name: Name of the table
            database: Snowflake database name
            schema: Snowflake schema name
            create_table_sql: SQL statement to create the table
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # Use database and schema
                cursor.execute(f"USE DATABASE {database}")
                cursor.execute(f"USE SCHEMA {schema}")

                # Check if table exists
                check_query = f"""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{schema.upper()}' 
                AND TABLE_NAME = '{table_name.upper()}'
                """
                cursor.execute(check_query)
                exists = cursor.fetchone()[0] > 0

                if not exists:
                    logger.info(f"Creating table {database}.{schema}.{table_name}")
                    cursor.execute(create_table_sql)
                    logger.info(f"Table {database}.{schema}.{table_name} created successfully")
                else:
                    logger.info(f"Table {database}.{schema}.{table_name} already exists")

            finally:
                cursor.close()

    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        database: str,
        schema: str,
        if_exists: str = "append",
        chunk_size: int = 10000,
    ) -> int:
        """
        Load a pandas DataFrame into a Snowflake table.

        Args:
            df: DataFrame to load
            table_name: Target table name
            database: Snowflake database name
            schema: Snowflake schema name
            if_exists: How to behave if table exists ('append', 'replace', 'fail')
            chunk_size: Number of rows to insert at once

        Returns:
            Number of rows loaded
        """
        if df.empty:
            logger.warning("DataFrame is empty, nothing to load")
            return 0

        rows_loaded = 0

        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # Use database and schema
                cursor.execute(f"USE DATABASE {database}")
                cursor.execute(f"USE SCHEMA {schema}")

                # Write DataFrame to Snowflake
                logger.info(f"Loading {len(df)} rows into {database}.{schema}.{table_name}")

                # Use pandas to_sql with Snowflake connection
                from snowflake.connector.pandas_tools import write_pandas

                success, nchunks, nrows, _ = write_pandas(
                    conn=conn,
                    df=df,
                    table_name=table_name.upper(),
                    database=database.upper(),
                    schema=schema.upper(),
                    chunk_size=chunk_size,
                    auto_create_table=False,  # We handle table creation separately
                    overwrite=(if_exists == "replace"),
                )

                if success:
                    rows_loaded = nrows
                    logger.info(
                        f"Successfully loaded {rows_loaded} rows in {nchunks} chunks "
                        f"to {database}.{schema}.{table_name}"
                    )
                else:
                    raise Exception("Failed to load data to Snowflake")

            except Exception as e:
                logger.error(f"Error loading data to Snowflake: {e}")
                raise
            finally:
                cursor.close()

        return rows_loaded

    def load_json_data(
        self,
        data: List[Dict[str, Any]],
        table_name: str,
        database: str,
        schema: str,
        if_exists: str = "append",
        chunk_size: int = 10000,
    ) -> int:
        """
        Load JSON data (list of dicts) into a Snowflake table.

        Args:
            data: List of dictionaries to load
            table_name: Target table name
            database: Snowflake database name
            schema: Snowflake schema name
            if_exists: How to behave if table exists ('append', 'replace', 'fail')
            chunk_size: Number of rows to insert at once

        Returns:
            Number of rows loaded
        """
        if not data:
            logger.warning("No data to load")
            return 0

        # Convert to DataFrame
        df = pd.DataFrame(data)

        return self.load_dataframe(
            df=df,
            table_name=table_name,
            database=database,
            schema=schema,
            if_exists=if_exists,
            chunk_size=chunk_size,
        )

    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[tuple]:
        """
        Execute a SQL query and return results.

        Args:
            query: SQL query to execute
            params: Optional query parameters

        Returns:
            List of result tuples
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                logger.info(f"Executing query: {query[:100]}...")
                cursor.execute(query, params or {})
                results = cursor.fetchall()
                logger.info(f"Query returned {len(results)} rows")
                return results
            finally:
                cursor.close()

    def truncate_table(self, table_name: str, database: str, schema: str) -> None:
        """
        Truncate a Snowflake table.

        Args:
            table_name: Name of the table to truncate
            database: Snowflake database name
            schema: Snowflake schema name
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(f"USE DATABASE {database}")
                cursor.execute(f"USE SCHEMA {schema}")

                truncate_query = f"TRUNCATE TABLE {table_name}"
                logger.info(f"Truncating table {database}.{schema}.{table_name}")
                cursor.execute(truncate_query)
                logger.info(f"Table {database}.{schema}.{table_name} truncated successfully")
            finally:
                cursor.close()

    def get_table_row_count(self, table_name: str, database: str, schema: str) -> int:
        """
        Get the row count of a Snowflake table.

        Args:
            table_name: Name of the table
            database: Snowflake database name
            schema: Snowflake schema name

        Returns:
            Number of rows in the table
        """
        query = f"""
        SELECT COUNT(*) 
        FROM {database}.{schema}.{table_name}
        """
        results = self.execute_query(query)
        return results[0][0] if results else 0
