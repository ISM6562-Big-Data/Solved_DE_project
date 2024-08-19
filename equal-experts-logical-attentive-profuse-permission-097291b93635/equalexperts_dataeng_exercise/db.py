import logging
import sys
import duckdb

logger = logging.getLogger()
if not logger.hasHandlers():
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s - %(levelname)s::: %(message)s"))
    logger.addHandler(handler)
    handler.setLevel(logging.INFO)
logger.setLevel(logging.INFO)


def connect_to_db(db_name):
    """
    Connects to the DuckDB database with database name and the read_only status flag
    and returns the connection object
    Parameters:
    """
    try:
        connection = duckdb.connect(db_name)
        logging.info("Connected to DuckDB.")
        return connection
    except Exception as e:
        logging.error(f"Failed to connect to DuckDB: {e}")
        raise


def close_connection(connection):
    """
    Closes the DuckDB connection.
    Parameters:
    connection (duckdbPyConnection): Connection object to be closed
    """
    try:
        if connection:
            connection.close()
            logging.info("Connection to DuckDB closed.")
    except Exception as e:
        logging.error(f"Error closing the DuckDB connection: {e}")
        raise


def create_schema(cursor, database_name, schema_name):
    """
    Creates a schema in duckdb database with specified name
    """
    create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
    try:
        cursor.execute(create_schema_query)
        logging.info(
            f"Created a schema named {schema_name} under the database named {database_name}.")

    except Exception as e:
        logging.error(f"Error creating the schema {e}")
        raise


def truncate_table(cursor, database_name, schema_name, table_name):
    """
    Truncates a table in duckdb database with specified name
    """
    create_schema = f"truncate table {schema_name}.{table_name}"
    try:
        cursor.execute(create_schema)
        logging.info(
            f"Truncated a table named {table_name} in schema named {schema_name} under the database named {database_name}.")
    except Exception:
        logging.error(
            f" Not able to truncate the table named {table_name} in schema named {schema_name} under the database named {database_name}.")
        raise


def create_table(cursor, db, schema_name, table_name, table_schema, primary_key) -> None:
    """
    Creates a table in DuckDB with the given schema and primary key.
    Parameters:
    db (str): Path to the DuckDB database file.
    table_name (str): Name of the table to be created.
    schema (dict): Dictionary containing column names as keys and data types as values.
    primary_key (str): Column name to be set as the primary key.
    """
    try:
        # Construct the schema definition part of the SQL query
        columns = ", ".join([f"{col} {dtype}" for col, dtype in table_schema.items()])

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            {columns}
        """
        # to add primary key constraint to the query
        if primary_key:
            create_table_query = create_table_query + "," + f"""PRIMARY KEY ({primary_key})
                        );
                            """
        else:
            create_table_query = create_table_query + f"""
                                );
                            """
        # Execute the SQL query
        cursor.execute(create_table_query)
        logging.info(
            f"Table '{table_name}' in db {db} created successfully with schema: {table_schema} and primary key: {primary_key}")

    except Exception as e:
        # Log any exceptions that occur
        logging.error(
            f"Failed to create table '{schema_name}.{table_name}' in db {db} . Error: {str(e)}")
        raise


def table_exists(cursor, schema_name, table_name):
    """
    Checks if a table exists in the DuckDB database.

    :param cursor: database connection object
    schema_name: schema_name under which table exists
    table_name: Name of the table to check
    :return: True if the table exists, False otherwise
    """
    try:
        check_table_exists_query = f"SELECT 1 FROM {schema_name}.{table_name} LIMIT 1"
        cursor.execute(check_table_exists_query)
        return True
    except duckdb.CatalogException:
        return False

    except Exception as e:
        logging.error(
            f"Error checking if table '{schema_name}.{table_name}' exists: {e}")
        raise


def schema_exists(cursor, schema_name):
    """
    Checks if a schema exists in the DuckDB database.

    :param cursor: duckdb connection object
    schema_name: Name of the schema to check
    :return: True if the schema exists, False otherwise
    """
    try:
        if (schema_name != ''):
            query = f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}';"
            result = cursor.execute(query).fetchone()
            return result is not None
        else:
            raise Exception

    except Exception as e:
        logging.error(f"Error checking if schema '{schema_name}' exists: {e}")
        raise
