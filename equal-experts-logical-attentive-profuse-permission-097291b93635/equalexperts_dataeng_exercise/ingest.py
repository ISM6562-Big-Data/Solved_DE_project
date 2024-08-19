import concurrent.futures
import json
import logging
import sys

import duckdb

from equalexperts_dataeng_exercise import db

DATABASE = "warehouse.db"  # Path to the DuckDB database file
TABLE_NAME = "votes"  # Name of the table to be created
SCHEMA_NAME = "blog_analysis"
PRIMARY_KEY = "Id"  # Column name to be set as the primary key or for checking duplicate records
NUM_THREADS = 3  # Num of threads using which the multithreading operation will run
SCHEMA = {
    "Id": "STRING",
    "PostId": "STRING",
    "VoteTypeId": "STRING",
    "CreationDate": "DATETIME"
}  # Dictionary defining the schema

logger = logging.getLogger()
if not logger.hasHandlers():
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s - %(levelname)s::: %(message)s"))
    logger.addHandler(handler)
    handler.setLevel(logging.INFO)
logger.setLevel(logging.INFO)


def fetch_data():
    """
    Fetch data from a JSON file specified in the command-line arguments.

    :return: List of data entries
    """
    data = []
    try:
        with open(sys.argv[1]) as file:
            for line in file:
                data.append(json.loads(line))
        logging.info(f"Fetched {len(data)} entries from the file.")
    except FileNotFoundError as e:
        logging.error(
            f"File not found. Please download the dataset using 'poetry run exercise fetch-data'. Error: {e}")
    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON. Error: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred while fetching data. Error: {e}")

    return data


def filtered_and_formatted_data(fetched_data):
    """
    Filter and format data entries based on the specified schema.

    :param fetched_data: List of data entries
    :return: List of filtered and formatted data entries
    """
    columns = SCHEMA.keys()
    filtered_entry = []
    seen = set()
    try:
        for entry in fetched_data:
            if entry[PRIMARY_KEY] not in seen:
                filtered_entry.append({col: entry[col]
                                      for col in columns if col in entry})
                seen.add(entry[PRIMARY_KEY])
        logging.info(f"Filtered down to {len(filtered_entry)} unique entries.")
    except Exception as e:
        logging.error(f"Error while filtering and formatting data: {e}")
        raise
    return filtered_entry


def pre_ingestion_db_activities():
    """
    Perform pre-ingestion database activities such as checking and creating schema and table.
    """
    cursor = None
    try:
        cursor = db.connect_to_db(DATABASE)

        if not db.schema_exists(cursor, SCHEMA_NAME):
            db.create_schema(cursor, DATABASE, SCHEMA_NAME)

        if not db.table_exists(cursor, SCHEMA_NAME, TABLE_NAME):
            db.create_table(cursor, DATABASE, SCHEMA_NAME,
                            TABLE_NAME, SCHEMA, primary_key=None)
        else:
            db.truncate_table(cursor, DATABASE, SCHEMA_NAME, TABLE_NAME)

        logging.info("Pre-ingestion database activities completed successfully.")

    except duckdb.CatalogException:
        logging.error(
            "Database, table or Schema detail not matching with the existing database")
        raise
    except duckdb.ParserException:
        logging.error("Failed in the parsing the query")
        raise
    except Exception as e:
        logging.error(f"Failed in the pre-ingestion activities with error:{e}")
        raise

    finally:
        if cursor:
            db.close_connection(cursor)


def insert_batch(conn, data_batch, parameter_length):
    """
    Insert a batch of data into the database.
    :param conn: Database connection object
    :param data_batch: List of data tuples to be inserted
    :param parameter_length: Number of parameters in the data tuples
    """
    placeholders = ", ".join(["?"] * parameter_length)
    insert_query = f"INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} VALUES ({placeholders})"
    try:
        conn.executemany(insert_query, data_batch)
        logging.info(f"Inserted batch : {len(data_batch)} rows.")
    except duckdb.InvalidInputException as e:
        logging.error(
            f"Prepared statement is {insert_query}. This is incorrect {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Failed to insert batch: {str(e)}")
        raise


def insert_data_using_multithreading(filtered_entry):
    """
    Inserts data into the database using multithreading.

    :param filtered_entry: List of dictionaries containing the data to be inserted
    """
    cursor = None
    try:
        cursor = db.connect_to_db(DATABASE)
        cursor.execute("BEGIN TRANSACTION")
        logging.info("Data insertion using multithreading starting now.")

        columns = SCHEMA.keys()
        batch_size = len(filtered_entry) // NUM_THREADS
        data_chunks = []
        parameter_length = len(filtered_entry[0])
        for i in range(0, len(filtered_entry), batch_size):
            batch = filtered_entry[i:i + batch_size]
            batch_values = [tuple(item[col] for col in columns) for item in batch]
            data_chunks.append(batch_values)

        # Insert data using multiple threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            futures = [executor.submit(
                insert_batch, cursor, chunk, parameter_length) for chunk in data_chunks]
            concurrent.futures.wait(futures)

        cursor.execute("COMMIT")
        logging.info("Data inserted successfully.")

    except Exception as e:
        if cursor:
            cursor.execute("ROLLBACK")
        logging.error(f"Error during data insertion: {e}")
        raise
    finally:
        if cursor:
            db.close_connection(cursor)


def main():
    """
    Main function to fetch data, process it, and perform database operations.
    """
    try:
        fetched_data = fetch_data()
        filtered_data = filtered_and_formatted_data(fetched_data)
        pre_ingestion_db_activities()
        insert_data_using_multithreading(filtered_data)

    except Exception as e:
        logging.error(f"An error occurred in the main function: {e}")


if __name__ == "__main__":
    main()
