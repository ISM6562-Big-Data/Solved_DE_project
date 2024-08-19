import pytest
import sys
import os
import json
import duckdb
from equalexperts_dataeng_exercise.ingest import (
    fetch_data,
    filtered_and_formatted_data,
    pre_ingestion_db_activities,
    insert_batch,
    insert_data_using_multithreading,
    main,
    SCHEMA_NAME,
    TABLE_NAME
)

DATABASE = "warehouse.db"

@pytest.fixture(scope="module")
def setup_database():
    """Fixture to set up the database and table for testing."""
    # Connect to DuckDB and create schema and table
    conn = duckdb.connect(DATABASE)
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
            Id STRING,
            PostId STRING,
            VoteTypeId STRING,
            CreationDate TIMESTAMP
        )
    """)
    yield conn
    # Clean up: Drop the schema and table after tests
    conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA_NAME} CASCADE")
    conn.close()
    if os.path.exists(DATABASE):
        os.remove(DATABASE)

@pytest.fixture
def setup_test_file():
    """
    Fixture to create a temporary JSON file for testing.
    """
    test_file_path = "test_data.jsonl"
    test_data = [
        {"Id": "1", "PostId": "100", "VoteTypeId": "1", "CreationDate": "2024-01-01T00:00:00"},
        {"Id": "2", "PostId": "101", "VoteTypeId": "2", "CreationDate": "2024-01-02T00:00:00"},
        {"Id": "3", "PostId": "100", "VoteTypeId": "1", "CreationDate": "2024-01-03T00:00:00"},
        {"Id": "4", "PostId": "101", "VoteTypeId": "2", "CreationDate": "2024-01-04T00:00:00"}
    ]
    with open(test_file_path, "w") as file:
        for entry in test_data:
            file.write(json.dumps(entry) + "\n")
    yield test_file_path
    os.remove(test_file_path)

def test_fetch_data(setup_test_file):
    """
    Test the fetch_data function to ensure it reads the JSON file correctly.
    """
    # Save the original sys.argv
    original_argv = sys.argv
    # Set sys.argv to point to the test file
    sys.argv = ["script_name.py", setup_test_file]  # Replace 'script_name.py' with the actual script name

    try:
        data = fetch_data()
        expected_data = [
            {"Id": "1", "PostId": "100", "VoteTypeId": "1", "CreationDate": "2024-01-01T00:00:00"},
            {"Id": "2", "PostId": "101", "VoteTypeId": "2", "CreationDate": "2024-01-02T00:00:00"},
            {"Id": "3", "PostId": "100", "VoteTypeId": "1", "CreationDate": "2024-01-03T00:00:00"},
            {"Id": "4", "PostId": "101", "VoteTypeId": "2", "CreationDate": "2024-01-04T00:00:00"}
        ]
        assert data == expected_data
    finally:
        # Restore original sys.argv
        sys.argv = original_argv

def test_fetch_data_file_not_found():
    original_argv = sys.argv
    sys.argv = ["script_name.py", "non_existent_file.jsonl"]
    try:
        data = fetch_data()
        assert data == []
    finally:
        sys.argv = original_argv

def test_fetch_data_json_decode_error():
    test_file_path = "invalid_data.jsonl"
    with open(test_file_path, "w") as file:
        file.write("{invalid_json}\n")
    original_argv = sys.argv
    sys.argv = ["script_name.py", test_file_path]
    try:
        data = fetch_data()
        assert data == []
    finally:
        sys.argv = original_argv
        os.remove(test_file_path)


def test_filtered_and_formatted_data():
    fetched_data = [
        {"Id": "1", "PostId": "100", "VoteTypeId": "1", "CreationDate": "2024-01-01T00:00:00"},
        {"Id": "2", "PostId": "101", "VoteTypeId": "2", "CreationDate": "2024-01-02T00:00:00"},
        {"Id": "3", "PostId": "100", "VoteTypeId": "1", "CreationDate": "2024-01-03T00:00:00"},
        {"Id": "4", "PostId": "101", "VoteTypeId": "2", "CreationDate": "2024-01-04T00:00:00"},
        {"Id": "1", "PostId": "100", "VoteTypeId": "1", "CreationDate": "2024-01-01T00:00:00"},
    ]
    filtered_data = filtered_and_formatted_data(fetched_data)
    assert len(filtered_data) == 4
    assert filtered_data[0]["Id"] == "1"
    assert filtered_data[1]["Id"] == "2"

def test_pre_ingestion_db_activities(setup_database):
    pre_ingestion_db_activities()
    conn = setup_database
    result = conn.execute(f"SELECT * FROM {SCHEMA_NAME}.{TABLE_NAME}").fetchall()
    assert result == []

def test_insert_batch(setup_database):
    conn = setup_database    
    data_batch = [
        ("1","100","1","2024-01-01T00:00:00"),
        ("2", "101","2","2024-01-02T00:00:00"),
        ("3", "101","2","2024-01-02T00:00:00"),
        ("4", "101","2","2024-01-02T00:00:00")
    ]
    insert_batch(conn, data_batch, len(data_batch[0]))
    result = conn.execute(f"SELECT * FROM {SCHEMA_NAME}.{TABLE_NAME}").fetchall()
    assert len(result) == 4

def test_insert_data_using_multithreading(setup_database):
    conn = setup_database
    conn.execute(f"TRUNCATE TABLE {SCHEMA_NAME}.{TABLE_NAME}")
    filtered_entry = [
        {"Id": "1", "PostId": "100", "VoteTypeId": "1", "CreationDate": "2024-01-01T00:00:00"},
        {"Id": "2", "PostId": "101", "VoteTypeId": "2", "CreationDate": "2024-01-02T00:00:00"},
        {"Id": "3", "PostId": "100", "VoteTypeId": "1", "CreationDate": "2024-01-03T00:00:00"},
        {"Id": "4", "PostId": "101", "VoteTypeId": "2", "CreationDate": "2024-01-04T00:00:00"}
    ]
    insert_data_using_multithreading(filtered_entry)
    result = conn.execute(f"SELECT * FROM {SCHEMA_NAME}.{TABLE_NAME}").fetchall()
    assert len(result) == 4

def test_filtered_and_formatted_data_missing_keys():
    fetched_data = [
        {"Id": "1", "PostId": "100", "VoteTypeId": "1"},
        {"Id": "2", "PostId": "101", "CreationDate": "2024-01-02T00:00:00"}
        ]
    filtered_data = filtered_and_formatted_data(fetched_data)
    expected_data = [
        {"Id": "1", "PostId": "100", "VoteTypeId": "1"},
        {"Id": "2", "PostId": "101", "CreationDate": "2024-01-02T00:00:00"}
    ]
    assert filtered_data == expected_data

def test_filtered_and_formatted_data_exception():
    fetched_data = "invalid_data"  # Not a list
    with pytest.raises(TypeError):
        filtered_and_formatted_data(fetched_data)

def test_insert_data_using_multithreading_failure():
    
    filtered_entry = [
        {"Id": "1", "PostId": "100", "VoteTypeId": "1", "CreationDate": "2024-01-01T00:00:00"},
        {"Id": "2", "PostId": "101", "VoteTypeId": "2", "CreationDate": "2024-01-02T00:00:00"},
        {"Idd": "4", "PostId": "102", "VoteTypeId": "3", "CreationDate": "2024-01-03T00:00:00"}
    ]

    with pytest.raises(Exception):
        insert_data_using_multithreading(filtered_entry)

def test_insert_batch_failure():
    conn = duckdb.connect(DATABASE)

    data_batch = [
        ("1", "100", "1", "2024-01-01T00:00:00"),
        ("2", "101", "2", "2024-01-02T00:00:00")
    ]
    parameter_length = len(data_batch[0])

    # Simulate failure by providing invalid data
    data_batch.append(("invalid",))
    
    with pytest.raises(duckdb.InvalidInputException):
        insert_batch(conn, data_batch, parameter_length)
    
    conn = duckdb.connect("invalid_database.db")

    with pytest.raises(Exception):
        insert_batch(conn, data_batch, parameter_length)

    conn.close()

def test_pre_ingestion_db_activities_failure():
    conn = duckdb.connect(DATABASE)
    cursor = conn.cursor()
    
    try:
        # Create schema to simulate an already existing schema
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
        
        # Simulate failure by providing invalid schema/table name
        with pytest.raises(duckdb.CatalogException):
            cursor.execute(f"CREATE TABLE invalid_schema.invalidtable (id INT)")
            pre_ingestion_db_activities()
        
        with pytest.raises(duckdb.ParserException):
            cursor.execute(f"CREATE TABLE {SCHEMA_NAME}.invalid-table(id INT)")
            pre_ingestion_db_activities()
        
    finally:
        cursor.execute(f"DROP SCHEMA IF EXISTS {SCHEMA_NAME} CASCADE")
        conn.close()

def test_main_success(setup_database, setup_test_file):
    """
    Test the main function for successful execution.
    """
    # Temporarily replace sys.argv to simulate command-line argument
    original_argv = sys.argv
    try:
        sys.argv = ['your_script_name', setup_test_file]

        # Run the main function
        main()

        # Check the database to verify the data was inserted
        conn = setup_database
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {SCHEMA_NAME}.{TABLE_NAME}")
        rows = cursor.fetchall()
        assert len(rows) == 4

    finally:
        sys.argv = original_argv


if __name__ == "__main__":
    pytest.main()