import pytest
import duckdb
import os
import logging
import sys
from equalexperts_dataeng_exercise import db
from equalexperts_dataeng_exercise.outliers import (
    VIEW_NAME,
    create_outliers_view,
    display_outliers_view,
    main,
    SCHEMA_NAME,
    TABLE_NAME,
    OUTLIER_QUERY
)
DATABASE = "warehouse_test.db"

# Configure logging
logger = logging.getLogger()
if not logger.hasHandlers():
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s::: %(message)s"))
    logger.addHandler(handler)
    handler.setLevel(logging.INFO)
logger.setLevel(logging.INFO)

@pytest.fixture(scope="module")
def setup_database():
    """Fixture to set up the database and table for testing."""
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
    conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA_NAME} CASCADE")
    conn.close()
    if os.path.exists(DATABASE):
        os.remove(DATABASE)

@pytest.fixture
def setup_test_data(setup_database):
    """Fixture to insert test data into the table."""
    test_data = [
        {"Id": "1", "PostId": "1", "VoteTypeId": "2", "CreationDate": "2022-01-02T00:00:00.000"},
        {"Id": "2", "PostId": "1", "VoteTypeId": "2", "CreationDate": "2022-01-09T00:00:00.000"},
        {"Id": "4", "PostId": "1", "VoteTypeId": "2", "CreationDate": "2022-01-09T00:00:00.000"},
        {"Id": "5", "PostId": "1", "VoteTypeId": "2", "CreationDate": "2022-01-09T00:00:00.000"},
        {"Id": "6", "PostId": "5", "VoteTypeId": "3", "CreationDate": "2022-01-16T00:00:00.000"},
        {"Id": "7", "PostId": "3", "VoteTypeId": "2", "CreationDate": "2022-01-16T00:00:00.000"},
        {"Id": "8", "PostId": "4", "VoteTypeId": "2", "CreationDate": "2022-01-16T00:00:00.000"},
        {"Id": "9", "PostId": "2", "VoteTypeId": "2", "CreationDate": "2022-01-23T00:00:00.000"},
        {"Id": "10", "PostId": "2", "VoteTypeId": "2", "CreationDate": "2022-01-23T00:00:00.000"},
        {"Id": "11", "PostId": "1", "VoteTypeId": "2", "CreationDate": "2022-01-30T00:00:00.000"},
        {"Id": "12", "PostId": "5", "VoteTypeId": "2", "CreationDate": "2022-01-30T00:00:00.000"},
        {"Id": "13", "PostId": "8", "VoteTypeId": "2", "CreationDate": "2022-02-06T00:00:00.000"},
        {"Id": "14", "PostId": "13", "VoteTypeId": "3", "CreationDate": "2022-02-13T00:00:00.000"},
        {"Id": "15", "PostId": "13", "VoteTypeId": "3", "CreationDate": "2022-02-20T00:00:00.000"},
        {"Id": "16", "PostId": "11", "VoteTypeId": "2", "CreationDate": "2022-02-20T00:00:00.000"},
        {"Id": "17", "PostId": "3", "VoteTypeId": "3", "CreationDate": "2022-02-27T00:00:00.000"}
    ]
    for entry in test_data:
        setup_database.execute(f"""
            INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} (Id, PostId, VoteTypeId, CreationDate)
            VALUES ('{entry['Id']}', '{entry['PostId']}', '{entry['VoteTypeId']}', '{entry['CreationDate']}')
        """)

def test_create_outliers_view_success(setup_database, setup_test_data):
    """Test successful creation of the outliers view."""
    cursor = setup_database.cursor()
    assert create_outliers_view(cursor) is True
    cursor.execute(f"SELECT * FROM {SCHEMA_NAME}.{VIEW_NAME}")
    result = cursor.fetchall()
    assert result is not None
    assert len(result) > 0

def test_display_outliers_view_success(setup_database, setup_test_data):
    """Test successful display of the outliers view."""
    cursor = setup_database.cursor()
    create_outliers_view(cursor)
    select_view_query = f"SELECT * FROM {SCHEMA_NAME}.{VIEW_NAME} ORDER BY Year, WeekNumber"
    result = cursor.sql(select_view_query).fetchall()
    assert result is not None
    assert len(result) > 0


def test_display_outliers_view_error(setup_database):
    """Test handling of errors during the display of the outliers view."""
    cursor = setup_database.cursor()
    create_outliers_view(cursor)
    select_view_query = f"SELECT * FROM {SCHEMA_NAME}.non_existent_view ORDER BY Year, WeekNumber"
    try:
        cursor.execute(select_view_query).fetchall()
    except Exception as e:
        assert "Catalog Error" in str(e)

def test_main_success(setup_database, setup_test_data):
    """Test successful execution of the main function."""
    main(DATABASE)
    cursor = setup_database.cursor()
    select_view_query = f"SELECT * FROM {SCHEMA_NAME}.{VIEW_NAME} ORDER BY Year, WeekNumber"
    result = cursor.execute(select_view_query).fetchall()
    assert result is not None
    assert len(result) > 0

def test_create_outliers_view_no_table(setup_database, caplog):
    """Test create_outliers_view when the table does not exist."""
    cursor = setup_database.cursor()
    cursor.execute(f"DROP TABLE {SCHEMA_NAME}.{TABLE_NAME}")
    result = create_outliers_view(cursor)
    assert result == False


def test_main_error(setup_database, caplog):
    """Test the main function with an error in the main block."""
    invalid_database = "invalid_path/warehouse_test.db"
    
    with pytest.raises(Exception):
        main(invalid_database)

def test_create_outliers_view_error(setup_database, caplog):
    """Test create_outliers_view with an error during view creation."""
    cursor = setup_database.cursor()
    
    # Use an invalid schema name to induce an error
    invalid_schema_name = "invalid_schema"
    invalid_query = OUTLIER_QUERY.replace(SCHEMA_NAME, invalid_schema_name)
    with pytest.raises(Exception):
        cursor.execute(invalid_query)
        result = create_outliers_view(cursor)


def test_sample_test_case(setup_database, caplog):
    cursor = setup_database.cursor()
    # Ensure the table exists and is populated
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
            Id STRING,
            PostId STRING,
            VoteTypeId STRING,
            CreationDate TIMESTAMP
        )
    """)
    cursor.execute(f"""
        INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} (Id, PostId, VoteTypeId, CreationDate) VALUES
        ('1', '1', '2', '2022-01-02T00:00:00.000'),
        ('2', '1', '2', '2022-01-09T00:00:00.000'),
        ('4', '1', '2', '2022-01-09T00:00:00.000'),
        ('5', '1', '2', '2022-01-09T00:00:00.000'),
        ('6', '5', '3', '2022-01-16T00:00:00.000'),
        ('7', '3', '2', '2022-01-16T00:00:00.000'),
        ('8', '4', '2', '2022-01-16T00:00:00.000'),
        ('9', '2', '2', '2022-01-23T00:00:00.000'),
        ('10', '2', '2', '2022-01-23T00:00:00.000'),
        ('11', '1', '2', '2022-01-30T00:00:00.000'),
        ('12', '5', '2', '2022-01-30T00:00:00.000'),
        ('13', '8', '2', '2022-02-06T00:00:00.000'),
        ('14', '13', '3', '2022-02-13T00:00:00.000'),
        ('15', '13', '3', '2022-02-20T00:00:00.000'),
        ('16', '11', '2', '2022-02-20T00:00:00.000'),
        ('17', '3', '3', '2022-02-27T00:00:00.000')
    """)
    expected_output = [
        ('2022', 0, 1),
        ('2022', 1, 3),
        ('2022', 2, 3),
        ('2022', 5, 1),
        ('2022', 6, 1),
        ('2022', 8, 1)
    ]
    cursor.execute(f"DROP VIEW IF EXISTS {SCHEMA_NAME}.outlier_weeks")
    # Run the main function
    with caplog.at_level(logging.INFO):
        main(DATABASE)
    # Verify the output
    result = cursor.execute(f"SELECT Year, WeekNumber, VoteCount FROM {SCHEMA_NAME}.{VIEW_NAME} ORDER BY Year, WeekNumber").fetchall()
    assert result == expected_output, f"Expected {expected_output}, but got {result}"