import pytest
from equalexperts_dataeng_exercise import db 

DATABASE_NAME = 'warehouse_test.db'
SCHEMA_NAME = 'test_schema'
TABLE_NAME = 'test_table'
TABLE_SCHEMA =  {          
    "Id": "STRING",
    "PostId": "STRING",
    "VoteTypeId": "STRING",
    "CreationDate": "DATETIME"
}
PRIMARY_KEY = 'Id'

@pytest.fixture
def connection():
    """Fixture to create and return a database connection."""
    conn = db.connect_to_db(DATABASE_NAME)
    yield conn
    db.close_connection(conn)

@pytest.fixture
def cursor(connection):
    """Fixture to create and return a database cursor."""
    return connection.cursor()

def test_connect_to_db():
    conn = db.connect_to_db(DATABASE_NAME)
    assert conn is not None
    db.close_connection(conn)

def test_close_connection(connection):
     # Creating and closing connection to check if any exceptions are raised
    conn = db.connect_to_db(DATABASE_NAME)
    db.close_connection(conn)
    with pytest.raises(Exception):
        conn.execute('SELECT 1')

def test_create_schema(cursor):
    db.create_schema(cursor, DATABASE_NAME, SCHEMA_NAME)
    assert db.schema_exists(cursor, SCHEMA_NAME)

def test_truncate_table(cursor):
    db.create_schema(cursor, DATABASE_NAME, SCHEMA_NAME)
    db.create_table(cursor, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME,TABLE_SCHEMA,primary_key=None)
    
    cursor.execute(f"INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} (Id, PostId, VoteTypeId, CreationDate) VALUES ('1', '100', '1', '2024-01-01T00:00:00')")
    db.truncate_table(cursor, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME)
    
    cursor.execute(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{TABLE_NAME}")
    count = cursor.fetchone()[0]
    assert count == 0

def test_create_table(cursor):
    db.create_schema(cursor, DATABASE_NAME, SCHEMA_NAME)
    db.create_table(cursor, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, TABLE_SCHEMA, primary_key=None)
    assert db.table_exists(cursor, SCHEMA_NAME, TABLE_NAME)

def test_table_exists(cursor):
    db.create_schema(cursor, DATABASE_NAME, SCHEMA_NAME)
    db.create_table(cursor, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, TABLE_SCHEMA, primary_key=None)
    assert db.table_exists(cursor, SCHEMA_NAME, TABLE_NAME)

def test_schema_exists(cursor):
    db.create_schema(cursor, DATABASE_NAME, SCHEMA_NAME)
    assert db.schema_exists(cursor, SCHEMA_NAME)

def test_table_creation_failure(cursor):
    with pytest.raises(Exception):
        db.create_table(cursor, DATABASE_NAME, SCHEMA_NAME, '', {}, primary_key=None)

def test_schema_creation_failure(cursor):
    with pytest.raises(Exception):
        db.create_schema(cursor, DATABASE_NAME, '')

def test_truncate_table_failure(cursor):
    with pytest.raises(Exception):
        db.truncate_table(cursor, DATABASE_NAME, SCHEMA_NAME, '')

def test_table_exists_failure(cursor):
    with pytest.raises(Exception):
        db.table_exists(cursor, SCHEMA_NAME, '')

def test_schema_exists_failure(cursor):
    with pytest.raises(Exception):
        db.schema_exists(cursor, '')

if __name__ == '__main__':
    pytest.main()