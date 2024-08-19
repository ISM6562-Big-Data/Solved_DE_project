import logging
import sys

from equalexperts_dataeng_exercise import db

# Configure logging
logger = logging.getLogger()
if not logger.hasHandlers():
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s - %(levelname)s::: %(message)s"))
    logger.addHandler(handler)
    handler.setLevel(logging.INFO)
logger.setLevel(logging.INFO)

DATABASE = "warehouse.db"   # Path to the DuckDB database file
TABLE_NAME = "votes"  # Name of the table to be created
SCHEMA_NAME = "blog_analysis"
PERCENT_OUTLIERS = 0.2
VIEW_NAME = "outlier_weeks"
ORDER_BY_COLUMNS = "Year,WeekNumber"
OUTLIER_QUERY = f"""CREATE OR REPLACE VIEW {SCHEMA_NAME}.{VIEW_NAME}
AS
WITH week_data as (
SELECT
Id,
PostId,
VoteTypeId,
strftime(CreationDate, '%Y-%m-%d') as CreationDate,
strftime(MIN(CreationDate) over(), '%Y-%m-%d') AS MinDate,
strftime(CreationDate, '%Y') as Year,
CAST(strftime('%W', CreationDate) as INTEGER) AS WeekNumber
FROM
{SCHEMA_NAME}.{TABLE_NAME}
),
count_votes_per_week AS (
SELECT
DISTINCT
Year,
WeekNumber,
count(CreationDate) over (partition by Year, WeekNumber) as VoteCount
FROM
week_data
),
mean_count AS (
SELECT
Year,
WeekNumber,
VoteCount,
round(avg(VoteCount) over()) as MeanVoteCount
FROM
count_votes_per_week
),
percent_diff AS (
SELECT
*,
abs(round(1 -(VoteCount / MeanVoteCount), 2)) as percent_diffrence
FROM
mean_count
)
SELECT
Year,
WeekNumber,
VoteCount
FROM
percent_diff
WHERE percent_diffrence > {PERCENT_OUTLIERS}
ORDER BY {ORDER_BY_COLUMNS}
"""


def create_outliers_view(cursor):
    """
    Create a view for detecting outliers in the votes data.

    :param cursor: Database cursor
    :return: True if the view is created successfully, False otherwise
    """
    try:
        if db.table_exists(cursor, SCHEMA_NAME, TABLE_NAME):
            logger.info(
                f"{SCHEMA_NAME}.{TABLE_NAME} table exists. Proceeding to create the outliers view")
            cursor.execute(OUTLIER_QUERY)
            return True
        else:
            logger.info(
                f"There is no table named '{SCHEMA_NAME}.{TABLE_NAME}'. Please ingest data using the command 'poetry run exercise ingest-data'.")
            return False
    except Exception as e:
        logger.error(
            f"Error while creating view using the query: {OUTLIER_QUERY}. Error: {e}")
        raise


def display_outliers_view(cursor):
    """
    Display the outliers view in a tabular format.

    :param cursor: Database cursor
    """
    try:
        select_view_query = f"SELECT * FROM {SCHEMA_NAME}.{VIEW_NAME} ORDER BY {ORDER_BY_COLUMNS}"
        # result = cursor.execute(select_view_query).fetchall()
        result = cursor.sql(select_view_query)
        result.show()
    except Exception as e:
        logger.error(
            f"Error while showing the view using the query: {select_view_query}. Error: {e}")
        raise


def main(DATABASE):
    """
    Main function to create the outliers view and display its contents.
    """
    cursor = None
    try:
        cursor = db.connect_to_db(DATABASE)
        view_created = create_outliers_view(cursor)
        if view_created:
            display_outliers_view(cursor)
    except Exception as e:
        logger.error(f"An error occurred in the main function: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
            db.close_connection(cursor)


if __name__ == "__main__":
    main(DATABASE)
