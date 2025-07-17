import mysql.connector
from mysql.connector import errorcode
from loguru import logger

DB_NAME = "stock_data"

TABLES = {
    "stock_prices": (
        """
        CREATE TABLE IF NOT EXISTS stock_prices (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ticker VARCHAR(10),
            date DATE,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            adj_close FLOAT,
            volume BIGINT,
            ma10 FLOAT,
            ma50 FLOAT,
            pct_change FLOAT,
            bb_upper FLOAT,
            bb_lower FLOAT
        )
        """
    )
}

def create_database(cursor):
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME} DEFAULT CHARACTER SET 'utf8'")
        logger.info(f"Database `{DB_NAME}` checked/created.")
    except mysql.connector.Error as err:
        logger.error(f"Failed creating database: {err}")
        exit(1)

def initialize_mysql():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password=""
        )
        cursor = conn.cursor()

        create_database(cursor)

        conn.database = DB_NAME

        for table_name, ddl in TABLES.items():
            logger.info(f"Creating table `{table_name}`...")
            cursor.execute(ddl)
            logger.info(f"Table `{table_name}` ready.")

        cursor.close()
        conn.close()
        logger.success("MySQL database and tables initialized successfully.")

    except mysql.connector.Error as err:
        logger.error(err)
