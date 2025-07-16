import json
import pandas as pd
from loguru import logger

def validate_schema(df: pd.DataFrame, schema_path="data/schema_reference.json") -> bool:
    with open(schema_path, "r") as f:
        schema = json.load(f)

    required_cols = schema["required_columns"]
    missing = [col for col in required_cols if col not in df.columns]

    if missing:
        logger.error(f"Schema mismatch! Missing columns: {missing}")
        return False

    logger.info("Schema validated successfully.")
    return True
