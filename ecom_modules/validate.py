
from datetime import datetime
import os
import re
import glob
import pandas as pd
from datetime import datetime
from airflow.models import Variable
from constant import VALIDATION_FORMATS, FORMAT_PATTERNS, DATE_FORMAT_MAPPING



def validate_csv(CSV_DIR, **kwargs):
    """
    Validates the contents of the latest CSV file in the specified directory against given formats and patterns.
    """
    list_of_files = glob.glob(os.path.join(CSV_DIR, "*.csv"))  
    if not list_of_files:
        raise FileNotFoundError("No CSV file found!")
    
    latest_file = max(list_of_files, key=os.path.getctime)  
    kwargs['ti'].xcom_push(key='latest_file', value=latest_file)
    df = pd.read_csv(latest_file)
    
    for index, row in df.iterrows():
        if not re.match(FORMAT_PATTERNS[VALIDATION_FORMATS["customer_id"]], str(row["customer_id"])) or \
           not re.match(FORMAT_PATTERNS[VALIDATION_FORMATS["customer_name"]], str(row["customer_name"])) or \
           not re.match(FORMAT_PATTERNS[VALIDATION_FORMATS["phone_number"]], str(row["phone_number"])) or \
           row["discount_code"] not in VALIDATION_FORMATS["discount_code"]:
            raise ValueError(f"Invalid data in column {df.columns[index]}: {row[index]}")

        date_format = DATE_FORMAT_MAPPING.get(VALIDATION_FORMATS["date-format"], "%Y-%m-%d")
        try:
            datetime.strptime(str(row["date"]), date_format)
        except ValueError:
            raise ValueError(f"Invalid date format: {row['date']}. Expected {VALIDATION_FORMATS['date_format']}")

