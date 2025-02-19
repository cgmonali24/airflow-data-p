from airflow.models import Variable


VALIDATION_FORMATS = {
    "customer_id": Variable.get("customer_id_format", default_var="Alphanumeric"),
    "customer_name": Variable.get("customer_name_format", default_var="Only Alphabets"),
    "phone_number": Variable.get("phone_format", default_var="+XX-XXXXXXXXXX"),
    "discount_code": Variable.get("valid_discount_codes", default_var="DISC10,DISC20,SAVE5").split(","),
    "date-format": Variable.get("date_format", default_var="YYYY-MM-DD"),
}


FORMAT_PATTERNS = {
    "Alphanumeric": r"^[a-zA-Z0-9]+$",
    "Only Alphabets": r"^[a-zA-Z ]+$",
    "+XX-XXXXXXXXX": r"^\+?[0-9]{10,15}$",
    "+XX-XXXXXXXXXX": r"^\+\d{2}-\d{10}$",
}

DATE_FORMAT_MAPPING = {
    "YYYY-MM-DD": "%Y-%m-%d",
    "DD-MM-YYYY": "%d-%m-%Y",
    "MM/DD/YYYY": "%m/%d/%Y"
}