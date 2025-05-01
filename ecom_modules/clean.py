import pandas as pd



def clean_data( **kwargs):
    file_path = kwargs['ti'].xcom_pull(task_ids='validate_csv', key='latest_file')
    df = pd.read_csv(file_path)
    
    df["customer_name"] = df["customer_name"].str.title().str.strip()
    

    df = df.drop_duplicates()
    
    cleaned_path = file_path.replace(".csv", "_cleaned.csv")
    df.to_csv(cleaned_path, index=False)

    cleaned_data = df.to_dict(orient="records")
    kwargs['ti'].xcom_push(key='cleaned_data', value=cleaned_data)
    return cleaned_path