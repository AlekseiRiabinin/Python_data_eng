import json
from core_etl_pipeline import ETLExtractor, ETLTransformer, ETLLoader 


def run_complete_etl_pipeline():
    """ETL pipeline."""
    extractor = ETLExtractor()
    transformer = ETLTransformer()
    loader = ETLLoader()


    try:
        raw_data = extractor.extract_csv_with_retry(
            "sales_data.csv",
            max_retries=3,
            retry_delay=1.5
        )
    except Exception as e:
        print(f"Extraction failed: {e}")
        return False
    
    transformation_rules = {
        "transaction_date": {"type": "date", "format": "%m/%d/%Y"},
        "amount": {"type": "numeric"},
        "product_id": {"type": "string"}
    }
    transformed_data = transformer.transform_data(raw_data, transformation_rules)
    
    load_success = loader.load_json(
        transformed_data,
        "processed_sales.json"
    )
    
    print("\nETL Execution Report:")
    print(f"- Extracted: {len(raw_data)} records")
    print(f"- Transformed: {len(transformed_data)} records")
    print(f"- Load successful: {load_success}")
    
    all_logs = extractor.logs + transformer.logs + loader.logs
    with open("etl_logs.json", "w") as f:
        json.dump(all_logs, f, indent=2)
    
    return load_success


if __name__ == "__main__":
    run_complete_etl_pipeline()
