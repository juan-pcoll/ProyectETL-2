from extract import extract_data
from extract_api import extract_api
from transform import transform_data
from validate import validate_data
from load import load_data
 
 
def main():
 
    # 1. EXTRACT
    df = extract_data()
    
    df_api = extract_api()
 
    # 2. TRANSFORM
    tables = transform_data(df, df_api)
    
    print("\n=== VALIDATING ===")

    tables = validate_data(tables)
 
    # 3. LOAD
    # False = solo CSV
    # True  = CSV + PostgreSQL
    load_data(tables, use_postgres=True)
 
 
if __name__ == "__main__":
    main()
 