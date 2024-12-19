import pandas as pd

def load_raw_data(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path, encoding='latin1', low_memory=False)

if __name__ == "__main__":
    file_path = "/Users/shalom_bergman/kodcode2/Data_engineering_course/Data_course_final_exam/Global_Terrorism_Analysis/csv_data/globalterrorismdb_raw.csv"
    df = load_raw_data(file_path)
    print(df.head())
 