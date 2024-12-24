import pandas as pd
import numpy as np
def clean_terrorism_data(df):
    cleaned_df = df[[
        'eventid', 
        'iyear', 'imonth', 'iday',
        'country_txt', 'region_txt', 'provstate', 'city',
        'latitude', 'longitude',
        'attacktype1_txt',
        'nkill', 'nwound',
        'target1', 'summary', 'gname',
        'targtype1_txt', 'weaptype1_txt',
        'motive',
        'nperps'
    ]].copy()
    
    column_mapping = {
        'iyear': 'Year',
        'imonth': 'Month',
        'iday': 'Day',
        'country_txt': 'Country',
        'region_txt': 'Region',
        'provstate': 'State',
        'attacktype1_txt': 'AttackType',
        'nkill': 'Killed',
        'nwound': 'Wounded',
        'target1': 'Target',
        'latitude': 'Latitude',
        'city': 'City',
        'longitude': 'Longitude',
        'gname': 'Group',
        'summary': 'Summary',
        'motive': 'Motive',
        'nperps': 'Nperps',
        'targtype1_txt': 'Target_type',
        'weaptype1_txt': 'Weapon_type'
    }
    cleaned_df = cleaned_df.rename(columns=column_mapping)
    numeric_columns = ['Killed', 'Wounded', 'Nperps']
    for col in numeric_columns:
        cleaned_df.loc[cleaned_df[col] < 0, col] = np.nan
    text_columns = ['Country', 'Region', 'State', 'City', 'AttackType', 
                   'Target', 'Group', 'Summary', 'Motive', 'Target_type', 'Weapon_type']
    for col in text_columns:
        cleaned_df[col] = cleaned_df[col].fillna('Unknown')

    
    return cleaned_df

if __name__ == "__main__":
    df = pd.read_csv('csv_data/globalterrorismdb_raw.csv', encoding='latin1', low_memory=False)
    clean_df = clean_terrorism_data(df)
    clean_df.to_csv('csv_data/globalterrorismdb_clean.csv', index=False)