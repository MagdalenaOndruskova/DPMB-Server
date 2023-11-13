import numpy as np
import pandas as pd
import requests
import geopandas as gpd


def get_data(from_time='2023-11-04 08:00:00', to_time='2023-11-10 08:00:00'):
    event_api_url = "https://gis.brno.cz/ags1/rest/services/Hosted/WazeJams/FeatureServer/0/"  # Replace with the actual API URL
    query = f"city='Brno' AND pubMillis >= TIMESTAMP '{from_time}' AND pubMillis <= TIMESTAMP '{to_time}'"

    url = f"{event_api_url}query?where=({query})&outFields=*&outSR=4326&f=json"
    response = requests.get(url)

    if response.status_code == 200:
        content = response.content.decode('utf-8')
        gdf = gpd.read_file(content)
        gdf['pubMillis'] = pd.to_datetime(gdf['pubMillis'], unit='ms', )
        gdf['street'] = gdf.apply(lambda row: fix_encoding(row['street']), axis=1)
        gdf['endNode'] = gdf.apply(lambda row: fix_encoding(row['endNode']), axis=1)  # do I need this col?
        gdf = gdf.drop(['blockingAlertUuid', 'objectid','globalid'], axis=1)
        return gdf
    return None



def fix_encoding(value):
    try:
        fixed = value.replace('Ã¡', 'á')
        fixed = fixed.replace('Ã\xad', 'í')
        fixed = fixed.replace('Åˆ', 'ň')
        fixed = fixed.replace('Ã½', 'ý')
        fixed = fixed.replace('Å™', 'ř')
        fixed = fixed.replace('Å¾', 'ž')
        fixed = fixed.replace('Ä�', 'č')
        fixed = fixed.replace('Å½', 'Ž')
        fixed = fixed.replace('Ã©', 'é')
        fixed = fixed.replace('Ä›', 'ě')
        fixed = fixed.replace('Å¡', 'š')
        fixed = fixed.replace('Å˜', 'Ř')
        fixed = fixed.replace('Å\xa0', 'Š')
        fixed = fixed.replace('ÄŒ', 'Č')
        fixed = fixed.replace('Å¯', 'ů')
        fixed = fixed.replace('Ãš', 'Ú')
        fixed = fixed.replace('Ãº', 'ú')
        fixed = fixed.replace('Ã¼º', 'ü')
        fixed = fixed.replace('Ã¼', 'ü')
        fixed = fixed.replace('Ã¶', 'ö')
        fixed = fixed.replace('Â»', '»')

        return fixed
    except Exception:
        return ''


def assign_color(df):
    df['color'] = np.select(
        [
            df['count'] < 50,
            (df['count'] >= 50) & (df['count'] <= 100),
            df['count'] > 100
        ],
        ['green', 'orange', 'red'],
    )
    return df


def get_color(df, street_name, column_name):
    if street_name in df[column_name].values:
        color = df.loc[df[column_name] == street_name, 'color'].values[0]
        return color
    else:
        return 'green'
