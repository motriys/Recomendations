import pandas as pd
import numpy as np
from sqlalchemy import create_engine


def batch_load_sql(query: str) -> pd.DataFrame:
    CHUNKSIZE = 200000
    engine = create_engine(
        "postgresql://robot-startml-ro:Password@"
        "postgres.lab.karpov.courses:6432/startml"
    )
    conn = engine.connect().execution_options(stream_results=True)
    chunks = []
    for chunk_dataframe in pd.read_sql(query, conn, chunksize=CHUNKSIZE):
        chunks.append(chunk_dataframe)
    conn.close()
    return pd.concat(chunks, ignore_index=True)

engine = create_engine(
        "postgresql://robot-startml-ro:Password@"
        "postgres.lab.karpov.courses:6432/startml")

#USER_DATA
user_data = batch_load_sql(
    """SELECT * FROM "user_data" """)

def age_cat(row):
    teenager = row['age'] < 18
    adolescent = 18 <= row['age'] < 25
    young = 25 <= row['age'] < 35
    grown_up = 35 <= row['age'] < 50
    old = row['age'] >= 50

    if teenager:
        return 'teenager'
    elif adolescent:
        return 'adolescent'
    elif young:
        return 'young'
    elif grown_up:
        return 'grown_up'
    elif old:
        return 'old'
    else:
        return 'unknown'


user_data = user_data.assign(age=user_data.apply(age_cat, axis=1))
features = user_data
#features.to_sql('korablin_ms_lesson_22_3', con=engine, if_exists='replace')


def load_features():
    features = batch_load_sql(
        """SELECT * FROM "korablin_ms_lesson_22_3" """)
    return features
