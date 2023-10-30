import os
from pydantic import BaseModel
from typing import List
from datetime import datetime
from fastapi import FastAPI
from catboost import CatBoostClassifier
from sqlalchemy import create_engine
import pandas as pd


class PostGet(BaseModel):
    id: int
    text: str
    topic: str

    class Config:
        orm_mode = True


app = FastAPI()


def get_model_path(path: str) -> str:
    if os.environ.get("IS_LMS") == "1":  # проверяем где выполняется код в лмс, или локально
        MODEL_PATH = '/workdir/user_input/model'
    else:
        MODEL_PATH = path
    return MODEL_PATH


#Функиця для загрузки модели с сервера
def load_models():
    from catboost import CatBoostClassifier
    model_path = get_model_path("MY_PATH/model")
    model = CatBoostClassifier()
    model = model.load_model(model_path)

    return model

#Загрузчик из БД
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


#Загрузчик обработанных фичей с сервера
def load_features():
    features = batch_load_sql(
        """SELECT * FROM "korablin_ms_fp_p3_nn" """)
    return features


#Загружаем обученную модель
model = CatBoostClassifier()
model = load_models()


cols = ['gender', 'age', 'country', 'city', 'exp_group', 'os',
       'source', 'index', 'topic', 'cluster', 'distance_to_0',
       'distance_to_1', 'distance_to_2', 'distance_to_3', 'distance_to_4',
       'distance_to_5', 'distance_to_6', 'distance_to_7', 'distance_to_8',
       'distance_to_9', 'distance_to_10', 'distance_to_11',
       'distance_to_12', 'distance_to_13', 'distance_to_14']
features = load_features()
users = batch_load_sql(f"""SELECT * FROM "user_data" """)
posts = batch_load_sql(f"""SELECT * FROM "post_text_df" """)


#Само приложение
@app.get("/post/recommendations/", response_model=List[PostGet])
def recommended_posts(
    id: int,
    time: datetime = 0,
    limit: int = 5) -> List[PostGet]:
    user = users[users.user_id == id]       #выбираем фичи пользователя
    table = features.assign(**user.iloc[0]) #соединяем фичи пользователя и все посты
    table = table.set_index(['user_id', 'post_id'])
    table = table[cols]             #перегруппировывваем таблицу в правильном порядке
    p = model.predict_proba(table)  #получаем вероятности лайка от пользователя для каждого поста
    table['prob'] = p[:, 1]         #заносим их в таблицу
    posts_to_show = table.loc[id, :].sort_values('prob', ascending=False).index[:limit].values #выбираем топ(5)
    recommends = posts[posts.post_id.isin(posts_to_show)]
    recommends = recommends.rename(columns={"post_id": "id"})
    ans = []
    for i in range(limit):
        ans.append(recommends.iloc[i].to_dict())
    return ans
