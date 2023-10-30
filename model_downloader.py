import os
from catboost import CatBoostClassifier

def get_model_path(path: str) -> str:
    if os.environ.get("IS_LMS") == "1":  # проверяем где выполняется код в лмс, или локально
        MODEL_PATH = '/workdir/user_input/model'
    else:
        MODEL_PATH = path
    return MODEL_PATH


def load_models():
    from catboost import CatBoostClassifier
    model_path = get_model_path("PATH/model")
    model = CatBoostClassifier()
    model = model.load_model(model_path)

    return model


model = load_models()
