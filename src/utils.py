import os
import sys
import dill
import numpy as np
import pandas as pd

from src.exception import CustomException
from sklearn.metrics import r2_score
from sklearn.model_selection import GridSearchCV


def save_object(file_path, obj):
    """
    Save any Python object (like a model, preprocessor, scaler, etc.) to disk.
    """
    try:
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path, exist_ok=True)

        with open(file_path, "wb") as file_obj:
            dill.dump(obj, file_obj)

    except Exception as e:
        raise CustomException(e, sys)


def load_object(file_path):
    """
    Load any Python object (model, preprocessor, scaler, etc.) from disk.
    """
    try:
        with open(file_path, "rb") as file_obj:
            return dill.load(file_obj)

    except Exception as e:
        raise CustomException(e, sys)


def evaluate_models(X_train, y_train, X_test, y_test, models, params):
    """
    Evaluate multiple ML models with GridSearchCV and return performance report.

    Args:
        X_train, y_train : Training data
        X_test, y_test   : Testing data
        models           : dict { "model_name": model_instance }
        params           : dict { "model_name": param_grid }

    Returns:
        dict : { "model_name": test_score }
    """
    try:
        report = {}

        for i, model_name in enumerate(models):
            model = models[model_name]
            param = params[model_name]

            gs = GridSearchCV(model, param, cv=3, n_jobs=-1, verbose=0)
            gs.fit(X_train, y_train)

            # set best params
            model.set_params(**gs.best_params_)
            model.fit(X_train, y_train)

            # predictions
            y_train_pred = model.predict(X_train)
            y_test_pred = model.predict(X_test)

            # scores
            train_score = r2_score(y_train, y_train_pred)
            test_score = r2_score(y_test, y_test_pred)

            report[model_name] = {
                "best_params": gs.best_params_,
                "train_score": train_score,
                "test_score": test_score,
            }

        return report

    except Exception as e:
        raise CustomException(e, sys)
