"""MLflow loader-module flavor: JSON bundle, no pickled estimator."""
import pandas as pd
from .model import Ranker


class AMLPredictor:
    def __init__(self, data_path):
        self.ranker = Ranker.load(data_path)

    def predict(self, data, params=None):
        if params:
            raise ValueError("runtime model parameters are not supported")
        return pd.DataFrame({"score": self.ranker.score(data)}, index=data.index)


def _load_pyfunc(data_path, model_config=None):
    if model_config:
        raise ValueError("runtime model config is not supported")
    return AMLPredictor(data_path)
