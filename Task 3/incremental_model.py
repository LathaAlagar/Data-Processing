# src/incremental_model.py
from sklearn.linear_model import SGDRegressor
import numpy as np
import pickle

class IncrementalRegression:
    def __init__(self):
        self.model = SGDRegressor(max_iter=1, warm_start=True)
        self.initialized = False

    def update(self, X, y):
        if not self.initialized:
            self.model.partial_fit(X, y)
            self.initialized = True
        else:
            self.model.partial_fit(X, y)
        print("Model updated.")

    def predict(self, X):
        return self.model.predict(X)

    def save(self, path="incremental_model.pkl"):
        with open(path, "wb") as f:
            pickle.dump(self.model, f)

