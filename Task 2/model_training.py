import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.model_selection import train_test_split
import joblib


# create synthetic dataset
rng = np.random.RandomState(42)
N = 2000
sensor_id = rng.randint(1, 4, size=N)
value = 50 + sensor_id * 2 + rng.normal(scale=4.0, size=N)
# create a target that somewhat depends on value
target = 0.5 * value + rng.normal(scale=2.0, size=N)


# features: value and rolling-like aggregated value — here we synthesize
df = pd.DataFrame({'sensor_id': sensor_id, 'value': value, 'target': target})
# feature engineering: create a fake rolling_avg by smoothing
df['rolling_avg'] = df['value'].rolling(window=5, min_periods=1).mean()
X = df[['value', 'rolling_avg']].values
y = df['target'].values


X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
model = Ridge()
model.fit(X_train, y_train)


print('Train R^2:', model.score(X_train, y_train))
print('Test R^2:', model.score(X_test, y_test))
joblib.dump(model, 'model.joblib')
print('Saved model.joblib')