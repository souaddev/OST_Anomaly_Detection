import joblib
import tensorflow as tf
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.layers import LSTM, Dense,Input,Dropout, BatchNormalization
from tensorflow.keras.models import Model
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
from sklearn.metrics import confusion_matrix, precision_score, recall_score, f1_score, roc_curve, roc_auc_score,accuracy_score,classification_report
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("AutoEncoder").getOrCreate()

print('start converting to pandas')
parquet_file_path = 'final_preprocessed_data.parquet'
table = pq.read_table(parquet_file_path)

# Convert PyArrow Table to Pandas DataFrame
transformed_data_pd = table.to_pandas()

print('done converting to pandas')

y= transformed_data_pd['Normal/Attack']
columns_to_drop = ['Timestamp', 'Normal/Attack']
transformed_data_pd.drop(columns=columns_to_drop, inplace=True, errors='ignore')

print('start spliting data to train and test')
#Data Split to train and test
train_data = transformed_data_pd.iloc[:700000, :]
df_test = transformed_data_pd.iloc[700000:, :]
target_test = y.iloc[700000:]

print('Start training the model ==>')

input_shape = train_data.shape[1]
# Define the encoding layer
input_data = Input(shape=(input_shape,))
encoded = Dense(32, activation="relu")(input_data)
encoded = BatchNormalization()(encoded)
encoded = Dropout(0.5)(encoded)

# Define the decoding layer
decoded = Dense(input_shape, activation='sigmoid')(encoded)


autoencoder = Model(input_data, decoded)

# Compile the model
autoencoder.compile(optimizer='adam', loss='mean_squared_error')
autoencoder.summary()

early_stopping = EarlyStopping(monitor='val_loss',patience=5,restore_best_weights=True)

# Train AutoEncoder
autoencoder_train=autoencoder.fit(train_data, train_data,
                epochs=100,
                batch_size=256,
                shuffle=True,
                validation_data=(df_test, df_test),callbacks=[early_stopping],verbose=1)


print('Start prediction ==>')
# Reconstruct samples from test data using the trained autoencoder
y_pred = autoencoder.predict(df_test)
mse = np.mean(np.power(df_test - y_pred, 2), axis=1)
y_predictions = y_pred[:, 0]
# Set a threshold for identifying anomalies
threshold = np.percentile(mse, 85)

# Classify data points as normal (0) or anomaly (1) based on the threshold
#y_pred = (mse > threshold).astype(int)
y_pred = (mse > threshold).astype(int)

print('Start evaluation ==>')

thresholds = np.linspace(min(mse), max(mse), num=100)

# Initialize lists to store precision, recall, and F1-score for each threshold
precision_list, recall_list, f1_list = [], [], []

# Calculate precision, recall, and F1-score for each threshold
thresholds = np.linspace(min(mse), max(mse), num=100)

# Initialize lists to store precision, recall, and F1-score for each threshold
precision_list, recall_list, f1_list = [], [], []

# Calculate precision, recall, and F1-score for each threshold
for threshold in thresholds:
    predicted_anomalies = (mse > threshold).astype(int)
    precision = precision_score(target_test, predicted_anomalies)
    recall = recall_score(target_test, predicted_anomalies)
    f1 = f1_score(target_test, predicted_anomalies)

    precision_list.append(precision)
    recall_list.append(recall)
    f1_list.append(f1)

# Find the index of the maximum F1-score
max_f1_index = np.argmax(f1_list)

# Retrieve the maximum F1-score and the threshold value associated with it
max_f1_score = f1_list[max_f1_index]
threshold_with_max_f1 = thresholds[max_f1_index]

# Print the maximum F1-score and the threshold value associated with it
print(f"The maximum F1-score is: {max_f1_score}")
print(f"The threshold value corresponding to the maximum F1-score is: {threshold_with_max_f1}")

# Evaluate model performance using labeled data
accuracy = accuracy_score(target_test, y_pred)
conf_matrix = confusion_matrix(target_test, y_pred)
print(f"Accuracy: {accuracy}")
print("Confusion Matrix:")
print(conf_matrix)

precision = precision_score(target_test, y_pred)
recall = recall_score(target_test, y_pred)
f1 = f1_score(target_test, y_pred)

# Print all evaluation metrics
print(f"Precision: {precision}")
print(f"Recall: {recall}")
print(f"F1-score: {f1}")

# Generate a classification report
print("Classification Report:")
print(classification_report(target_test, y_pred))
# Save the model
joblib.dump(autoencoder, 'autoencoder.pkl')



