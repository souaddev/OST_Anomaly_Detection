import joblib
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix, precision_score, recall_score, f1_score, accuracy_score, classification_report
from keras.models import Sequential
from keras.layers import LSTM, Dense


path = 'final_preprocessed_data.parquet'

print('Start converting to pandas')
parquet_file_path = path
table = pq.read_table(parquet_file_path)

# Convert PyArrow Table to Pandas DataFrame
transformed_data_pd = table.to_pandas()
print('Done converting to pandas')

y = transformed_data_pd['Normal/Attack']
columns_to_drop = ['Timestamp', 'Normal/Attack']
transformed_data_pd.drop(columns=columns_to_drop, inplace=True, errors='ignore')

print('Start splitting data to train and test')
# Data Split to train and test
train_data, df_test, target_train, target_test = train_test_split(
    transformed_data_pd, y, test_size=0.3, random_state=42
)

# Reshape the data for LSTM input (assuming your data is sequential)
train_data = train_data.values.reshape((train_data.shape[0], 1, train_data.shape[1]))
df_test = df_test.values.reshape((df_test.shape[0], 1, df_test.shape[1]))

print('Start training the model ==>')

# Build and train LSTM model
lstm_model = Sequential()
lstm_model.add(LSTM(units=50, input_shape=(train_data.shape[1], train_data.shape[2])))
lstm_model.add(Dense(1, activation='sigmoid'))  # Adjust the number of units and activation based on your task
lstm_model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])
lstm_model.fit(train_data, target_train, epochs=10, batch_size=32)

print('Start prediction ==>')
# Make predictions using the trained LSTM model
y_pred_prob = lstm_model.predict(df_test)
y_pred = (y_pred_prob > 0.5).astype(int)  # Adjust the threshold based on your needs

print('Start evaluation ==>')

# Evaluate model performance using labeled data
accuracy = accuracy_score(target_test, y_pred)
conf_matrix = confusion_matrix(target_test, y_pred)
precision = precision_score(target_test, y_pred)
recall = recall_score(target_test, y_pred)
f1 = f1_score(target_test, y_pred)

# Print all evaluation metrics
print(f"Accuracy: {accuracy}")
print("Confusion Matrix:")
print(conf_matrix)
print(f"Precision: {precision}")
print(f"Recall: {recall}")
print(f"F1-score: {f1}")

# Generate a classification report
print("Classification Report:")
print(classification_report(target_test, y_pred))

# Save the model
lstm_model.save('/content/drive/My Drive/datasets/lstm_model.pkl')