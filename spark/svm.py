import joblib
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC
from sklearn.metrics import confusion_matrix, precision_score, recall_score, f1_score, accuracy_score, classification_report
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("SVM").getOrCreate()

print('Start converting to pandas')
parquet_file_path = 'final_preprocessed_data.parquet'
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

print('Start training the model ==>')

# Train SVM model
svm_model = SVC(kernel='rbf', C=1.0, gamma='scale')  # You can adjust kernel, C, and gamma based on your requirements
svm_model.fit(train_data, target_train)

print('Start prediction ==>')
# Make predictions using the trained SVM model
y_pred = svm_model.predict(df_test)

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

''' 
                    Start prediction ==>
                    Start evaluation ==>
                    Accuracy: 0.9872190299138077
                    Confusion Matrix:
                    [[267477    165]
                    [  3465  12909]]
                    Precision: 0.9873795318953649
                    Recall: 0.7883840234518138
                    F1-score: 0.8767318663406682

'''

# Generate a classification report
print("Classification Report:")
print(classification_report(target_test, y_pred))


'''

Classification Report:
              precision    recall  f1-score   support

           0       0.99      1.00      0.99    267642
           1       0.99      0.79      0.88     16374

    accuracy                           0.99    284016
   macro avg       0.99      0.89      0.93    284016
weighted avg       0.99      0.99      0.99    284016



'''
# Save the model
joblib.dump(svm_model, 'svm_model.pkl')

'''  

    To run this file: 
        #spark-submit  /sparkScripts/svm.py
        - it takes time, maybe hours... 


'''

