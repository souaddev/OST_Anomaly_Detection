from sklearn.svm import OneClassSVM
import joblib
import pickle
import pandas as pd
import pyarrow.parquet as pq
from sklearn.metrics import confusion_matrix, precision_score, recall_score, f1_score, roc_curve, roc_auc_score,accuracy_score,classification_report
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("OneClassSVM").getOrCreate()

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
# Train One-Class SVM
oc_svm = OneClassSVM(kernel='rbf', gamma=0.001, nu=0.03)
oc_svm.fit(train_data)

print('Start prediction ==>')
# Predict using the model (Note: One-Class SVM labels outliers as -1 and inliers as 1)
predictions = oc_svm.predict(df_test)
binary_predictions = [1 if pred == -1 else 0 for pred in predictions]
true_labels = target_test
predicted_labels = binary_predictions
# Calculate Performance metrics
accuracy = accuracy_score(true_labels, predicted_labels)
confusion = confusion_matrix(true_labels, predicted_labels)
precision = precision_score(true_labels, predicted_labels)
recall = recall_score(true_labels, predicted_labels)
f1 = f1_score(true_labels, predicted_labels)

print("Accuracy:", accuracy)
print("Confusion Matrix:")
print(confusion)
print("Precision:", precision)
print("Recall:", recall)
print("F1 Score:", f1)

# Generate a classification report
print("Classification Report:")
print(classification_report(true_labels, predicted_labels))
# Save the model
joblib.dump(oc_svm, 'one_class_svm.pkl')



