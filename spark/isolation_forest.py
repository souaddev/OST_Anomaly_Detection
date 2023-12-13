from sklearn.ensemble import IsolationForest
import joblib
import pickle
from sklearn.metrics import confusion_matrix, precision_score, recall_score, f1_score, roc_curve, roc_auc_score,accuracy_score,classification_report
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("LoadPreprocessedData").getOrCreate()

# Load the saved Parquet data into a PySpark DataFrame
swat_transformed = spark.read.parquet("final_preprocessed_data.parquet")
swat_transformed.printSchema()
swat_transformed.show(5)
transformed_data_pd = swat_transformed.toPandas()

y= transformed_data_pd['Normal/Attack']
columns_to_drop = ['Timestamp', 'Normal/Attack']
transformed_data_pd.drop(columns=columns_to_drop, inplace=True, errors='ignore')

#Data Split to train and test
train_data = transformed_data_pd.iloc[:700000, :]
df_test = transformed_data_pd.iloc[700000:, :]
target_test = y.iloc[700000:]

# # Train the Isolation Forest model
isolation_forest = IsolationForest(n_estimators=100, max_samples='auto', contamination=float(.01),
                             max_features=1.0, bootstrap=False, n_jobs=-1, random_state=42, verbose=0)
isolation_forest.fit(train_data)

predictions = isolation_forest.predict(df_test)
binary_predictions = [1 if pred == -1 else 0 for pred in predictions]

true_labels = target_test
predicted_labels = binary_predictions

accuracy = accuracy_score(true_labels, predicted_labels)
confusion = confusion_matrix(true_labels, predicted_labels)

# Calculate Performance metrics
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

# # Save the model
joblib.dump(isolation_forest, 'isolation_forest.pkl')

# with open('./models/iso_log.pickle', 'wb') as f:
#             pickle.dump(isolation_forest, f)
