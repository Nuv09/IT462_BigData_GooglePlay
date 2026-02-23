# IT462 – Big Data Project  
## Google Play Store Data Preprocessing using Apache Spark (Scala)

### 📌 Project Overview
This project implements a complete data preprocessing pipeline for the Google Play Store dataset using Apache Spark with Scala.  
The goal is to clean, validate, and prepare the dataset for further analytical tasks such as data reduction, transformation, and large-scale analysis.

## 📊 Dataset
- Dataset: Google Play Store
- Size: ~2.3 million records
- The raw dataset is NOT included in this repository due to GitHub size limitations.
- Each team member must place the dataset manually in:
  data/raw/Google-Playstore.csv

  ## ⚙️ How to Run the Project

### 1️⃣ Open Spark Shell

```bash
spark-shell
``` 
2️⃣ Load the preprocessing script
``` 
:load code/01_DataPreprocessing.scala
3️⃣ Output
``` 
After execution, the cleaned dataset will be generated inside:
``` 
data/cleaned/
``` 
