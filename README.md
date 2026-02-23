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
```
---
## 🔹 Spark Execution & Team Workflow Guidelines

### 🟢 Running the Script in Spark Shell

When executing:
``` 
:load code/01_DataPreprocessing.scala
```

The output will appear:

- In the same **spark-shell session**
- In the same **terminal window**
- Any `println`, `.show()`, or `.count()` results will be displayed there

---

## ❗ Important Academic Note

If a team member continues writing new steps (e.g., Data Reduction) directly inside the terminal:

- The code will **NOT be saved**
- It will disappear once the Spark session is closed

This is academically incorrect.  
All required preprocessing steps must exist inside Scala source files.

---

## 🎯 Correct Project Workflow

### 1️⃣ Write All Official Code in Scala Files

Do **not** rely on terminal-only code.

All preprocessing steps must be written inside a file such as:

---

### 2️⃣ Testing Before Saving

You may:

- Test logic inside `spark-shell`
- Once verified:
  - Copy the working code
  - Paste it into the Scala file
  - Push the file

---

### 3️⃣ Team Collaboration with Git

After completing a step (e.g., Reduction):
``` 
git add .
git commit -m "Added reduction step"
git push
```
Other team members should then run:
``` 
git pull
```
