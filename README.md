# IT462 – Big Data Project  

### 📌 Project Overview
This project implements a complete data preprocessing pipeline for the Google Play Store dataset using Apache Spark with Scala.  
The goal is to clean, validate, and prepare the dataset for further analytical tasks such as data reduction, transformation, and large-scale analysis.

## 📊 Dataset
- Dataset: Google Play Store
- Size: ~2.3 million records
- The raw dataset is NOT included in this repository due to GitHub size limitations.
- Each team member must place the dataset manually in:
  data/raw/Google-Playstore.csv
  
---
## 🚀 Setup Instructions (For Team Members)
Follow these steps to run the project correctly:

### 1️⃣ Clone the Repository
```bash
git clone https://github.com/Nuv09/IT462_BigData_GooglePlay.git
cd IT462_BigData_GooglePlay
```

---
### 2️⃣ Place the Dataset in the Correct Path
⚠ The dataset is not included in the repository due to GitHub size limitations.
```bash
data/raw/Google-Playstore.csv
```

---
### 3️⃣ Pull the Latest Updates
Before running the project, always make sure you have the latest version:
```bash
git pull
```

---
### 4️⃣ Run the Scala Preprocessing Script
Open Spark Shell:
```bash
spark-shell
```
Then load the preprocessing file:
```bash
:load code/01_DataPreprocessing.scala
```

---
### 5️⃣ Output
The output will appear:

- In the same **spark-shell session**
- In the same **terminal window**
- Any `println`, `.show()`, or `.count()` results will be displayed there


---
## ❗ Important Notes
If a team member continues writing new steps (e.g., Data Reduction) directly inside the terminal:
- The code will **NOT be saved**
- It will disappear once the Spark session is closed

All required preprocessing steps must exist inside Scala source files "`01_DataPreprocessing.scala`" for submitting. (Read the instructions below)

---
## 🎯 Correct Project Workflow

### 1️⃣ Testing Before Saving
- You may test your code logic inside `spark-shell`.

### 2️⃣ Write All Official Code in Scala Files 
- Once you verified your code, Copy the working code and paste it into the Scala file.
- Note: There is a specific syntax, Let ChatGBT do it for you and tell you how to paste.

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
