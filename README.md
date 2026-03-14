# IT462 – Big Data Project  

### 📌 Project Overview
This project implements a complete data preprocessing pipeline for the Google Play Store dataset using Apache Spark with Scala.  
The goal is to clean, validate, and prepare the dataset for further large-scale analysis.

## 📊 Dataset
- Dataset: Google Play Store
- Size: ~2.3 million records
- The raw dataset is NOT included in this repository due to GitHub size limitations.
- Each team member must place the dataset manually in:
  data/raw/Google-Playstore.csv
  
---
## Setup Instructions (For Team Members)
Follow these steps to run the project correctly:

---
## ❗ Important Notes
All preprocessing logic must be written inside the Scala source files located in:
- 
```bash
src/main/scala/
```
### 1️⃣ Clone the Repository
Using this Link:
```bash
https://www.scala-sbt.org/download/
```
version: sbt-1.12.4.msi

### 2️⃣ Clone the Repository
```bash
git clone https://github.com/Nuv09/IT462_BigData_GooglePlay.git
cd IT462_BigData_GooglePlay
```

---
### 3️⃣ Place the Dataset in the Correct Path
⚠ The dataset is not included in the repository due to GitHub size limitations.
```bash
data/raw/Google-Playstore.csv
```

---
### 4️⃣ Pull the Latest Updates
Before running the project, always make sure you have the latest version:
```bash
git pull
```

---
### 5️⃣ Run the Scala file on Visual Studio 
Open Visual Studio and write:
```bash
sbt "Data_Preprocessing" // or any file name you want to run
```

---
### 6️⃣ Output
The results of the preprocessing pipeline will appear:
- In the terminal where SBT is executed
- Any `println`, `.show()`, or `.count()` results will be displayed there


### 7️⃣ Team Collaboration with Git
After completing a step:
``` 
git add .
git commit -m "SOME TEXT..."
git push
```
Other team members should then run:
``` 
git pull
```
