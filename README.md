# ✈️ Automated Reddit Data Pipeline for Airline Sentiment Analysis

This project automates the extraction, transformation, and loading (ETL) of Reddit posts related to major airline brands.  
It uses **Prefect** for orchestration, **AWS S3** as a data lake, and **Python** for data processing and visualization.

---

## 🚀 Project Overview

The workflow:
1. **Extracts** Reddit posts using the **PRAW API** (Python Reddit API Wrapper).  
2. **Transforms** and cleans the text data using **Pandas**.  
3. **Loads** the processed data into an **AWS S3 bucket**.  
4. **Schedules** and automates the entire process using **Prefect**.  
5. **Visualizes** trends and sentiment insights using **Matplotlib**.

---



## 🛠️ Dependencies

Install the following dependencies before running the project:

```bash
pip install prefect
pip install praw
pip install pandas
pip install boto3
pip install matplotlib
pip install seaborn wordcloud


⚙️ Environment Setup
1. Create and Activate Virtual Environment
python -m venv prefect_env
.\prefect_env\Scripts\Activate.ps1  # On Windows PowerShell

2. Set Up AWS Credentials
$env:AWS_ACCESS_KEY_ID="YOUR_AWS_ACCESS_KEY"
$env:AWS_SECRET_ACCESS_KEY="YOUR_AWS_SECRET_KEY"
$env:AWS_DEFAULT_REGION="ap-south-1"

3. Set Up Prefect Cloud Account (Optional)
Create an account on Prefect Cloud
Connect your workspace:
prefect cloud login



How to Run the Project
1️⃣ Run the ETL Flow
python reddit_to_s3.py

2️⃣ Automate with Prefect (Cron Scheduling)
python reddit.py
This will:
Deploy and serve your Prefect flow
Automatically schedule it


💡 Why Prefect?
Prefect was chosen over Apache Airflow due to:
Easier setup and minimal configuration
Cloud-native orchestration (free Prefect Cloud tier)
Developer-friendly Python-based flow definition
Simpler for academic and small-scale research projects