# 🌦️ Automated Weather Data Pipeline

![Python](https://img.shields.io/badge/Python-3.10-blue)
![ETL](https://img.shields.io/badge/Pipeline-ETL-green)

## 📋 Project Overview
This project is an automated **ETL (Extract, Transform, Load) pipeline** designed to support marketing analytics.

**The Business Goal:** 
 This pipeline automatically collects daily weather forecasts for (Berlin, Paris, New York) to enable downstream correlation analysis with sales data.

## ⚙️ Architecture

The pipeline runs entirely on **GitHub Actions**, removing the need for a dedicated server.

```mermaid
graph TD
    A[☁️ OpenWeatherMap API] -->|Fetch JSON| B(GitHub Actions Runner)
    B -->|Run ETL Script| C{Python Logic}
    
    subgraph Transformation
    C -->|1. Extract| D[Raw Data]
    D -->|2. Clean & Dedupe| E[Pandas DataFrame]
    end
    
    E -->|Save| F[(📂 weather_forecast.csv)]


1.  **Extract:** Python script calls the **OpenWeatherMap API** (5-day/3-hour forecast endpoint).
2.  **Transform:**
    * Parses nested JSON responses.
    * Flattens data into a tabular format.
    * Converts units (Kelvin → Celsius).
    * Adds audit timestamps.
3.  **Load:** Appends new data to `data/weather_forecast.csv`, handling deduplication to ensure data integrity.
4.  **Automate:** Scheduled via **cron** to run every morning at 07:00 UTC.

## 🛠️ Tech Stack

* **Language:** Python 3.10
* **Libraries:** `pandas`, `requests`
* **Automation:** GitHub Actions (YAML)
* **Optimization:** dependency caching (`pip`)
* **Storage:** CSV (Version Controlled)

## 📂 Project Structure

```text
├── .github/
│   └── workflows/
│       └── main.yml      # The Automation Logic (Cron Schedule)
├── data/
│   └── weather_forecast.csv  # The Dataset (Auto-updated)
├── run_etl.py            # The ETL Script
├── requirements.txt      # Python Dependencies
└── README.md             # Documentation