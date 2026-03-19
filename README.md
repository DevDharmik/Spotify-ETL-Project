# 🎵 Spotify ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)
![pandas](https://img.shields.io/badge/pandas-1.5+-150458?style=flat&logo=pandas&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-336791?style=flat&logo=postgresql&logoColor=white)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0+-D71F00?style=flat)
![Jupyter](https://img.shields.io/badge/Jupyter-Notebook-F37626?style=flat&logo=jupyter&logoColor=white)

An end-to-end **ETL (Extract, Transform, Load) pipeline** that processes Spotify track data using Python and loads it into a structured PostgreSQL database. Built to demonstrate core data engineering skills including data ingestion, cleaning, transformation, and database integration.

---

## 📌 Project Overview

This project simulates a real-world data engineering workflow:

- **Extract** raw Spotify track data from a CSV source
- **Transform** the data using pandas — handling nulls, normalising columns, enforcing data types
- **Load** the cleaned, structured data into a PostgreSQL database via SQLAlchemy

The pipeline is designed to be modular, readable, and reproducible.

---

## 🗂️ Project Structure
```
Spotify-ETL-Project/
│
├── data/
│   └── spotify_tracks.csv        # Raw input data
│
├── notebook/
│   └── spotify_etl.ipynb         # Exploratory notebook
│
├── scripts/
│   └── etl.py                    # Production ETL script
│
├── sql/
│   └── create_tables.sql         # PostgreSQL schema
│
├── .env.example                  # Environment variable template
├── requirements.txt              # Python dependencies
└── README.md
```

---

## ⚙️ Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.10+ | Core scripting language |
| pandas | Data extraction and transformation |
| SQLAlchemy | ORM-based database connection |
| PostgreSQL | Relational database for structured storage |
| Jupyter Notebook | Exploratory analysis and walkthrough |
| python-dotenv | Secure credential management |

---

## 🚀 Getting Started

### 1. Clone the repository
```bash
git clone https://github.com/DevDharmik/Spotify-ETL-Project.git
cd Spotify-ETL-Project
```

### 2. Create a virtual environment
```bash
python -m venv venv
source venv/bin/activate        # On Windows: venv\Scripts\activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Set up environment variables
```bash
cp .env.example .env
```
Edit `.env` with your PostgreSQL credentials:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=spotify_db
DB_USER=your_username
DB_PASSWORD=your_password
```

### 5. Set up the database
```bash
psql -U your_username -d spotify_db -f sql/create_tables.sql
```

### 6. Run the pipeline
```bash
python scripts/etl.py
```

---

## 🔄 ETL Pipeline Flow
```
Raw CSV (data/)
      │
      ▼
 [EXTRACT]  ──── Load CSV using pandas
      │
      ▼
 [TRANSFORM] ─── Clean nulls, normalise columns,
      │           enforce types, deduplicate
      ▼
  [LOAD]  ──────── Write to PostgreSQL via SQLAlchemy
      │
      ▼
 PostgreSQL (spotify_tracks table)
```

---

## 🧹 Transformation Steps

- Drop duplicate track entries based on `track_id`
- Handle missing values in `artist_name`, `album`, `duration_ms`
- Convert `duration_ms` → `duration_min` (float, 2 decimal places)
- Normalise string columns (strip whitespace)
- Parse `release_date` into datetime format
- Enforce correct data types before loading

---

## 🗃️ Database Schema
```sql
CREATE TABLE IF NOT EXISTS spotify_tracks (
    track_id        VARCHAR(50)  PRIMARY KEY,
    track_name      VARCHAR(255) NOT NULL,
    artist_name     VARCHAR(255),
    album           VARCHAR(255),
    release_date    DATE,
    duration_min    FLOAT,
    popularity      INTEGER CHECK (popularity BETWEEN 0 AND 100),
    explicit        BOOLEAN DEFAULT FALSE,
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## 🔐 Environment Variables

Never commit your `.env` file. Use `.env.example` as a safe reference:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=spotify_db
DB_USER=your_username
DB_PASSWORD=your_password
```

---

## 🛣️ Future Improvements

- [ ] Add scheduling with Apache Airflow or cron
- [ ] Integrate live Spotify API via Spotipy
- [ ] Add data validation with Great Expectations
- [ ] Containerise with Docker
- [ ] Add unit tests with pytest

---

## 👤 Author

**Dharmik Champaneri**
M.Sc. Data Science — University of Europe for Applied Sciences, Potsdam

[![LinkedIn](https://img.shields.io/badge/LinkedIn-dharmikchampaneri-0077B5?style=flat&logo=linkedin)](https://linkedin.com/in/dharmikchampaneri)
[![GitHub](https://img.shields.io/badge/GitHub-DevDharmik-181717?style=flat&logo=github)](https://github.com/DevDharmik)
