# Flight Price Data Pipeline

End-to-end Apache Airflow pipeline for processing and analyzing Bangladesh flight price data.

## 🏗️ Architecture

```
CSV File → [Airflow DAG] → PostgreSQL (Staging & Analytics)
                ↓
    ┌──────────┴──────────┐
    │  Pipeline Tasks     │
    ├─────────────────────┤
    │ 1. Ingest CSV       │
    │ 2. Validate Data    │
    │ 3. Transform Data   │
    │ 4. Compute KPIs     │
    │ 5. Save Report      │
    └─────────────────────┘
```

## 📋 Prerequisites

- Docker & Docker Compose (v2.14.0+)
- 4GB+ RAM available for Docker
- Kaggle dataset: [Flight Price Dataset of Bangladesh](https://www.kaggle.com/datasets)

## 🚀 Quick Start

### 1. Clone and Setup

```bash
cd flight-price-pipeline

# Create required directories
mkdir -p logs plugins data
```

### 2. Add Dataset

Download the CSV from Kaggle and place it in the `data/` folder:

```
data/Flight_Price_Dataset_of_Bangladesh.csv
```

### 3. Start Services

```bash
# Set Airflow UID (Linux/Mac)
echo -e "AIRFLOW_UID=$(id -u)" >> .env

# Or for Windows/PowerShell
# Add AIRFLOW_UID=50000 to .env

# Start all services
docker-compose up -d

# Wait for services to be healthy (check with docker-compose ps)
```

### 4. Setup Connections

```bash
# Run connection setup script
docker-compose exec airflow-webserver python /opt/airflow/scripts/setup_connections.py
```

### 5. Access Airflow UI

- **URL:** http://localhost:8080
- **Username:** admin
- **Password:** admin

### 6. Run the Pipeline

1. In the Airflow UI, go to DAGs
2. Find `flight_price_pipeline`
3. Click the play button to trigger a manual run
4. Monitor progress in the UI

## 📊 Pipeline Overview

The pipeline processes flight price data through:

1. **Data Ingestion**: Load CSV into PostgreSQL staging table (chunked for scalability)
2. **Data Validation**: Check for missing values, data types, and inconsistencies
3. **Data Transformation**: Clean data, parse dates, add computed columns
4. **KPI Computation**: Calculate metrics like average fare by airline, seasonal variations
5. **Reporting**: Save validation reports and KPIs to database

## 🛠️ Development

### Key Files

- `dags/flight_price_pipeline.py`: Main DAG definition
- `docker-compose.yml`: Service configuration
- `scripts/init_postgres.sql`: Database schema setup
- `scripts/setup_connections.py`: Airflow connection setup

### Environment Variables

Configure in `.env`:

```env
# Airflow
AIRFLOW_UID=50000

# PostgreSQL
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow_password
POSTGRES_DB=analytics
```

## 📈 KPIs Computed

- Average Fare by Airline
- Seasonal Fare Variation (Peak vs Non-Peak)
- Booking Count by Airline
- Most Popular Routes

## 🔧 Troubleshooting

### Common Issues

1. **Container fails to start**: Ensure Docker has enough RAM (4GB+)
2. **Database connection errors**: Check PostgreSQL health with `docker-compose ps`
3. **DAG not appearing**: Check logs in `logs/scheduler/`
4. **Task failures**: View task logs in Airflow UI or `logs/dag_id=.../`

### Logs

- **Scheduler logs**: `logs/scheduler/`
- **Task logs**: `logs/dag_id=flight_price_pipeline/run_id=.../task_id=.../`
- **Container logs**: `docker-compose logs [service]`

## 📚 Documentation

- [Pipeline Technical Documentation](docs/PIPELINE_DOCUMENTATION.md)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes and test
4. Submit a pull request

## 📄 License

This project is for educational purposes.

# Build and start all services
docker-compose up -d --build

# Check service status
docker-compose ps
```

### 4. Setup Airflow Connections

```bash
# Wait for services to be healthy, then run:
docker exec flight-airflow-worker python /opt/airflow/scripts/setup_connections.py
```

### 5. Access Airflow UI

- URL: http://localhost:8080
- Username: `admin`
- Password: `admin`

### 6. Run the Pipeline

1. Find `flight_price_pipeline` in the DAGs list
2. Toggle the DAG to "Active"
3. Click "Trigger DAG" to run manually

## 📊 KPI Outputs

| KPI                     | Table                          | Description                   |
| ----------------------- | ------------------------------ | ----------------------------- |
| Average Fare by Airline | `kpi_avg_fare_by_airline`      | Mean fares per airline        |
| Seasonal Variation      | `kpi_seasonal_fare_variation`  | Peak vs non-peak comparison   |
| Booking Counts          | `kpi_booking_count_by_airline` | Bookings per airline by class |
| Popular Routes          | `kpi_popular_routes`           | Top 20 routes by volume       |

## 🔍 Query Results

```bash
# Connect to PostgreSQL
docker exec -it flight-postgres psql -U airflow -d analytics

# View KPIs
SELECT * FROM kpi_avg_fare_by_airline ORDER BY avg_total_fare DESC;
SELECT * FROM kpi_seasonal_fare_variation;
SELECT * FROM kpi_popular_routes WHERE rank <= 10;
```

## 📁 Project Structure

```
flight-price-pipeline/
├── docker-compose.yml     # Docker services configuration
├── Dockerfile             # Custom Airflow image
├── .env                   # Environment variables
├── requirements.txt       # Python dependencies
├── dags/
│   └── flight_price_pipeline.py  # Main Airflow DAG
├── scripts/
│   ├── init_mysql.sql     # MySQL schema
│   ├── init_postgres.sql  # PostgreSQL schema
│   └── setup_connections.py
├── data/                  # CSV dataset location
├── logs/                  # Airflow logs
└── plugins/               # Custom Airflow plugins
```

## 🛠️ Troubleshooting

**Services not starting:**

```bash
docker-compose logs -f
```

**Database connection issues:**

```bash
# Test MySQL
docker exec flight-mysql mysql -u airflow -pairflow_password -e "SHOW DATABASES;"

# Test PostgreSQL
docker exec flight-postgres psql -U airflow -d analytics -c "\dt"
```

**Reset everything:**

```bash
docker-compose down -v
docker-compose up -d --build
```

## 📄 License

MIT License
