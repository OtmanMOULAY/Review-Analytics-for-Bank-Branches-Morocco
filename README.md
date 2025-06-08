# Review Analytics for Bank Branches in Morocco

A comprehensive data engineering and analytics solution for extracting, processing, and analyzing customer reviews for bank branches across Morocco. This project leverages modern data stack technologies to provide actionable insights into customer sentiment and banking service quality.

## 🏗️ Architecture Overview

This project implements a robust data pipeline using:
- **Airflow**: Orchestrates the entire data workflow
- **dbt**: Handles data transformation and modeling
- **Docker**: Ensures consistent deployment and environment management
- **Web Scraping**: Automated extraction of customer reviews from various platforms

## 🚀 Features

- **Automated Review Extraction**: Collects customer reviews from multiple sources
- **Data Quality Assurance**: Implements validation and cleansing processes
- **Sentiment Analysis**: Analyzes customer sentiment across different bank branches
- **Performance Metrics**: Tracks key performance indicators for banking services
- **Scalable Architecture**: Containerized solution for easy deployment and scaling

## 🛠️ Tech Stack

- **Orchestration**: Apache Airflow
- **Data Transformation**: dbt (Data Build Tool)
- **Containerization**: Docker & Docker Compose
- **Data Processing**: Python, Pandas
- **Database**: PostgreSQL
- **Monitoring**: Airflow UI for pipeline monitoring

## 📁 Project Structure

```
├── dags/
│   ├── DAG (include sentiment analysis)/              # Airflow DAGs
│   ├── data extraction/           # Custom Airflow plugins
│   └── data loading/            # Airflow configuration
├── dbt/
│   ├── models/            # dbt transformation models
│   ├── tests/             # Data quality tests
│   └── macros/            # Reusable dbt macros

├── Dockerfile         # Container definitions
├── docker-compose.yml # Multi-service orchestration

└── docs/                  # Documentation
```

## 🏃‍♂️ Quick Start

### Prerequisites
- Docker and Docker Compose installed
- Python 3.8+ (for development)
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/OtmanMOULAY/Review-Analytics-for-Bank-Branches-Morocco.git
   cd Review-Analytics-for-Bank-Branches-Morocco
   ```

2. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. **Start the services**
   ```bash
   docker-compose up -d
   ```

4. **Access Airflow UI**
   - Navigate to `http://localhost:8080`
   - Default credentials: `airflow/airflow`

5. **Initialize dbt**
   ```bash
   docker-compose exec dbt dbt deps
   docker-compose exec dbt dbt run
   ```

## 📊 Data Pipeline

### 1. Extraction Phase
- Automated web scraping of bank review platforms
- Data validation and initial cleaning
- Storage in staging tables

### 2. Transformation Phase 
- Data cleansing and standardization
- Sentiment analysis integration
- Aggregation and metric calculation


### 3. Loading Phase
- Final data models for analytics
- Performance metrics calculation
- Report generation

## 🔧 Configuration

### Airflow Configuration
- Configure connections in Airflow UI
- Adjust DAG schedules as needed

### dbt Configuration
- Update `profiles.yml` for database connections
- Configure model materialization strategies
- Set up test thresholds

## 📈 Key Metrics & Analytics

- **Customer Satisfaction Scores**: Branch-level satisfaction ratings
- **Sentiment Trends**
- **Service Quality Indicators**: Performance metrics by service type
- **Issue Identification**: Common complaint categorization


## 📝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request



## 🔍 Monitoring & Troubleshooting

- **Airflow Logs**: Available in Airflow UI under each DAG run
- **dbt Logs**: Check dbt run logs for transformation issues
- **Container Logs**: `docker-compose logs [service-name]`

## 🤝 Support

For questions or issues:
- Create an issue in this repository
- Review existing issues for similar problems



## 🙏 Acknowledgments

- Apache Airflow community

- Open source contributors and maintainers

---

**Built with ❤️ for better banking analytics in Morocco**
