# Data Pipeline Project

This project implements a data pipeline for processing and storing orders and inventory data in MongoDB. It includes data validation, removal of duplicates, enrichment of the data, and querying for business insights, such as best-selling products and inventory status.

## Table of Contents
- [Prerequisites](#prerequisites)
- [Project Setup](#project-setup)
- [Running the Application](#running-the-application)
- [File Structure](#file-structure)
- [Notes](#notes)

---

## Prerequisites

Before you start, ensure you have the following tools installed:

- [Docker](https://www.docker.com/get-started) (for containerization)
- [Docker Compose](https://docs.docker.com/compose/install/) (to manage multi-container applications)
- [Git](https://git-scm.com/) (to clone the repository)

Ensure you have access to the MongoDB container (which runs on port `27017`).

---

## Project Setup

1. **Clone the repository**:
   
   ```bash
   git clone https://github.com/your-username/data_pipeline.git
   cd data_pipeline


## Running the Application

1. **Build and run the containers**:

    ```bash
    docker-compose up --build

2. **Check logs**:

    ```bash
    docker logs python_app

## File Structure

    data_pipeline/
    ├── data/
    │   ├── raw/            # Raw input data (orders.csv, inventory.csv)
    │   └── processed/      # Processed output data (after validation and cleaning)
    ├── src/
    │   ├── ingestion.py    # Data loading functions
    │   ├── validation.py   # Data validation functions
    │   ├── mongodb_utils.py # MongoDB interaction functions
    │   └── main.py         # Main script to run the pipeline
    ├── Dockerfile          # Dockerfile to build the Python application
    ├── docker-compose.yml  # Docker Compose configuration
    ├── requirements.txt    # Python dependencies
    └── README.md           # Project documentation

## Note

    The MongoDB collections are configured as follows:

    raw_orders: Stores the raw orders data.
    raw_inventory: Stores the raw inventory data.
    orders: Stores the processed orders data (after validation and deduplication).
    inventory: Stores the processed inventory data (after enrichment and updates).
    The project is designed to be easily extendable, so you can add more processing steps as needed.

    Ensure that your data files (orders.csv and inventory.csv) are correctly formatted before running the pipeline.