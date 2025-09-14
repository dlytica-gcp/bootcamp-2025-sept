# Running the System

This project consists of two main services: **Apache NiFi** and **Apache Airflow**, both containerized with Docker Compose. Follow the steps below to bring the system up and running.

---

## Prerequisites

- Install [Docker](https://docs.docker.com/get-docker/)
- Install [Docker Compose](https://docs.docker.com/compose/)
- Ensure both NiFi and Airflow directories contain valid `docker-compose.yml` files.

---

## Steps to Run

1. Navigate to the **NiFi** directory and start the containers:

   ```bash
   cd nifi
   docker compose up -d


1. Navigate to the **airflow** directory and start the containers:

   ```bash
   cd airflow
   docker compose up -d