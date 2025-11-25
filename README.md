# LOL-DWH-PROJECT

This project makes a full data warehouse and ELT pipeline for professional League of Legends (LoL) e-sports matches. The goal is to turn raw match logs into a dimensional model using Kimball methodology and give analytical insights like champion pick/ban priority, team composition strength, kill/death events, and player performance.
---

## 🚀 Dimensional Data Warehouse (Kimball Style)

This project includes a fully designed star schema with conformed dimensions.

### **Fact Tables**
- Champion draft picks  
- Champion bans  
- Kill & death combat events  
- Assist events  

### **Dimension Tables**
- Match  
- Team  
- Player  
- Champion  
- Role  

These models support analysis on player–champion synergy, team strategy, and combat efficiency.

---

## ⚙️ ELT Pipeline Architecture

This project uses a modern ELT approach:

- **Luigi** → Orchestration  
- **dbt** → SQL transformations (staging → core → marts)  
- **PostgreSQL (Dockerized)** → Data Warehouse  
- **pgAdmin / DBeaver** → Database management  
- **Cron** → Optional scheduling  
- **Sentry** → Error monitoring for production-style pipelines  

The pipeline loads raw CSV sources, stages them, and transforms them into clean analytical models.

---

## 📦 Key Features

- Raw → Staging → Core → Marts transformation flow  
- Reproducible warehouse schemas  
- End-to-end lineage via dbt  
- Modular and maintainable pipeline structure  
- Logging directories & environment configuration  
- Optional monitoring using Sentry DSN  
- Docker setup for Postgres environments  

---
