# LOL-DWH-PROJECT

This project builds a complete end-to-end data warehouse and ELT pipeline for professional League of Legends (LoL) e-sports matches.
The goal is to transform raw match logs into a dimensional model using Kimball methodology, and deliver analytical insights such as champion pick/ban priority, team composition strength, kill/death events, and player performance.
The project includes:
🏗️ Dimensional Data Warehouse (Kimball Style)
Fully designed star schema with conformed dimensions
Fact tables:
Champion draft picks
Champion bans
Kill & death combat events
Assist events
Dimension tables:
Match, Team, Player, Champion, Role
Models support analysis on player and champion synergy, team strategy, and combat efficiency.
⚙️ ELT Pipeline Architecture
This project uses a modern ELT approach:
Luigi → Orchestration
dbt → SQL transformation (staging → core → marts)
PostgreSQL → Data Warehouse (dockerized)
pgAdmin / DBeaver → Database management
Cron → Optional scheduling
Sentry → Error monitoring for production-style pipelines
The pipeline loads raw CSV sources, stages them, and transforms them into clean analytical models.
📂 Key Features
Raw → Staging → Core → Marts transformation flow
Reproducible warehouse schemas
End-to-end lineage via dbt
Modular and maintainable pipeline structure
Logging, temp directories, and environment configuration
Optional monitoring using Sentry DSN

Docker setup for Postgres environments
