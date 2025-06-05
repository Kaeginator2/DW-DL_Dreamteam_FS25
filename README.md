# SkyChain: Linking Weather, Aviation, and Air Quality through Data

This repository contains all code and resources for the SkyChain project developed in the Datawarehouse and Data Lakes course at HSLU (Lucerne University of Applied Sciences and Arts). The project combines aviation, weather, and pollution data to enable interactive, persona-driven insights via a modern AWS-based data infrastructure.

## Project Overview

SkyChain explores how weather conditions and air pollution affect airport operations and flight traffic. Using real-time APIs, we ingest and integrate environmental and aviation datasets into a cloud-native data lake and data warehouse. The results are visualized in a responsive Tableau dashboard tailored to the needs of airline operation managers and frequent travelers.

See the public Tableau dashboard:  
https://public.tableau.com/app/profile/nicolas.borer/viz/Cockpit_DWDL/Story1

## Architecture Summary

- Ingestion: AWS Lambda and EventBridge for automated API calls
- Storage: AWS S3 as Data Lake; Amazon Aurora MySQL for Data Warehouse
- Transformation: AWS Glue ETL jobs (flattening, cleaning, type mapping)
- Visualization: Tableau dashboards using RDS and .hyper extracts

## File Descriptions

- `Cockpit_DWDL.twb`  
  Tableau workbook with interactive dashboards and world maps. Visualizes KPIs like delays, CO levels, and weather conditions. Connects to RDS and `.hyper` extract for performance.

- `DreamTeam_apiWeatherMapData.py`  
  Lambda function using OpenWeatherMap Weather API to fetch hourly weather data. Stores `.json` files in S3 using location coordinates.

- `lambda_function_pollution.py`  
  Retrieves hourly air pollution data using the OpenWeatherMap Air Pollution API. Outputs metrics like CO, PM2.5, and AQI to S3 in time-partitioned folders.

- `DreamTeam_get_updaten_flight_airports.py`  
  Downloads and updates airport metadata from OurAirports. Performs a full refresh and stores structured CSV in S3.

- `flight_lambda.py`  
  Daily ingestion of flight departure data via FlightAPI.io. Data is fetched in batches, then consolidated into a full `.json` and saved in S3.

- `RDS.sql`  
  Defines schema for `airports`, `weather_recordings`, `pollution_recordings`, and `flight_recordings` tables. Used to initialize the Aurora MySQL data warehouse.

- `zipper.py`  
  Helper Lambda function to compress data from the learner lab S3 bucket for local download and migration to the production environment.
  
- `flight_transform.py`  
  AWS Lambda function that transforms daily grouped flight data from the data lake into a flat, analytics-ready CSV format. I
  This serves as the primary transformation step from raw flight API JSON to structured warehouse input.

## How to Run

### AWS-Based ETL Pipelines
1. Deploy Lambda Functions for:
   - Weather and Pollution ingestion (hourly)
   - Flight data ingestion (daily, batched)
2. Use EventBridge to trigger each function as per schedule.
3. Store API output in S3 with structured, time-based folder hierarchy.
4. Process data via AWS Glue, transforming JSON to analytics-ready tables in Aurora.

### Tableau Visualization
1. Install Tableau Desktop
2. Open `Cockpit_DWDL.twb`
3. Connect to Amazon RDS and/or `.hyper` extract
4. Filter by airport, date, or KPI to explore insights

## Personas and Use Cases

- Airline Operations Manager  
  Optimize scheduling, anticipate delays, monitor environmental risk.

- Frequent Traveler  
  Assess delay risk and plan accordingly using real-time conditions.
