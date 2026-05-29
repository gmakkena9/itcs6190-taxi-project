# ITCS 6190 – Cloud Computing for Data Analysis
## Course Project: Data Analysis with Apache Spark

**Team:** Team 3 (Solo) — Gopi Bharath Makkena (GitHub: @gmakkena9)
**Chosen dataset:** NYC TLC Yellow Taxi Trip Records (+ Taxi Zone Lookup)
Source: <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>

### Project description (placeholder)
An end-to-end Apache Spark pipeline analyzing NYC Yellow Taxi trips. The project
will use the Structured (DataFrame) API for ingestion, cleaning, feature
engineering, and joins; Spark SQL to surface demand patterns; Structured
Streaming to process a simulated real-time feed of trip events; and MLlib to
train a regression model that predicts **trip duration** from distance, time of
day, and pickup location. _This description will be refined as the project
progresses (see the Proposal Issue)._

### Planned Spark components
- **Structured APIs** — typed ingestion, cleaning, feature engineering, zone join
- **Spark SQL** — demand-by-hour and top-pickup-zone aggregations
- **Structured Streaming** — micro-batched trip feed with running aggregations
- **MLlib** — trip-duration regression with evaluation metrics

### Status
Week 1 — project setup. Implementation lands incrementally via weekly PRs
Week 1 & Week 2 : ingestion + EDA
Week 2: streaming + MLlib · 
Week 3: full pipeline
