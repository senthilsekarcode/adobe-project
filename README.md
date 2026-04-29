# Adobe Data Engineer Assessment – Search Attribution Pipeline

## Table of Contents

1. [Project Overview](#project-overview)
2. [Business Question](#business-question)
3. [Input Data and Appendix Mapping](#input-data-and-appendix-mapping)
4. [Solution Approach](#solution-approach)
5. [Output (Required Deliverable)](#output-required-deliverable)
6. [Repository Structure](#repository-structure)
8. [Run Instructions (AWS Free Tier)](#run-instructions-aws-free-tier)
9. [Testing](#testing)
10. [Assumptions](#assumptions)
11. [Scalability Considerations (10GB+)](#scalability-considerations-10gb)
13. [Future Improvements](#future-improvements)

---

## Project Overview

This project implements a **Version 1 batch Python pipeline** for Adobe’s Data Engineer programming exercise.

The pipeline reads hit-level tab-separated data (`data.txt`), identifies visits originating from external search engines, attributes purchase revenue to search keywords, and generates the required tab-delimited output for client review.

---

## Business Question

> How much revenue is the client getting from external search engines (e.g., Google, Yahoo, MSN), and which keywords are performing best based on revenue?

---

## Input Data and Appendix Mapping

The implementation follows the assessment’s Appendix A/B structure:

- **Appendix A fields used**:
  - `hit_time_gmt`
  - `date_time`
  - `user_agent`
  - `ip`
  - `geo_city`, `geo_region`, `geo_country`
  - `pagename`, `page_url`
  - `referrer`
  - `event_list`
  - `product_list`

- **Appendix A event mapping used**:
  - `1` = Purchase
  - `2` = Product View
  - `10` = Shopping Cart Open
  - `11` = Shopping Cart Checkout
  - `12` = Shopping Cart Add
  - `13` = Shopping Cart Remove
  - `14` = Shopping Cart View

- **Appendix B product_list parsing**:
  - Products are comma-delimited
  - Product attributes are semicolon-delimited
  - Revenue is parsed from the 4th semicolon field (`Total Revenue`)
  - Revenue is attributed only when purchase event (`1`) is present

---

## Solution Approach

### 1) Extract
- Read TSV input safely using `csv.DictReader` with tab delimiter.
- Convert each row into typed hit records.

### 2) Transform
- Parse `referrer` URL to detect external search traffic.
- Extract:
  - Search engine domain (`google.com`, `bing.com`, `search.yahoo.com`)
  - Search keyword (`q`, `p`, etc.)
- Build session identity using:
  - `ip + user_agent`
  - 30-minute inactivity timeout
- Apply **first-touch attribution** within each session.
- Detect purchase via `event_list` containing `1`.
- Parse and sum revenue from `product_list`.

### 3) Load / Output
- Generate required deliverable `.tab` file.
- Generate helper summary outputs for validation and analysis.

---

## Output (Required Deliverable)

The required file is generated at runtime in `outputs/`:

YYYY-mm-dd_SearchKeywordPerformance.tab

## Repository Structure
```
adobe-project/
├─ data.txt
├─ main.py
├─ README.md
├─ Data-Engineer_Applicant_Programming_Exercise.pdf
├─ src/
│  ├─ __init__.py
│  └─ pipeline.py
├─ tests/
│  └─ test_pipeline.py
├─ outputs/
│  └─ (runtime-generated artifacts)
└─ docs/
   ├─ aws-runbook.md
   └─ screenshots/
```
## Run Instructions (AWS Free Tier)
```
AWS execution evidence and command transcript are documented in
docs/aws-runbook.md
Screenshots are stored under:
docs/screenshots
```

## Testing
```
The test suite includes:
1. Referrer parsing tests
  Search engine detection
  Keyword extraction
  Internal referrer exclusion
2. Event and revenue tests
  Purchase event detection (event_list)
  Product revenue parsing (product_list)
3. End-to-end tests
  Pipeline execution
  Required output file creation
  Required header validation
  Processed row count validation
```

## Assumptions
```
Session definition: ip + user_agent with 30-minute inactivity timeout.
Attribution model: first external search touchpoint in a session.
External search coverage: Google, Bing/MSN, Yahoo.
Purchase detection: event_list includes event 1.
Revenue source: 4th product_list semicolon field (Total Revenue).
Monetary formatting: output revenue rounded to 2 decimals.
```

## Scalability Considerations (10GB+)
```
The current implementation is suitable for this assessment dataset. For production-scale files (>10GB), recommended enhancements are:

1. Streaming-first architecture
    Continue strict row-by-row processing to minimize memory use.
    Avoid full-file materialization.
2. Externalized state for attribution
    Move session state from in-memory dict to Redis/RocksDB for high-cardinality traffic.
    Add bounded TTL/eviction policies.
3. Partitioning and parallelism
    Partition by date or hashed visitor key.
    Use multiprocessing for single-node scale.
    Use Spark (EMR/Glue) for distributed execution.
4. Cloud-native data layout
    Land raw files in S3.
    Convert TSV to partitioned Parquet for downstream analytics.
    Query with Athena/Redshift as needed.
5. Reliability and observability
    Add structured logs, metrics, malformed-record counters.
    Add checkpoint/restart strategy and idempotent runs.
    Add performance tests with large synthetic datasets.
6. Cost-performance optimization
    Use EC2 for small/adhoc jobs.
    Use EMR/Glue for recurring large-scale workloads.
```
## Future Improvements
```
1. Add CLI flags for:
    attribution model (first-touch / last-touch)
    custom session timeout
    configurable output naming
2. Add schema validation with clearer row-level diagnostics.
3. Add integration test fixtures for edge-case referrer/event formats.
4. Add CI pipeline (GitHub Actions) for automatic test execution.
```

