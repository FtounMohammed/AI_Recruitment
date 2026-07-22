# AI Recruitment Platform

**An End-to-End Data Engineering Pipeline for Smart Hiring**

Authors: **Ftoon Al-Zanan** · **Rawaa Almohaimeed**
College of Computer and Information Sciences · Software Engineering

Completed under the **Modern Data Engineering for AI Systems** program — SDAIA Academy, DICO
Trainer: Mohammed Albeladi · July 2026

-----

## Project Description

Recruitment data typically arrives scattered, unvalidated, and unorganized — matching candidates to roles is slow and manual, and bad records can reach reporting undetected.

This project builds a **governed, end-to-end data pipeline** that streams job applications, validates them at the ingestion boundary, stores them in a lakehouse with enforced schema and upserts, and answers hiring-related questions using grounded, hybrid AI search — all orchestrated automatically with quality gates that halt the pipeline when data is bad.

All five stages run on real, production-grade libraries — no simulated components.

## Scope

|Stage              |Tools                                                           |
|-------------------|----------------------------------------------------------------|
|Streaming ingestion|Kafka + Pydantic data contract                                  |
|Lakehouse          |Delta Lake — Bronze / Silver / Gold                             |
|AI search          |Hybrid RAG (dense + BM25 + RRF) with cross-encoder reranking    |
|Orchestration      |Apache Airflow                                                  |
|Governance         |Great Expectations (quality gate) + OpenLineage (lineage events)|

## System Architecture

```
Kafka Ingestion → Bronze (Delta) → Silver (Delta MERGE) → Quality Gate → Gold (Aggregates) → RAG (AI Search)
```

- **Apache Airflow** orchestrates every stage; a failed quality gate halts the pipeline before downstream stages run.
- **Great Expectations** and **OpenLineage** emit START / COMPLETE / FAIL events per stage.
- Rejected records are routed to a dead-letter topic before they ever reach Bronze.

## Pipeline Stages

### 1. Ingestion — Kafka & Data Contract

- A `kafka-python` producer publishes applicant events to the `applicants` topic.
- A **Pydantic** model enforces the data contract: positive IDs, experience between 0–50, non-empty skills.
- Valid records continue to Bronze; invalid ones are diverted, never silently dropped.
- Rejects are published to `applicants_dlq` with the exact rejection reason recorded.

### 2. Delta Lakehouse — Bronze / Silver / Gold

- **Bronze**: raw validated events written in Delta format, one row per accepted application.
- **Silver**: deduplicated and joined to jobs via a real Delta **MERGE** keyed on `applicant_id` (`whenMatchedUpdateAll` · `whenNotMatchedInsertAll`).
- **Gold**: genuine aggregates — applications per job, average experience per role, salary statistics per city.

**Schema enforcement**: a write with a mismatched type (e.g. `applicant_id` as a string) is refused by Delta at the storage layer — the table schema is never silently widened.

### 3. RAG Pipeline — Hybrid Search

1. **Chunking** — job and applicant profiles split with overlap
1. **Embeddings** — multilingual sentence-transformers model
1. **Vector store** — Chroma with cosine similarity
1. **Hybrid + RRF** — dense results fused with BM25 keyword ranks (`score = Σ 1 / (60 + rank)`)
1. **Reranking** — cross-encoder reorders the fused candidates
1. **Grounded answer** — the response cites the job or applicant it came from

### 4. Data Quality & Lineage

Great Expectations checks that gate the pipeline:

- `applicant_id` and `job_id` must not be null
- `experience_years` must fall between 0 and 50
- `salary` must be greater than zero
- `application_id` must be unique

A failed expectation raises an error and stops the run before Gold or RAG execute. OpenLineage emits a run event (START / COMPLETE / FAIL) for every stage, so the full execution history is traceable.

### 5. Orchestration with Airflow

DAG: `ingestion → lakehouse → quality_gate → gold_aggregation → rag_index`

- **Clean run**: all five tasks succeed; the gate passes and both downstream stages run to completion.
- **Corrupted run**: `quality_gate` fails; `gold_aggregation` and `rag_index` are marked `upstream_failed` and never execute.

## Repository Structure

```
.
├── 01_ingestion.ipynb          # Kafka producer/consumer + Pydantic data contract
├── 02_ingestion.ipynb          # Bronze/Silver Delta writes, schema enforcement test
├── 03_rag.ipynb                # Chunking, embeddings, hybrid search, reranking
├── 04_quality_lineage.ipynb    # Great Expectations gate + OpenLineage events
├── 05_orchestration.ipynb      # Airflow DAG definition and run logs
├── data/                       # Raw and sample application data
├── delta/                      # Delta Lake tables (Bronze / Silver / Gold)
├── output/                     # Pipeline outputs and run artifacts
└── README.md
```

## Prerequisites

- Python 3.10+
- Apache Kafka (local broker or Docker container)
- Java 8/11 (required by PySpark / Delta Lake)
- Apache Airflow
- `pip` packages: `kafka-python`, `pydantic`, `pyspark`, `delta-spark`, `chromadb`, `sentence-transformers`, `great-expectations`, `openlineage-python`

## Setup & Installation

```bash
# Clone the repository
git clone https://github.com/<your-username>/ai-recruitment-platform.git
cd ai-recruitment-platform

# Create a virtual environment
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Start Kafka locally (if using Docker)
docker-compose up -d
```

## How to Run

1. **Ingestion** — run `01_ingestion.ipynb` and `02_ingestion.ipynb` to start the Kafka producer/consumer and write validated records into the Bronze/Silver Delta tables.
1. **RAG** — run `03_rag.ipynb` to build the vector index and query the hybrid search pipeline.
1. **Quality & Lineage** — run `04_quality_lineage.ipynb` to execute the Great Expectations suite and emit OpenLineage events.
1. **Orchestration** — run `05_orchestration.ipynb` or trigger the Airflow DAG directly to execute the full pipeline end to end.

## Expected Output

- A quarantine table of rejected applicant records with rejection reasons.
- Delta table row counts before/after MERGE, and `history()` output showing the MERGE operation.
- A caught schema-enforcement exception when an invalid write is attempted.
- Side-by-side dense / BM25 / RRF rankings with a grounded, cited answer.
- A passing Great Expectations run vs. a deliberately corrupted run showing a FAIL lineage event.
- Two Airflow run logs: one successful end-to-end run, and one halted at the quality gate.

## Results

- **Validated ingestion** — malformed applications are quarantined with a recorded reason before storage.
- **Governed lakehouse** — Delta layers with an upsert on the business key and enforced schema.
- **Grounded AI search** — hybrid retrieval with reranking returns answers tied to their sources.
- **Automated pipeline** — a single DAG runs the flow and stops it when data quality fails.

## Training Program Attribution

This project was completed under the **Modern Data Engineering for AI Systems** training program at **SDAIA Academy** (DICO), July 2026 cohort, under trainer Mohammed Albeladi.



## Authors

- **Ftoon Al-Zanan**
- **Rawaa Almohaimeed**