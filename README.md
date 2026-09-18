# PySpark on Azure Databricks Academy

A job-focused PySpark data engineering course on Azure Databricks, currently
under active development. The course is designed to progress from beginner
Spark fundamentals to production batch data engineering, using a single
connected [rideshare dataset](docs/data/dataset-guide.md) as the running
example throughout, and to culminate in a deployable batch data engineering
project.

This course covers **batch data engineering only**. Structured Streaming,
Auto Loader, streaming tables, machine learning, and general Azure
infrastructure administration are out of scope.

## Who this is for

You should already know:
- Basic Python syntax
- Basic SQL (not advanced SQL)

You do **not** need prior experience with:
- Apache Spark or Azure Databricks
- Production data engineering
- Local Databricks development/deployment workflows

To complete the hands-on exercises, you need access to an Azure Databricks
workspace and permission to use suitable compute. Module-specific environment
and privilege requirements are documented in each module's `README.md`.

This course is authored and maintained in [Cursor](https://cursor.com). You
still run every notebook in Azure Databricks (Git folder). Cursor is how the
materials are written; it is not required to finish the labs.

Unfamiliar concepts are explained before they're used.

## Technical baseline

| Component | Version / Detail |
|---|---|
| Cloud platform | Microsoft Azure |
| Platform | Azure Databricks, Premium tier |
| Databricks Runtime | 17.3 LTS |
| Apache Spark | 4.0.0 |
| Python | 3.12 |
| Scala runtime | 2.13 |
| Primary language | Python with PySpark |
| SQL | Spark SQL in Databricks notebooks (`%sql` and `spark.sql()`) |
| Governance | Unity Catalog |
| Version control | GitHub |
| Notebook format | Databricks source `.py` notebooks |
| Course authoring | [Cursor](https://cursor.com) |

Compute is selected per module (classic all-purpose Standard/Dedicated, jobs
compute, or serverless) based on that module's APIs and learning objectives.
Each module's `README.md` states what that module needs.

## Where to start

The course runs in five phases, from Spark and Databricks foundations to a
deployable batch capstone — see the [phase list](COURSE_MODULES.md#phases).

- **Full roadmap and current status:** [`COURSE_MODULES.md`](COURSE_MODULES.md) — module purposes, topics, prerequisites, and planned progression
- **Start here (learners):** [`01 - Azure Databricks and Spark Foundations`](01%20-%20Azure%20Databricks%20and%20Spark%20Foundations/)
- **Modules 1–2:** Read the matching page in the course Notion hub before each Module 1 notebook and Module 2 notebooks **01–05**. The notebooks are labs.
- **Before Module 5:** Review the [module-specific environment and privilege requirements](05%20-%20Reading%2C%20Writing%2C%20and%20Schemas/README.md#before-notebook-01).

## License

Copyright (c) 2026 Rajsekhar. All rights reserved. These materials are for
**personal learning only**. You may clone the repository and run the
notebooks as a student. You may not use them for production, commercial
training, or republish them as your own course. See [`LICENSE`](LICENSE).


## This repository

This is the learner GitHub repository. In Azure Databricks, create a Git
folder that points here — not at the author/maintainer repository.

The course is authored in [Cursor](https://cursor.com). You do not need Cursor
installed to complete the labs.

`data/lab/fare_dv_lab.parquet` (~300 MB) is not stored in Git. Your instructor
will share a download link before Module 11 notebook **00**. Place the file at
`data/lab/fare_dv_lab.parquet`.

See [`LICENSE`](LICENSE). Use is limited to personal learning.
