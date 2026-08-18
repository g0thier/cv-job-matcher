# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Add scheduled and startup Airflow DAGs for Indeed offers around Geneva and Lausanne.
- Add Indeed search pagination, detail extraction, date-window filtering, deduplication, vectorization, persistence, and source-specific UI support.
- Add SeleniumBase/Xvfb browser support while reusing the Chromium installation managed by Playwright.

### Changed

- Pin SeleniumBase, packaging, and PyTorch releases compatible with the Airflow 2.9 dependency set.

## [1.3.0] - 2026-08-13

### Added

- Add scheduled and startup Airflow DAGs for JobUp offers in Geneva and Lausanne.
- Add JobUp collection, detail extraction, deduplication, persistence, tests, and source-specific UI support.
- Add a JobUp exploration notebook and source icon.

### Changed

- Normalize JobUp employment types and group descriptions into complete paragraphs for semantic matching.

### Removed

- Remove the obsolete manual title-embedding migration DAG and its dedicated migration helpers.

## [1.2.0] - 2026-07-15

### Added

- Add hourly and startup Airflow DAGs for État de Genève offers.
- Add RSS collection, offer enrichment, deduplication, vectorization, tests, and source-specific UI support.
- Add an État de Genève exploration notebook and source icon.

### Changed

- Rank LinkedIn and État de Genève offers together through the shared matching pipeline.
- Normalize whitespace and bullet lists to preserve complete description sections.

## [1.1.0] - 2026-07-11

### Added

- Add a startup LinkedIn ingestion DAG covering offers published since local midnight.
- Add automatic startup-DAG triggering with database-backed claims to prevent duplicate runs after restarts.
- Add title embeddings and prioritize search results by title similarity.
- Add hourly lookback and configurable result limits to the CLI and Streamlit interface.
- Filter already known offers before fetching LinkedIn detail pages.
- Preload the configured embedding model in the Docker image for offline execution.

### Fixed

- Handle missing optional fields on incomplete LinkedIn detail pages.
- Store title embeddings without Pandas assignment errors.
- Deduplicate offers before persistence to avoid unique-constraint failures.

## [1.0.0] - 2026-07-08

### Added

- Initialize the CV Job Matcher project.
- Add the scheduled LinkedIn ingestion DAG and configurable LinkedIn searches.
- Add the shared ingestion pipeline, PostgreSQL and pgvector persistence, text embeddings, and CV-to-offer semantic search.
- Add the Streamlit interface, CLI, Docker Compose environment, project documentation, and governance files.
