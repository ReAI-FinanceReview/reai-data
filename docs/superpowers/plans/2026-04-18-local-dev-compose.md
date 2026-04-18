# Local Dev Compose Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a local development stack with PostgreSQL and MinIO plus a matching env template and usage docs.

**Architecture:** Keep the scope intentionally small: one root `docker-compose.yml` for infrastructure services only, one `.env.local.example` for host-based local app execution, and one short guide explaining startup and endpoint mapping. Reuse the existing `docker-compose.test.yml` unchanged for test-only workflows.

**Tech Stack:** Docker Compose, PostgreSQL 15, MinIO, pytest, PyYAML

---

### Task 1: Guard the local dev contract with tests

**Files:**
- Create: `tests/test_local_dev_setup.py`

- [ ] **Step 1: Write the failing test**

```python
def test_local_dev_compose_declares_postgres_and_minio():
    compose = load_compose()
    assert "postgres" in compose["services"]
    assert "minio" in compose["services"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=. uv run pytest tests/test_local_dev_setup.py -q`
Expected: FAIL because `docker-compose.yml` and `.env.local.example` do not exist yet

- [ ] **Step 3: Write minimal implementation**

```yaml
services:
  postgres:
    image: postgres:15-alpine
  minio:
    image: minio/minio:RELEASE.2026-01-18T03-12-05Z
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=. uv run pytest tests/test_local_dev_setup.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_local_dev_setup.py docker-compose.yml .env.local.example docs/local-development.md
git commit -m "test: cover local dev compose setup"
```

### Task 2: Add the local development compose stack

**Files:**
- Create: `docker-compose.yml`

- [ ] **Step 1: Write the failing test**

```python
def test_postgres_and_minio_ports_are_exposed_for_host_use():
    compose = load_compose()
    assert compose["services"]["postgres"]["ports"] == ["5432:5432"]
    assert compose["services"]["minio"]["ports"] == ["9000:9000", "9001:9001"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=. uv run pytest tests/test_local_dev_setup.py -q`
Expected: FAIL because the initial compose file does not yet declare the required ports, healthcheck, command, and volumes

- [ ] **Step 3: Write minimal implementation**

```yaml
services:
  postgres:
    image: postgres:15-alpine
    ports:
      - "5432:5432"
  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"
      - "9001:9001"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=. uv run pytest tests/test_local_dev_setup.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml tests/test_local_dev_setup.py
git commit -m "feat: add local postgres and minio compose stack"
```

### Task 3: Add a host-friendly env template and docs

**Files:**
- Create: `.env.local.example`
- Create: `docs/local-development.md`

- [ ] **Step 1: Write the failing test**

```python
def test_local_env_template_uses_localhost_endpoints():
    env = read_env_template(".env.local.example")
    assert env["DATABASE_URL"] == "postgresql+psycopg2://reai:reai@localhost:5432/reai"
    assert env["MINIO_ENDPOINT"] == "localhost:9000"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=. uv run pytest tests/test_local_dev_setup.py -q`
Expected: FAIL because the env template and docs are missing

- [ ] **Step 3: Write minimal implementation**

```dotenv
DATABASE_URL=postgresql+psycopg2://reai:reai@localhost:5432/reai
MINIO_ENDPOINT=localhost:9000
MINIO_BUCKET=reai-data
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=. uv run pytest tests/test_local_dev_setup.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add .env.local.example docs/local-development.md tests/test_local_dev_setup.py
git commit -m "docs: document local development stack"
```
