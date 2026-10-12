# 🧊 Iceberg Lakehouse Lab: Docker Ecosystem

This is a self-contained data lakehouse that runs on one Linux machine (tested on Fedora with SELinux enforcing). It uses **Apache Iceberg** tables stored in **MinIO**, cataloged by **Project Nessie**, and queried by **Spark, Trino, Flink and Dremio**.

| Component | Image | Role |
|---|---|---|
| MinIO | `alpine/minio:RELEASE.2025-10-15T17-29-55Z` | S3-compatible object storage; holds every data and metadata file |
| mc | `amazon/aws-cli:latest` | One-shot job that creates the `warehouse` and `landing` buckets, then exits |
| nessie-db | `postgres:16-alpine` | Nessie's database for commits, branches and pointers |
| Nessie | `ghcr.io/projectnessie/nessie:0.99.0` | Iceberg catalog with Git-like branches |
| Spark | `tabulario/spark-iceberg:3.5.5_1.8.1` | Batch ETL and Jupyter (Spark 3.5.5 + Iceberg 1.8.1) |
| Trino | `trinodb/trino:449` | Interactive SQL for DBeaver and Power BI |
| Flink | `iceberg-lab/flink:1.18.1` (built locally) | Streaming engine (Flink 1.18.1 + Iceberg 1.5.2) |
| Dremio | `dremio/dremio-oss:latest` | SQL UI, semantic layer, reflections, Power BI via Arrow Flight |

---

## 1. 📁 Folder layout

```text
iceberg-lab/
├── docker-compose.yml          # every service, network and volume
├── .env                        # credentials (MinIO root = S3 keys for all)
├── Makefile                    # shortcuts: make up / smoke / trino / health
├── spark/conf/spark-defaults.conf   # Spark catalog "nessie"
├── trino/catalog/
│   ├── iceberg.properties      # Trino catalog "iceberg" (Nessie REST)
│   └── tpch.properties         # built-in sample data generator
├── flink/
│   ├── Dockerfile              # adds Iceberg/Nessie/S3/Kafka jars to Flink
│   └── sql/01_bootstrap.sql    # CREATE CATALOG for every Flink SQL session
├── scripts/                    # mounted into Spark at /home/iceberg/scripts
│   ├── 00_smoke_test.py
│   └── 02_trino_queries.sql
└── notebooks/                  # mounted into Jupyter as "lab/"
```

**Rule:** configuration lives in host files that are bind-mounted into the containers. Data lives in named volumes (`minio_data`, `nessie_pg_data`, `dremio_data`). Anything else written inside a container is lost when the container is recreated.

---

## 2. 🗺️ How the components work together

```text
              Jupyter · DBeaver · Power BI · Dremio UI          (consumption)
                                   │
        ┌────────────┬─────────────┼─────────────┬────────────┐
      Spark        Trino         Flink        Dremio                (engines)
   Nessie API   Iceberg REST   Nessie API   Nessie API
        └────────────┴──────┬──────┴─────────────┘
                            ▼
                  Nessie  ──►  PostgreSQL (nessie-db)              (catalog)
            "table → current metadata.json"
                            │  (location only)
                            ▼
   MinIO  s3://warehouse/<ns>/<table>_<uuid>/{metadata,data}/      (storage)
```

The three layers have separate jobs:

- **MinIO only stores bytes.** It has no idea what tables, rows or schemas are.
- **Iceberg is a library inside each engine, not a separate server.** It writes and reads `metadata.json` → manifest list → manifests → Parquet data and delete files.
- **Nessie stores a pointer and nothing else.** For each table it keeps the location of the current `metadata.json`, as a commit on a branch. Nessie never reads or writes MinIO for Spark. It returns the location, and the engine's Iceberg library reads the files itself.

### Read path (e.g. `SELECT * FROM nessie.lab.sales`)
1. The engine's Iceberg library asks Nessie for `lab.sales` on branch `main`.
2. Nessie answers with the `metadataLocation`, which it looks up in PostgreSQL.
3. The engine reads `metadata.json` from MinIO and finds the current snapshot.
4. It reads that snapshot's **manifest list**. The list can include both **data manifests** and **delete manifests**.
5. It reads the manifests and uses partition values and min/max statistics to skip files that can't match.
6. It reads only the needed Parquet columns and row groups, then applies any delete files.

### Write path (e.g. `INSERT` / `MERGE`)
1. The engine writes new Parquet files to `…/data/`. Existing files are never edited.
2. It writes new manifests, a new manifest list and a new `metadata.json`.
3. It asks Nessie to **atomically** move the pointer from the old metadata file to the new one. If another writer committed first, the engine re-reads and retries (optimistic concurrency).
4. Nessie records the commit in PostgreSQL, and the new data is now visible to every engine.

### Two ways engines reach Nessie
| Engine | Protocol | Endpoint (inside the network) | Iceberg integration |
|---|---|---|---|
| Spark | Nessie native API | `http://nessie:19120/api/v2` | `iceberg-spark-runtime` (bundled in the image) |
| Flink | Nessie native API | `http://nessie:19120/api/v2` | jars added by `flink/Dockerfile` |
| Dremio | Nessie native API | `http://nessie:19120/api/v2` | built in; configured in the UI |
| Trino | Iceberg REST | `http://nessie:19120/iceberg/main` | bundled connector; `iceberg.properties` |

---

## 3. ✅ Prerequisites

- Docker Engine 24+ with **Compose v2** (`docker compose version`)
- RAM: **12–16 GB** for the full stack. Dremio alone can use up to 6 GB.
- About 30 GB of free disk. Internet access is needed only for the first pull and build.
- **Fedora/RHEL (SELinux):** the compose file already labels bind mounts (`:z` / `:Z`). Don't disable SELinux.

---

## 4. 🚀 First startup

```bash
cd ~/iceberg-lab                      # Download the repo and add the all components in folder and name it with "iceberg-lab"
docker compose config --quiet && echo "YAML OK"

# optional: confirm every image tag exists before pulling
for img in $(grep -hoP '^\s*image:\s*\K\S+' docker-compose.yml | grep -v '^iceberg-lab/'); do
  docker manifest inspect "$img" >/dev/null 2>&1 && echo " OK $img" || echo " MISSING $img"
done

docker compose up -d --build          # or: make up
docker compose ps -a                  # or: make ps
```

### Startup order (enforced by `depends_on`)
1. **minio**, **nessie-db** and **dremio** start in parallel.
2. **mc** starts once MinIO is *healthy*, creates the buckets and exits with **`Exited (0)`**. That status is correct.
3. **nessie** starts once PostgreSQL is *healthy*.
4. **trino** and **flink-jobmanager** start once Nessie is *healthy*.
5. **spark-iceberg** starts once Nessie is *healthy* **and** mc has *completed successfully*.
6. **flink-taskmanager** starts after the jobmanager.

### Verify
```bash
make health                            # minio 200 / nessie 200 / trino 200
docker logs mc                         # "Buckets ready: warehouse, landing"
make smoke                             # Spark creates nessie.lab.smoke with 3 rows
docker exec trino trino --execute "SELECT * FROM iceberg.lab.smoke"
```
> Trino can answer *"Trino server is still initializing"* for up to a minute after it starts. Wait for it. Restarting Trino starts the initialization over.

---

## 5. 🌐 Endpoints and credentials

| Service | From your machine | From another container | Login |
|---|---|---|---|
| MinIO console | http://127.0.0.1:9001 | — | `admin` / `password` |
| MinIO S3 API | http://127.0.0.1:9000 | `http://minio:9000` | same keys |
| Nessie UI/API | http://localhost:19120 | `http://nessie:19120/api/v2` | none |
| Jupyter | http://localhost:8888 | — | token: `docker exec spark-iceberg jupyter server list` |
| Spark UI | http://localhost:8080 · :4040 | — | — |
| Trino | http://localhost:**8082** | `http://trino:8080` | user `admin`, no password |
| Flink UI | http://localhost:8081 | `flink-jobmanager:8081` | — |
| Dremio | http://localhost:9047 · Flight `:32010` | `dremio:9047` | admin created on first visit |
| PostgreSQL | not published | `nessie-db:5432` | `nessie` / `nessie` |

⚠️ **Never use `localhost` inside a config file.** Inside a container, `localhost` means that container itself. Configs must use service names (`minio`, `nessie`) and container ports (Trino's is `8080`, not `8082`).

MinIO is published only on `127.0.0.1` because this MinIO build has a known authentication-bypass issue. Keep it that way.

---

## 6. 🧪 Using each engine

### Spark (Jupyter or CLI): catalog name **`nessie`**
```bash
docker exec -it spark-iceberg spark-sql
```
```sql
CREATE NAMESPACE IF NOT EXISTS nessie.lab;
CREATE TABLE nessie.lab.sales (id INT, city STRING, amount DECIMAL(10,2), ts TIMESTAMP)
  USING iceberg PARTITIONED BY (days(ts));
INSERT INTO nessie.lab.sales VALUES (1,'Riyadh',100.00,current_timestamp());
UPDATE nessie.lab.sales SET amount = 120 WHERE id = 1;
SELECT * FROM nessie.lab.sales.snapshots;      -- metadata tables
SELECT * FROM nessie.lab.sales.files;
```
Reading raw files from the **landing** bucket and loading them into Iceberg in the **warehouse** bucket:
```python
df = spark.read.csv("s3a://landing/customers/customers.csv", header=True, inferSchema=True)
df.writeTo("nessie.bronze.customers").createOrReplace()
```
> `s3a://` uses Hadoop S3A (`spark.hadoop.fs.s3a.*`), which is a different client from Iceberg's S3FileIO. If S3A isn't configured in your image, upload the files to `./scripts` and read them from `/home/iceberg/scripts/`.

New tables go to `s3://warehouse/` by default because `spark.sql.catalog.nessie.warehouse` points there. Override it per table with `LOCATION`, or add a second catalog.

### Trino (CLI or DBeaver): catalog name **`iceberg`**
```bash
docker exec -it trino trino --catalog iceberg     # or: make trino
```
DBeaver settings: driver **Trino**, host `localhost`, port **8082**, user `admin`, empty password, SSL off. JDBC URL: `jdbc:trino://localhost:8082`.
```sql
SHOW SCHEMAS FROM iceberg;
SELECT * FROM iceberg.lab."sales$snapshots";
CREATE TABLE iceberg.lab.orders AS SELECT * FROM tpch.tiny.orders;   -- ETL from sample data
```

### Flink SQL: catalog name **`iceberg`** (it has to be created in every session)
```bash
make flink     # sql-client with -i /opt/flink/sql/01_bootstrap.sql
```
Flink commits to Iceberg at each checkpoint (every **30 s**), so new rows become visible on that schedule.

### Dremio: source name **`nessie`** (configured in the UI)
Open http://localhost:9047, create the admin user, then go to **Sources → Add Source → Nessie**:

| Field | Value |
|---|---|
| Name | `nessie` |
| Nessie endpoint URL | `http://nessie:19120/api/v2` |
| Authentication | None |
| AWS access / secret key | `admin` / `password` |
| AWS root path | `warehouse` |
| Encrypt connection | off |
| Connection properties | `fs.s3a.path.style.access=true` · `fs.s3a.endpoint=minio:9000` · `dremio.s3.compat=true` |

### Nessie branches (Write-Audit-Publish)
```sql
-- Spark
CREATE BRANCH IF NOT EXISTS etl_dev IN nessie FROM main;
USE REFERENCE etl_dev IN nessie;
-- ...write and validate...
MERGE BRANCH etl_dev INTO main IN nessie;
```
```bash
curl -s http://localhost:19120/api/v2/trees | python3 -m json.tool
```

### Inspecting files in MinIO
Use the console at http://127.0.0.1:9001 and go to `warehouse/<namespace>/<table>_<uuid>/metadata` and `/data`. To list from the command line:
```bash
docker run --rm --network iceberg-lab_iceberg_net -e AWS_ACCESS_KEY_ID=admin \
  -e AWS_SECRET_ACCESS_KEY=password -e AWS_DEFAULT_REGION=us-east-1 \
  amazon/aws-cli --endpoint-url http://minio:9000 s3 ls s3://warehouse/ --recursive
```

---

## 7. 🛠️ Daily operations

| Task | Command |
|---|---|
| Start / resume | `docker compose up -d` |
| Pause (keep containers and data) | `docker compose stop` · `docker compose start` |
| Remove containers, keep data | `docker compose down` |
| **Wipe everything** | `docker compose down -v` (deletes all tables, history and Dremio setup) |
| Logs | `docker compose logs --tail=100 -f <service>` |
| Resources | `docker stats --no-stream` |
| Save RAM | `docker compose stop dremio flink-jobmanager flink-taskmanager` |

### After changing a configuration file
| You changed | Action |
|---|---|
| `.env` or `docker-compose.yml` | `docker compose up -d` (**not** `restart`) |
| `spark-defaults.conf` | Start a new Spark session (restart the notebook kernel). If the change still doesn't show: `docker compose up -d --force-recreate spark-iceberg` |
| `trino/catalog/*.properties` | `docker compose restart trino` |
| `flink/Dockerfile` | `docker compose build flink-jobmanager && docker compose up -d` |
| Nessie environment | `docker compose up -d nessie` |
| Dremio source | Save in the UI |

### Changing the MinIO password
Update it in **all** of these places: `.env`, `trino/catalog/iceberg.properties` (it doesn't read `.env`), the Dremio source settings, and any scripts where the keys are hard-coded. Then run `docker compose up -d && docker compose restart trino`.

### Backup (storage and catalog must come from the same moment)
```bash
mkdir -p backups && docker compose stop
for v in minio_data nessie_pg_data dremio_data; do
  docker run --rm -v iceberg-lab_$v:/src -v "$PWD/backups:/dst" alpine \
    tar czf /dst/$v-$(date +%Y%m%d).tgz -C /src .
done
docker compose start
```
⚠️ Never reset just one of `minio_data` or `nessie_pg_data`. Wiping only MinIO leaves Nessie pointing at files that no longer exist. Wiping only PostgreSQL leaves orphaned files.

---

## 8. 🩺 Troubleshooting (check from the bottom of the stack up)

1. `docker compose ps -a`: every service should be Up or healthy, and `mc` should show `Exited (0)`. `Exited (137)` means the container ran out of memory.
2. Storage: `curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:9000/minio/health/live` should return `200`.
3. Catalog database: `docker exec nessie-db pg_isready -U nessie`
4. Catalog: `curl -s localhost:19120/api/v2/trees` should return JSON that includes `main`.
5. Network from inside a container: `docker exec trino curl -s -o /dev/null -w '%{http_code}\n' http://nessie:19120/api/v2/config`
6. Check that the config reached the container: `docker exec spark-iceberg cat /opt/spark/conf/spark-defaults.conf` and `docker exec trino ls -l /etc/trino/catalog`

| Symptom | Cause | Fix |
|---|---|---|
| MinIO `FATAL … file access denied`; console dead; `mc` fails | `minio_data` was created by an older image that ran as root | Already fixed with `user: "0:0"`. Alternatively, `chown` the volume or recreate it. |
| `401 UNAUTHORIZED` from quay.io, or MinIO image "not found" | MinIO images were removed from Docker Hub, and quay.io now refuses anonymous pulls | Use `alpine/minio` + `amazon/aws-cli`, as this compose file does |
| Trino shows only the `system` catalog; `ls /etc/trino/catalog: Permission denied` | SELinux label is missing | Mount with `:ro,Z` (already set) and run `docker compose up -d --force-recreate trino` |
| `Trino server is still initializing` | Trino is still starting | Wait and check `docker compose logs --tail=100 trino`. Don't restart it in a loop. |
| `Failed to get database nessie` / catalog not found | `spark-defaults.conf` wasn't loaded. If the host file was missing, Docker created a directory in its place. | Make sure the file exists, then `--force-recreate spark-iceberg` |
| `Connection refused` | Target is down, or the host port was used inside the network | Use service names and container ports |
| `UnknownHost` / `warehouse.minio` | `localhost` in a config, or path-style access is off | Use `minio:9000` and `path-style-access=true` |
| `403` / `SignatureDoesNotMatch` | Keys differ between MinIO and a client | See the password-change checklist in section 7 |
| `NoSuchKey` on `.parquet`/`.avro` | MinIO and Nessie are out of sync | Restore both from the same backup |
| `ClassNotFoundException` in Flink | A jar is missing | Rebuild the Flink image |

---

## 9. 🔐 Security note
This lab is intentionally insecure: one shared admin key, no catalog or Trino authentication, plain HTTP, and a MinIO build with a known CVE. Use it only on your own machine. In production you would add per-service storage users, TLS, OAuth/LDAP authentication and a secrets vault.

## 10. ⬆️ Version compatibility
- The Iceberg runtime must match the engine's minor version: `iceberg-spark-runtime-3.5_2.12` for Spark 3.5, and `iceberg-flink-runtime-1.18` for Flink 1.18.
- Engines can use different Iceberg library versions (Spark 1.8.1 and Flink 1.5.2 here) because they share the same table format version.
- Before upgrading anything, take a backup, check the tag with `docker manifest inspect`, and upgrade one service at a time.
