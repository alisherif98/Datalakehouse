up:      ; docker compose up -d --build
down:    ; docker compose down
ps:      ; docker compose ps -a
logs:    ; docker compose logs --tail=100 -f
smoke:   ; docker exec spark-iceberg spark-submit /home/iceberg/scripts/00_smoke_test.py
trino:   ; docker exec -it trino trino --catalog iceberg
flink:   ; docker exec -it flink-jobmanager ./bin/sql-client.sh -i /opt/flink/sql/01_bootstrap.sql
health:
	@curl -s -o /dev/null -w 'minio  %{http_code}\n' http://127.0.0.1:9000/minio/health/live
	@curl -s -o /dev/null -w 'nessie %{http_code}\n' http://localhost:19120/api/v2/config
	@curl -s -o /dev/null -w 'trino  %{http_code}\n' http://localhost:8082/v1/info
clean:   ; docker compose down -v   # WIPES ALL DATA
.PHONY: up down ps logs smoke trino flink health clean
