.PHONY: install build start stop logs clean

install:
	pip install -r requirements.txt

build:
	docker-compose build

start:
	docker-compose up -d
	@echo "Waiting 10 seconds for DB to initialize..."
	sleep 10
	./scripts/setup_db.sh

stop:
	docker-compose down

logs:
	docker-compose logs -f

clean:
	docker-compose down -v
	rm -rf ./data/timescaledb/*
	rm -rf ./data/qdrant_storage/*