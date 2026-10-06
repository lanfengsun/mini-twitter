.PHONY: up down reset seed seed-small scale logs test tidy fmt vet load-mixed load-ramp load-burst load-auth drill-shard drill-redis

N ?= 2
RATE ?= 1000

up:            ## build and start everything
	docker compose up -d --build
	@echo "app: http://localhost:8080   grafana: http://localhost:3000   prometheus: http://localhost:9090"

down:          ## stop (data volumes are kept)
	docker compose down

reset:         ## stop and delete all data volumes
	docker compose down -v

seed:          ## load the default dataset (500k users, 2M posts, ~3M follows, 1 celebrity with 300k followers)
	docker compose --profile tools run --rm seed

seed-small:    ## quick dataset for smoke tests
	USERS=20000 POSTS=100000 CELEB_FOLLOWERS=15000 LOAD_SESSIONS=2000 docker compose --profile tools run --rm seed

scale:         ## run N API replicas: make scale N=4
	docker compose up -d --no-recreate --scale api=$(N)

logs:
	docker compose logs -f api worker

test:          ## unit tests for the pure-logic packages
	go test ./internal/idgen ./internal/shard ./internal/feed

tidy:          ## resolve dependencies and write go.sum (needs Docker, no local Go)
	docker run --rm -v "$(PWD)":/src -w /src golang:1.24 go mod tidy

fmt:
	gofmt -l -w .

vet:
	go vet ./...

load-mixed:    ## steady mixed load: make load-mixed RATE=2000
	k6 run -e RATE=$(RATE) -e DURATION=3m loadtest/k6/mixed.js

load-ramp:     ## ramp until saturation
	k6 run loadtest/k6/ramp.js

load-burst:    ## write burst
	k6 run loadtest/k6/burst.js

load-auth:     ## signup/login storm (bcrypt)
	k6 run loadtest/k6/auth.js

drill-shard:   ## failure drill: kill one Postgres shard (restore with: docker compose start pg1)
	docker compose kill pg1

drill-redis:   ## failure drill: stop Redis (restore with: docker compose start redis)
	docker compose stop redis
