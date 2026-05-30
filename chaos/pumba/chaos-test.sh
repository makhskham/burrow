#!/usr/bin/env bash
set -euo pipefail

echo "==> Starting 3-broker cluster"
docker compose up -d --build
sleep 5

echo "==> Writing 500 messages to broker1"
for i in $(seq 1 500); do
  docker compose exec -T broker1 /burrow-cli produce --broker localhost:9092 --topic pumba-test "msg-${i}" &
done
wait

echo "==> Killing leader (broker1) via Pumba"
docker run --rm -v /var/run/docker.sock:/var/run/docker.sock \
  gaiaadm/pumba kill --signal SIGKILL burrow-broker1
KILL_TIME=$(date +%s)

echo "==> Waiting for new leader election"
sleep 5
ELAPSED=$(( $(date +%s) - KILL_TIME ))
echo "Election completed in ~${ELAPSED}s"

if [ "$ELAPSED" -gt 10 ]; then
  echo "FAIL: leader election took too long (${ELAPSED}s > 10s)"
  docker compose down -v
  exit 1
fi

echo "==> Writing 500 more messages to broker2"
for i in $(seq 501 1000); do
  docker compose exec -T broker2 /burrow-cli produce --broker localhost:9092 --topic pumba-test "msg-${i}" &
done
wait

echo "==> Reading all messages back"
MESSAGES=$(docker compose exec -T broker2 /burrow-cli consume --broker localhost:9092 --topic pumba-test --from 0 2>/dev/null | wc -l | tr -d ' ')
echo "Messages recovered: $MESSAGES"

if [ "$MESSAGES" -lt 900 ]; then
  echo "FAIL: expected ~1000 messages, got ${MESSAGES}"
  docker compose down -v
  exit 1
fi

echo "PASS: no significant message loss after leader kill"
docker compose down -v
