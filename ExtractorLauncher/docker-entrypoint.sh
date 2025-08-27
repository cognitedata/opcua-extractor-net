#!/bin/sh
# docker-entrypoint.sh

# STARTUP_DELAY_SECONDS 환경 변수가 설정되어 있고, 0보다 큰 숫자인지 확인
if [ -n "$STARTUP_DELAY_SECONDS" ] && [ "$STARTUP_DELAY_SECONDS" -gt 0 ]; then
  echo "Startup delay configured. Waiting for $STARTUP_DELAY_SECONDS seconds..."
  sleep "$STARTUP_DELAY_SECONDS"
fi

echo "Starting application..."
# exec "$@"는 이 스크립트로 전달된 모든 인자(CMD)를 실행합니다.
exec "$@"
