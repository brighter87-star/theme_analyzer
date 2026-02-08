#!/bin/bash
# 일일 파이프라인 실행 래퍼
# 크론에서 직접 호출하거나 수동 실행용

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

source venv/bin/activate
python run_pipeline.py "$@"
