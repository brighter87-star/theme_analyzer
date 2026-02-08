#!/bin/bash
# 테마 분석기 크론 등록 스크립트
# 사용법: bash scripts/setup_cron.sh

set -e

# ── 설정 ──
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON="${PROJECT_DIR}/venv/bin/python"
SCRIPT="${PROJECT_DIR}/run_pipeline.py"
LOG_DIR="${PROJECT_DIR}/logs"

mkdir -p "$LOG_DIR"

# Python 경로 확인
if [ ! -f "$PYTHON" ]; then
    echo "venv가 없습니다. 먼저 설치하세요:"
    echo "  cd $PROJECT_DIR"
    echo "  python3 -m venv venv"
    echo "  source venv/bin/activate"
    echo "  pip install -r requirements.txt"
    exit 1
fi

# ── 크론 엔트리 ──
# 매일 06:00 KST (서버 timezone이 KST인 경우)
# UTC 서버면 21:00 UTC = 06:00 KST (전날 21시)
CRON_LINE="0 6 * * * cd ${PROJECT_DIR} && ${PYTHON} ${SCRIPT} >> ${LOG_DIR}/cron.log 2>&1"

# 기존 크론에 이미 등록되어 있는지 확인
if crontab -l 2>/dev/null | grep -q "run_pipeline.py"; then
    echo "이미 등록되어 있습니다. 기존 엔트리를 교체합니다."
    crontab -l | grep -v "run_pipeline.py" | crontab -
fi

# 크론 추가
(crontab -l 2>/dev/null; echo "$CRON_LINE") | crontab -

echo "크론 등록 완료!"
echo ""
echo "등록된 크론:"
crontab -l | grep "run_pipeline.py"
echo ""
echo "서버 timezone 확인: $(timedatectl show -p Timezone --value 2>/dev/null || echo '확인 불가')"
echo ""
echo "※ 서버가 UTC라면 시간을 조정하세요:"
echo "   21 * * * (UTC 21:00 = KST 06:00)"
echo ""
echo "로그 확인: tail -f ${LOG_DIR}/cron.log"
