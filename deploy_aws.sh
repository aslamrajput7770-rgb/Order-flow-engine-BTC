#!/usr/bin/env bash
#
# Deploy the order flow engine to a Linux host (AWS EC2 / any systemd box).
# Run this ON the target host, as a user with sudo.
#
#   DELTA_API_KEY=... DELTA_API_SECRET=... sudo -E bash deploy_aws.sh
#
# Optional:
#   OLD_SERVICE="a.service b.service"  stop+disable+remove previously running engines
#   INSTALL_DIR=/opt/orderflow         install location
#   GO_LIVE=1                          start with --live (requires API keys)
#
set -euo pipefail

INSTALL_DIR="${INSTALL_DIR:-/opt/orderflow}"
SERVICE_NAME="${SERVICE_NAME:-orderflow-engine}"
PYTHON="${PYTHON:-python3}"
GO_LIVE="${GO_LIVE:-0}"
OLD_SERVICE="${OLD_SERVICE:-}"

SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ $EUID -ne 0 ]]; then
  echo "ERROR: run with sudo (this installs a systemd service)." >&2
  exit 1
fi

echo "==> 1/6 install system packages"
if command -v dnf >/dev/null 2>&1; then
  dnf install -y -q python3 python3-pip >/dev/null
elif command -v apt-get >/dev/null 2>&1; then
  apt-get update -qq
  apt-get install -y -qq python3 python3-pip python3-venv >/dev/null
else
  echo "ERROR: no dnf/apt-get found" >&2
  exit 1
fi

echo "==> 2/6 create $INSTALL_DIR and virtualenv"
mkdir -p "$INSTALL_DIR"
if [[ ! -x "$INSTALL_DIR/venv/bin/python" ]]; then
  "$PYTHON" -m venv "$INSTALL_DIR/venv"
fi
"$INSTALL_DIR/venv/bin/pip" install --quiet --upgrade pip
"$INSTALL_DIR/venv/bin/pip" install --quiet -r "$SRC_DIR/requirements.txt"

echo "==> 3/6 copy engine files"
install -m 0644 "$SRC_DIR/orderflow_engine.py" "$INSTALL_DIR/orderflow_engine.py"
install -m 0644 "$SRC_DIR/dashboard.py" "$INSTALL_DIR/dashboard.py"
install -m 0644 "$SRC_DIR/orderflow-engine.service" "/etc/systemd/system/${SERVICE_NAME}.service"

echo "==> 4/6 write credentials (mode 0600) to /etc/orderflow/orderflow.env"
mkdir -p /etc/orderflow
umask 077
if [[ -n "${DELTA_API_KEY:-}" && -n "${DELTA_API_SECRET:-}" ]]; then
  cat > /etc/orderflow/orderflow.env <<EOF
DELTA_API_KEY=${DELTA_API_KEY}
DELTA_API_SECRET=${DELTA_API_SECRET}
EOF
  echo "    credentials stored (0600)"
else
  : > /etc/orderflow/orderflow.env
  echo "    WARNING: no DELTA_API_KEY/SECRET given -> engine will run in PAPER mode"
fi
chmod 0600 /etc/orderflow/orderflow.env

echo "==> 5/6 stop and remove any previous engine(s)"
if [[ -n "$OLD_SERVICE" ]]; then
  for svc in $OLD_SERVICE; do
    echo "    stopping old service: $svc"
    systemctl stop "$svc" 2>/dev/null || true
    systemctl disable "$svc" 2>/dev/null || true
    rm -f "/etc/systemd/system/$svc"
    rm -rf "/etc/systemd/system/$svc.d"
    echo "    $svc stopped, disabled and unit removed"
  done
else
  echo "    OLD_SERVICE not set; skipping. Set OLD_SERVICE=\"<name>.service ...\" to remove it."
fi

echo "==> 6/6 install and start new service"
UNIT="/etc/systemd/system/${SERVICE_NAME}.service"
# point the unit at the venv python + env file
sed -i "s#^WorkingDirectory=.*#WorkingDirectory=$INSTALL_DIR#" "$UNIT"
sed -i "s#^ExecStart=.*#ExecStart=$INSTALL_DIR/venv/bin/python $INSTALL_DIR/orderflow_engine.py --watchdog --stats-interval 30 --min-premium ${MIN_PREMIUM:-200} --max-premium ${MAX_PREMIUM:-400} --premium-take-profit-pct ${TP_PCT:-0.5} --max-open-positions ${MAX_OPEN:-5}#" "$UNIT"
# EnvironmentFile: keep the '-' prefix so a missing/empty file is non-fatal
sed -i "s|^# *EnvironmentFile=.*|EnvironmentFile=-/etc/orderflow/orderflow.env|" "$UNIT"
sed -i "s|^EnvironmentFile=.*|EnvironmentFile=-/etc/orderflow/orderflow.env|" "$UNIT"
sed -i "s#^ReadWritePaths=.*#ReadWritePaths=$INSTALL_DIR#" "$UNIT"

# arm live mode only if explicitly requested
if [[ "$GO_LIVE" == "1" ]]; then
  sed -i "s#orderflow_engine.py --watchdog#orderflow_engine.py --live --watchdog#" "$UNIT"
  echo "    *** GO_LIVE=1 : service will place REAL orders ***"
fi

systemctl daemon-reload
systemctl enable "$SERVICE_NAME" >/dev/null
systemctl restart "$SERVICE_NAME"

sleep 3
systemctl --no-pager --full status "$SERVICE_NAME" | head -20
echo
echo "Logs:  journalctl -u $SERVICE_NAME -f"
echo "       tail -f $INSTALL_DIR/orderflow_engine.log"
