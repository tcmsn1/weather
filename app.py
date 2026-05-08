"""
区域天气预报 + 个人物联网气象站（MySQL 8.4）
- 首页公开：各地点预报（Open-Meteo，无需 API Key）
- 登录后：管理自有设备，设备数据以「气象卡片」展示；硬件上报仍用 device_token

环境变量：MYSQL_*、SECRET_KEY、MQTT_ENABLE=1（可选）
运行：pip install -r requirements.txt && python app.py
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
import sys
import threading
import socket
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from typing import Any

import pymysql
from pymysql import err as mysql_err
from pymysql.cursors import DictCursor
from werkzeug.security import check_password_hash, generate_password_hash

from flask import (
    Flask,
    Response,
    abort,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

# Open-Meteo（免费、无需密钥）https://open-meteo.com/
GEOCODE_URL = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

WMO_WEATHER_ZH: dict[int, str] = {
    0: "晴朗",
    1: "晴间多云",
    2: "多云",
    3: "阴天",
    45: "雾",
    48: "雾凇",
    51: "小毛毛雨",
    53: "毛毛雨",
    55: "浓毛毛雨",
    56: "冻毛毛雨",
    57: "强冻毛毛雨",
    61: "小雨",
    63: "中雨",
    65: "大雨",
    66: "冻雨",
    67: "强冻雨",
    71: "小雪",
    73: "中雪",
    75: "大雪",
    77: "雪粒",
    80: "小阵雨",
    81: "阵雨",
    82: "强阵雨",
    85: "小阵雪",
    86: "阵雪",
    95: "雷暴",
    96: "雷暴伴冰雹",
    99: "强雷暴伴冰雹",
}


def _wmo_label_zh(code: int | None) -> str:
    if code is None:
        return "—"
    try:
        c = int(code)
    except (TypeError, ValueError):
        return "—"
    return WMO_WEATHER_ZH.get(c, f"天气代码 {c}")


def _http_get_json(url: str, timeout: float = 14.0) -> Any:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "WeatherForecastStudent/1.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except socket.timeout as e:
        raise TimeoutError("天气接口超时") from e
    except json.JSONDecodeError as e:
        raise RuntimeError("天气接口返回无效数据") from e


def _utc_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


# -----------------------------------------------------------------------------
# 配置
# -----------------------------------------------------------------------------
MYSQL_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3306"))
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE", "weather_iot")
MYSQL_AUTOCREATE_DB = os.environ.get("MYSQL_AUTOCREATE_DB", "0") == "1"

SECRET_KEY = os.environ.get("SECRET_KEY")
if not SECRET_KEY:
    SECRET_KEY = "dev-only-change-me"
    print("警告: 未设置环境变量 SECRET_KEY，会话可被伪造；部署前请设置强随机密钥。")

MQTT_BROKER = os.environ.get("MQTT_BROKER", "127.0.0.1")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_ENABLE = os.environ.get("MQTT_ENABLE", "0") == "1"
MQTT_TOPIC_PATTERN = "weather/+/data"

_db_lock = threading.Lock()
app = Flask(__name__)
app.secret_key = SECRET_KEY

USERNAME_RE = re.compile(r"^[a-zA-Z0-9_]{3,32}$")


def _safe_db_name(name: str) -> bool:
    return bool(name) and bool(re.fullmatch(r"[A-Za-z0-9_]+", name))


def _mysql_params(with_db: bool) -> dict[str, Any]:
    p: dict[str, Any] = {
        "host": MYSQL_HOST,
        "port": MYSQL_PORT,
        "user": MYSQL_USER,
        "password": MYSQL_PASSWORD,
        "charset": "utf8mb4",
        "cursorclass": DictCursor,
        "autocommit": True,
    }
    if with_db:
        p["database"] = MYSQL_DATABASE
    return p


def get_conn() -> pymysql.connections.Connection:
    return pymysql.connect(**_mysql_params(with_db=True))


def maybe_create_database() -> None:
    if not MYSQL_AUTOCREATE_DB:
        return
    if not _safe_db_name(MYSQL_DATABASE):
        raise ValueError("MYSQL_DATABASE 只能包含字母数字下划线")
    with pymysql.connect(**_mysql_params(with_db=False)) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` "
                "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )


def _column_exists(cur, table: str, column: str) -> bool:
    cur.execute(
        """
        SELECT COUNT(*) AS c FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
        """,
        (MYSQL_DATABASE, table, column),
    )
    return int(cur.fetchone()["c"]) > 0


def _constraint_exists(cur, table: str, name: str) -> bool:
    cur.execute(
        """
        SELECT COUNT(*) AS c FROM information_schema.TABLE_CONSTRAINTS
        WHERE CONSTRAINT_SCHEMA = %s AND TABLE_NAME = %s AND CONSTRAINT_NAME = %s
        """,
        (MYSQL_DATABASE, table, name),
    )
    return int(cur.fetchone()["c"]) > 0


def migrate_devices_user_id() -> None:
    """旧库 devices 无 user_id 时：加列、归入占位账号、NOT NULL、外键。"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            if _column_exists(cur, "devices", "user_id"):
                if not _constraint_exists(cur, "devices", "fk_devices_user"):
                    try:
                        cur.execute(
                            """
                            ALTER TABLE devices
                            ADD CONSTRAINT fk_devices_user
                            FOREIGN KEY (user_id) REFERENCES users (id)
                            ON DELETE CASCADE ON UPDATE CASCADE
                            """
                        )
                    except mysql_err.MySQLError:
                        pass
                return
            cur.execute(
                "ALTER TABLE devices ADD COLUMN user_id BIGINT UNSIGNED NULL"
            )
            cur.execute(
                "SELECT id FROM users WHERE username = %s",
                ("__legacy_devices__",),
            )
            row = cur.fetchone()
            if not row:
                cur.execute(
                    """
                    INSERT INTO users (username, password_hash, created_at)
                    VALUES (%s, %s, %s)
                    """,
                    (
                        "__legacy_devices__",
                        generate_password_hash("!"),
                        _utc_naive(),
                    ),
                )
                legacy_uid = int(cur.lastrowid)
            else:
                legacy_uid = int(row["id"])
            cur.execute(
                "UPDATE devices SET user_id = %s WHERE user_id IS NULL",
                (legacy_uid,),
            )
            cur.execute(
                "ALTER TABLE devices MODIFY user_id BIGINT UNSIGNED NOT NULL"
            )
            if not _constraint_exists(cur, "devices", "fk_devices_user"):
                cur.execute(
                    """
                    ALTER TABLE devices
                    ADD CONSTRAINT fk_devices_user
                    FOREIGN KEY (user_id) REFERENCES users (id)
                    ON DELETE CASCADE ON UPDATE CASCADE
                    """
                )


def init_db() -> None:
    maybe_create_database()
    stmts = [
        """
        CREATE TABLE IF NOT EXISTS users (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            username VARCHAR(64) NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            created_at DATETIME(6) NOT NULL,
            PRIMARY KEY (id),
            UNIQUE KEY uk_users_username (username)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS devices (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            name VARCHAR(128) NOT NULL,
            token CHAR(32) NOT NULL,
            created_at DATETIME(6) NOT NULL,
            PRIMARY KEY (id),
            UNIQUE KEY uk_devices_token (token)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS readings (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            device_id BIGINT UNSIGNED NOT NULL,
            ts DATETIME(6) NOT NULL,
            temperature DOUBLE NULL,
            humidity DOUBLE NULL,
            pressure DOUBLE NULL,
            source VARCHAR(16) NOT NULL DEFAULT 'http',
            PRIMARY KEY (id),
            KEY idx_readings_device_ts (device_id, ts),
            CONSTRAINT fk_readings_device
                FOREIGN KEY (device_id) REFERENCES devices (id)
                ON DELETE CASCADE ON UPDATE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
    ]
    with get_conn() as conn:
        with conn.cursor() as cur:
            for s in stmts:
                cur.execute(s.strip())
    migrate_devices_user_id()


def save_reading(
    device_id: int,
    temperature: float | None,
    humidity: float | None,
    pressure: float | None,
    source: str = "http",
) -> None:
    ts = _utc_naive()
    sql = """
        INSERT INTO readings (device_id, ts, temperature, humidity, pressure, source)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    with _db_lock, get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (device_id, ts, temperature, humidity, pressure, source),
            )


def token_for_device_id(token: str) -> int | None:
    sql = "SELECT id FROM devices WHERE token = %s LIMIT 1"
    with _db_lock, get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (token,))
            row = cur.fetchone()
    return int(row["id"]) if row else None


def _device_owned(device_id: int, user_id: int) -> dict[str, Any] | None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, token, created_at, user_id
                FROM devices WHERE id = %s AND user_id = %s
                """,
                (device_id, user_id),
            )
            return cur.fetchone()


def _reading_stats(device_id: int) -> dict[str, Any] | None:
    sql = """
        SELECT
            COUNT(*) AS n,
            AVG(temperature) AS avg_t,
            MIN(temperature) AS min_t,
            MAX(temperature) AS max_t,
            AVG(humidity) AS avg_h,
            AVG(pressure) AS avg_p
        FROM readings WHERE device_id = %s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (device_id,))
            row = cur.fetchone()
    if not row or row["n"] == 0:
        return None
    return dict(row)


def current_user_id() -> int | None:
    uid = session.get("user_id")
    return int(uid) if uid is not None else None


def login_required_redirect():
    if current_user_id() is None:
        return redirect(url_for("login", next=request.path))
    return None


# -----------------------------------------------------------------------------
# 公开：区域天气预报（Open-Meteo）
# -----------------------------------------------------------------------------
@app.get("/api/weather/search")
def api_weather_search():
    q = (request.args.get("q") or "").strip()
    if len(q) < 1:
        return jsonify({"results": []})
    if len(q) > 64:
        return jsonify({"error": "关键词过长"}), 400
    params = urllib.parse.urlencode(
        {"name": q, "count": 10, "language": "zh", "format": "json"}
    )
    url = f"{GEOCODE_URL}?{params}"
    try:
        data = _http_get_json(url)
    except urllib.error.HTTPError as e:
        return jsonify({"error": str(e)}), 502
    except urllib.error.URLError as e:
        reason = getattr(e, "reason", e)
        return jsonify({"error": "无法连接天气服务", "detail": str(reason)}), 502
    except (TimeoutError, OSError, RuntimeError) as e:
        return jsonify({"error": str(e)}), 504
    results = []
    for r in data.get("results") or []:
        parts = [r.get("name"), r.get("admin1"), r.get("country")]
        results.append(
            {
                "name": r.get("name"),
                "admin1": r.get("admin1"),
                "country": r.get("country"),
                "latitude": r.get("latitude"),
                "longitude": r.get("longitude"),
                "label": "，".join(str(x) for x in parts if x),
            }
        )
    return jsonify({"results": results})


@app.get("/api/weather/current")
def api_weather_current():
    try:
        lat = float(request.args.get("lat", ""))
        lon = float(request.args.get("lon", ""))
    except (TypeError, ValueError):
        return jsonify({"error": "需要有效的 lat、lon 参数"}), 400
    if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
        return jsonify({"error": "经纬度超出范围"}), 400
    params = urllib.parse.urlencode(
        {
            "latitude": lat,
            "longitude": lon,
            "current": ",".join(
                [
                    "temperature_2m",
                    "relative_humidity_2m",
                    "weather_code",
                    "surface_pressure",
                    "wind_speed_10m",
                    "wind_direction_10m",
                ]
            ),
            "daily": "weather_code,temperature_2m_max,temperature_2m_min",
            "timezone": "auto",
            "forecast_days": 7,
            "wind_speed_unit": "ms",
        }
    )
    url = f"{FORECAST_URL}?{params}"
    try:
        data = _http_get_json(url)
    except urllib.error.HTTPError as e:
        return jsonify({"error": str(e)}), 502
    except urllib.error.URLError as e:
        reason = getattr(e, "reason", e)
        return jsonify({"error": "无法连接天气服务", "detail": str(reason)}), 502
    except (TimeoutError, OSError, RuntimeError) as e:
        return jsonify({"error": str(e)}), 504

    cur = data.get("current") or {}
    daily = data.get("daily") or {}
    times = daily.get("time") or []
    dmax = daily.get("temperature_2m_max") or []
    dmin = daily.get("temperature_2m_min") or []
    dcodes = daily.get("weather_code") or []
    daily_out: list[dict[str, Any]] = []
    for i, t in enumerate(times):
        code = dcodes[i] if i < len(dcodes) else None
        daily_out.append(
            {
                "date": t,
                "tmax": dmax[i] if i < len(dmax) else None,
                "tmin": dmin[i] if i < len(dmin) else None,
                "weather_code": code,
                "label_zh": _wmo_label_zh(code),
            }
        )
    wc = cur.get("weather_code")
    return jsonify(
        {
            "current": {
                "time": cur.get("time"),
                "temperature_2m": cur.get("temperature_2m"),
                "relative_humidity_2m": cur.get("relative_humidity_2m"),
                "surface_pressure": cur.get("surface_pressure"),
                "wind_speed_10m": cur.get("wind_speed_10m"),
                "wind_direction_10m": cur.get("wind_direction_10m"),
                "weather_code": wc,
                "weather_zh": _wmo_label_zh(wc),
            },
            "daily": daily_out,
            "current_units": data.get("current_units") or {},
            "daily_units": data.get("daily_units") or {},
        }
    )


# -----------------------------------------------------------------------------
# 认证页面
# -----------------------------------------------------------------------------
@app.route("/register", methods=["GET", "POST"])
def register():
    if current_user_id() is not None:
        return redirect(url_for("index"))
    err: str | None = None
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        pw = request.form.get("password") or ""
        pw2 = request.form.get("password2") or ""
        if not USERNAME_RE.match(username):
            err = "用户名须为 3～32 位字母、数字或下划线"
        elif len(pw) < 8:
            err = "密码至少 8 位"
        elif pw != pw2:
            err = "两次密码不一致"
        else:
            ph = generate_password_hash(pw)
            try:
                with _db_lock, get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO users (username, password_hash, created_at)
                            VALUES (%s, %s, %s)
                            """,
                            (username, ph, _utc_naive()),
                        )
                        uid = int(cur.lastrowid)
                session.clear()
                session["user_id"] = uid
                session["username"] = username
                return redirect(url_for("index"))
            except mysql_err.IntegrityError:
                err = "用户名已被占用"
    return render_template("register.html", error=err)


@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user_id() is not None:
        return redirect(url_for("index"))
    err: str | None = None
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        pw = request.form.get("password") or ""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, password_hash FROM users WHERE username = %s",
                    (username,),
                )
                row = cur.fetchone()
        if not row or not check_password_hash(row["password_hash"], pw):
            err = "用户名或密码错误"
        else:
            session.clear()
            session["user_id"] = int(row["id"])
            session["username"] = username
            nxt = request.form.get("next") or request.args.get("next") or url_for("index")
            if not nxt.startswith("/"):
                nxt = url_for("index")
            return redirect(nxt)
    return render_template("login.html", error=err)


@app.route("/logout", methods=["GET", "POST"])
def logout():
    session.clear()
    return redirect(url_for("index"))


# -----------------------------------------------------------------------------
# 业务页面与 API
# -----------------------------------------------------------------------------
@app.route("/")
def index():
    """首页公开：区域预报；登录后额外展示个人物联网气象卡片。"""
    uid = current_user_id()
    devices: list[Any] = []
    latest: dict[int, dict | None] = {}
    if uid is not None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, name, token, created_at
                    FROM devices WHERE user_id = %s ORDER BY id
                    """,
                    (uid,),
                )
                devices = cur.fetchall()
                for d in devices:
                    cur.execute(
                        """
                        SELECT ts, temperature, humidity, pressure, source
                        FROM readings WHERE device_id = %s
                        ORDER BY id DESC LIMIT 1
                        """,
                        (d["id"],),
                    )
                    r = cur.fetchone()
                    latest[int(d["id"])] = dict(r) if r else None
    return render_template(
        "index.html",
        devices=devices,
        latest=latest,
        username=session.get("username", ""),
        logged_in=uid is not None,
    )


@app.route("/device/<int:device_id>")
def device_detail(device_id: int):
    redir = login_required_redirect()
    if redir:
        return redir
    uid = current_user_id()
    assert uid is not None
    dev = _device_owned(device_id, uid)
    if not dev:
        abort(404)
    stats = _reading_stats(device_id)
    return render_template("device.html", device=dev, stats=stats)


@app.post("/api/devices")
def api_register_device():
    uid = current_user_id()
    if uid is None:
        return jsonify({"error": "请先登录"}), 401
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip() or "未命名设备"
    if len(name) > 128:
        return jsonify({"error": "名称过长"}), 400
    token = uuid.uuid4().hex
    created = _utc_naive()
    sql = """
        INSERT INTO devices (name, token, created_at, user_id)
        VALUES (%s, %s, %s, %s)
    """
    with _db_lock, get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (name, token, created, uid))
            dev_id = int(cur.lastrowid)
    return jsonify({"id": dev_id, "name": name, "token": token}), 201


@app.patch("/api/devices/<int:device_id>")
def api_patch_device(device_id: int):
    uid = current_user_id()
    if uid is None:
        return jsonify({"error": "请先登录"}), 401
    if not _device_owned(device_id, uid):
        return jsonify({"error": "设备不存在"}), 404
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    if not name or len(name) > 128:
        return jsonify({"error": "名称无效或过长"}), 400
    with _db_lock, get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE devices SET name = %s WHERE id = %s AND user_id = %s",
                (name, device_id, uid),
            )
    return jsonify({"ok": True, "name": name}), 200


@app.delete("/api/devices/<int:device_id>")
def api_delete_device(device_id: int):
    uid = current_user_id()
    if uid is None:
        return jsonify({"error": "请先登录"}), 401
    with _db_lock, get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM devices WHERE id = %s AND user_id = %s",
                (device_id, uid),
            )
            if cur.rowcount == 0:
                return jsonify({"error": "设备不存在"}), 404
    return jsonify({"ok": True}), 200


@app.post("/api/ingest")
def api_ingest():
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "需要 JSON body"}), 400
    token = data.get("device_token") or data.get("token")
    if not token:
        return jsonify({"error": "缺少 device_token"}), 400
    dev_id = token_for_device_id(str(token))
    if dev_id is None:
        return jsonify({"error": "无效的 device_token"}), 401

    def f(key: str) -> float | None:
        v = data.get(key)
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    save_reading(
        dev_id,
        f("temperature"),
        f("humidity"),
        f("pressure"),
        source="http",
    )
    return jsonify({"ok": True}), 200


@app.get("/api/devices/<int:device_id>/readings")
def api_readings(device_id: int):
    uid = current_user_id()
    if uid is None:
        return jsonify({"error": "请先登录"}), 401
    if not _device_owned(device_id, uid):
        return jsonify({"error": "设备不存在"}), 404
    limit = min(int(request.args.get("limit", 200)), 500)
    sql = """
        SELECT ts, temperature, humidity, pressure, source
        FROM readings WHERE device_id = %s
        ORDER BY id DESC LIMIT %s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (device_id, limit))
            rows = cur.fetchall()
    out = []
    for r in rows:
        item = dict(r)
        ts = item.get("ts")
        if hasattr(ts, "isoformat"):
            item["ts"] = ts.isoformat() + "Z" if ts.tzinfo is None else ts.isoformat()
        out.append(item)
    return jsonify(out)


@app.get("/api/devices/<int:device_id>/export.csv")
def api_export_csv(device_id: int):
    uid = current_user_id()
    if uid is None:
        return jsonify({"error": "请先登录"}), 401
    if not _device_owned(device_id, uid):
        return jsonify({"error": "设备不存在"}), 404
    cap = min(int(request.args.get("limit", 5000)), 20000)
    sql = """
        SELECT ts, temperature, humidity, pressure, source
        FROM readings WHERE device_id = %s
        ORDER BY id ASC
        LIMIT %s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (device_id, cap))
            rows = cur.fetchall()

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["ts", "temperature", "humidity", "pressure", "source"])
    for r in rows:
        ts = r["ts"]
        ts_s = ts.isoformat(sep=" ") if hasattr(ts, "isoformat") else str(ts)
        w.writerow(
            [
                ts_s,
                r["temperature"],
                r["humidity"],
                r["pressure"],
                r["source"],
            ]
        )
    fn = f"device_{device_id}_readings.csv"
    return Response(
        buf.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{fn}"',
        },
    )


@app.get("/api/health")
def api_health():
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT VERSION() AS v")
                row = cur.fetchone()
        ver = row["v"] if row else None
        return jsonify({"ok": True, "mysql_version": ver}), 200
    except pymysql.MySQLError as e:
        return jsonify({"ok": False, "error": str(e)}), 503


# -----------------------------------------------------------------------------
# MQTT（可选）
# -----------------------------------------------------------------------------
def start_mqtt_if_enabled() -> None:
    if not MQTT_ENABLE:
        print("MQTT 未开启（设置环境变量 MQTT_ENABLE=1 并启动 broker）")
        return

    try:
        import paho.mqtt.client as mqtt
    except ImportError:
        print("未安装 paho-mqtt，跳过 MQTT")
        return

    def on_connect(client, _userdata, _flags, reason_code, _properties=None):
        rc = getattr(reason_code, "value", reason_code)
        if rc == 0:
            client.subscribe(MQTT_TOPIC_PATTERN)
            print(f"MQTT 已订阅: {MQTT_TOPIC_PATTERN}")
        else:
            print("MQTT 连接失败:", reason_code)

    def on_message(_client, _userdata, msg):
        try:
            parts = msg.topic.split("/")
            if len(parts) < 3 or parts[0] != "weather":
                return
            token = parts[1]
            payload = json.loads(msg.payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
            return
        dev_id = token_for_device_id(token)
        if dev_id is None:
            return

        def g(key: str) -> float | None:
            v = payload.get(key)
            if v is None:
                return None
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        save_reading(
            dev_id,
            g("temperature"),
            g("humidity"),
            g("pressure"),
            source="mqtt",
        )

    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    except (AttributeError, TypeError):
        client = mqtt.Client()

    client.on_connect = on_connect
    client.on_message = on_message
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
    except OSError as e:
        print("MQTT 连接失败:", e)
        return

    threading.Thread(target=client.loop_forever, daemon=True).start()
    print(f"MQTT 后台线程已启动 -> {MQTT_BROKER}:{MQTT_PORT}")


# -----------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        init_db()
    except mysql_err.OperationalError as e:
        errno = e.args[0] if e.args else None
        print("数据库连接失败:", e, file=sys.stderr)
        if errno == 1045:
            print(
                "\n说明：1045 表示 MySQL 拒绝了登录（常见是「用了空密码」或密码不对）。\n"
                "程序默认 MYSQL_USER=root、MYSQL_PASSWORD 为空；若你的 root 有密码，\n"
                "请在**同一 PowerShell 窗口**里先设置再启动，例如：\n"
                '  $env:MYSQL_USER="root"\n'
                '  $env:MYSQL_PASSWORD="你的MySQL密码"\n'
                '  $env:MYSQL_DATABASE="weather_iot"\n'
                "若使用专用账号 iot，请把 MYSQL_USER / MYSQL_PASSWORD 改成对应值。\n",
                file=sys.stderr,
            )
        elif errno == 2003:
            print(
                "\n说明：2003 多为 MySQL 服务未启动，或 MYSQL_HOST / MYSQL_PORT 配置错误。\n",
                file=sys.stderr,
            )
        raise SystemExit(1) from e

    start_mqtt_if_enabled()
    use_reloader = os.environ.get("FLASK_DEBUG_RELOAD", "1") == "1" and not MQTT_ENABLE
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "5000")),
        debug=True,
        use_reloader=use_reloader,
    )
