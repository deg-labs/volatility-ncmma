#!/usr/bin/env python3
"""
CMMA API 価格監視バッチシステム
- 重複投稿防止機能 (SQLite)
- ログファイル自動削除機能
- 時間足に応じた再通知制御
- Discord通知の保持件数制限 (古いメッセージをwebhook経由で定期削除)
"""

import hashlib
import json
import logging
import math
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path

import requests
from dotenv import load_dotenv


class CmmaPriceMonitor:
    def __init__(self, config_path=None):
        # スクリプトのディレクトリを基準にパスを設定
        self.script_dir = Path(__file__).parent.absolute()
        self.config_path = config_path or self.script_dir / ".env"
        self.log_dir = self.script_dir / "logs"
        self.cache_dir = self.script_dir / "cache"
        self.heartbeat_path = self.cache_dir / "heartbeat.json"

        # ディレクトリの作成
        self.log_dir.mkdir(exist_ok=True)
        self.cache_dir.mkdir(exist_ok=True)

        # 設定の読み込みとロギング設定
        self._load_config()
        self._setup_logging()

        # データベースの初期化
        self.data_dir = self.script_dir / "data"
        self.data_dir.mkdir(exist_ok=True)
        self.db_path = self.data_dir / "ncmma.db"
        self._init_db()

        if not self.discord_webhook_url:
            raise ValueError("DISCORD_WEBHOOK_URL not found in environment variables")

    @staticmethod
    def _get_env(key, default):
        """環境変数を安全に取得する。未設定・空文字はデフォルト値を返す。"""
        raw = os.getenv(key)
        if raw is None or raw.strip() == "":
            return default
        return raw

    @staticmethod
    def _get_env_int(key, default, minimum=None, maximum=None):
        """環境変数を検証済みのintへ変換する。明示された不正値は起動を止める。"""
        raw = os.getenv(key)
        if raw is None or raw.strip() == "":
            return default
        try:
            value = int(raw.strip())
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{key} must be an integer") from exc
        if minimum is not None and value < minimum:
            raise ValueError(f"{key} must be >= {minimum}")
        if maximum is not None and value > maximum:
            raise ValueError(f"{key} must be <= {maximum}")
        return value

    @staticmethod
    def _get_env_float(key, default, minimum=None):
        """環境変数を有限値のfloatへ変換する。明示された不正値は起動を止める。"""
        raw = os.getenv(key)
        if raw is None or raw.strip() == "":
            return default
        try:
            value = float(raw.strip())
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{key} must be a number") from exc
        if not math.isfinite(value):
            raise ValueError(f"{key} must be finite")
        if minimum is not None and value < minimum:
            raise ValueError(f"{key} must be >= {minimum}")
        return value

    def _load_config(self):
        """設定ファイルの読み込み"""
        if self.config_path.exists():
            load_dotenv(self.config_path)
        else:
            load_dotenv()

        self.discord_webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
        self.volatility_api_url = self._get_env(
            "CMMA_VOLATILITY_API_URL", "https://stg.api.1btc.love/volatility"
        )

        # APIクエリパラメータ
        self.timeframe = self._get_env("TIMEFRAME", "4h")
        self.threshold = self._get_env_float("THRESHOLD", 5.0, minimum=0.0)
        if self.threshold <= 0:
            raise ValueError("THRESHOLD must be > 0")
        self.direction = self._get_env("DIRECTION", "up")
        if self.direction not in {"up", "down", "both"}:
            raise ValueError("DIRECTION must be one of: up, down, both")
        self.volatility_sort = self._get_env(
            "CMMA_VOLATILITY_API_SORT", "volatility_desc"
        )
        self.volatility_limit = self._get_env_int(
            "CMMA_VOLATILITY_API_LIMIT", 100, minimum=1, maximum=100
        )
        self.offset = self._get_env_int("OFFSET", 5, minimum=0)

        # 監視設定
        self.max_notifications = self._get_env_int(
            "MAX_NOTIFICATIONS", 20, minimum=1, maximum=25
        )
        self.renotify_buffer_minutes = self._get_env_int(
            "RENOTIFY_BUFFER_MINUTES", 60, minimum=0
        )
        self.check_interval_seconds = self._get_env_int(
            "CHECK_INTERVAL_SECONDS", 300, minimum=1
        )

        # Discordメッセージ保持設定
        self.discord_keep_messages = self._get_env_int(
            "DISCORD_KEEP_MESSAGES", 50, minimum=1
        )
        self.discord_cleanup_interval_hours = self._get_env_int(
            "DISCORD_CLEANUP_INTERVAL_HOURS", 24, minimum=1
        )
        self.discord_delete_interval_seconds = self._get_env_float(
            "DISCORD_DELETE_INTERVAL_SECONDS", 0.5, minimum=0.0
        )

        # ログ設定
        self.log_max_size_mb = self._get_env_int("LOG_MAX_SIZE_MB", 10, minimum=1)

        # 出来高フィルター設定
        self.volume_api_url = self._get_env(
            "CMMA_VOLUME_API_URL", "https://stg.api.1btc.love/volume"
        )
        self.volume_threshold = self._get_env_float(
            "VOLUME_THRESHOLD", 0.0, minimum=0.0
        )
        self.volume_timeframe = self._get_env("VOLUME_TIMEFRAME", "1h")
        self.volume_period = self._get_env("VOLUME_PERIOD", "24h")
        self.volume_sort = self._get_env("CMMA_VOLUME_API_SORT", "turnover_desc")
        self.volume_limit = self._get_env_int(
            "CMMA_VOLUME_API_LIMIT", 100, minimum=1, maximum=100
        )

    def _init_db(self):
        """データベースの初期化"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS notifications (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        notification_hash TEXT UNIQUE NOT NULL,
                        symbol TEXT NOT NULL,
                        timeframe TEXT NOT NULL,
                        direction TEXT NOT NULL,
                        change_pct REAL NOT NULL,
                        notified_at TIMESTAMP NOT NULL
                    )
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS discord_messages (
                        message_id TEXT PRIMARY KEY,
                        posted_at TIMESTAMP NOT NULL
                    )
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS meta (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    )
                """)
                conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Database initialization failed: {e}")
            raise

    def _setup_logging(self):
        """ログ設定"""
        log_file = self.log_dir / "ncmma_monitor.log"

        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )

        # ファイルハンドラー（ローテーション付き）
        file_handler = RotatingFileHandler(
            log_file, maxBytes=5 * 1024 * 1024, backupCount=3
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)

        # コンソールハンドラー
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)

        # ロガー設定
        self.logger = logging.getLogger("CmmaMonitor")
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        self.logger.propagate = False

    def _write_heartbeat(self, status, extra=None):
        """ヘルスチェック用のheartbeatファイルを更新"""
        payload = {
            "status": status,
            "timestamp": datetime.now().isoformat(),
        }
        if extra:
            payload.update(extra)

        # 一時ファイルへの書き込み + os.replace でアトミックに更新し、
        # 読み取り側が部分書き込み・破損JSONを検出しないようにする
        tmp_path = self.heartbeat_path.with_suffix(".json.tmp")
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
            os.replace(tmp_path, self.heartbeat_path)
        except Exception:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:  # noqa: S110
                pass
            raise

    @staticmethod
    def _is_managed_log(file_path):
        """自前命名規則に一致するファイルかを判定する。
        削除対象は ncmma_monitor.log (ローテーションを含む) と results_*.json に限定する。"""
        name = file_path.name
        return bool(
            re.fullmatch(r"ncmma_monitor\.log(?:\.\d+)?", name)
            or re.fullmatch(r"results_.+\.json", name)
        )

    def _cleanup_logs(self):
        """ログディレクトリの自動削除（10MB以上）。削除対象は自前命名規則のファイルのみ。"""
        try:
            total_size = 0
            log_files = []

            # ログディレクトリのファイルサイズを計算
            for file_path in self.log_dir.rglob("*"):
                if file_path.is_file():
                    size = file_path.stat().st_size
                    if self._is_managed_log(file_path):
                        total_size += size
                        log_files.append((file_path, size, file_path.stat().st_mtime))

            total_size_mb = total_size / (1024 * 1024)
            self.logger.info(f"Total log directory size: {total_size_mb:.2f}MB")

            # 10MB以上の場合、古いファイルから削除
            if total_size_mb > self.log_max_size_mb:
                self.logger.info(
                    f"Log directory exceeds {self.log_max_size_mb}MB, cleaning up..."
                )

                # ファイルを作成日時でソート（古い順）
                log_files.sort(key=lambda x: x[2])

                deleted_size = 0
                deleted_count = 0

                for file_path, size, mtime in log_files:
                    # 現在のログファイルとメインログは削除しない
                    if file_path.name == "ncmma_monitor.log":
                        continue

                    try:
                        file_path.unlink()
                        deleted_size += size
                        deleted_count += 1
                        self.logger.info(f"Deleted old log file: {file_path.name}")

                        # 目標サイズ以下になったら停止
                        if (total_size - deleted_size) / (
                            1024 * 1024
                        ) <= self.log_max_size_mb * 0.8:
                            break

                    except Exception as e:
                        self.logger.warning(f"Failed to delete {file_path.name}: {e}")

                self.logger.info(
                    f"Cleanup completed: {deleted_count} files deleted, {deleted_size/(1024*1024):.2f}MB freed"
                )

        except Exception as e:
            self.logger.error(f"Error during log cleanup: {e}")

    def _generate_notification_hash(self, symbol, direction):
        """トークンの通知用ハッシュを生成"""
        # シンボル + 変動方向 + 時間足でハッシュ化
        hash_input = f"{symbol}_{direction}_{self.timeframe}"
        return hashlib.md5(hash_input.encode()).hexdigest()

    def _should_notify(self, notification_hash):
        """通知すべきかどうかをDBで判定"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT notified_at FROM notifications WHERE notification_hash = ?",
                    (notification_hash,),
                )
                result = cursor.fetchone()

                if result:
                    last_notified_at = datetime.fromisoformat(result[0])
                    time_diff = datetime.now() - last_notified_at

                    if time_diff.total_seconds() < self.renotify_buffer_minutes * 60:
                        return False
                return True
        except (sqlite3.Error, ValueError, TypeError) as e:
            self.logger.error(f"Failed to check notification history from DB: {e}")
            return True  # 履歴を読めない場合は金融アラートを抑止しない

    def _record_notification(
        self, notification_hash, symbol, timeframe, direction, change_pct
    ):
        """通知履歴をDBに記録"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO notifications (notification_hash, symbol, timeframe, direction, change_pct, notified_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (
                        notification_hash,
                        symbol,
                        timeframe,
                        direction,
                        change_pct,
                        datetime.now().isoformat(),
                    ),
                )
                conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Failed to record notification to DB: {e}")

    def _discord_post_url(self):
        """wait=true を付けた投稿用URLを返す (レスポンスからメッセージIDを取得するために必要)"""
        base, sep, query = self.discord_webhook_url.partition("?")
        if sep:
            return f"{base}?{query}&wait=true"
        return f"{base}?wait=true"

    def _discord_delete_url(self, message_id):
        """webhookが投稿したメッセージを削除するURLを返す"""
        base, sep, query = self.discord_webhook_url.partition("?")
        url = f"{base}/messages/{message_id}"
        return f"{url}?{query}" if sep else url

    def _record_discord_message(self, message_id):
        """投稿したDiscordメッセージのIDをDBに記録"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO discord_messages (message_id, posted_at) VALUES (?, ?)",
                    (str(message_id), datetime.now().isoformat()),
                )
                conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Failed to record Discord message ID: {e}")

    def _get_meta(self, key):
        """metaテーブルから値を取得"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT value FROM meta WHERE key = ?", (key,))
                result = cursor.fetchone()
                return result[0] if result else None
        except sqlite3.Error:
            return None

    def _set_meta(self, key, value):
        """metaテーブルに値を記録"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                    (key, value),
                )
                conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Failed to write meta '{key}': {e}")

    def _delete_discord_message(self, message_id):
        """webhook経由で古いDiscordメッセージを削除する。成功時True"""
        for _ in range(3):
            try:
                response = requests.delete(
                    self._discord_delete_url(message_id), timeout=30
                )
                if response.status_code == 429:
                    try:
                        retry_after = float(response.json().get("retry_after", "1"))
                    except ValueError:
                        retry_after = 1.0
                    self.logger.warning(
                        f"Rate limited while deleting message {message_id}, waiting {retry_after}s"
                    )
                    time.sleep(retry_after)
                    continue
                if response.status_code == 404:
                    return True
                response.raise_for_status()
                return True
            except requests.exceptions.RequestException as e:
                self.logger.warning(
                    f"Failed to delete Discord message {message_id}: {e}"
                )
                time.sleep(1)
        return False

    def _cleanup_old_discord_messages(self, force=False):
        """保持件数を超えた古いDiscordメッセージをwebhook経由で削除する。"""
        if not self.discord_webhook_url:
            return

        last_cleanup = self._get_meta("last_discord_cleanup")
        if not force and last_cleanup:
            try:
                elapsed_hours = (
                    datetime.now() - datetime.fromisoformat(last_cleanup)
                ).total_seconds() / 3600
                if elapsed_hours < self.discord_cleanup_interval_hours:
                    return
            except ValueError:
                pass

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT message_id FROM discord_messages
                    ORDER BY posted_at DESC, rowid DESC
                    LIMIT -1 OFFSET ?
                    """,
                    (max(self.discord_keep_messages, 0),),
                )
                stale_ids = [row[0] for row in cursor.fetchall()]

            if not stale_ids:
                self.logger.info(
                    f"Discord message retention check: within limit ({self.discord_keep_messages})"
                )
                self._set_meta("last_discord_cleanup", datetime.now().isoformat())
                return

            self.logger.info(
                f"Deleting {len(stale_ids)} old Discord messages (keep={self.discord_keep_messages})"
            )
            deleted = 0
            for message_id in stale_ids:
                if self._delete_discord_message(message_id):
                    deleted += 1
                    with sqlite3.connect(self.db_path) as conn:
                        conn.execute(
                            "DELETE FROM discord_messages WHERE message_id = ?",
                            (message_id,),
                        )
                time.sleep(self.discord_delete_interval_seconds)

            self.logger.info(f"Deleted {deleted}/{len(stale_ids)} old Discord messages")
            self._set_meta("last_discord_cleanup", datetime.now().isoformat())
        except sqlite3.Error as e:
            self.logger.error(f"Database error during Discord message cleanup: {e}")

    def fetch_volatility_data(self):
        """CMMA APIから価格変動データを取得"""
        params = {
            "timeframe": self.timeframe,
            "threshold": self.threshold,
            "offset": self.offset,
            "direction": self.direction,
            "sort": self.volatility_sort,
            "limit": self.volatility_limit,
        }
        try:
            self.logger.info(f"Fetching data from CMMA API with params: {params}")
            response = requests.get(self.volatility_api_url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            if not isinstance(data, dict):
                self.logger.warning("API returned a non-object response.")
                return []
            if "error" in data:
                self.logger.error(f"API Error from CMMA: {data['error']}")
                return []
            records = data.get("data")
            count = data.get("count", 0)
            if (
                not isinstance(records, list)
                or not isinstance(count, int)
                or isinstance(count, bool)
                or count < 0
            ):
                self.logger.error("CMMA API returned malformed data payload")
                return []
            if count > 0:
                self.logger.info(f"Successfully fetched {count} records from CMMA API.")
                return records
            if records:
                self.logger.warning("CMMA API returned records with count=0")
                return records
            else:
                self.logger.info("No significant moves found from CMMA API.")
                return []

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error while fetching from CMMA API: {e}")
            return []
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to decode JSON response from CMMA API: {e}")
            return []
        except Exception as e:
            self.logger.error(f"An unexpected error occurred during API fetch: {e}")
            return []

    def fetch_high_volume_data(self):
        """CMMA APIから閾値以上の出来高を持つ銘柄データを取得"""
        if not self.volume_threshold > 0:
            return {}

        params = {
            "timeframe": self.volume_timeframe,
            "period": self.volume_period,
            "min_volume_target": "turnover",
            "min_volume": self.volume_threshold,
            "limit": self.volume_limit,
            "sort": self.volume_sort,
        }
        try:
            self.logger.info(
                f"Fetching high volume data from CMMA API with params: {params}"
            )
            response = requests.get(self.volume_api_url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            if not isinstance(data, dict):
                self.logger.warning("Volume API returned a non-object response.")
                return {}
            if "error" in data:
                self.logger.error(
                    f"API Error from CMMA volume endpoint: {data['error']}"
                )
                return {}
            records = data.get("data")
            if not isinstance(records, list):
                self.logger.error("CMMA volume API returned malformed data payload")
                return {}
            if "data" in data:
                self.logger.info(
                    f"Successfully fetched {len(records)} high volume symbols."
                )
                result = {}
                for item in records:
                    if isinstance(item, dict) and item.get("symbol"):
                        result[item["symbol"]] = item
                return result
            else:
                self.logger.info("No high volume data found from CMMA API.")
                return {}

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error while fetching from CMMA volume API: {e}")
            return {}
        except json.JSONDecodeError as e:
            self.logger.error(
                f"Failed to decode JSON response from CMMA volume API: {e}"
            )
            return {}
        except Exception as e:
            self.logger.error(
                f"An unexpected error occurred during volume API fetch: {e}"
            )
            return {}

    def _format_currency(self, num):
        """数値をK, M, B, Tの単位を持つドル表記文字列にフォーマットする"""
        if num is None:
            return "N/A"
        try:
            num = float(num)
        except (TypeError, ValueError):
            return "N/A"
        # NaN/Infはループが終了せず、例外で監視・通知が停止するため有限値のみ処理する
        if not math.isfinite(num):
            return f"{num:.2f}$"
        if abs(num) < 1000:
            return f"{num:,.2f}$"

        suffixes = ["", "K", "M", "B", "T"]
        magnitude = 0
        while abs(num) >= 1000 and magnitude < len(suffixes) - 1:
            magnitude += 1
            num /= 1000.0

        return f"{num:.2f}{suffixes[magnitude]}$"

    @staticmethod
    def _extract_change(token):
        """API応答のtokenからchange情報を安全に取得する。構造が不正ならNoneを返す。"""
        if not isinstance(token, dict):
            return None
        change = token.get("change")
        if not isinstance(change, dict):
            return None
        direction = change.get("direction")
        if direction not in ("up", "down", "both"):
            return None
        try:
            pct = float(change.get("pct"))
        except (TypeError, ValueError):
            return None
        if not math.isfinite(pct):
            return None
        return (pct, direction)

    @staticmethod
    def _extract_price(token):
        """API応答のtokenからprice情報を安全に取得する。構造が不正ならNoneを返す。"""
        if not isinstance(token, dict):
            return None
        price = token.get("price")
        if not isinstance(price, dict):
            return None
        return price

    def send_discord_notification(self, token_data):
        """Discordに通知を送信（重複チェック付き）"""
        if not token_data:
            return False

        # 重複チェック済みのトークンのみを通知
        filtered_tokens = []
        for token in token_data:
            change = self._extract_change(token)
            if change is None:
                self.logger.warning(
                    "Skipping token with invalid change data: %s", token
                )
                continue
            _, direction = change
            symbol = token.get("symbol")
            if not symbol:
                self.logger.warning("Skipping token without symbol: %s", token)
                continue
            notification_hash = self._generate_notification_hash(symbol, direction)

            if self._should_notify(notification_hash):
                filtered_tokens.append((token, notification_hash))

        if not filtered_tokens:
            self.logger.info("All tokens were filtered out due to recent notifications")
            return False

        # 最大通知数に制限
        limited_tokens_with_hash = filtered_tokens[: self.max_notifications]

        # タイトルと色を変動方向によって変更
        direction_map = {"up": "上昇", "down": "下落", "both": "変動"}
        title_direction = direction_map.get(self.direction, "変動")

        title = f"🚀 価格{title_direction}アラート"
        color = (
            0x00FF00
            if self.direction == "up"
            else (0xFF0000 if self.direction == "down" else 0x0099FF)
        )

        description = f"{len(limited_tokens_with_hash)}個のトークンが{self.timeframe}足で{self.threshold}%以上の{title_direction}を検知！"

        embed = {
            "title": title,
            "description": description,
            "color": color,
            "timestamp": datetime.utcnow().isoformat(),
            "fields": [],
            "footer": {
                "text": f"監視: CMMA API | 閾値: {self.threshold}% | 時間足: {self.timeframe} | 方向: {self.direction}"
            },
        }

        # フッターに出来高条件を追加
        if self.volume_threshold > 0:
            formatted_vol_threshold = self._format_currency(self.volume_threshold)
            embed["footer"][
                "text"
            ] += (
                f" | 出来高(Turnover)({self.volume_period}): >{formatted_vol_threshold}"
            )

        sent_tokens_with_hash = []
        for token, notification_hash in limited_tokens_with_hash:
            change = self._extract_change(token)
            if change is None:
                continue
            change_pct, direction = change
            price = self._extract_price(token)
            if price is None:
                continue
            try:
                close = float(price.get("close"))
                prev_close = float(price.get("prev_close"))
            except (TypeError, ValueError):
                self.logger.warning("Skipping token with invalid price data: %s", token)
                continue
            if not math.isfinite(close) or not math.isfinite(prev_close):
                self.logger.warning(
                    "Skipping token with non-finite price data: %s", token
                )
                continue
            direction_char = "📈" if direction == "up" else "📉"
            sign = "+" if direction == "up" else ""

            value = f"**{sign}{change_pct:.2f}%**\n`{prev_close:.6f}` → `{close:.6f}`"
            if "turnover" in token and token["turnover"] is not None:
                formatted_turnover = self._format_currency(token["turnover"])
                value += f"\nTurnover: `{formatted_turnover}`"

            embed["fields"].append(
                {
                    "name": f"{direction_char} {token.get('symbol', 'UNKNOWN')}",
                    "value": value,
                    "inline": True,
                }
            )
            sent_tokens_with_hash.append(
                (token, notification_hash, change_pct, direction)
            )

        if not sent_tokens_with_hash:
            self.logger.warning(
                "No tokens with valid price data remained for notification"
            )
            return False
        embed["description"] = (
            f"{len(sent_tokens_with_hash)}個のトークンが{self.timeframe}足で"
            f"{self.threshold}%以上の{title_direction}を検知！"
        )

        # 表示制限による省略がある場合
        if len(filtered_tokens) > len(sent_tokens_with_hash):
            embed["fields"].append(
                {
                    "name": "その他",
                    "value": f"さらに{len(filtered_tokens) - len(sent_tokens_with_hash)}個のトークンが条件を満たしています...",
                    "inline": False,
                }
            )

        # フィルターされたトークンがある場合の注記
        if len(token_data) > len(filtered_tokens):
            skipped_count = len(token_data) - len(filtered_tokens)
            embed[
                "description"
            ] += f"\n（{skipped_count}個は最近通知済みのためスキップ）"

        payload = {"embeds": [embed]}

        try:
            response = requests.post(self._discord_post_url(), json=payload, timeout=30)
            response.raise_for_status()
            self.logger.info(
                f"Discord notification sent: {len(sent_tokens_with_hash)} tokens."
            )

            try:
                message_id = response.json().get("id")
                if message_id:
                    self._record_discord_message(message_id)
            except ValueError:
                self.logger.warning("Discord response did not contain a message id")

            # 通知が成功したトークンの履歴をDBに記録
            for (
                token,
                notification_hash,
                change_pct,
                direction,
            ) in sent_tokens_with_hash:
                self._record_notification(
                    notification_hash,
                    token.get("symbol", ""),
                    self.timeframe,
                    direction,
                    change_pct,
                )
            self.logger.info(
                f"Recorded {len(sent_tokens_with_hash)} notifications to database."
            )
            return True

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to send Discord notification: {e}")
            return False

    def _cleanup_old_files(self):
        """古いファイルのクリーンアップ"""
        try:
            current_time = datetime.now()

            # 結果ファイルのクリーンアップ（7日以上前）
            cutoff_time = current_time - timedelta(days=7)
            deleted_results = 0

            for file_path in self.log_dir.glob("results_*.json"):
                try:
                    file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                    if file_mtime < cutoff_time:
                        file_path.unlink()
                        deleted_results += 1
                except Exception as e:
                    self.logger.warning(f"Failed to delete {file_path.name}: {e}")

            if deleted_results > 0:
                self.logger.info(f"Cleaned up {deleted_results} old result files")

            # ログディレクトリサイズチェック (削除対象は自前命名規則のファイルのみ)
            total_size = 0
            deletable_files = []
            for file_path in self.log_dir.rglob("*"):
                if not file_path.is_file():
                    continue
                if self._is_managed_log(file_path):
                    total_size += file_path.stat().st_size
                    deletable_files.append(file_path)
            total_size_mb = total_size / (1024 * 1024)

            if total_size_mb > self.log_max_size_mb:
                self.logger.info(
                    f"Log directory size ({total_size_mb:.2f}MB) exceeds limit ({self.log_max_size_mb}MB)"
                )

                # ファイルを古い順にソート
                files_by_age = []
                for file_path in deletable_files:
                    try:
                        files_by_age.append(
                            (
                                file_path,
                                file_path.stat().st_mtime,
                                file_path.stat().st_size,
                            )
                        )
                    except Exception as e:
                        self.logger.warning(f"Failed to stat {file_path.name}: {e}")

                files_by_age.sort(key=lambda x: x[1])  # 古い順

                deleted_size = 0
                for file_path, mtime, size in files_by_age:
                    if file_path.name == "ncmma_monitor.log":
                        continue
                    try:
                        file_path.unlink()
                        deleted_size += size
                        self.logger.info(
                            f"Deleted: {file_path.name} ({size/(1024*1024):.2f}MB)"
                        )

                        # 目標サイズ以下になったら停止
                        if (total_size - deleted_size) / (
                            1024 * 1024
                        ) <= self.log_max_size_mb * 0.8:
                            break
                    except Exception as e:
                        self.logger.warning(f"Failed to delete {file_path.name}: {e}")

                self.logger.info(
                    f"Log cleanup completed: {deleted_size/(1024*1024):.2f}MB freed"
                )

        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

    def monitor_volatility(self):
        """CMMA APIをチェックして閾値以上の変動がある銘柄を検出・通知"""
        start_time = datetime.now()
        self._write_heartbeat("running")

        self.logger.info("Starting CMMA API monitoring job")
        self.logger.info(
            f"Settings: timeframe={self.timeframe}, threshold={self.threshold}%, direction={self.direction}"
        )

        # ファイルクリーンアップを実行
        self._cleanup_old_files()

        # 古いDiscordメッセージの定期削除 (内部でインターバル制御される)
        self._cleanup_old_discord_messages()

        significant_moves = self.fetch_volatility_data()

        # 出来高フィルターが有効な場合
        if significant_moves and self.volume_threshold > 0:
            self.logger.info(
                f"Applying volume filter with threshold: {self.volume_threshold:,.2f}"
            )

            # 出来高の大きい銘柄データを取得
            high_volume_data = self.fetch_high_volume_data()

            if high_volume_data:
                filtered_moves = []
                for token in significant_moves:
                    symbol = token.get("symbol")
                    if not symbol:
                        self.logger.warning("Skipping token without symbol: %s", token)
                        continue
                    # 出来高データにシンボルが存在するかチェック
                    if symbol in high_volume_data:
                        # 出来高の値をtokenに追加
                        token["turnover"] = high_volume_data[symbol].get(
                            "total_turnover"
                        )
                        filtered_moves.append(token)
                    else:
                        self.logger.info(
                            f"Symbol {symbol} filtered out by volume. Not in high volume list."
                        )

                self.logger.info(
                    f"{len(significant_moves)} tokens -> {len(filtered_moves)} tokens after volume filter."
                )
                significant_moves = filtered_moves
            else:
                self.logger.warning(
                    "Could not fetch volume data or no symbols met volume threshold. All tokens will be filtered out."
                )
                significant_moves = []  # 出来高データが取れなかったら、何も通知しない

        # 実行時間計算
        execution_time = datetime.now() - start_time

        # 結果サマリー
        self.logger.info(
            f"Batch job completed in {execution_time.total_seconds():.1f}s"
        )
        self.logger.info(f"Found {len(significant_moves)} tokens meeting all criteria")

        # 結果をファイルに保存
        self._save_results(
            significant_moves,
            {"execution_time_seconds": execution_time.total_seconds()},
        )

        # Discord通知送信
        if significant_moves:
            notification_sent = self.send_discord_notification(significant_moves)
            if not notification_sent:
                self.logger.warning(
                    "Discord notification failed or all tokens were filtered"
                )
        else:
            self.logger.info("No tokens found meeting criteria")

        self._write_heartbeat(
            "healthy",
            {
                "execution_time_seconds": execution_time.total_seconds(),
                "tokens_found": len(significant_moves),
            },
        )

        return significant_moves

    def _save_results(self, significant_moves, stats):
        """結果をJSONファイルに保存"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = self.log_dir / f"results_{timestamp}.json"

        result_data = {
            "timestamp": datetime.now().isoformat(),
            "threshold": self.threshold,
            "timeframe": self.timeframe,
            "direction": self.direction,
            "volume_threshold": self.volume_threshold,
            "total_found": len(significant_moves),
            "stats": stats,
            "tokens": significant_moves,
        }

        try:
            with open(results_file, "w", encoding="utf-8") as f:
                json.dump(result_data, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Results saved to {results_file}")
        except Exception as e:
            self.logger.error(f"Failed to save results: {e}")


def main():
    """メイン実行関数"""
    try:
        # 開始ログ
        print(f"CMMA Price Monitor Batch - Started at {datetime.now().isoformat()}")

        # コマンドライン引数からの設定ファイルパス取得
        config_path = sys.argv[1] if len(sys.argv) > 1 else None

        monitor = CmmaPriceMonitor(config_path)
        monitor._write_heartbeat("starting")

        while True:
            try:
                significant_moves = monitor.monitor_volatility()
            except Exception as exc:
                monitor.logger.exception("Monitoring loop failed")
                monitor._write_heartbeat("error", {"error": str(exc)})
                raise

            # 結果出力
            if significant_moves:
                print(
                    f"SUCCESS: {len(significant_moves)} tokens found meeting criteria"
                )

                # 上位5件を表示
                print("Top moving tokens:")
                for i, token in enumerate(significant_moves[:5], 1):
                    change = monitor._extract_change(token)
                    if change is None:
                        continue
                    pct, direction = change
                    sign = "+" if direction == "up" else ""
                    print(f"  {i}. {token.get('symbol', 'UNKNOWN')}: {sign}{pct:.2f}%")
            else:
                print("INFO: No tokens found meeting criteria")

            print(
                f"Waiting for {monitor.check_interval_seconds} seconds until the next check..."
            )
            time.sleep(monitor.check_interval_seconds)

    except KeyboardInterrupt:
        print("INFO: Process interrupted by user.")
        sys.exit(0)
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
