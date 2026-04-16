#!/usr/bin/env python3
"""
CMMA API 価格監視バッチシステム
- 重複投稿防止機能 (SQLite)
- ログファイル自動削除機能
- 時間足に応じた再通知制御
"""

import requests
import json
import time
import sys
import os
import hashlib
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
import logging
from logging.handlers import RotatingFileHandler

class CmmaPriceMonitor:
    def __init__(self, config_path=None):
        # スクリプトのディレクトリを基準にパスを設定
        self.script_dir = Path(__file__).parent.absolute()
        self.config_path = config_path or self.script_dir / '.env'
        self.log_dir = self.script_dir / 'logs'
        self.cache_dir = self.script_dir / 'cache'
        self.heartbeat_path = self.cache_dir / 'heartbeat.json'
        
        # ディレクトリの作成
        self.log_dir.mkdir(exist_ok=True)
        self.cache_dir.mkdir(exist_ok=True)
        
        # 設定の読み込みとロギング設定
        self._load_config()
        self._setup_logging()
        
        # データベースの初期化
        self.data_dir = self.script_dir / 'data'
        self.data_dir.mkdir(exist_ok=True)
        self.db_path = self.data_dir / 'ncmma.db'
        self._init_db()

        if not self.discord_webhook_url:
            raise ValueError("DISCORD_WEBHOOK_URL not found in environment variables")
    
    def _load_config(self):
        """設定ファイルの読み込み"""
        if self.config_path.exists():
            load_dotenv(self.config_path)
        else:
            load_dotenv()
        
        self.discord_webhook_url = os.getenv('DISCORD_WEBHOOK_URL')
        self.volatility_api_url = os.getenv('CMMA_VOLATILITY_API_URL', 'https://stg.api.1btc.love/volatility')
        
        # APIクエリパラメータ
        self.timeframe = os.getenv('TIMEFRAME', '4h')
        self.threshold = float(os.getenv('THRESHOLD', '5.0'))
        self.direction = os.getenv('DIRECTION', 'up')
        self.volatility_sort = os.getenv('CMMA_VOLATILITY_API_SORT', 'volatility_desc')
        self.volatility_limit = int(os.getenv('CMMA_VOLATILITY_API_LIMIT', '100'))
        self.offset = int(os.getenv('OFFSET', '5'))

        # 監視設定
        self.max_notifications = int(os.getenv('MAX_NOTIFICATIONS', '20'))
        self.renotify_buffer_minutes = int(os.getenv('RENOTIFY_BUFFER_MINUTES', '60'))
        self.check_interval_seconds = int(os.getenv('CHECK_INTERVAL_SECONDS', '300'))
        
        # ログ設定
        self.log_max_size_mb = int(os.getenv('LOG_MAX_SIZE_MB', '10'))

        # 出来高フィルター設定
        self.volume_api_url = os.getenv('CMMA_VOLUME_API_URL', 'https://stg.api.1btc.love/volume')
        self.volume_threshold = float(os.getenv('VOLUME_THRESHOLD', '0.0'))
        self.volume_timeframe = os.getenv('VOLUME_TIMEFRAME', '1h')
        self.volume_period = os.getenv('VOLUME_PERIOD', '24h')
        self.volume_sort = os.getenv('CMMA_VOLUME_API_SORT', 'turnover_desc')
        self.volume_limit = int(os.getenv('CMMA_VOLUME_API_LIMIT', '100'))

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
                conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Database initialization failed: {e}")
            raise

    def _setup_logging(self):
        """ログ設定"""
        log_file = self.log_dir / 'ncmma_monitor.log'
        
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # ファイルハンドラー（ローテーション付き）
        file_handler = RotatingFileHandler(
            log_file, maxBytes=5*1024*1024, backupCount=3
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)
        
        # コンソールハンドラー
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)
        
        # ロガー設定
        self.logger = logging.getLogger('CmmaMonitor')
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        self.logger.propagate = False

    def _write_heartbeat(self, status, extra=None):
        """ヘルスチェック用のheartbeatファイルを更新"""
        payload = {
            'status': status,
            'timestamp': datetime.now().isoformat(),
        }
        if extra:
            payload.update(extra)

        with open(self.heartbeat_path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False)
    
    def _cleanup_logs(self):
        """ログディレクトリの自動削除（10MB以上）"""
        try:
            total_size = 0
            log_files = []
            
            # ログディレクトリのファイルサイズを計算
            for file_path in self.log_dir.rglob('*'):
                if file_path.is_file():
                    size = file_path.stat().st_size
                    total_size += size
                    log_files.append((file_path, size, file_path.stat().st_mtime))
            
            total_size_mb = total_size / (1024 * 1024)
            self.logger.info(f"Total log directory size: {total_size_mb:.2f}MB")
            
            # 10MB以上の場合、古いファイルから削除
            if total_size_mb > self.log_max_size_mb:
                self.logger.info(f"Log directory exceeds {self.log_max_size_mb}MB, cleaning up...")
                
                # ファイルを作成日時でソート（古い順）
                log_files.sort(key=lambda x: x[2])
                
                deleted_size = 0
                deleted_count = 0
                
                for file_path, size, mtime in log_files:
                    # 現在のログファイルとメインログは削除しない
                    if (file_path.name == 'ncmma_monitor.log' or 
                        file_path.name.startswith('ncmma_monitor.log.')):
                        continue
                    
                    try:
                        file_path.unlink()
                        deleted_size += size
                        deleted_count += 1
                        self.logger.info(f"Deleted old log file: {file_path.name}")
                        
                        # 目標サイズ以下になったら停止
                        if (total_size - deleted_size) / (1024 * 1024) <= self.log_max_size_mb * 0.8:
                            break
                            
                    except Exception as e:
                        self.logger.warning(f"Failed to delete {file_path.name}: {e}")
                
                self.logger.info(f"Cleanup completed: {deleted_count} files deleted, {deleted_size/(1024*1024):.2f}MB freed")
        
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
                    (notification_hash,)
                )
                result = cursor.fetchone()

                if result:
                    last_notified_at = datetime.fromisoformat(result[0])
                    time_diff = datetime.now() - last_notified_at
                    
                    if time_diff.total_seconds() < self.renotify_buffer_minutes * 60:
                        remaining_minutes = self.renotify_buffer_minutes - (time_diff.total_seconds() / 60)
                        # self.logger.debug(f"Skipping hash {notification_hash[:8]}...: {remaining_minutes:.1f}min remaining")
                        return False
                return True
        except sqlite3.Error as e:
            self.logger.error(f"Failed to check notification history from DB: {e}")
            return False # DBエラー時は通知しない

    def _record_notification(self, notification_hash, symbol, timeframe, direction, change_pct):
        """通知履歴をDBに記録"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO notifications (notification_hash, symbol, timeframe, direction, change_pct, notified_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (notification_hash, symbol, timeframe, direction, change_pct, datetime.now().isoformat()))
                conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Failed to record notification to DB: {e}")

    def fetch_volatility_data(self):
        """CMMA APIから価格変動データを取得"""
        params = {
            'timeframe': self.timeframe,
            'threshold': self.threshold,
            'offset': self.offset,
            'direction': self.direction,
            'sort': self.volatility_sort,
            'limit': self.volatility_limit,
        }
        try:
            self.logger.info(f"Fetching data from CMMA API with params: {params}")
            response = requests.get(self.volatility_api_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if 'data' in data and data['count'] > 0:
                self.logger.info(f"Successfully fetched {data['count']} records from CMMA API.")
                return data['data']
            elif 'error' in data:
                self.logger.error(f"API Error from CMMA: {data['error']}")
                return []
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
            'timeframe': self.volume_timeframe,
            'period': self.volume_period,
            'min_volume_target': 'turnover',
            'min_volume': self.volume_threshold,
            'limit': self.volume_limit,
            'sort': self.volume_sort
        }
        try:
            self.logger.info(f"Fetching high volume data from CMMA API with params: {params}")
            response = requests.get(self.volume_api_url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            if 'data' in data:
                self.logger.info(f"Successfully fetched {len(data['data'])} high volume symbols.")
                return {item['symbol']: item for item in data['data']}
            elif 'error' in data:
                self.logger.error(f"API Error from CMMA volume endpoint: {data['error']}")
                return {}
            else:
                self.logger.info("No high volume data found from CMMA API.")
                return {}

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error while fetching from CMMA volume API: {e}")
            return {}
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to decode JSON response from CMMA volume API: {e}")
            return {}
        except Exception as e:
            self.logger.error(f"An unexpected error occurred during volume API fetch: {e}")
            return {}
        
    def _format_currency(self, num):
        """数値をK, M, B, Tの単位を持つドル表記文字列にフォーマットする"""
        if num is None:
            return "N/A"
        num = float(num)
        if num < 1000:
            return f"{num:,.2f}$"
        
        magnitude = 0
        while abs(num) >= 1000:
            magnitude += 1
            num /= 1000.0
        
        return f"{num:.2f}{['', 'K', 'M', 'B', 'T'][magnitude]}$"

    def send_discord_notification(self, token_data):
        """Discordに通知を送信（重複チェック付き）"""
        if not token_data:
            return False
        
        # 重複チェック済みのトークンのみを通知
        filtered_tokens = []
        for token in token_data:
            change_pct = token['change']['pct']
            direction = token['change']['direction']
            notification_hash = self._generate_notification_hash(token['symbol'], direction)
            
            if self._should_notify(notification_hash):
                filtered_tokens.append((token, notification_hash))
        
        if not filtered_tokens:
            self.logger.info("All tokens were filtered out due to recent notifications")
            return False
        
        # 最大通知数に制限
        limited_tokens_with_hash = filtered_tokens[:self.max_notifications]
        
        # タイトルと色を変動方向によって変更
        direction_map = {'up': '上昇', 'down': '下落', 'both': '変動'}
        title_direction = direction_map.get(self.direction, "変動")

        title = f"🚀 価格{title_direction}アラート"
        color = 0x00ff00 if self.direction == 'up' else (0xff0000 if self.direction == 'down' else 0x0099ff)
            
        description = f"{len(limited_tokens_with_hash)}個のトークンが{self.timeframe}足で{self.threshold}%以上の{title_direction}を検知！"

        embed = {
            "title": title,
            "description": description,
            "color": color,
            "timestamp": datetime.utcnow().isoformat(),
            "fields": [],
            "footer": {
                "text": f"監視: CMMA API | 閾値: {self.threshold}% | 時間足: {self.timeframe} | 方向: {self.direction}"
            }
        }
        
        # フッターに出来高条件を追加
        if self.volume_threshold > 0:
            formatted_vol_threshold = self._format_currency(self.volume_threshold)
            embed["footer"]["text"] += f" | 出来高(Turnover)({self.volume_period}): >{formatted_vol_threshold}"

        for token, _ in limited_tokens_with_hash:
            change_pct = token['change']['pct']
            direction_char = "📈" if token['change']['direction'] == 'up' else "📉"
            sign = "+" if token['change']['direction'] == 'up' else ""

            value = f"**{sign}{change_pct:.2f}%**\n`{token['price']['prev_close']:.6f}` → `{token['price']['close']:.6f}`"
            if 'turnover' in token and token['turnover'] is not None:
                formatted_turnover = self._format_currency(token['turnover'])
                value += f"\nTurnover: `{formatted_turnover}`"

            embed["fields"].append({
                "name": f"{direction_char} {token['symbol']}",
                "value": value,
                "inline": True
            })
        
        # 表示制限による省略がある場合
        if len(filtered_tokens) > self.max_notifications:
            embed["fields"].append({
                "name": "その他",
                "value": f"さらに{len(filtered_tokens) - self.max_notifications}個のトークンが条件を満たしています...",
                "inline": False
            })
        
        # フィルターされたトークンがある場合の注記
        if len(token_data) > len(filtered_tokens):
            skipped_count = len(token_data) - len(filtered_tokens)
            embed["description"] += f"\n（{skipped_count}個は最近通知済みのためスキップ）"
        
        payload = {
            "embeds": [embed]
        }
        
        try:
            response = requests.post(self.discord_webhook_url, json=payload, timeout=30)
            response.raise_for_status()
            self.logger.info(f"Discord notification sent: {len(limited_tokens_with_hash)} tokens.")

            # 通知が成功したトークンの履歴をDBに記録
            for token, notification_hash in limited_tokens_with_hash:
                self._record_notification(
                    notification_hash,
                    token['symbol'],
                    self.timeframe,
                    token['change']['direction'],
                    token['change']['pct']
                )
            self.logger.info(f"Recorded {len(limited_tokens_with_hash)} notifications to database.")
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
            
            for file_path in self.log_dir.glob('results_*.json'):
                try:
                    file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                    if file_mtime < cutoff_time:
                        file_path.unlink()
                        deleted_results += 1
                except Exception as e:
                    self.logger.warning(f"Failed to delete {file_path.name}: {e}")
            
            if deleted_results > 0:
                self.logger.info(f"Cleaned up {deleted_results} old result files")
            
            # ログディレクトリサイズチェック
            total_size = sum(
                f.stat().st_size for f in self.log_dir.rglob('*') if f.is_file()
            )
            total_size_mb = total_size / (1024 * 1024)
            
            if total_size_mb > self.log_max_size_mb:
                self.logger.info(f"Log directory size ({total_size_mb:.2f}MB) exceeds limit ({self.log_max_size_mb}MB)")
                
                # ファイルを古い順にソート
                files_by_age = []
                for file_path in self.log_dir.rglob('*'):
                    if file_path.is_file() and not file_path.name.endswith('.log'):  # 現在のログファイルは除外
                        files_by_age.append((file_path, file_path.stat().st_mtime, file_path.stat().st_size))
                
                files_by_age.sort(key=lambda x: x[1])  # 古い順
                
                deleted_size = 0
                for file_path, mtime, size in files_by_age:
                    try:
                        file_path.unlink()
                        deleted_size += size
                        self.logger.info(f"Deleted: {file_path.name} ({size/(1024*1024):.2f}MB)")
                        
                        # 目標サイズ以下になったら停止
                        if (total_size - deleted_size) / (1024 * 1024) <= self.log_max_size_mb * 0.8:
                            break
                    except Exception as e:
                        self.logger.warning(f"Failed to delete {file_path.name}: {e}")
                
                self.logger.info(f"Log cleanup completed: {deleted_size/(1024*1024):.2f}MB freed")
        
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
    
    def monitor_volatility(self):
        """CMMA APIをチェックして閾値以上の変動がある銘柄を検出・通知"""
        start_time = datetime.now()
        self._write_heartbeat('running')
        
        self.logger.info(f"Starting CMMA API monitoring job")
        self.logger.info(f"Settings: timeframe={self.timeframe}, threshold={self.threshold}%, direction={self.direction}")
        
        # ファイルクリーンアップを実行
        self._cleanup_old_files()

        significant_moves = self.fetch_volatility_data()
        
        # 出来高フィルターが有効な場合
        if significant_moves and self.volume_threshold > 0:
            self.logger.info(f"Applying volume filter with threshold: {self.volume_threshold:,.2f}")
            
            # 出来高の大きい銘柄データを取得
            high_volume_data = self.fetch_high_volume_data()

            if high_volume_data:
                filtered_moves = []
                for token in significant_moves:
                    symbol = token['symbol']
                    # 出来高データにシンボルが存在するかチェック
                    if symbol in high_volume_data:
                        # 出来高の値をtokenに追加
                        token['turnover'] = high_volume_data[symbol].get('total_turnover')
                        filtered_moves.append(token)
                    else:
                        self.logger.info(f"Symbol {symbol} filtered out by volume. Not in high volume list.")
                
                self.logger.info(f"{len(significant_moves)} tokens -> {len(filtered_moves)} tokens after volume filter.")
                significant_moves = filtered_moves
            else:
                self.logger.warning("Could not fetch volume data or no symbols met volume threshold. All tokens will be filtered out.")
                significant_moves = [] # 出来高データが取れなかったら、何も通知しない

        # 実行時間計算
        execution_time = datetime.now() - start_time
        
        # 結果サマリー
        self.logger.info(f"Batch job completed in {execution_time.total_seconds():.1f}s")
        self.logger.info(f"Found {len(significant_moves)} tokens meeting all criteria")
        
        # 結果をファイルに保存
        self._save_results(significant_moves, {
            'execution_time_seconds': execution_time.total_seconds()
        })
        
        # Discord通知送信
        if significant_moves:
            notification_sent = self.send_discord_notification(significant_moves)
            if not notification_sent:
                self.logger.warning("Discord notification failed or all tokens were filtered")
        else:
            self.logger.info("No tokens found meeting criteria")

        self._write_heartbeat('healthy', {
            'execution_time_seconds': execution_time.total_seconds(),
            'tokens_found': len(significant_moves),
        })
        
        return significant_moves
    
    def _save_results(self, significant_moves, stats):
        """結果をJSONファイルに保存"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        results_file = self.log_dir / f'results_{timestamp}.json'
        
        result_data = {
            'timestamp': datetime.now().isoformat(),
            'threshold': self.threshold,
            'timeframe': self.timeframe,
            'direction': self.direction,
            'volume_threshold': self.volume_threshold,
            'total_found': len(significant_moves),
            'stats': stats,
            'tokens': significant_moves
        }
        
        try:
            with open(results_file, 'w', encoding='utf-8') as f:
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
        monitor._write_heartbeat('starting')
        
        while True:
            try:
                significant_moves = monitor.monitor_volatility()
            except Exception as exc:
                monitor.logger.exception("Monitoring loop failed")
                monitor._write_heartbeat('error', {'error': str(exc)})
                raise
            
            # 結果出力
            if significant_moves:
                print(f"SUCCESS: {len(significant_moves)} tokens found meeting criteria")
                
                # 上位5件を表示
                print("Top moving tokens:")
                for i, token in enumerate(significant_moves[:5], 1):
                    sign = "+" if token['change']['direction'] == 'up' else ""
                    print(f"  {i}. {token['symbol']}: {sign}{token['change']['pct']:.2f}%")
            else:
                print(f"INFO: No tokens found meeting criteria")
            
            print(f"Waiting for {monitor.check_interval_seconds} seconds until the next check...")
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
