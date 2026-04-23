import json
import threading
import time
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus

import pypinyin
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from app.core.config import settings
from app.core.event import Event, eventmanager
from app.helper.mediaserver import MediaServerHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import ServiceInfo
from app.schemas.types import EventType, NotificationType
from plexapi.library import LibrarySection

from .poster import build_overlay_poster, download_poster, is_overlay_poster

lock = threading.Lock()


class MPPlexTools(_PluginBase):
    plugin_name = "MP Plex工具箱"
    plugin_desc = "为 MoviePilot V2 提供 Plex 中文本地化、Fanart 封面优选和海报信息叠加。"
    plugin_icon = "https://github.com/miniers/MoviePilot-Plugins/blob/main/icons/mpplextools.jpg?raw=true"
    plugin_version = "0.1.13"
    plugin_author = "miniers"
    author_url = "https://github.com/miniers/MoviePilot-Plugins"
    plugin_config_prefix = "mpplextools_"
    plugin_order = 96
    auth_level = 1

    mediaserver_helper = None
    _enabled = False
    _onlyonce = False
    _cron = None
    _notify = True
    _mediaservers: List[str] = []
    _libraries: List[str] = []
    _execute_transfer = True
    _delay = 180
    _translate_tags = True
    _sort_title = True
    _fanart = True
    _overlay_poster = False
    _lock_metadata = False
    _verbose_logging = False
    _collection = False
    _run_mode = "run_all"
    _recent_limit = 10
    _batch_size = 100
    _custom_tags_json = ""
    _scheduler = None
    _event = threading.Event()
    _transfer_debounce: Dict[str, float] = {}
    _transfer_refresh_retries = 6
    _transfer_refresh_interval = 10
    _backup_retention_days = 30
    _last_run_stats: Dict[str, Any] = {}
    _recent_skip_reasons: List[Dict[str, str]] = []
    _processed_index: Optional[Dict[str, List[str]]] = None
    _processed_index_dirty = False

    _default_tags = {
        "Anime": "动画",
        "Action": "动作",
        "Mystery": "悬疑",
        "Tv Movie": "电视电影",
        "Animation": "动画",
        "Crime": "犯罪",
        "Family": "家庭",
        "Fantasy": "奇幻",
        "Disaster": "灾难",
        "Adventure": "冒险",
        "Short": "短片",
        "Horror": "恐怖",
        "History": "历史",
        "Suspense": "悬疑",
        "Biography": "传记",
        "Sport": "运动",
        "Comedy": "喜剧",
        "Romance": "爱情",
        "Thriller": "惊悚",
        "Documentary": "纪录",
        "Indie": "独立",
        "Music": "音乐",
        "Sci-Fi": "科幻",
        "Western": "西部",
        "Children": "儿童",
        "Martial Arts": "武侠",
        "Drama": "剧情",
        "War": "战争",
        "Musical": "歌舞",
        "Film-noir": "黑色",
        "Science Fiction": "科幻",
        "Film-Noir": "黑色",
        "Food": "饮食",
        "War & Politics": "战争与政治",
        "Mini-Series": "迷你剧",
        "Reality": "真人秀",
        "Talk Show": "脱口秀"
    }

    def init_plugin(self, config: dict = None):
        self.mediaserver_helper = MediaServerHelper()
        if not config:
            return

        self._enabled = bool(config.get("enabled"))
        self._onlyonce = bool(config.get("onlyonce"))
        self._notify = bool(config.get("notify", True))
        self._execute_transfer = bool(config.get("execute_transfer", True))
        self._translate_tags = bool(config.get("translate_tags", True))
        self._sort_title = bool(config.get("sort_title", True))
        self._fanart = bool(config.get("fanart", True))
        self._overlay_poster = bool(config.get("overlay_poster", False))
        self._lock_metadata = bool(config.get("lock_metadata", False))
        self._verbose_logging = bool(config.get("verbose_logging", False))
        self._collection = bool(config.get("collection", False))
        self._run_mode = config.get("run_mode") or "run_all"
        try:
            self._recent_limit = int(config.get("recent_limit", 10))
        except Exception:
            self._recent_limit = 10
        self._cron = config.get("cron") or "30 3 * * *"
        self._mediaservers = config.get("mediaservers") or []
        self._libraries = config.get("libraries") or []
        self._custom_tags_json = config.get("custom_tags_json") or self._preset_tags_json()
        try:
            self._delay = int(config.get("delay", 180))
        except Exception:
            self._delay = 180
        try:
            self._batch_size = int(config.get("batch_size", 100))
        except Exception:
            self._batch_size = 100
        try:
            self._backup_retention_days = int(config.get("backup_retention_days", 30))
        except Exception:
            self._backup_retention_days = 30

        self.stop_service()

        self._scheduler = BackgroundScheduler(timezone=settings.TZ)
        if self._onlyonce:
            self._scheduler.add_job(
                func=self.run_full_scan,
                trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                kwargs={"trigger_source": "onlyonce", "ignore_overlay_marker": True},
                name=self.plugin_name,
            )
            self._onlyonce = False

        self.update_config({
            "enabled": self._enabled,
            "onlyonce": False,
            "notify": self._notify,
            "execute_transfer": self._execute_transfer,
            "translate_tags": self._translate_tags,
            "sort_title": self._sort_title,
            "fanart": self._fanart,
            "overlay_poster": self._overlay_poster,
            "lock_metadata": self._lock_metadata,
            "verbose_logging": self._verbose_logging,
            "collection": self._collection,
            "run_mode": self._run_mode,
            "cron": self._cron,
            "mediaservers": self._mediaservers,
            "libraries": self._libraries,
            "delay": self._delay,
            "recent_limit": self._recent_limit,
            "batch_size": self._batch_size,
            "backup_retention_days": self._backup_retention_days,
            "custom_tags_json": self._custom_tags_json,
        })

        if self._scheduler.get_jobs():
            self._scheduler.start()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [
            {
                "cmd": "/mp_plex_tools",
                "event": EventType.PluginAction,
                "desc": "运行 MP Plex工具箱",
                "category": "Plex",
                "data": {"action": "mp_plex_tools_run"},
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/run",
                "endpoint": self.api_run,
                "methods": ["POST"],
                "summary": "运行 Plex 整理",
                "description": "执行一次 MP Plex工具箱 全量或指定媒体库整理",
            }
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        if self._enabled and self._cron:
            return [{
                "id": "MPPlexTools",
                "name": self.plugin_name,
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.run_full_scan,
                "kwargs": {"trigger_source": "schedule"},
            }]
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        plex_items = [
            {"title": config.name, "value": config.name}
            for config in self.mediaserver_helper.get_configs().values()
            if config.type == "plex"
        ]

        content = [
            self._alert_row(
                "MP Plex工具箱会在你选择的 Plex 服务器与媒体库上执行标签中文化、拼音排序、Fanart 优选、海报叠加和元数据锁定整理。建议先只选择一个媒体库做小范围验证，确认效果后再扩大范围。"
            ),
            self._alert_row(
                "入库事件模式会在 TransferComplete 后尝试触发 Plex 按路径局部刷新，并在短时间内重试定位新条目。如果宿主挂载、Plex 媒体库路径或扫描权限异常，入库后整理可能会延迟或跳过。",
                alert_type="warning",
            ),
            self._alert_row(
                "一、执行入口：控制插件何时触发。建议先用“立即运行一次”验证，再视情况开启定时和入库后执行。",
                variant="outlined",
            ),
            self._row(
                self._switch_col("enabled", "启用插件", "关闭后不会响应定时任务、命令或入库事件。"),
                self._switch_col("onlyonce", "立即运行一次", "保存后约 3 秒触发一次最近条目整理，仅生效一次并自动复位。"),
                self._switch_col("notify", "发送通知", "整理完成后发送站内通知，便于确认处理条目数和运行模式。"),
                self._switch_col("execute_transfer", "入库后执行", "收到 TransferComplete 后按目标路径触发 Plex 局部刷新，再尝试命中新入库条目继续整理。"),
            ),
            self._row(
                self._cron_col("cron", "执行周期", "仅在插件启用时生效；建议避开 Plex 自身的大规模扫描时间段。"),
                self._text_col("delay", "入库延迟秒数", "TransferComplete 后先等待这段时间，再触发局部刷新与新条目定位。Plex 扫描慢时可适当调大。"),
            ),
            self._alert_row(
                "二、整理能力：决定插件对命中的媒体做什么处理。海报叠加会联动 show、season、episode，建议先在单库验证样式。",
                variant="outlined",
            ),
            self._row(
                self._switch_col("translate_tags", "标签中文化", "将命中的英文类型标签替换为内置或自定义中文标签。"),
                self._switch_col("sort_title", "拼音排序", "根据标题首字母生成 `titleSort`，便于中文媒体在 Plex 中按拼音排序。"),
                self._switch_col("fanart", "优选 Fanart", "优先选取 provider 为 fanarttv 的海报和背景；对应字段已锁定时会自动跳过。"),
            ),
            self._row(
                self._switch_col("overlay_poster", "海报叠加媒体信息", "在原始海报底部叠加分辨率、HDR/DV、时长和评分，并优先使用未叠加海报或历史备份。"),
                self._switch_col("lock_metadata", "整理后锁定相关元数据", "对已写入的海报、背景或标签调用 Plex 锁定接口，避免后续刮削覆盖。"),
                self._switch_col("collection", "处理合集", "整理时额外扫描合集对象；数量多时会明显增加执行时间。"),
            ),
            self._row(
                self._select_col(
                    "run_mode",
                    "运行模式",
                    [
                        {"title": "全部整理", "value": "run_all"},
                        {"title": "仅锁定海报背景", "value": "run_locked"},
                        {"title": "仅解锁海报背景", "value": "run_unlocked"},
                    ],
                    "全部整理会执行所有已开启能力；锁定/解锁模式只处理海报和背景锁定状态，不做标签、排序或海报叠加。",
                    md=8,
                ),
            ),
            self._alert_row(
                "三、执行范围：限制插件作用在哪些 Plex 服务器、媒体库和条目范围上。最近条目模式按 addedAt 倒序截取，全量模式按批次数量处理。",
                variant="outlined",
            ),
            self._row(
                self._text_col("recent_limit", "最近条目数", "最近条目模式下每个媒体库最多处理多少个最新条目；建议先用小值验证。"),
                self._text_col("batch_size", "全量模式处理数", "全量模式下每个媒体库本轮最多处理多少个条目；数值越大执行时间越长。"),
                self._switch_col("verbose_logging", "输出详细日志", "打印海报选择、局部刷新、条目搜索和字段处理细节，排障时开启即可."),
            ),
            self._row(
                self._multi_select_col(
                    "mediaservers",
                    "Plex 媒体服务器",
                    plex_items,
                    "留空表示处理所有在线的 Plex 服务；建议先只选一个服务做效果验证。",
                ),
                full_width=True,
            ),
            self._row(
                self._multi_select_col(
                    "libraries",
                    "媒体库（留空=全部）",
                    self._library_options(),
                    "支持选择“服务名:媒体库名”精确范围；留空表示扫描所选服务的全部非照片媒体库。",
                ),
                full_width=True,
            ),
            self._alert_row(
                "四、高级与自定义：只有在默认内置标签不满足需求时，再修改自定义标签 JSON。JSON 解析失败时会自动回退到内置映射。",
                variant="outlined",
            ),
            self._row(
                self._text_col("backup_retention_days", "海报备份保留天数", "海报原始备份超过该天数后会在执行开始时清理；填 0 或负数表示不自动清理。"),
            ),
            self._row(
                {
                    "component": "VCol",
                    "props": {"cols": 12},
                    "content": [{
                        "component": "VAceEditor",
                        "props": {"modelvalue": "custom_tags_json", "lang": "json", "theme": "monokai", "style": "height: 20rem"},
                    }],
                }
            ),
            self._alert_row(
                "自定义标签 JSON 格式示例：{\"Action\": \"动作\", \"Science Fiction\": \"科幻\"}。插件会先追加中文标签，再移除命中的英文标签。"
            ),
        ]

        return [
            {
                "component": "VForm",
                "content": content,
            }
        ], self._form_defaults()

    @staticmethod
    def _row(*content: dict, full_width: bool = False) -> Dict[str, Any]:
        if full_width:
            return {"component": "VRow", "content": list(content)}
        return {"component": "VRow", "content": list(content)}

    @staticmethod
    def _col(content: List[dict], cols: int = 12, md: Optional[int] = None) -> Dict[str, Any]:
        props = {"cols": cols}
        if md is not None:
            props["md"] = md
        return {"component": "VCol", "props": props, "content": content}

    def _switch_col(self, model: str, label: str, hint: str, md: int = 4) -> Dict[str, Any]:
        return self._col([
            {"component": "VSwitch", "props": {"model": model, "label": label, "hint": hint, "persistent-hint": True}}
        ], md=md)

    def _text_col(self, model: str, label: str, hint: str, md: int = 4) -> Dict[str, Any]:
        return self._col([
            {"component": "VTextField", "props": {"model": model, "label": label, "hint": hint, "persistent-hint": True}}
        ], md=md)

    def _cron_col(self, model: str, label: str, hint: str, md: int = 4) -> Dict[str, Any]:
        return self._col([
            {"component": "VCronField", "props": {"model": model, "label": label, "hint": hint, "persistent-hint": True}}
        ], md=md)

    def _select_col(self, model: str, label: str, items: List[Dict[str, Any]], hint: str, md: int = 4) -> Dict[str, Any]:
        return self._col([
            {"component": "VSelect", "props": {"model": model, "label": label, "items": items, "hint": hint, "persistent-hint": True}}
        ], md=md)

    def _multi_select_col(self, model: str, label: str, items: List[Dict[str, Any]], hint: str) -> Dict[str, Any]:
        return self._col([
            {
                "component": "VSelect",
                "props": {
                    "multiple": True,
                    "chips": True,
                    "clearable": True,
                    "model": model,
                    "label": label,
                    "items": items,
                    "hint": hint,
                    "persistent-hint": True,
                },
            }
        ])

    def _alert_row(self, text: str, alert_type: str = "info", variant: str = "tonal") -> Dict[str, Any]:
        return self._row(self._col([
            {"component": "VAlert", "props": {"type": alert_type, "variant": variant, "text": text}}
        ]))

    def _form_defaults(self) -> Dict[str, Any]:
        return {
            "enabled": self._enabled,
            "onlyonce": False,
            "notify": self._notify,
            "execute_transfer": self._execute_transfer,
            "translate_tags": self._translate_tags,
            "sort_title": self._sort_title,
            "fanart": self._fanart,
            "overlay_poster": self._overlay_poster,
            "lock_metadata": self._lock_metadata,
            "verbose_logging": self._verbose_logging,
            "collection": self._collection,
            "run_mode": self._run_mode,
            "cron": self._cron or "30 3 * * *",
            "delay": self._delay,
            "recent_limit": self._recent_limit,
            "batch_size": self._batch_size,
            "backup_retention_days": self._backup_retention_days,
            "mediaservers": self._mediaservers,
            "libraries": self._libraries,
            "custom_tags_json": self._custom_tags_json or self._preset_tags_json(),
        }

    def get_page(self) -> List[dict]:
        stats = self._last_run_stats or {}
        summary = stats.get("summary") or "暂无执行记录，先运行一次整理后这里会展示结果。"
        details = stats.get("details") or []
        skip_reasons = self._recent_skip_reasons[-20:]
        preview = stats.get("poster_preview") or {}

        detail_text = "\n".join(details) if details else "暂无分媒体库明细。"
        skip_text = "\n".join(
            f"- [{item.get('stage', 'unknown')}] {item.get('title', 'unknown')}: {item.get('reason', '未记录原因')}"
            for item in skip_reasons
        ) if skip_reasons else "暂无跳过记录。"
        preview_text = preview.get("message") or "暂无海报调试预览记录。"

        return [
            {
                "component": "VRow",
                "content": [
                    self._col([
                        {"component": "VAlert", "props": {"type": "info", "variant": "tonal", "text": summary}}
                    ])
                ],
            },
            {
                "component": "VRow",
                "content": [
                    self._col([
                        {"component": "VAlert", "props": {"type": "info", "variant": "outlined", "text": "最近一次执行明细"}},
                        {"component": "VAlert", "props": {"type": "info", "variant": "tonal", "text": detail_text}},
                    ])
                ],
            },
            {
                "component": "VRow",
                "content": [
                    self._col([
                        {"component": "VAlert", "props": {"type": "warning", "variant": "outlined", "text": "最近跳过 / 诊断记录"}},
                        {"component": "VAlert", "props": {"type": "warning", "variant": "tonal", "text": skip_text}},
                    ])
                ],
            },
            {
                "component": "VRow",
                "content": [
                    self._col([
                        {"component": "VAlert", "props": {"type": "info", "variant": "outlined", "text": "最近一次海报调试预览"}},
                        {"component": "VAlert", "props": {"type": "info", "variant": "tonal", "text": preview_text}},
                    ])
                ],
            },
            {
                "component": "VRow",
                "content": [
                    self._col([
                        {"component": "VAlert", "props": {"type": "info", "variant": "outlined", "text": f"预览图片路径：{preview.get('image_url') or '暂无'}"}}
                    ])
                ],
            },
        ]


    def stop_service(self):
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._event.set()
                    self._scheduler.shutdown()
                    self._event.clear()
                self._scheduler = None
        except Exception as err:
            logger.info(str(err))

    def api_run(self, data: Optional[dict] = None):
        payload = data or {}
        libraries = payload.get("libraries") if isinstance(payload, dict) else None
        mode = payload.get("mode", "recent") if isinstance(payload, dict) else "recent"
        run_mode = payload.get("run_mode") if isinstance(payload, dict) else None
        collection = payload.get("collection") if isinstance(payload, dict) else None
        recent_only = mode != "full"
        kwargs = {"libraries": libraries, "recent_only": recent_only, "trigger_source": "api"}
        if run_mode:
            kwargs["run_mode"] = run_mode
        if collection is not None:
            kwargs["collection"] = bool(collection)
        threading.Thread(target=self.run_full_scan, kwargs=kwargs, daemon=True).start()
        return {"success": True, "message": "任务已启动", "mode": mode, "run_mode": run_mode or self._run_mode}

    @eventmanager.register(EventType.PluginAction)
    def handle_command(self, event: Event = None):
        if not event or not event.event_data:
            return
        if event.event_data.get("action") != "mp_plex_tools_run":
            return
        threading.Thread(target=self.run_full_scan, kwargs={"recent_only": True, "trigger_source": "command"}, daemon=True).start()

    @eventmanager.register(EventType.TransferComplete)
    def handle_transfer(self, event: Event = None):
        if not self._enabled or not self._execute_transfer:
            return
        if not event or not event.event_data:
            return
        transferinfo = event.event_data.get("transferinfo")
        mediainfo = event.event_data.get("mediainfo")
        if not transferinfo or not mediainfo:
            return
        target_path = None
        debounce_key = None
        if getattr(transferinfo, "target_item", None) and getattr(transferinfo.target_item, "path", None):
            target_path = str(transferinfo.target_item.path)
            debounce_key = str(Path(target_path).parent)
        elif getattr(transferinfo, "target_diritem", None) and getattr(transferinfo.target_diritem, "path", None):
            target_path = str(transferinfo.target_diritem.path)
            debounce_key = target_path
        if not target_path:
            return
        debounce_key = debounce_key or self._transfer_debounce_key(target_path)
        now = time.time()
        if now - self._transfer_debounce.get(debounce_key, 0.0) < max(self._delay, 1):
            return
        self._transfer_debounce[debounce_key] = now
        threading.Thread(target=self._process_transfer_path, args=(target_path, mediainfo.title), daemon=True).start()

    @staticmethod
    def _transfer_debounce_key(target_path: str) -> str:
        path = Path(target_path)
        return path.parent.as_posix() if path.parent else path.as_posix()

    def run_full_scan(
        self,
        event: Event = None,
        libraries: Optional[List[str]] = None,
        recent_only: bool = True,
        run_mode: Optional[str] = None,
        collection: Optional[bool] = None,
        ignore_overlay_marker: bool = False,
        trigger_source: str = "manual",
    ):
        if not self._enabled and not libraries and event is None:
            return
        current_run_mode = run_mode or self._run_mode
        current_collection = self._collection if collection is None else collection
        self._recent_skip_reasons = []
        stats = {
            "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "trigger_source": trigger_source,
            "recent_only": recent_only,
            "run_mode": current_run_mode,
            "services": [],
            "processed_titles": [],
            "processed": 0,
            "skipped": 0,
            "errors": 0,
        }
        self._last_run_stats = stats
        self._cleanup_old_backups()
        self._ensure_processed_index_loaded()
        with lock:
            total = 0
            for service in self._service_infos().values():
                plex = self._get_plex(service)
                if not plex:
                    continue
                for section in self._iter_sections(service, plex, libraries):
                    processed, skipped, errors = self._process_section(
                        service=service,
                        section=section,
                        recent_only=recent_only,
                        run_mode=current_run_mode,
                        collection=current_collection,
                        ignore_overlay_marker=ignore_overlay_marker,
                        trigger_source=trigger_source,
                    )
                    total += processed
                    stats["processed"] += processed
                    stats["skipped"] += skipped
                    stats["errors"] += errors
                    stats["services"].append(
                        f"{service.name}/{section.title}: processed={processed}, skipped={skipped}, errors={errors}"
                    )
            self._flush_processed_index()
            stats["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            scope = "最近条目" if recent_only else "全量媒体库"
            stats["summary"] = (
                f"最近一次执行：{scope}，来源={self._trigger_source_label(trigger_source)}，模式={current_run_mode}，"
                f"处理 {stats['processed']}，跳过 {stats['skipped']}，错误 {stats['errors']}。"
            )
            stats["details"] = stats["services"]
            self._last_run_stats = stats
            if self._notify:
                self.post_message(
                    mtype=NotificationType.SiteMessage,
                    title=f"【{self.plugin_name}】",
                    text=(
                        f"{scope}整理完成，处理条目 {total} 个，模式：{current_run_mode}"
                        f"{self._build_message_detail(stats)}"
                    ),
                )

    def _cleanup_old_backups(self):
        if self._backup_retention_days <= 0:
            return
        try:
            base_dir = Path(self.get_data_path()) if hasattr(self, "get_data_path") else Path("/tmp") / "mpplextools"
            backup_dir = base_dir / "poster_backup"
            if not backup_dir.exists():
                return
            expire_before = time.time() - self._backup_retention_days * 86400
            removed = 0
            for file_path in backup_dir.glob("*.jpg"):
                try:
                    if file_path.stat().st_mtime < expire_before:
                        file_path.unlink(missing_ok=True)
                        removed += 1
                except Exception as err:
                    self._verbose(f"清理海报备份失败: {file_path} - {err}")
            if removed:
                logger.info(f"{self.plugin_name} 已清理过期海报备份 {removed} 个，保留天数: {self._backup_retention_days}")
        except Exception as err:
            logger.warning(f"{self.plugin_name} 清理海报备份失败: {err}")

    def _process_transfer_path(self, target_path: str, title: str):
        time.sleep(max(self._delay, 1))
        for service in self._service_infos().values():
            plex = self._get_plex(service)
            if not plex:
                continue
            self._trigger_partial_refresh(service, plex, None, target_path)
            section = self._match_transfer_section(service, plex, target_path)
            item = self._wait_for_transfer_item(service, plex, section, target_path, title)
            if item:
                with lock:
                    self._ensure_processed_index_loaded()
                    self._process_item(item, trigger_source="transfer")
                    self._flush_processed_index()
                return

    def _match_transfer_section(self, service: ServiceInfo, plex, target_path: str) -> Optional[LibrarySection]:
        target = Path(target_path)
        for section in plex.library.sections():
            if section.type == "photo":
                continue
            locations = getattr(section, "locations", []) or []
            for location in locations:
                if self._is_subpath(target, Path(str(location))):
                    return section
        return None

    def _trigger_partial_refresh(
        self,
        service: ServiceInfo,
        plex,
        section: Optional[LibrarySection],
        target_path: str,
    ) -> bool:
        target = Path(target_path)
        instance = getattr(service, "instance", None)
        refresh_item = SimpleNamespace(target_path=target)
        if instance and hasattr(instance, "refresh_library_by_items"):
            try:
                logger.info(f"{self.plugin_name} 触发 Plex 局部刷新: {service.name} - {target}")
                instance.refresh_library_by_items([refresh_item])
                return True
            except Exception as err:
                logger.warning(f"{self.plugin_name} 调用宿主局部刷新失败，改用插件内回退: {service.name} - {err}")

        if not section:
            logger.warning(f"{self.plugin_name} 未能根据入库路径匹配到媒体库，无法使用插件内局部刷新回退: {service.name} - {target_path}")
            return False

        refresh_path = self._refresh_path_from_target(target)
        try:
            logger.info(f"{self.plugin_name} 使用回退方式触发 Plex 局部刷新: {service.name}/{section.title} - {refresh_path}")
            plex.query(f"/library/sections/{section.key}/refresh?path={quote_plus(refresh_path.as_posix())}")
            return True
        except Exception as err:
            logger.warning(f"{self.plugin_name} 触发 Plex 局部刷新失败: {service.name}/{section.title} - {err}")
            return False

    def _wait_for_transfer_item(
        self,
        service: ServiceInfo,
        plex,
        section: Optional[LibrarySection],
        target_path: str,
        title: str,
    ):
        attempts = max(self._transfer_refresh_retries, 1)
        interval = max(self._transfer_refresh_interval, 1)
        for attempt in range(1, attempts + 1):
            item = self._search_item_by_path(plex, target_path, title, section=section)
            if item:
                if attempt > 1:
                    scope = f"{service.name}/{section.title}" if section else service.name
                    logger.info(f"{self.plugin_name} 在第 {attempt} 次重试后命中入库条目: {scope} - {getattr(item, 'title', title) or title}")
                return item
            if attempt < attempts:
                scope = f"{service.name}/{section.title}" if section else service.name
                logger.info(f"{self.plugin_name} 尚未定位到入库条目，等待 Plex 局部刷新完成后重试 ({attempt}/{attempts}): {scope} - {target_path}")
                time.sleep(interval)
        scope = f"{service.name}/{section.title}" if section else service.name
        logger.warning(f"{self.plugin_name} 局部刷新后仍未定位到入库条目，跳过本次入库整理: {scope} - {target_path}")
        return None

    @staticmethod
    def _refresh_path_from_target(target_path: Path) -> Path:
        parent = target_path.parent
        return parent if parent != target_path else target_path

    @staticmethod
    def _is_subpath(path: Path, parent: Path) -> bool:
        try:
            target = path.resolve(strict=False)
            base = parent.resolve(strict=False)
        except Exception:
            target = path
            base = parent
        return target.parts[:len(base.parts)] == base.parts

    def _service_infos(self) -> Dict[str, ServiceInfo]:
        services = self.mediaserver_helper.get_services(name_filters=self._mediaservers, type_filter="plex")
        active = {}
        for name, service in (services or {}).items():
            if service.instance and not service.instance.is_inactive():
                active[name] = service
        return active

    def _get_plex(self, service: ServiceInfo):
        instance = getattr(service, "instance", None)
        if not instance:
            return None
        if hasattr(instance, "get_plex"):
            return instance.get_plex()
        return getattr(instance, "plex", None)

    def _library_options(self) -> List[Dict[str, str]]:
        options = []
        seen = set()
        for service in self._service_infos().values():
            try:
                plex = self._get_plex(service)
                if not plex:
                    continue
                for section in plex.library.sections():
                    key = f"{service.name}:{section.title}"
                    if key in seen or section.type == "photo":
                        continue
                    seen.add(key)
                    options.append({"title": key, "value": key})
            except Exception:
                continue
        return options

    def _iter_sections(self, service: ServiceInfo, plex, libraries: Optional[List[str]] = None):
        selected = libraries or self._libraries or []
        for section in plex.library.sections():
            if section.type == "photo":
                continue
            section_key = f"{service.name}:{section.title}"
            if selected and section.title not in selected and section_key not in selected:
                continue
            yield section

    def _process_section(
        self,
        service: ServiceInfo,
        section: LibrarySection,
        recent_only: bool = True,
        run_mode: str = "run_all",
        collection: bool = False,
        ignore_overlay_marker: bool = False,
        trigger_source: str = "manual",
    ) -> Tuple[int, int, int]:
        count = 0
        skipped = 0
        errors = 0
        if collection:
            processed, skipped_count, error_count = self._process_collections(
                service=service,
                section=section,
                recent_only=recent_only,
                run_mode=run_mode,
                ignore_overlay_marker=ignore_overlay_marker,
                trigger_source=trigger_source,
            )
            count += processed
            skipped += skipped_count
            errors += error_count
        target_items = self._target_section_items(section, recent_only=recent_only)
        for item in target_items:
            if self._event.is_set():
                return count, skipped, errors
            try:
                processed = self._process_item(
                    item,
                    run_mode=run_mode,
                    ignore_overlay_marker=ignore_overlay_marker,
                    trigger_source=trigger_source,
                )
                if processed:
                    count += 1
                else:
                    skipped += 1
                    if not self._has_recent_skip_reason(item):
                        self._record_skip_reason(item, "process", "当前配置或条目条件下未执行任何变更")
            except Exception as err:
                logger.error(f"处理 {section.title}/{getattr(item, 'title', 'unknown')} 失败: {err}")
                errors += 1
                self._record_skip_reason(item, "error", str(err))
        return count, skipped, errors

    def _target_section_items(self, section: LibrarySection, recent_only: bool = True):
        limit = max(self._recent_limit, 1) if recent_only else self._batch_size
        if recent_only:
            items = self._recently_added_items(section, limit)
            if items:
                return items
        items = section.all()
        items.sort(key=lambda item: getattr(item, "addedAt", datetime.min), reverse=True)
        return items[:limit]

    def _recently_added_items(self, section: LibrarySection, limit: int):
        try:
            item_type = getattr(section, "type", "")
            type_id = 1 if item_type == "movie" else 2 if item_type == "show" else None
            server = getattr(section, "_server", None)
            section_id = getattr(section, "key", None)
            if not type_id or not server or not section_id or not hasattr(server, "fetchItems"):
                return []
            return server.fetchItems(
                f"/hubs/home/recentlyAdded?type={type_id}&sectionID={section_id}",
                container_start=0,
                container_size=limit,
                maxresults=limit,
            ) or []
        except Exception as err:
            self._verbose(f"获取 {getattr(section, 'title', 'unknown')} 最近新增条目失败，回退到 section.all(): {err}")
            return []

    def _process_collections(
        self,
        service: ServiceInfo,
        section: LibrarySection,
        recent_only: bool = True,
        run_mode: str = "run_all",
        ignore_overlay_marker: bool = False,
        trigger_source: str = "manual",
    ) -> Tuple[int, int, int]:
        count = 0
        skipped = 0
        errors = 0
        try:
            collections = section.collections() or []
        except Exception:
            return 0, 0, 0
        collections.sort(key=lambda item: getattr(item, "addedAt", datetime.min), reverse=True)
        limit = max(self._recent_limit, 1) if recent_only else self._batch_size
        target_items = collections[:limit]
        for collection in target_items:
            try:
                processed = self._process_item(
                    collection,
                    run_mode=run_mode,
                    ignore_overlay_marker=ignore_overlay_marker,
                    trigger_source=trigger_source,
                )
                if processed:
                    count += 1
                else:
                    skipped += 1
                    if not self._has_recent_skip_reason(collection):
                        self._record_skip_reason(collection, "collection", "当前配置下合集未执行任何变更")
            except Exception as err:
                logger.error(f"处理合集 {section.title}/{getattr(collection, 'title', 'unknown')} 失败: {err}")
                errors += 1
                self._record_skip_reason(collection, "collection-error", str(err))
        return count, skipped, errors

    def _process_item(
        self,
        item,
        run_mode: str = "run_all",
        ignore_overlay_marker: bool = False,
        trigger_source: str = "manual",
    ) -> bool:
        if self._should_skip_processed_item(item, run_mode=run_mode, trigger_source=trigger_source):
            self._record_skip_reason(item, "processed", "该条目已整理过，当前仅“立即运行一次”会强制重复整理")
            return False
        if run_mode == "run_locked":
            self._lock_item_images(item)
            self._process_related_children(item, run_mode=run_mode, trigger_source=trigger_source)
            self._mark_item_processed(item, run_mode=run_mode)
            self._record_processed_title(item)
            return True
        if run_mode == "run_unlocked":
            self._unlock_item_images(item)
            self._process_related_children(item, run_mode=run_mode, trigger_source=trigger_source)
            self._mark_item_processed(item, run_mode=run_mode)
            self._record_processed_title(item)
            return True
        changed = False
        if self._fanart:
            self._apply_fanart(item)
            changed = True
        if self._translate_tags:
            self._translate_item_tags(item)
            changed = True
        if self._sort_title:
            self._update_sort_title(item)
            changed = True
        if self._overlay_poster and getattr(item, "type", "") not in {"show", "season"}:
            self._overlay_item_poster(item, ignore_overlay_marker=ignore_overlay_marker, trigger_source=trigger_source)
            changed = True
        self._process_related_children(
            item,
            run_mode=run_mode,
            ignore_overlay_marker=ignore_overlay_marker,
            trigger_source=trigger_source,
        )
        processed = changed or (self._overlay_poster and getattr(item, "type", "") in {"show", "season"})
        if processed:
            self._mark_item_processed(item, run_mode=run_mode)
            self._record_processed_title(item)
        return processed

    def _record_skip_reason(self, item, stage: str, reason: str):
        self._recent_skip_reasons.append({
            "item_key": self._processed_item_key(item),
            "title": getattr(item, "title", "unknown") or "unknown",
            "stage": stage,
            "reason": reason,
        })
        self._recent_skip_reasons = self._recent_skip_reasons[-50:]

    def _has_recent_skip_reason(self, item) -> bool:
        if not self._recent_skip_reasons:
            return False
        item_key = self._processed_item_key(item)
        if item_key:
            return self._recent_skip_reasons[-1].get("item_key") == item_key
        return self._recent_skip_reasons[-1].get("title") == (getattr(item, "title", "unknown") or "unknown")

    def _record_processed_title(self, item):
        title = getattr(item, "title", "") or ""
        if not title:
            return
        processed_titles = self._last_run_stats.setdefault("processed_titles", [])
        if title not in processed_titles:
            processed_titles.append(title)

    @staticmethod
    def _should_force_reprocess(trigger_source: str) -> bool:
        return trigger_source == "onlyonce"

    def _should_skip_processed_item(self, item, run_mode: str, trigger_source: str) -> bool:
        if self._should_force_reprocess(trigger_source):
            return False
        item_key = self._processed_item_key(item)
        if not item_key:
            return False
        self._ensure_processed_index_loaded()
        profile = self._processing_profile(run_mode)
        profiles = (self._processed_index or {}).get(item_key, [])
        return profile in profiles

    def _mark_item_processed(self, item, run_mode: str):
        item_key = self._processed_item_key(item)
        if not item_key:
            return
        self._ensure_processed_index_loaded()
        profile = self._processing_profile(run_mode)
        profiles = (self._processed_index or {}).setdefault(item_key, [])
        if profile not in profiles:
            profiles.append(profile)
            self._processed_index_dirty = True

    def _processing_profile(self, run_mode: str) -> str:
        if run_mode in {"run_locked", "run_unlocked"}:
            return run_mode
        flags = [
            f"fanart={int(self._fanart)}",
            f"translate_tags={int(self._translate_tags)}",
            f"sort_title={int(self._sort_title)}",
            f"overlay_poster={int(self._overlay_poster)}",
            f"lock_metadata={int(self._lock_metadata)}",
        ]
        return f"{run_mode}|{'|'.join(flags)}"

    def _processed_item_key(self, item) -> str:
        item_type = getattr(item, "type", "unknown") or "unknown"
        raw_key = getattr(item, "ratingKey", None) or getattr(item, "key", None)
        if raw_key:
            return f"{item_type}:{raw_key}"
        title = getattr(item, "title", "") or "unknown"
        return f"{item_type}:title:{title}"

    def _ensure_processed_index_loaded(self):
        if self._processed_index is not None:
            return
        path = self._processed_index_path()
        try:
            if path.exists():
                self._processed_index = json.loads(path.read_text(encoding="utf-8")) or {}
            else:
                self._processed_index = {}
        except Exception as err:
            logger.warning(f"{self.plugin_name} 读取已整理索引失败，改用空索引继续执行: {err}")
            self._processed_index = {}
        self._processed_index_dirty = False

    def _flush_processed_index(self):
        if not self._processed_index_dirty or self._processed_index is None:
            return
        path = self._processed_index_path()
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(self._processed_index, ensure_ascii=False, indent=2), encoding="utf-8")
            self._processed_index_dirty = False
        except Exception as err:
            logger.warning(f"{self.plugin_name} 保存已整理索引失败: {err}")

    def _processed_index_path(self) -> Path:
        return self._data_dir() / "processed_items.json"

    def _data_dir(self) -> Path:
        return Path(self.get_data_path()) if hasattr(self, "get_data_path") else Path("/tmp") / "mpplextools"

    @staticmethod
    def _build_message_detail(stats: Dict[str, Any], limit: int = 10) -> str:
        changed_titles = [title for title in (stats.get("processed_titles") or []) if title]
        if not changed_titles:
            return ""
        unique_titles = list(dict.fromkeys(changed_titles))
        preview = "\n".join(f"- {title}" for title in unique_titles[:limit])
        remaining = len(unique_titles) - limit
        if remaining > 0:
            preview = f"{preview}\n- 其余 {remaining} 项省略"
        return f"\n\n本次处理条目：\n{preview}"

    def _lock_item_images(self, item):
        try:
            if hasattr(item, "lockPoster"):
                item.lockPoster()
            if hasattr(item, "lockArt"):
                item.lockArt()
        except Exception as err:
            logger.debug(f"锁定海报背景失败: {err}")

    def _unlock_item_images(self, item):
        try:
            if hasattr(item, "unlockPoster"):
                item.unlockPoster()
            if hasattr(item, "unlockArt"):
                item.unlockArt()
        except Exception as err:
            logger.debug(f"解锁海报背景失败: {err}")

    def _process_related_children(
        self,
        item,
        run_mode: str = "run_all",
        ignore_overlay_marker: bool = False,
        trigger_source: str = "manual",
    ):
        item_type = getattr(item, "type", "")
        if run_mode in {"run_locked", "run_unlocked"}:
            targets = []
            try:
                if item_type == "show":
                    targets.extend(item.seasons() or [])
                elif item_type == "season":
                    targets.extend(item.episodes() or [])
            except Exception:
                return
            for child in targets:
                if run_mode == "run_locked":
                    self._lock_item_images(child)
                else:
                    self._unlock_item_images(child)
            return

        if not self._overlay_poster:
            return
        try:
            if item_type == "show":
                self._overlay_item_poster(item, ignore_overlay_marker=ignore_overlay_marker, trigger_source=trigger_source)
                for season in item.seasons() or []:
                    self._overlay_item_poster(season, ignore_overlay_marker=ignore_overlay_marker, trigger_source=trigger_source)
                    for episode in season.episodes() or []:
                        self._overlay_item_poster(episode, ignore_overlay_marker=ignore_overlay_marker, trigger_source=trigger_source)
            elif item_type == "season":
                self._overlay_item_poster(item, ignore_overlay_marker=ignore_overlay_marker, trigger_source=trigger_source)
                for episode in item.episodes() or []:
                    self._overlay_item_poster(episode, ignore_overlay_marker=ignore_overlay_marker, trigger_source=trigger_source)
        except Exception as err:
            logger.debug(f"处理关联海报失败: {err}")

    def _apply_fanart(self, item):
        locked = self._locked_fields(item)
        if "thumb" not in locked:
            posters = item.posters() or []
            selected = next((poster for poster in posters if getattr(poster, "provider", "") == "fanarttv"), None)
            if selected:
                item.setPoster(selected)
                if self._lock_metadata:
                    item.lockPoster()
        if "art" not in locked:
            arts = item.arts() or []
            selected = next((art for art in arts if getattr(art, "provider", "") == "fanarttv"), None)
            if selected:
                item.setArt(selected)
                if self._lock_metadata:
                    item.lockArt()

    def _translate_item_tags(self, item):
        tags = self._tags()
        genres = list(getattr(item, "genres", []) or [])
        if not genres:
            return
        english = []
        chinese = []
        existing = {genre.tag for genre in genres if hasattr(genre, "tag")}
        for genre in genres:
            name = genre.tag if hasattr(genre, "tag") else str(genre)
            if name in tags:
                english.append(name)
                if tags[name] not in existing:
                    chinese.append(tags[name])
        if chinese:
            item.addGenre(chinese, locked=self._lock_metadata)
        if english:
            item.removeGenre(english, locked=self._lock_metadata)

    def _update_sort_title(self, item):
        locked = self._locked_fields(item)
        if "titleSort" in locked:
            return
        title = getattr(item, "title", "") or ""
        if not title:
            return
        letters = pypinyin.pinyin(title, style=pypinyin.FIRST_LETTER, heteronym=False)
        sort_title = "".join((entry[0] or "").upper() for entry in letters)
        if sort_title:
            item.editSortTitle(sort_title)


    @staticmethod
    def _trigger_source_label(trigger_source: str) -> str:
        return {
            "onlyonce": "立即运行一次",
            "schedule": "定时任务",
            "api": "API 调用",
            "command": "插件命令",
            "transfer": "入库事件",
            "manual": "手动任务",
        }.get(trigger_source, "当前任务")

    def _overlay_item_poster(self, item, ignore_overlay_marker: bool = False, trigger_source: str = "manual"):
        item_type = getattr(item, "type", "")
        if item_type not in {"movie", "show", "season", "episode"}:
            self._record_skip_reason(item, "overlay", f"不支持的条目类型: {item_type}")
            return
        poster_path = self._source_poster_path(item, ignore_overlay_marker=ignore_overlay_marker, trigger_source=trigger_source)
        if not poster_path:
            self._record_skip_reason(item, "overlay", "未找到可用于叠加的原始海报")
            return
        resolution, dynamic_range, duration_text, rating_text = self._media_overlay_info(item)
        self._verbose(
            f"{getattr(item, 'title', 'unknown')} 海报叠加参数: 分辨率={resolution or '-'} 动态范围={dynamic_range or '-'} 时长={duration_text or '-'} 评分={rating_text or '-'}"
        )
        overlay_path = build_overlay_poster(
            poster_path=poster_path,
            asset_root=Path(__file__).resolve().parent,
            title=getattr(item, "title", ""),
            resolution=resolution,
            dynamic_range=dynamic_range,
            duration_text=duration_text,
            rating_text=rating_text,
            debug_log=lambda message: self._verbose(f"{getattr(item, 'title', 'unknown')} {message}"),
        )
        item.uploadPoster(filepath=str(overlay_path))
        self._verbose(f"{getattr(item, 'title', 'unknown')} 海报叠加完成并已上传: {overlay_path}")
        self._last_run_stats.setdefault("poster_preview", {
            "message": "",
            "image_url": "",
        })
        self._last_run_stats["poster_preview"] = {
            "message": (
                f"标题: {getattr(item, 'title', 'unknown')}\n"
                f"类型: {item_type}\n"
                f"分辨率: {resolution or '-'}\n"
                f"动态范围: {dynamic_range or '-'}\n"
                f"时长: {duration_text or '-'}\n"
                f"评分: {rating_text or '-'}\n"
                f"原始海报: {poster_path}\n"
                f"叠加输出: {overlay_path}"
            ),
            "image_url": self._local_preview_image_url(overlay_path),
        }
        if self._lock_metadata:
            item.lockPoster()

    @staticmethod
    def _local_preview_image_url(image_path: Path) -> str:
        return image_path.as_posix() if image_path else ""

    def _verbose(self, message: str):
        if self._verbose_logging:
            logger.info(f"[{self.plugin_name}][调试] {message}")

    def _poster_backup_path(self, item) -> Optional[Path]:
        try:
            base_dir = Path(self.get_data_path()) if hasattr(self, "get_data_path") else Path("/tmp") / "mpplextools"
            backup_dir = base_dir / "poster_backup"
            backup_dir.mkdir(parents=True, exist_ok=True)
            rating_key = getattr(item, "ratingKey", None) or getattr(item, "key", None) or getattr(item, "title", "unknown")
            safe_key = str(rating_key).strip("/").replace("/", "_").replace(":", "_")
            return backup_dir / f"{safe_key}.jpg"
        except Exception as err:
            self._verbose(f"计算海报备份路径失败: {err}")
            return None

    def _save_poster_backup(self, source_path: Path, backup_path: Path):
        backup_path.write_bytes(source_path.read_bytes())

    def _poster_variant_urls(self, item) -> List[str]:
        urls = []
        seen = set()
        server = getattr(item, "_server", None)
        try:
            posters = item.posters() or []
        except Exception as err:
            self._verbose(f"{getattr(item, 'title', 'unknown')} 获取海报候选列表失败: {err}")
            return urls

        for poster in posters:
            if getattr(poster, "selected", False):
                continue
            key = getattr(poster, "key", "") or ""
            if key.startswith(("http://", "https://")):
                url = key
                if url not in seen:
                    seen.add(url)
                    urls.append(url)
                continue
            poster_server = getattr(poster, "_server", None) or server
            if not key or not poster_server or not hasattr(poster_server, "url"):
                continue
            try:
                try:
                    url = poster_server.url(key, includeToken=True)
                except TypeError:
                    url = poster_server.url(key)
            except Exception as err:
                self._verbose(f"{getattr(item, 'title', 'unknown')} 生成候选海报 URL 失败: {err}")
                continue
            if url and url not in seen:
                seen.add(url)
                urls.append(url)
        return urls

    def _download_non_overlay_poster(self, url: str, title: str, source_name: str) -> Tuple[Optional[Path], bool]:
        try:
            poster_path = download_poster(url)
        except Exception as err:
            self._verbose(f"{title} 下载{source_name}海报失败: {err}")
            return None, False
        if not poster_path:
            return None, False
        if is_overlay_poster(poster_path):
            self._verbose(f"{title} 的{source_name}海报已带 overlay 标记，跳过")
            return None, True
        return poster_path, False

    def _source_poster_path(self, item, ignore_overlay_marker: bool = False, trigger_source: str = "manual") -> Optional[Path]:
        title = getattr(item, "title", "unknown")
        backup_path = self._poster_backup_path(item)
        current_url = getattr(item, "posterUrl", "") or ""

        if current_url:
            current_poster, current_has_overlay = self._download_non_overlay_poster(current_url, title, "当前已选")
            if current_poster:
                if backup_path:
                    try:
                        self._save_poster_backup(current_poster, backup_path)
                        self._verbose(f"{title} 使用当前已选未处理海报，并刷新备份: {backup_path}")
                        return backup_path
                    except Exception as err:
                        self._verbose(f"{title} 保存原始海报备份失败，改用临时文件: {err}")
                self._verbose(f"{title} 使用当前已选未处理海报进行叠加")
                return current_poster
            if current_has_overlay:
                source_label = self._trigger_source_label(trigger_source)
                if ignore_overlay_marker:
                    self._verbose(f"{title} 当前已选海报已带 overlay 标记，但本次执行来源为{source_label}，允许忽略并继续尝试历史备份或其他候选海报")
                else:
                    self._verbose(f"{title} 当前已选海报已带 overlay 标记，本次执行来源为{source_label}，跳过本次海报叠加")
                    return None
            self._verbose(f"{title} 当前已选海报不可直接用于叠加，尝试历史备份或其他候选海报")

        if backup_path and backup_path.exists():
            if not is_overlay_poster(backup_path):
                self._verbose(f"{title} 使用历史备份原始海报: {backup_path}")
                return backup_path
            self._verbose(f"{title} 的历史备份海报异常带 overlay 标记，忽略该备份")

        for index, poster_url in enumerate(self._poster_variant_urls(item), start=1):
            candidate, _ = self._download_non_overlay_poster(poster_url, title, f"候选#{index}")
            if not candidate:
                continue
            if backup_path:
                try:
                    self._save_poster_backup(candidate, backup_path)
                    self._verbose(f"{title} 从候选海报恢复原始海报，并保存备份: {backup_path}")
                    return backup_path
                except Exception as err:
                    self._verbose(f"{title} 保存候选海报备份失败，改用临时文件: {err}")
            self._verbose(f"{title} 使用候选海报作为原始海报进行叠加")
            return candidate

        logger.warning(f"{title} 未找到未叠加的原始海报，跳过海报叠加")
        return None

    def _safe_media_list(self, item):
        medias = getattr(item, "media", None) or []
        return medias if isinstance(medias, list) else list(medias)

    def _safe_parts(self, media):
        parts = getattr(media, "parts", None) or []
        return parts if isinstance(parts, list) else list(parts)

    def _preferred_media(self, item):
        media_list = self._safe_media_list(item)
        if media_list:
            return media_list[0]

        item_type = getattr(item, "type", "")
        children = []
        try:
            if item_type == "show":
                children = getattr(item, "episodes", lambda: [])() or []
            elif item_type == "season":
                children = getattr(item, "episodes", lambda: [])() or []
            elif item_type == "movie":
                children = getattr(item, "versions", lambda: [])() or []
        except Exception as err:
            self._verbose(f"{getattr(item, 'title', 'unknown')} 获取子媒体失败: {err}")
            children = []

        for child in children:
            child_media = self._safe_media_list(child)
            if child_media:
                return child_media[0]
        return None

    @staticmethod
    def _stream_display_title(media) -> str:
        parts = getattr(media, "parts", None) or []
        if not parts:
            return ""
        streams = getattr(parts[0], "streams", None) or []
        if not streams:
            return ""
        for stream in streams:
            stream_type = getattr(stream, "streamType", None)
            if stream_type in (1, "1"):
                return str(getattr(stream, "displayTitle", "") or "").lower()
        return str(getattr(streams[0], "displayTitle", "") or "").lower()

    def _item_rating_text(self, item) -> str:
        candidates = [item]
        try:
            item_type = getattr(item, "type", "")
            if item_type in {"show", "season"}:
                episodes = getattr(item, "episodes", lambda: [])() or []
                candidates.extend(episodes[:5])
        except Exception as err:
            self._verbose(f"{getattr(item, 'title', 'unknown')} 获取评分候选失败: {err}")

        for candidate in candidates:
            rating = getattr(candidate, "audienceRating", None) or getattr(candidate, "rating", None)
            if rating in [None, ""]:
                continue
            try:
                return f"{float(rating):.1f}"
            except Exception:
                continue
        return ""

    def _media_overlay_info(self, item) -> Tuple[str, str, str, str]:
        resolution = ""
        dynamic_range = "SDR"
        duration_text = ""
        rating_text = ""
        try:
            media = self._preferred_media(item)
            if not media:
                return resolution, dynamic_range, duration_text, rating_text
            resolution_raw = str(getattr(media, "videoResolution", "") or "").lower()
            resolution = resolution_raw.upper() if resolution_raw == "4k" else f"{resolution_raw.upper()}P" if resolution_raw else ""
            display_title = self._stream_display_title(media)
            if "dovi" in display_title or " dv" in display_title:
                dynamic_range = "DV"
            elif "hdr" in display_title:
                dynamic_range = "HDR"
            duration_ms = int(getattr(item, "duration", 0) or 0)
            if not duration_ms:
                duration_ms = int(getattr(media, "duration", 0) or 0)
            duration_text = self._format_duration(duration_ms)
            rating_text = self._item_rating_text(item)
        except Exception as err:
            self._verbose(f"{getattr(item, 'title', 'unknown')} 提取海报叠加信息失败: {err}")
        return resolution, dynamic_range, duration_text, rating_text

    @staticmethod
    def _format_duration(duration_ms: int) -> str:
        if not duration_ms:
            return ""
        minutes = duration_ms // 60000
        if minutes >= 60:
            hours = minutes // 60
            minutes = minutes % 60
            return f"{hours}时{minutes}分" if minutes else f"{hours}时"
        return f"{minutes}分"

    def _search_item_by_path(
        self,
        plex,
        target_path: str,
        fallback_title: str = "",
        section: Optional[LibrarySection] = None,
    ):
        target = Path(target_path)
        target_norm = target_path.replace("\\", "/")
        search_terms: List[str] = []
        for raw in [fallback_title, target.stem, target.name, target.parent.name]:
            value = str(raw or "").strip()
            if not value:
                continue
            for candidate in [value, value.replace(".", " ")]:
                candidate = candidate.strip()
                if candidate and candidate not in search_terms:
                    search_terms.append(candidate)

        fallback_item = None
        section_key = str(getattr(section, "key", "")) if section else ""
        for search_term in search_terms:
            try:
                results = plex.library.search(search_term)
            except Exception as err:
                self._verbose(f"搜索入库条目失败: 关键词={search_term} 错误={err}")
                continue
            filtered = []
            for item in results:
                if section_key and str(getattr(item, "librarySectionID", "")) != section_key:
                    continue
                filtered.append(item)
                locations = [str(path).replace("\\", "/") for path in getattr(item, "locations", []) or []]
                media_parts = []
                for media in getattr(item, "media", []) or []:
                    for part in getattr(media, "parts", []) or []:
                        media_parts.append(str(getattr(part, "file", "")).replace("\\", "/"))
                all_paths = locations + media_parts
                if any(target_norm in path or path in target_norm for path in all_paths):
                    return item
            if not fallback_item and len(filtered) == 1:
                fallback_item = filtered[0]
        return fallback_item

    def _locked_fields(self, item) -> List[str]:
        fields = []
        for field in getattr(item, "fields", []) or []:
            name = getattr(field, "name", None)
            if name:
                fields.append(name)
        return fields

    def _guid_value(self, item, prefix: str) -> Optional[str]:
        for guid in getattr(item, "guids", []) or []:
            value = guid.id if hasattr(guid, "id") else str(guid)
            if value.startswith(f"{prefix}://"):
                return value.split("://", 1)[1]
        return None

    def _tags(self) -> Dict[str, str]:
        try:
            raw = self._custom_tags_json or self._preset_tags_json()
            return json.loads(raw)
        except Exception:
            return dict(self._default_tags)

    def _preset_tags_json(self) -> str:
        import json
        return json.dumps(self._default_tags, ensure_ascii=False, indent=2)
