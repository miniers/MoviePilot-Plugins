import json
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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
    plugin_version = "0.1.7"
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
    _last_transfer_at = 0.0

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

        self.stop_service()

        self._scheduler = BackgroundScheduler(timezone=settings.TZ)
        if self._onlyonce:
            self._scheduler.add_job(
                func=self.run_full_scan,
                trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
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
                "kwargs": {},
            }]
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{"component": "VSwitch", "props": {"model": "onlyonce", "label": "立即运行一次"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{"component": "VSwitch", "props": {"model": "notify", "label": "发送通知"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{"component": "VSwitch", "props": {"model": "execute_transfer", "label": "入库后执行"}}],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{"component": "VSwitch", "props": {"model": "translate_tags", "label": "标签中文化"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{"component": "VSwitch", "props": {"model": "sort_title", "label": "拼音排序"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{"component": "VSwitch", "props": {"model": "fanart", "label": "优选 Fanart"}}],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{"component": "VSwitch", "props": {"model": "collection", "label": "处理合集"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
                                "content": [{
                                    "component": "VSelect",
                                    "props": {
                                        "model": "run_mode",
                                        "label": "运行模式",
                                        "items": [
                                            {"title": "全部整理", "value": "run_all"},
                                            {"title": "仅锁定海报背景", "value": "run_locked"},
                                            {"title": "仅解锁海报背景", "value": "run_unlocked"}
                                        ]
                                    }
                                }],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{"component": "VSwitch", "props": {"model": "overlay_poster", "label": "海报叠加媒体信息"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{"component": "VSwitch", "props": {"model": "lock_metadata", "label": "整理后锁定相关元数据"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{"component": "VSwitch", "props": {"model": "verbose_logging", "label": "输出详细日志"}}],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{"component": "VCronField", "props": {"model": "cron", "label": "执行周期"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{"component": "VTextField", "props": {"model": "delay", "label": "入库延迟秒数"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{"component": "VTextField", "props": {"model": "recent_limit", "label": "最近条目数"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{"component": "VTextField", "props": {"model": "batch_size", "label": "全量模式处理数"}}],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [{
                                    "component": "VSelect",
                                    "props": {
                                        "multiple": True,
                                        "chips": True,
                                        "clearable": True,
                                        "model": "mediaservers",
                                        "label": "Plex 媒体服务器",
                                        "items": [{"title": config.name, "value": config.name}
                                                  for config in self.mediaserver_helper.get_configs().values()
                                                  if config.type == "plex"],
                                    },
                                }],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [{
                                    "component": "VSelect",
                                    "props": {
                                        "multiple": True,
                                        "chips": True,
                                        "clearable": True,
                                        "model": "libraries",
                                        "label": "媒体库（留空=全部）",
                                        "items": self._library_options(),
                                    },
                                }],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [{"component": "VAceEditor", "props": {"modelvalue": "custom_tags_json", "lang": "json", "theme": "monokai", "style": "height: 20rem"}}],
                            }
                        ],
                    },
                ],
            }
        ], self._form_defaults()

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
            "mediaservers": self._mediaservers,
            "libraries": self._libraries,
            "custom_tags_json": self._custom_tags_json or self._preset_tags_json(),
        }

    def get_page(self) -> List[dict]:
        pass


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
        kwargs = {"libraries": libraries, "recent_only": recent_only}
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
        threading.Thread(target=self.run_full_scan, kwargs={"recent_only": True}, daemon=True).start()

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
        if getattr(transferinfo, "target_item", None) and getattr(transferinfo.target_item, "path", None):
            target_path = str(transferinfo.target_item.path)
        elif getattr(transferinfo, "target_diritem", None) and getattr(transferinfo.target_diritem, "path", None):
            target_path = str(transferinfo.target_diritem.path)
        if not target_path:
            return
        if time.time() - self._last_transfer_at < max(self._delay, 1):
            return
        self._last_transfer_at = time.time()
        threading.Thread(target=self._process_transfer_path, args=(target_path, mediainfo.title), daemon=True).start()

    def run_full_scan(
        self,
        event: Event = None,
        libraries: Optional[List[str]] = None,
        recent_only: bool = True,
        run_mode: Optional[str] = None,
        collection: Optional[bool] = None,
    ):
        if not self._enabled and not libraries and event is None:
            return
        current_run_mode = run_mode or self._run_mode
        current_collection = self._collection if collection is None else collection
        with lock:
            total = 0
            for service in self._service_infos().values():
                plex = self._get_plex(service)
                if not plex:
                    continue
                for section in self._iter_sections(service, plex, libraries):
                    total += self._process_section(section, recent_only=recent_only, run_mode=current_run_mode, collection=current_collection)
            if self._notify:
                scope = "最近条目" if recent_only else "全量媒体库"
                self.post_message(
                    mtype=NotificationType.SiteMessage,
                    title=f"【{self.plugin_name}】",
                    text=f"{scope}整理完成，处理条目 {total} 个，模式：{current_run_mode}",
                )

    def _process_transfer_path(self, target_path: str, title: str):
        time.sleep(max(self._delay, 1))
        with lock:
            for service in self._service_infos().values():
                plex = self._get_plex(service)
                if not plex:
                    continue
                item = self._search_item_by_path(plex, target_path, title)
                if item:
                    self._process_item(item)

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

    def _process_section(self, section: LibrarySection, recent_only: bool = True, run_mode: str = "run_all", collection: bool = False) -> int:
        count = 0
        if collection:
            count += self._process_collections(section, recent_only=recent_only, run_mode=run_mode)
        items = section.all()
        items.sort(key=lambda item: getattr(item, "addedAt", datetime.min), reverse=True)
        limit = max(self._recent_limit, 1) if recent_only else self._batch_size
        target_items = items[:limit]
        for item in target_items:
            if self._event.is_set():
                return count
            try:
                self._process_item(item, run_mode=run_mode)
                count += 1
            except Exception as err:
                logger.error(f"处理 {section.title}/{getattr(item, 'title', 'unknown')} 失败: {err}")
        return count

    def _process_collections(self, section: LibrarySection, recent_only: bool = True, run_mode: str = "run_all") -> int:
        count = 0
        try:
            collections = section.collections() or []
        except Exception:
            return 0
        collections.sort(key=lambda item: getattr(item, "addedAt", datetime.min), reverse=True)
        limit = max(self._recent_limit, 1) if recent_only else self._batch_size
        target_items = collections[:limit]
        for collection in target_items:
            try:
                self._process_item(collection, run_mode=run_mode)
                count += 1
            except Exception as err:
                logger.error(f"处理合集 {section.title}/{getattr(collection, 'title', 'unknown')} 失败: {err}")
        return count

    def _process_item(self, item, run_mode: str = "run_all"):
        if run_mode == "run_locked":
            self._lock_item_images(item)
            self._process_related_children(item, run_mode=run_mode)
            return
        if run_mode == "run_unlocked":
            self._unlock_item_images(item)
            self._process_related_children(item, run_mode=run_mode)
            return
        if self._fanart:
            self._apply_fanart(item)
        if self._translate_tags:
            self._translate_item_tags(item)
        if self._sort_title:
            self._update_sort_title(item)
        if self._overlay_poster:
            self._overlay_item_poster(item)
        self._process_related_children(item, run_mode=run_mode)

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

    def _process_related_children(self, item, run_mode: str = "run_all"):
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
                self._overlay_item_poster(item)
                for season in item.seasons() or []:
                    self._overlay_item_poster(season)
                    for episode in season.episodes() or []:
                        self._overlay_item_poster(episode)
            elif item_type == "season":
                self._overlay_item_poster(item)
                for episode in item.episodes() or []:
                    self._overlay_item_poster(episode)
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


    def _overlay_item_poster(self, item):
        item_type = getattr(item, "type", "")
        if item_type not in {"movie", "show", "season", "episode"}:
            return
        poster_path = self._source_poster_path(item)
        if not poster_path:
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
        if self._lock_metadata:
            item.lockPoster()

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

    def _download_non_overlay_poster(self, url: str, title: str, source_name: str) -> Optional[Path]:
        try:
            poster_path = download_poster(url)
        except Exception as err:
            self._verbose(f"{title} 下载{source_name}海报失败: {err}")
            return None
        if not poster_path:
            return None
        if is_overlay_poster(poster_path):
            self._verbose(f"{title} 的{source_name}海报已带 overlay 标记，跳过")
            return None
        return poster_path

    def _source_poster_path(self, item) -> Optional[Path]:
        title = getattr(item, "title", "unknown")
        backup_path = self._poster_backup_path(item)
        current_url = getattr(item, "posterUrl", "") or ""

        if current_url:
            current_poster = self._download_non_overlay_poster(current_url, title, "当前已选")
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
            self._verbose(f"{title} 当前已选海报不可直接用于叠加，尝试历史备份或其他候选海报")

        if backup_path and backup_path.exists():
            if not is_overlay_poster(backup_path):
                self._verbose(f"{title} 使用历史备份原始海报: {backup_path}")
                return backup_path
            self._verbose(f"{title} 的历史备份海报异常带 overlay 标记，忽略该备份")

        for index, poster_url in enumerate(self._poster_variant_urls(item), start=1):
            candidate = self._download_non_overlay_poster(poster_url, title, f"候选#{index}")
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

    def _search_item_by_path(self, plex, target_path: str, fallback_title: str = ""):
        search_title = Path(target_path).stem or fallback_title
        results = plex.library.search(search_title) if search_title else []
        target_norm = target_path.replace("\\", "/")
        for item in results:
            locations = [str(path).replace("\\", "/") for path in getattr(item, "locations", []) or []]
            media_parts = []
            for media in getattr(item, "media", []) or []:
                for part in getattr(media, "parts", []) or []:
                    media_parts.append(str(getattr(part, "file", "")).replace("\\", "/"))
            all_paths = locations + media_parts
            if any(target_norm in path or path in target_norm for path in all_paths):
                return item
        return results[0] if results else None

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
