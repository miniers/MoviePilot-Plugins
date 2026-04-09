import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.core.context import MediaInfo
from app.core.event import Event, eventmanager
from app.core.meta import MetaBase
from app.helper.mediaserver import MediaServerHelper
from app.log import logger
from app.plugins import _PluginBase
from app.plugins.plexpersonmeta.helper import read_json_file
from app.plugins.plexpersonmeta.scrape import ScrapeHelper
from app.schemas import ServiceInfo
from app.schemas.types import EventType, NotificationType

lock = threading.Lock()


class PlexPersonMeta(_PluginBase):
    # 插件名称
    plugin_name = "Plex演职人员刮削"
    # 插件描述
    plugin_desc = "实现刮削演职人员中文名称及角色。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/InfinityPacer/MoviePilot-Plugins/main/icons/plexpersonmeta.png"
    # 插件版本
    plugin_version = "2.4.1"
    # 插件作者
    plugin_author = "InfinityPacer"
    # 作者主页
    author_url = "https://github.com/InfinityPacer"
    # 插件配置项ID前缀
    plugin_config_prefix = "plexpersonmeta_"
    # 加载顺序
    plugin_order = 91
    # 可使用的用户级别
    auth_level = 1

    # region 私有属性
    mediaserver_helper = None
    # 是否开启
    _enabled = False
    # 立即运行一次
    _onlyonce = False
    # 任务执行间隔
    _cron = None
    # 最近一次定时任务运行的入库时间范围，单位分钟
    _cron_added_time = None
    # 发送通知
    _notify = False
    _lock = False
    # 需要处理的媒体库
    _libraries = None
    # 入库后运行一次
    _execute_transfer = None
    # 入库后延迟执行时间
    _delay = None
    # 最近一次入库时间
    _transfer_time = None
    _scrape_type = "all"
    _remove_no_zh = False
    _douban_scrape = True
    _dry_run = True
    _backup_enabled = True
    _restore_backup = False
    _last_run_stats: Dict[str, Any] = {}
    # 清理缓存
    _clear_cache = None
    _transfer_job_id = "plexpersonmeta_transfer_once"
    # 定时器
    _scheduler = None
    # 退出事件
    _event = threading.Event()

    # endregion

    def init_plugin(self, config: dict = None):
        self.mediaserver_helper = MediaServerHelper()
        if not config:
            return
        self._enabled = config.get("enabled")
        self._onlyonce = config.get("onlyonce")
        self._cron = config.get("cron")
        self._notify = config.get("notify")
        self._lock = bool(config.get("lock"))
        self._libraries = config.get("libraries", [])
        self._clear_cache = config.get("clear_cache")
        self._execute_transfer = config.get("execute_transfer")
        self._scrape_type = config.get("scrape_type", "all") or "all"
        self._remove_no_zh = bool(config.get("remove_no_zh", False))
        self._douban_scrape = bool(config.get("douban_scrape", True))
        self._dry_run = bool(config.get("dry_run", True))
        self._backup_enabled = bool(config.get("backup_enabled", True))
        self._restore_backup = bool(config.get("restore_backup", False))
        try:
            self._delay = int(config.get("delay", 200))
        except ValueError:
            self._delay = 200

        # 如果开启了入库后运行一次，延迟时间又不填，默认为200s
        if self._execute_transfer and not self._delay:
            self._delay = 200

        # 最近一次定时任务运行的入库时间范围，单位分钟
        try:
            self._cron_added_time = int(config.get("cron_added_time", 0))
        except ValueError:
            self._cron_added_time = 0

        # 停止现有任务
        self.stop_service()

        # 启动服务
        self._scheduler = BackgroundScheduler(timezone=settings.TZ)
        if self._clear_cache:
            logger.info(f"{self.plugin_name} 清理缓存一次")
            self._scheduler.add_job(
                func=ScrapeHelper.clear_cache,
                trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                name=f"{self.plugin_name}",
            )
            # 关闭清理缓存
            self._clear_cache = False
            config["clear_cache"] = False
            self.update_config(config=config)

        if self._onlyonce:
            logger.info(f"{self.plugin_name}服务，立即运行一次")
            self._scheduler.add_job(
                func=self.scrape_library,
                trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=6),
                kwargs={"trigger_source": "onlyonce"},
                name=f"{self.plugin_name}",
            )
            # 关闭一次性开关
            self._onlyonce = False
            config["onlyonce"] = False
            self.update_config(config=config)

        if self._restore_backup:
            logger.info(f"{self.plugin_name} 恢复最近一次演员备份")
            self._scheduler.add_job(
                func=self.restore_last_backup,
                trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                name=f"{self.plugin_name}-restore",
            )
            self._restore_backup = False
            config["restore_backup"] = False
            self.update_config(config=config)

        # 启动服务
        if self._scheduler.get_jobs():
            self._scheduler.print_jobs()
            self._scheduler.start()

    def service_infos(self, name_filters: Optional[List[str]] = None) -> Optional[Dict[str, ServiceInfo]]:
        """
        服务信息
        """
        services = self.mediaserver_helper.get_services(name_filters=name_filters, type_filter="plex")
        if not services:
            logger.warning("获取媒体服务器实例失败，请检查配置")
            return None

        active_services = {}
        for service_name, service_info in services.items():
            if service_info.instance.is_inactive():
                logger.warning(f"媒体服务器 {service_name} 未连接，请检查配置")
            else:
                active_services[service_name] = service_info

        if not active_services:
            logger.warning("没有已连接的媒体服务器，请检查配置")
            return None

        return active_services

    def service_info(self, name: str) -> Optional[ServiceInfo]:
        """
        服务信息
        """
        service = self.mediaserver_helper.get_service(name=name, type_filter="plex")
        if not service:
            logger.warning("获取媒体服务器实例失败，请检查配置")
            return None

        if service.instance.is_inactive():
            logger.warning(f"媒体服务器 {name} 未连接，请检查配置")
            return None

        return service

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [{
            "cmd": "/plex_person_meta",
            "event": EventType.PluginAction,
            "desc": "运行 Plex 演职人员刮削",
            "category": "Plex",
            "data": {"action": "plex_person_meta_run"},
        }]

    def get_api(self) -> List[Dict[str, Any]]:
        return [{
            "path": "/run",
            "endpoint": self.api_run,
            "methods": ["POST"],
            "summary": "运行 Plex 演职人员刮削",
            "description": "支持手动触发全量/最近入库、dry-run 预演，以及恢复最近一次备份",
        }]

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务
        [{
            "id": "服务ID",
            "name": "服务名称",
            "trigger": "触发器：cron/interval/date/CronTrigger.from_crontab()",
            "func": self.xxx,
            "kwargs": {} # 定时器参数
        }]
        """
        if self._enabled and self._cron:
            logger.info(f"{self.plugin_name}定时服务启动，时间间隔 {self._cron} ")
            return [{
                "id": "PlexPersonMeta",
                "name": f"{self.plugin_name}服务",
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.scrape_library,
                "kwargs": {"trigger_source": "schedule"}
            }]

    def stop_service(self):
        """
        退出插件
        """
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._event.set()
                    self._scheduler.shutdown()
                    self._event.clear()
                self._scheduler = None
        except Exception as e:
            logger.info(str(e))

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面，需要返回两块数据：1、页面配置；2、数据结构
        """
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                            'hint': '开启后插件将处于激活状态',
                                            'persistent-hint': True
                                        },
                                    }
                                ],
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'notify',
                                            'label': '发送通知',
                                            'hint': '是否在特定事件发生时发送通知',
                                            'persistent-hint': True
                                        },
                                    }
                                ],
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'clear_cache',
                                            'label': '清理缓存',
                                            'hint': '清理元数据识别缓存',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'onlyonce',
                                            'label': '立即运行一次',
                                            'hint': '插件将立即运行一次',
                                            'persistent-hint': True
                                        },
                                    }
                                ],
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'restore_backup',
                                            'label': '恢复最近一次备份',
                                            'hint': '保存后恢复最近一次写回前生成的演员备份，仅执行一次。',
                                            'persistent-hint': True
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'lock',
                                            'label': '锁定元数据',
                                            'hint': '开启后元数据将锁定，须手工解锁后才允许修改',
                                            'persistent-hint': True
                                        },
                                    }
                                ],
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'execute_transfer',
                                            'label': '入库后运行一次',
                                            'hint': '在媒体入库后运行一次操作',
                                            'persistent-hint': True
                                        },
                                    }
                                ],
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'douban_scrape',
                                            'label': '豆瓣辅助识别',
                                            'hint': '提高识别率的同时将会降低性能',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'dry_run',
                                            'label': '仅预演不写回',
                                            'hint': '开启后只分析将要修改的人物信息，不向 Plex 写入。',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'backup_enabled',
                                            'label': '写回前自动备份',
                                            'hint': '关闭 dry-run 后，写回前先备份原始演员信息，便于回滚。',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            }
                        ],
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            # {
                            #     'component': 'VCol',
                            #     'props': {
                            #         'cols': 12,
                            #         'md': 4
                            #     },
                            #     'content': [
                            #         {
                            #             'component': 'VSwitch',
                            #             'props': {
                            #                 'model': 'remove_no_zh',
                            #                 'label': '删除非中文演员',
                            #                 'hint': '开启后将删除所有非中文演员',
                            #                 'persistent-hint': True
                            #             }
                            #         }
                            #     ]
                            # },
                            # {
                            #     'component': 'VCol',
                            #     'props': {
                            #         'cols': 12,
                            #         'md': 4
                            #     },
                            #     'content': [
                            #         {
                            #             'component': 'VSwitch',
                            #             'props': {
                            #                 'model': 'reserve_tag_key',
                            #                 'label': '保留在线元数据（实验性功能）',
                            #                 'hint': '尝试保留在线元数据，需结合脚本使用',
                            #                 'persistent-hint': True
                            #             }
                            #         }
                            #     ]
                            # }
                        ],
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VCronField',
                                        'props': {
                                            'model': 'cron',
                                            'label': '执行周期',
                                            'placeholder': '5位cron表达式',
                                            'hint': '使用cron表达式指定执行周期，如 0 8 * * *',
                                            'persistent-hint': True
                                        },
                                    }
                                ],
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'cron_added_time',
                                            'label': '刮削时间范围',
                                            'placeholder': '指定定时刮削的入库时间范围（分钟）',
                                            'hint': '定时刮削时，指定入库时间范围（分钟），如 60，表示只刮削最近60分钟入库的媒体，留空或0表示刮削所有媒体',
                                            'persistent-hint': True
                                        },
                                    }
                                ],
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'delay',
                                            'label': '延迟时间（秒）',
                                            'placeholder': '入库后延迟运行时间',
                                            'hint': '入库后延迟运行的时间（秒）',
                                            'persistent-hint': True
                                        },
                                    }
                                ],
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'scrape_type',
                                            'label': '刮削条件',
                                            'items': [
                                                {'title': '全部', 'value': 'all'},
                                                {'title': '演员非中文', 'value': 'name'},
                                                {'title': '角色非中文', 'value': 'role'},
                                            ],
                                            'hint': '选择刮削条件',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            }
                        ],
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'multiple': True,
                                            'chips': True,
                                            'clearable': True,
                                            'model': 'libraries',
                                            'label': '媒体库',
                                            'items': self.__get_service_library_options(),
                                            'hint': '选择要处理的媒体库',
                                            'persistent-hint': True
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VRow',
                                'content': [
                                    {
                                        'component': 'VCol',
                                        'props': {
                                            'cols': 12,
                                        },
                                        'content': [
                                            {
                                                'component': 'VAlert',
                                                'props': {
                                                    'type': 'info',
                                                    'variant': 'tonal'
                                                },
                                                'content': [
                                                    {
                                                        'component': 'span',
                                                        'text': '基于 '
                                                    },
                                                    {
                                                        'component': 'a',
                                                        'props': {
                                                            'href': 'https://github.com/jxxghp/MoviePilot-Plugins',
                                                            'target': '_blank',
                                                            'style': 'text-decoration: underline;'
                                                        },
                                                        'content': [
                                                            {
                                                                'component': 'u',
                                                                'text': '官方插件'
                                                            }
                                                        ]
                                                    },
                                                    {
                                                        'component': 'span',
                                                        'text': ' 编写，并参考了 '
                                                    },
                                                    {
                                                        'component': 'a',
                                                        'props': {
                                                            'href': 'https://github.com/Bespertrijun/PrettyServer',
                                                            'target': '_blank',
                                                            'style': 'text-decoration: underline;'
                                                        },
                                                        'content': [
                                                            {
                                                                'component': 'u',
                                                                'text': 'PrettyServer'
                                                            }
                                                        ]
                                                    },
                                                    {
                                                        'component': 'span',
                                                        'text': ' 项目，特此感谢 '
                                                    },
                                                    {
                                                        'component': 'a',
                                                        'props': {
                                                            'href': 'https://github.com/jxxghp',
                                                            'target': '_blank',
                                                            'style': 'text-decoration: underline;'
                                                        },
                                                        'content': [
                                                            {
                                                                'component': 'u',
                                                                'text': 'jxxghp'
                                                            }
                                                        ]
                                                    },
                                                    {
                                                        'component': 'span',
                                                        'text': '、'
                                                    },
                                                    {
                                                        'component': 'a',
                                                        'props': {
                                                            'href': 'https://github.com/Bespertrijun',
                                                            'target': '_blank',
                                                            'style': 'text-decoration: underline;'
                                                        },
                                                        'content': [
                                                            {
                                                                'component': 'u',
                                                                'text': 'Bespertrijun'
                                                            }
                                                        ]
                                                    }
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': 'Plex 的 API 实现较为复杂，我在尝试为 actor.tag.tagKey 赋值时遇到了问题，'
                                                    '如果您对此有所了解，请不吝赐教，可以通过新增一个 issue 与我联系，特此感谢'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'error',
                                            'variant': 'tonal',
                                            'text': '警告：由于tagKey的问题，当执行刮削后，可能会出现丢失在线元数据，无法在Plex中点击人物查看详情等问题'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    # {
                    #     'component': 'VRow',
                    #     'content': [
                    #         {
                    #             'component': 'VCol',
                    #             'props': {
                    #                 'cols': 12,
                    #             },
                    #             'content': [
                    #                 {
                    #                     'component': 'VAlert',
                    #                     'props': {
                    #                         'type': 'error',
                    #                         'variant': 'tonal',
                    #                         'text': '免责声明：如开启「保留在线元数据」选项，该功能尚处于实验性阶段，开启后将大幅降低刮削效率，同时需结合数据库脚本使用，'
                    #                                 '可能会引发元数据丢失、播放问题甚至Plex数据库文件损坏等风险，请慎重使用，详细信息请查阅 '
                    #                     },
                    #                     'content': [
                    #                         {
                    #                             'component': 'a',
                    #                             'props': {
                    #                                 'href': 'https://github.com/InfinityPacer/MoviePilot-Plugins/blob/main/plugins/plexpersonmeta/README.md',
                    #                                 'target': '_blank'
                    #                             },
                    #                             'content': [
                    #                                 {
                    #                                     'component': 'u',
                    #                                     'text': 'README'
                    #                                 }
                    #                             ]
                    #                         }
                    #                     ]
                    #                 }
                    #             ]
                    #         }
                    #     ]
                    # },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '注意：如刮削没有达到预期的效果，请尝试在Plex中修改配置，设置->在线媒体资源->发现更多->停用发现来源'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '注意：如开启锁定元数据，则刮削后需要在Plex中手动解锁才允许修改，'
                                                    '请先在测试媒体库验证无问题后再继续使用'
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ],
            }
        ], {
            "enabled": bool(self._enabled),
            "notify": True if self._notify is None else bool(self._notify),
            "clear_cache": False,
            "onlyonce": False,
            "cron": self._cron or "0 1 * * *",
            "cron_added_time": self._cron_added_time or 0,
            "lock": bool(self._lock),
            "execute_transfer": bool(self._execute_transfer),
            "delay": self._delay or 200,
            "scrape_type": self._scrape_type or "all",
            "libraries": self._libraries or [],
            "remove_no_zh": bool(self._remove_no_zh),
            "dry_run": bool(self._dry_run),
            "backup_enabled": bool(self._backup_enabled),
            "restore_backup": False,
            # "reserve_tag_key": False,
            "douban_scrape": bool(self._douban_scrape)
        }

    def get_page(self) -> List[dict]:
        stats = self._last_run_stats or {}
        summary = stats.get("summary") or "暂无执行记录。建议先使用 dry-run 预演确认差异，再关闭预演写回 Plex。"
        details = stats.get("items") or []
        skip_reasons = stats.get("skip_reasons") or []
        backups = stats.get("backups") or []
        restore = stats.get("restore") or {}

        detail_text = "\n".join(details[:20]) if details else "暂无变更明细。"
        skip_text = "\n".join(
            f"- [{item.get('stage', 'unknown')}] {item.get('title', 'unknown')}: {item.get('reason', '未记录原因')}"
            for item in skip_reasons[:20]
        ) if skip_reasons else "暂无跳过或错误记录。"
        backup_text = "\n".join(
            f"- {item.get('title', 'unknown')} ({item.get('rating_key', '-')}) -> {item.get('backup_path', '-')}"
            for item in backups[:10]
        ) if backups else "本轮未产生备份。"
        restore_text = restore.get("message") or "暂无恢复记录。"

        return [
            {
                'component': 'VRow',
                'content': [{
                    'component': 'VCol',
                    'props': {'cols': 12},
                    'content': [
                        {'component': 'VAlert', 'props': {'type': 'info', 'variant': 'tonal', 'text': summary}},
                    ],
                }],
            },
            {
                'component': 'VRow',
                'content': [{
                    'component': 'VCol',
                    'props': {'cols': 12},
                    'content': [
                        {'component': 'VAlert', 'props': {'type': 'info', 'variant': 'outlined', 'text': '最近一次执行明细'}},
                        {'component': 'VAlert', 'props': {'type': 'info', 'variant': 'tonal', 'text': detail_text}},
                    ],
                }],
            },
            {
                'component': 'VRow',
                'content': [{
                    'component': 'VCol',
                    'props': {'cols': 12},
                    'content': [
                        {'component': 'VAlert', 'props': {'type': 'warning', 'variant': 'outlined', 'text': '最近跳过 / 错误记录'}},
                        {'component': 'VAlert', 'props': {'type': 'warning', 'variant': 'tonal', 'text': skip_text}},
                    ],
                }],
            },
            {
                'component': 'VRow',
                'content': [{
                    'component': 'VCol',
                    'props': {'cols': 12},
                    'content': [
                        {'component': 'VAlert', 'props': {'type': 'info', 'variant': 'outlined', 'text': '最近一次备份'}},
                        {'component': 'VAlert', 'props': {'type': 'info', 'variant': 'tonal', 'text': backup_text}},
                    ],
                }],
            },
            {
                'component': 'VRow',
                'content': [{
                    'component': 'VCol',
                    'props': {'cols': 12},
                    'content': [
                        {'component': 'VAlert', 'props': {'type': 'info', 'variant': 'outlined', 'text': '最近一次恢复'}},
                        {'component': 'VAlert', 'props': {'type': 'info', 'variant': 'tonal', 'text': restore_text}},
                    ],
                }],
            },
        ]

    def api_run(self, data: Optional[dict] = None):
        payload = data or {}
        mode = payload.get("mode", "full") if isinstance(payload, dict) else "full"
        dry_run = payload.get("dry_run") if isinstance(payload, dict) else None
        restore = bool(payload.get("restore_last_backup")) if isinstance(payload, dict) else False

        if restore:
            threading.Thread(target=self.restore_last_backup, daemon=True).start()
            return {"success": True, "message": "恢复任务已启动", "restore": True}

        kwargs = {"dry_run": self._dry_run if dry_run is None else bool(dry_run), "trigger_source": "api"}
        target = self.scrape_library
        if mode == "recent":
            added_time = datetime.now(tz=pytz.timezone(settings.TZ)) - timedelta(minutes=max(self._cron_added_time or 60, 1))
            target = self.scrape_library_by_added_time
            kwargs["added_time"] = int(added_time.timestamp())
        threading.Thread(target=target, kwargs=kwargs, daemon=True).start()
        return {"success": True, "message": "任务已启动", "mode": mode, "dry_run": kwargs["dry_run"]}

    def __get_service_library_options(self):
        """
        获取媒体库选项
        """
        library_options = []
        service_infos = self.service_infos()
        if not service_infos:
            return library_options

        # 获取所有媒体库
        for service in service_infos.values():
            plex = service.instance
            if not plex or not plex.get_plex():
                continue
            plex_server = plex.get_plex()
            libraries = sorted(plex_server.library.sections(), key=lambda x: x.key)
            # 遍历媒体库，创建字典并添加到列表中
            for library in libraries:
                # 仅支持电影、剧集媒体库
                if library.TYPE != "show" and library.TYPE != "movie":
                    continue
                library_dict = {
                    "title": f"{service.name} - {library.key}. {library.title} ({library.TYPE})",
                    "value": f"{service.name}.{library.key}"
                }
                library_options.append(library_dict)
        return library_options

    def __get_service_libraries(self) -> Optional[Dict[str, Dict[int, Any]]]:
        """
        获取 Plex 媒体库信息
        """
        if not self._libraries:
            return None

        service_libraries = defaultdict(set)

        # 1. 处理本地 _libraries，提取出 service_name 和 library_key
        for library in self._libraries:
            if not library:
                continue
            if "." in library:
                service_name, library_key = library.split(".", 1)
                service_libraries[service_name].add(library_key)

        # 2. 获取 service_infos 对象
        service_infos = self.service_infos(name_filters=list(service_libraries.keys()))
        if not service_infos:
            return None

        # 创建存放交集的字典，value 也是字典，key 为 int(library.key)，value 为 library 对象
        intersected_libraries = {}

        # 3. 遍历 service_infos，验证 Plex 实例并获取媒体库
        for service_name, library_keys in service_libraries.items():
            service_info = service_infos.get(service_name)
            if not service_info or not service_info.instance:
                continue

            plex = service_info.instance
            plex_server = plex.get_plex()
            if not plex_server:
                continue

            libraries = plex_server.library.sections()

            # 4. 获取 Plex 实例中的有效媒体库，进行比对
            remote_libraries = {
                int(library.key): library  # 键为 int(library.key)，值为 library 对象
                for library in libraries if library.TYPE != "photo"
            }

            # 计算本地库和远程库的交集，保留匹配的库
            matched_libraries = {
                key: library
                for key, library in remote_libraries.items()
                if str(key) in library_keys
            }

            # 如果存在交集，添加到最终结果
            if matched_libraries:
                intersected_libraries[service_name] = matched_libraries

        # 5. 返回交集
        return intersected_libraries if intersected_libraries else None

    @eventmanager.register(EventType.TransferComplete)
    def scrape_rt(self, event: Event):
        """
        根据事件实时刮削演员信息
        """
        if not self._enabled:
            return

        if not self._execute_transfer:
            return

        event_info: dict = event.event_data
        if not event_info:
            return

        mediainfo: MediaInfo = event_info.get("mediainfo")
        meta: MetaBase = event_info.get("meta")
        if not mediainfo or not meta:
            return

        # 获取媒体信息，确定季度和集数信息，如果存在则添加前缀空格
        season_episode = f" {meta.season_episode}" if meta.season_episode else ""
        media_desc = f"{mediainfo.title_year}{season_episode}"

        # 如果最近一次入库时间为None，这里才进行赋值，否则可能是存在尚未执行的任务待执行
        if not self._transfer_time:
            self._transfer_time = datetime.now(tz=pytz.timezone(settings.TZ))

        # 根据是否有延迟设置不同的日志消息
        delay_message = f"{self._delay} 秒后运行一次{self.plugin_name}服务" if self._delay else f"准备运行一次{self.plugin_name}服务"
        logger.info(f"{media_desc} 已入库，{delay_message}")

        if not self._scheduler:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)

        self._scheduler.add_job(
            func=self.__scrape_by_transfer,
            trigger="date",
            run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=self._delay),
            id=self._transfer_job_id,
            replace_existing=True,
            name=f"{self.plugin_name}-transfer",
        )

        # 启动任务
        if self._scheduler.get_jobs() and not self._scheduler.running:
            self._scheduler.print_jobs()
            self._scheduler.start()

    @eventmanager.register(EventType.PluginAction)
    def handle_command(self, event: Event = None):
        if not event or not event.event_data:
            return
        if event.event_data.get("action") != "plex_person_meta_run":
            return
        threading.Thread(
            target=self.scrape_library,
            kwargs={"dry_run": self._dry_run, "trigger_source": "command"},
            daemon=True,
        ).start()

    def __scrape_by_transfer(self):
        """入库后运行一次"""
        if not self._transfer_time:
            logger.info(f"没有获取到最近一次的入库时间，取消执行{self.plugin_name}服务")
            return

        logger.info(f"正在运行一次{self.plugin_name}服务，入库时间 {self._transfer_time.strftime('%Y-%m-%d %H:%M:%S')}")

        adjusted_time = self._transfer_time - timedelta(minutes=5)
        logger.info(f"为保证入库数据完整性，前偏移5分钟后的时间：{adjusted_time.strftime('%Y-%m-%d %H:%M:%S')}")

        self.scrape_library_by_added_time(added_time=int(adjusted_time.timestamp()), trigger_source="transfer")
        self._transfer_time = None

    def scrape_library(self, dry_run: Optional[bool] = None, trigger_source: str = "manual"):
        """
        刮削媒体库中所有媒体的演员信息
        """
        run_dry = self._dry_run if dry_run is None else bool(dry_run)
        if not self.__check_plex_media_server():
            self._last_run_stats = {
                "summary": "Plex 配置不正确或未选择媒体库，无法执行任务。",
                "items": [],
                "skip_reasons": [],
                "backups": [],
            }
            return
        
        # 如果设置了最近一次定时任务运行的入库时间范围，则优先处理该逻辑
        if trigger_source in {"schedule", "onlyonce"} and self._cron_added_time and self._cron_added_time > 0:
            logger.info(f"定时刮削，准备刮削最近 {self._cron_added_time} 分钟入库的媒体")
            added_time = datetime.now(tz=pytz.timezone(settings.TZ)) - timedelta(minutes=self._cron_added_time)
            self.scrape_library_by_added_time(
                added_time=int(added_time.timestamp()),
                dry_run=run_dry,
                trigger_source=trigger_source,
            )
            return

        with lock:
            overall_start_time = time.time()
            plugin_config = self.get_config()
            service_libraries = self.__get_service_libraries()
            if not service_libraries:
                self._last_run_stats = {
                    "summary": "未找到可处理的 Plex 媒体库。",
                    "items": [],
                    "skip_reasons": [],
                    "backups": [],
                }
                return
            batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
            total_stats = {"processed": 0, "changed": 0, "skipped": 0, "errors": 0, "backed_up": 0, "items": [], "skip_reasons": [], "backups": []}
            for service_name, libraries in service_libraries.items():
                service = self.service_info(name=service_name)
                if not service or not service.instance:
                    logger.info(f"获取媒体服务器 {service_name} 实例失败，跳过处理")
                    continue
                service_start_time = time.time()
                scrape_helper = ScrapeHelper(config=plugin_config, event=self._event, chain=self.chain,
                                             service=service, libraries=libraries,
                                             data_dir=self.__data_dir().as_posix(), dry_run=run_dry,
                                             backup_enabled=self._backup_enabled, backup_batch_id=batch_id)
                logger.info(f"开始处理媒体服务器 {service.name} 的媒体库")

                for library_id, library in libraries.items():
                    logger.info(f"开始刮削媒体库 {library.title} 的演员信息 ...")
                    try:
                        rating_items = scrape_helper.list_rating_items(library=library)
                        if not rating_items:
                            logger.info(f"媒体库 {library.title} 没有找到任何媒体信息，跳过刮削")
                            continue

                        self.__merge_run_stats(total_stats, scrape_helper.scrape_rating_items(rating_items=rating_items))
                        logger.info(f"媒体库 {library.title} 的演员信息刮削完成")
                    except Exception as e:
                        logger.error(f"媒体库 {library.title} 刮削过程中出现异常，{str(e)}")
                        total_stats["errors"] += 1
                        total_stats["skip_reasons"].append({"title": library.title, "stage": "library", "reason": str(e)})

                total_stats["backups"].extend(scrape_helper.backup_records())

                service_elapsed_time = time.time() - service_start_time
                logger.info(f"媒体服务器 {service.name} 处理完成，耗时 {service_elapsed_time:.2f} 秒")

            overall_elapsed_time = time.time() - overall_start_time
            mode_text = "dry-run 预演" if run_dry else "写回模式"
            message_text = f"演员信息{mode_text}完成，用时 {overall_elapsed_time:.2f} 秒，处理 {total_stats['processed']}，变更 {total_stats['changed']}，跳过 {total_stats['skipped']}，错误 {total_stats['errors']}"
            self._last_run_stats = {
                "summary": f"最近一次执行：来源={trigger_source}，模式={mode_text}，处理 {total_stats['processed']}，变更 {total_stats['changed']}，跳过 {total_stats['skipped']}，错误 {total_stats['errors']}，备份 {total_stats['backed_up']}。",
                "items": total_stats["items"][-20:],
                "skip_reasons": total_stats["skip_reasons"][-20:],
                "backups": total_stats["backups"][-10:],
            }
            self.__send_message(title="【媒体库演员信息刮削】", text=message_text)
            logger.info(message_text)

    def scrape_library_by_added_time(self, added_time: int, dry_run: Optional[bool] = None, trigger_source: str = "manual"):
        """根据入库时间刮削媒体库中的演员信息"""
        if not self.__check_plex_media_server():
            self._last_run_stats = {
                "summary": "Plex 配置不正确或未选择媒体库，无法执行任务。",
                "items": [],
                "skip_reasons": [],
                "backups": [],
            }
            return

        with lock:
            overall_start_time = time.time()
            plugin_config = self.get_config()
            service_libraries = self.__get_service_libraries()
            if not service_libraries:
                self._last_run_stats = {
                    "summary": "未找到可处理的 Plex 媒体库。",
                    "items": [],
                    "skip_reasons": [],
                    "backups": [],
                }
                return
            run_dry = self._dry_run if dry_run is None else bool(dry_run)
            batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
            total_stats = {"processed": 0, "changed": 0, "skipped": 0, "errors": 0, "backed_up": 0, "items": [], "skip_reasons": [], "backups": []}
            for service_name, libraries in service_libraries.items():
                service = self.service_info(name=service_name)
                if not service or not service.instance:
                    logger.info(f"获取媒体服务器 {service_name} 实例失败，跳过处理")
                    continue
                service_start_time = time.time()
                scrape_helper = ScrapeHelper(config=plugin_config, event=self._event, chain=self.chain,
                                             service=service, libraries=libraries,
                                             data_dir=self.__data_dir().as_posix(), dry_run=run_dry,
                                             backup_enabled=self._backup_enabled, backup_batch_id=batch_id)
                logger.info(f"开始处理媒体服务器 {service.name} 的媒体库")

                for library_id, library in libraries.items():
                    rating_items = {}
                    episode_items = {}
                    recent_added_items = scrape_helper.list_rating_items_by_added(added_time=added_time)

                    for rating_item in recent_added_items:
                        section_id = rating_item.get("librarySectionID")
                        if section_id != library_id:
                            continue

                        rating_key = rating_item.get("ratingKey")
                        if not rating_key:
                            continue

                        rating_type = rating_item.get("type")
                        # 先获取show和movie的key，后续直接进行刮削
                        if rating_type in ["show", "movie"]:
                            rating_items[rating_key] = rating_item
                        # 如果是季，这里直接当成show进行处理
                        elif rating_type == "season":
                            parent_key = scrape_helper.extract_key_from_url(rating_item.get("parentKey"))
                            if parent_key and parent_key not in rating_items:
                                try:
                                    rating_items[parent_key] = scrape_helper.fetch_item(rating_key=parent_key)
                                except Exception as e:
                                    logger.error(f"媒体项 {rating_item.get('parentTitle')} 获取详细信息失败，{e}")
                        # 如果是集的，先判断对应的父级key是否已经在rating_keys中增加，如果是，则忽略，如果不是，则追加到集的key中，后续独立进行刮削
                        elif rating_type == "episode":
                            parent_key = scrape_helper.extract_key_from_url(rating_item.get("grandparentKey"))
                            if parent_key and parent_key not in rating_items:
                                episode_items.setdefault(parent_key, []).append(rating_item)

                    logger.info(f"开始刮削媒体库 {library.title} 最近入库的演员信息 ...")
                    if not rating_items and not episode_items:
                        logger.info(f"媒体库 {library.title} 最近入库没有找到任何符合条件的媒体信息，跳过刮削")
                    else:
                        self.__merge_run_stats(total_stats, scrape_helper.scrape_rating_items(rating_items=list(rating_items.values())))
                        self.__merge_run_stats(total_stats, scrape_helper.scrape_episode_items(episode_items=episode_items))

                total_stats["backups"].extend(scrape_helper.backup_records())

                service_elapsed_time = time.time() - service_start_time
                logger.info(f"媒体服务器 {service.name} 处理完成，耗时 {service_elapsed_time:.2f} 秒")

            overall_elapsed_time = time.time() - overall_start_time
            formatted_added_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(added_time))
            mode_text = "dry-run 预演" if run_dry else "写回模式"
            message_text = f"最近一次入库时间：{formatted_added_time}，演员信息{mode_text}完成，用时 {overall_elapsed_time:.2f} 秒，处理 {total_stats['processed']}，变更 {total_stats['changed']}，跳过 {total_stats['skipped']}，错误 {total_stats['errors']}"
            self._last_run_stats = {
                "summary": f"最近一次执行：来源={trigger_source}，范围=最近入库，模式={mode_text}，处理 {total_stats['processed']}，变更 {total_stats['changed']}，跳过 {total_stats['skipped']}，错误 {total_stats['errors']}，备份 {total_stats['backed_up']}。",
                "items": total_stats["items"][-20:],
                "skip_reasons": total_stats["skip_reasons"][-20:],
                "backups": total_stats["backups"][-10:],
            }
            self.__send_message(title="【媒体库演员信息刮削】", text=message_text)
            logger.info(message_text)

    def restore_last_backup(self):
        restore_dir = self.__latest_backup_dir()
        if not restore_dir:
            self._last_run_stats["restore"] = {"message": "未找到可恢复的演员备份。"}
            return

        restored = 0
        errors: List[str] = []
        with lock:
            for backup_file in sorted(restore_dir.glob("*.json")):
                payload = read_json_file(backup_file)
                if not payload:
                    continue
                service_name = payload.get("service_name")
                rating_key = payload.get("rating_key")
                actors = payload.get("actors") or []
                if not service_name or not rating_key or not actors:
                    continue
                service = self.service_info(service_name)
                if not service or not service.instance:
                    errors.append(f"{payload.get('title', rating_key)}: 服务不可用")
                    continue
                helper = ScrapeHelper(config=self.get_config(), event=self._event, chain=self.chain,
                                      service=service, libraries={}, data_dir=self.__data_dir().as_posix(),
                                      dry_run=False, backup_enabled=False)
                try:
                    item = helper.fetch_item(rating_key=rating_key)
                    if not item:
                        errors.append(f"{payload.get('title', rating_key)}: 未找到条目")
                        continue
                    helper.put_actors(item=item, actors=actors, locked=self._lock)
                    restored += 1
                except Exception as err:
                    errors.append(f"{payload.get('title', rating_key)}: {err}")

        message = f"恢复最近一次备份完成，成功 {restored} 项"
        if errors:
            message = f"{message}，失败 {len(errors)} 项：" + "；".join(errors[:5])
        self._last_run_stats.setdefault("restore", {})
        self._last_run_stats["restore"] = {"message": message}
        self.__send_message(title="【媒体库演员信息恢复】", text=message)

    @staticmethod
    def __merge_run_stats(total_stats: Dict[str, Any], delta: Dict[str, Any]):
        for key in ["processed", "changed", "skipped", "errors", "backed_up"]:
            total_stats[key] += int(delta.get(key, 0) or 0)
        total_stats["items"].extend(delta.get("items", []) or [])
        total_stats["skip_reasons"].extend(delta.get("skip_reasons", []) or [])

    def __data_dir(self) -> Path:
        return Path(self.get_data_path()) if hasattr(self, "get_data_path") else Path("/tmp") / "plexpersonmeta"

    def __latest_backup_dir(self) -> Optional[Path]:
        backup_root = self.__data_dir() / "actor_backups"
        if not backup_root.exists():
            return None
        candidates = [path for path in backup_root.iterdir() if path.is_dir()]
        if not candidates:
            return None
        return sorted(candidates)[-1]

    def __send_message(self, title: str, text: str):
        """
        发送消息
        """
        if not self._notify:
            return

        self.post_message(mtype=NotificationType.SiteMessage, title=title, text=text)

    def __check_plex_media_server(self) -> bool:
        """检查Plex媒体服务器配置"""
        service_libraries = self.__get_service_libraries()
        if not service_libraries:
            logger.error(f"Plex 配置不正确，请检查")
            return False
        return True
