"""
helper.py

这个模块定义了用于存储媒体项目信息的 `RatingInfo` 数据类以及缓存、限流等装饰器
"""
import functools
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from app.core.cache import Cache
from app.log import logger

CACHE_TTL = 60 * 60 * 24 * 3
NEGATIVE_CACHE_TTL = 60 * 5

# 创建缓存实例
cache_backend = Cache(maxsize=100000, ttl=CACHE_TTL)
negative_cache_backend = Cache(maxsize=100000, ttl=NEGATIVE_CACHE_TTL)


def clear_cache_regions():
    """清理插件使用的所有缓存区域。"""
    for backend in (cache_backend, negative_cache_backend):
        backend.clear(region="plex_tmdb_media")
        backend.clear(region="plex_tmdb_person")
        backend.clear(region="plex_douban_media")


def sanitize_filename(value: str) -> str:
    """将任意文本转换为稳定的文件名片段。"""
    return str(value or "unknown").strip().replace("/", "_").replace(":", "_").replace(" ", "_")


def write_json_file(path: Path, payload: Any):
    """写入 JSON 文件，并确保父目录存在。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def read_json_file(path: Path) -> Any:
    """读取 JSON 文件；不存在或读取失败时返回 None。"""
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as err:
        logger.error(f"读取 JSON 文件失败: {path} - {err}")
        return None


@dataclass
class RatingInfo:
    """
    媒体项目信息的数据类
    """
    key: Optional[str] = None  # 媒体项目的唯一标识
    type: Optional[str] = None  # 媒体项目的类型（例如：电影、电视剧）
    title: Optional[str] = None  # 媒体项目的标题
    search_title: Optional[str] = None  # 用于搜索的标题
    tmdbid: Optional[int] = None  # TMDB 的唯一标识，可选


def cache_with_logging(region, source):
    """
    装饰器，用于在函数执行时处理缓存逻辑和日志记录。
    :param region: 缓存区，用于存储和检索缓存数据
    :param source: 数据来源，用于日志记录（例如：PERSON 或 MEDIA）
    :return: 装饰器函数
    """

    def decorator(func):

        @functools.wraps(func)
        def wrapped_func(*args, **kwargs):
            # 生成缓存键
            func_name = func.__name__
            args_str = str(args) + str(sorted(kwargs.items()))
            key = hashlib.md5((func_name + args_str).encode()).hexdigest()

            negative_exists_cache = negative_cache_backend.exists(key, region=region)
            if negative_exists_cache:
                value = negative_cache_backend.get(key, region=region)
                if value == "None":
                    logger.info(f"从缓存中获取到 {source} 信息为 None，可能是之前触发限流或网络异常")
                    return None

            exists_cache = cache_backend.exists(key, region=region)
            if exists_cache:
                value = cache_backend.get(key, region=region)
                if value is not None:
                    if source == "PERSON":
                        logger.info(f"从缓存中获取到 {source} 人物信息")
                    else:
                        logger.info(f"从缓存中获取到 {source} 媒体信息: {kwargs.get('title', 'Unknown Title')}")
                    return value
                return None

            # 执行被装饰的函数
            result = func(*args, **kwargs)

            if result is None:
                # 如果结果为 None，说明触发限流或网络等异常，缓存5分钟，以免高频次调用
                negative_cache_backend.set(key, "None", region=region)
            else:
                # 结果不为 None，使用默认 TTL 缓存
                cache_backend.set(key, result, region=region)

            return result

        return wrapped_func

    return decorator
