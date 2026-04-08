import tempfile
from pathlib import Path
from typing import Optional

import requests
from PIL import Image, ImageDraw, ImageEnhance, ImageFilter, ImageFont

OVERLAY_EXIF_TAG = 0x04BC
OVERLAY_EXIF_VALUE = "mpplextools_overlay"


def _font(asset_root: Path, name: str, size: int):
    candidate = asset_root / "overlays" / "font" / name
    if candidate.exists():
        try:
            return ImageFont.truetype(str(candidate), size)
        except Exception:
            pass
    return ImageFont.load_default()


def download_poster(poster_url: str, timeout: int = 60) -> Optional[Path]:
    if not poster_url:
        return None
    response = requests.get(poster_url, timeout=timeout)
    response.raise_for_status()
    temp_dir = Path(tempfile.mkdtemp(prefix="mpplextools-poster-"))
    temp_path = temp_dir / "poster.jpg"
    temp_path.write_bytes(response.content)
    return temp_path


def is_overlay_poster(poster_path: Path) -> bool:
    try:
        poster = Image.open(poster_path)
        exif_tags = poster.getexif()
        return exif_tags.get(OVERLAY_EXIF_TAG) == OVERLAY_EXIF_VALUE
    except Exception:
        return False


def _load_badge(asset_root: Path, folder: str, name: str, height: int) -> Optional[Image.Image]:
    candidate = asset_root / "overlays" / "img" / folder / f"{name}.png"
    if not candidate.exists():
        return None
    try:
        image = Image.open(candidate).convert("RGBA")
    except Exception:
        return None
    width = int(height * image.width / image.height)
    return image.resize((width, height), Image.LANCZOS)


def _enhance_background(image: Image.Image) -> Image.Image:
    blurred = image.filter(ImageFilter.GaussianBlur(radius=28))
    dark = Image.new("RGBA", image.size, (0, 0, 0, 105))
    merged = Image.alpha_composite(blurred.convert("RGBA"), dark)
    merged = ImageEnhance.Contrast(merged).enhance(1.15)
    merged = ImageEnhance.Color(merged).enhance(1.1)
    return merged


def build_overlay_poster(
    poster_path: Path,
    asset_root: Path,
    title: str,
    resolution: str,
    dynamic_range: str,
    duration_text: str,
    rating_text: str,
) -> Path:
    image = Image.open(poster_path).convert("RGB")
    width, height = image.size
    canvas = _enhance_background(image)
    foreground = image.convert("RGBA")
    canvas.alpha_composite(foreground)
    draw = ImageDraw.Draw(canvas, "RGBA")

    bar_height = max(int(height * 0.12), 110)
    padding = max(int(width * 0.03), 24)
    top = height - bar_height - padding
    left = padding
    right = width - padding
    bottom = height - padding
    radius = max(int(bar_height * 0.25), 18)
    draw.rounded_rectangle([(left, top), (right, bottom)], radius=radius, fill=(0, 0, 0, 165))
    draw.rounded_rectangle([(left - 2, top - 2), (right + 2, bottom + 2)], radius=radius + 2, outline=(255, 255, 255, 28), width=2)

    title_font = _font(asset_root, "fzlth.ttf", max(int(height * 0.028), 28))
    meta_font = _font(asset_root, "fzlth.ttf", max(int(height * 0.022), 22))
    rating_font = _font(asset_root, "ALIBABA_Bold.otf", max(int(height * 0.044), 34))

    badge_height = max(int(bar_height * 0.45), 34)
    cursor_x = left + padding
    center_y = top + bar_height // 2

    for badge_name in [resolution, dynamic_range]:
        if not badge_name:
            continue
        badge = _load_badge(asset_root, "empty", badge_name, badge_height)
        if not badge:
            continue
        pos_y = center_y - badge.height // 2
        canvas.alpha_composite(badge, (cursor_x, pos_y))
        cursor_x += badge.width + max(int(width * 0.012), 12)

    title_text = title[:36]
    title_y = top + max(int(bar_height * 0.12), 10)
    meta_y = title_y + title_font.size + 8
    draw.text((cursor_x, title_y), title_text, font=title_font, fill=(255, 255, 255, 255))

    meta_text = duration_text or ""
    if meta_text:
        draw.text((cursor_x, meta_y), meta_text, font=meta_font, fill=(226, 226, 226, 255))

    if rating_text:
        bbox = draw.textbbox((0, 0), rating_text, font=rating_font)
        text_width = bbox[2] - bbox[0]
        rating_x = right - padding - text_width
        rating_y = center_y - rating_font.size // 2 - 4
        draw.text((rating_x, rating_y), rating_text, font=rating_font, fill=(255, 165, 0, 255))

    output = canvas.convert("RGB")
    exif_tags = output.getexif()
    exif_tags[OVERLAY_EXIF_TAG] = OVERLAY_EXIF_VALUE
    out_path = poster_path.parent / "overlay.jpg"
    output.save(out_path, quality=96, exif=exif_tags)
    return out_path
