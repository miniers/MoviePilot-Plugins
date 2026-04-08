import tempfile
from pathlib import Path
from typing import Optional, Tuple

import requests
from PIL import Image, ImageDraw, ImageEnhance, ImageFilter, ImageFont

OVERLAY_EXIF_TAG = 0x04BC
OVERLAY_EXIF_VALUE = "mpplextools_overlay"


def _truetype_font(asset_root: Path, name: str, size: int):
    candidate = asset_root / "overlays" / "font" / name
    if not candidate.exists():
        return None
    try:
        return ImageFont.truetype(str(candidate), size)
    except Exception:
        return None


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
    if not name:
        return None
    candidate = asset_root / "overlays" / "img" / folder / f"{name}.png"
    if not candidate.exists():
        return None
    try:
        image = Image.open(candidate).convert("RGBA")
    except Exception:
        return None
    width = int(height * image.width / image.height)
    return image.resize((width, height), Image.LANCZOS)


def _resize_and_fill_canvas(image: Image.Image, canvas_size: Tuple[int, int]) -> Image.Image:
    canvas_width, canvas_height = canvas_size
    img_width, img_height = image.size
    canvas_ratio = canvas_width / canvas_height
    image_ratio = img_width / img_height

    if image_ratio >= canvas_ratio:
        new_width = int(canvas_height * image_ratio)
        new_height = canvas_height
    else:
        new_width = canvas_width
        new_height = int(canvas_width / image_ratio)

    resized = image.resize((new_width, new_height), Image.BICUBIC)
    canvas = Image.new("RGBA", (canvas_width, canvas_height), (255, 255, 255, 0))
    x_offset = (canvas_width - new_width) // 2
    y_offset = (canvas_height - new_height) // 2
    canvas.paste(resized, (x_offset, y_offset))
    return canvas


def _adjust_bottom_brightness(rgba_image: Image.Image, threshold: int = 88) -> Image.Image:
    rgb_image = rgba_image.convert("RGB")
    width, height = rgb_image.size
    region_height = min(190, height)
    rgb_bottom_region = rgb_image.crop((0, height - region_height, width, height))

    for x in range(width):
        for y in range(region_height):
            r, g, b = rgb_bottom_region.getpixel((x, y))
            luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b
            if luminance > threshold:
                r = int(r * threshold / luminance)
                g = int(g * threshold / luminance)
                b = int(b * threshold / luminance)
            rgb_bottom_region.putpixel((x, y), (r, g, b))

    adjusted = Image.new("RGB", (width, height))
    adjusted.paste(rgb_image, (0, 0))
    adjusted.paste(rgb_bottom_region, (0, height - region_height))
    return adjusted.convert("RGBA")


def _normalize_duration(duration_text: str, portrait: bool, resolution: str, dynamic_range: str) -> Tuple[str, float]:
    scale = 1.215 if portrait else 0.7
    duration = duration_text or ""
    if not portrait:
        return duration, scale

    if not duration:
        return duration, 1.198

    compact_duration = duration.replace(" ", "")
    if resolution == "1080P" and "小时" in compact_duration and "分钟" in compact_duration:
        return compact_duration, 1.192
    if dynamic_range == "DV" and "小时" in compact_duration and "分钟" in compact_duration:
        return compact_duration, 1.18
    return duration, scale


def _build_reference_overlay(
    poster_path: Path,
    asset_root: Path,
    resolution: str,
    dynamic_range: str,
    duration_text: str,
    rating_text: str,
) -> Path:
    original_image = Image.open(poster_path).convert("RGBA")
    original_width, original_height = original_image.size
    portrait = original_height >= original_width

    duration_text, scale = _normalize_duration(duration_text, portrait, resolution, dynamic_range)
    poster_width, poster_height = (1000, 1500) if portrait else (1000, 563)
    mode = "movie" if portrait else "show"

    resized_image = _resize_and_fill_canvas(original_image, (poster_width, poster_height))
    new_image = Image.new("RGBA", (poster_width, poster_height), (0, 0, 0, 255))
    new_image.paste(resized_image, (0, 0))
    blurred_image = new_image.filter(ImageFilter.GaussianBlur(radius=77))

    radius = int(20 * scale)
    x = int(22 * scale) - 4 if portrait else int(22 * scale)
    bottom = int(28 * scale)
    bar_height = int(110 * scale)
    y = poster_height - bottom - bar_height
    right = poster_width - x

    bottom_region = new_image.crop((0, y - 2, poster_width, poster_height))
    brightness = sum(bottom_region.convert("L").getdata()) / ((bar_height + 2 + bottom) * poster_width)
    brightness = int(brightness)

    if mode == "movie":
        overlay_alpha = 165 if brightness >= 60 else 145
    else:
        overlay_alpha = 140 if brightness >= 60 else 135

    node = 80
    outline_middle = 30 if node < brightness < 80 else 0
    outline_alpha = outline_middle if mode == "movie" else 0

    if brightness < node:
        contrast_factor = 1.65
        overlay_layer = Image.new("RGBA", (poster_width, poster_height), (0, 0, 0, overlay_alpha))
        poster_image = Image.alpha_composite(blurred_image, overlay_layer)
    else:
        overlay_alpha_black = 0
        if mode == "movie":
            overlay_alpha = 50
            if brightness < 19:
                overlay_alpha = 62
            if brightness > 30:
                overlay_alpha_black = 40
            if brightness > 35:
                overlay_alpha_black = 50
        else:
            overlay_alpha = 36
            if brightness < 19:
                overlay_alpha = 42
            if brightness > 35:
                overlay_alpha_black = 30

        overlay_layer = Image.new("RGBA", (poster_width, poster_height), (255, 255, 255, overlay_alpha))
        poster_image = Image.alpha_composite(blurred_image, overlay_layer)
        overlay_layer_black = Image.new("RGBA", (poster_width, poster_height), (0, 0, 0, overlay_alpha_black))
        poster_image = Image.alpha_composite(poster_image, overlay_layer_black)
        contrast_factor = 1.3

    try:
        pixels = poster_image.load()
        for py in range(poster_image.height):
            for px in range(poster_image.width):
                r, g, b, a = pixels[px, py]
                pixels[px, py] = (r * a // 255, g * a // 255, b * a // 255, a)

        if brightness < 36:
            poster_image = _adjust_bottom_brightness(poster_image)

        saturation_factor = 1.8 if mode == "movie" else 1.5
        poster_image = ImageEnhance.Color(poster_image).enhance(saturation_factor)
        poster_image = ImageEnhance.Contrast(poster_image).enhance(contrast_factor)
    except Exception:
        pass

    poster = Image.new("RGBA", (poster_width, poster_height))
    mask = Image.new("L", (poster_width, poster_height))
    draw = ImageDraw.Draw(mask)
    draw.rounded_rectangle([(x, y), (right, y + bar_height)], radius, fill=255)

    outline = Image.new("RGBA", (poster_width, poster_height))
    draw = ImageDraw.Draw(outline)
    if mode == "movie":
        draw.rounded_rectangle(
            [(x - 2, y - 2), (right + 2, y + bar_height + 2)],
            radius + 2,
            fill=(255, 255, 255, outline_alpha),
        )
    else:
        draw.rounded_rectangle(
            [(x - 1, y - 1), (right + 1, y + bar_height + 1)],
            radius + 1,
            fill=(255, 255, 255, outline_alpha),
        )

    poster.paste(resized_image, (0, 0))
    poster = Image.alpha_composite(poster, outline)
    poster.paste(poster_image, (0, 0), mask=mask)

    badge_height = int(62 * scale)
    x_resolution = int(x + 22 * scale)
    y_resolution = int(y + bar_height / 2 - badge_height / 2)

    resolution_badge = _load_badge(asset_root, "empty", resolution, badge_height)
    if resolution_badge:
        resolution_image = Image.new("RGBA", (poster_width, poster_height))
        resolution_image.paste(resolution_badge, (x_resolution, y_resolution))
        poster = Image.alpha_composite(poster, resolution_image)
    else:
        resolution_badge = Image.new("RGBA", (0, badge_height))

    x_dynamic_range = int(x_resolution + resolution_badge.width + 20 * scale)
    dynamic_range_badge = _load_badge(asset_root, "empty", dynamic_range, badge_height)
    if dynamic_range_badge:
        dynamic_range_image = Image.new("RGBA", (poster_width, poster_height))
        dynamic_range_image.paste(dynamic_range_badge, (x_dynamic_range, y_resolution))
        poster = Image.alpha_composite(poster, dynamic_range_image)
    else:
        dynamic_range_badge = Image.new("RGBA", (0, badge_height))

    draw = ImageDraw.Draw(poster)
    duration_font_size = int((51 if dynamic_range == "DV" and mode == "movie" else 54) * scale)
    rating_font_size = int(75 * scale)
    duration_font = _truetype_font(asset_root, "fzlth.ttf", duration_font_size)
    rating_font = _truetype_font(asset_root, "ALIBABA_Bold.otf", rating_font_size)

    duration_width = int(draw.textlength(duration_text, duration_font)) if duration_text and duration_font else 0
    rating_width = int(draw.textlength(rating_text, rating_font)) if rating_text and rating_font else 0
    text_height = 52 * scale

    y_duration = int(y + bar_height / 2 - text_height / 2)
    if mode == "movie":
        x_duration = int(x_resolution + resolution_badge.width + 20 * scale + dynamic_range_badge.width + 22 * scale)
    else:
        x_duration = int(x_resolution + resolution_badge.width + 20 * scale + dynamic_range_badge.width + 30 * scale)
    y_rating = int(y + bar_height / 2 - text_height / 2)

    if duration_text and duration_font:
        if mode == "movie":
            draw.text((x_duration, y_duration - 3 * scale), duration_text, fill=(255, 255, 255, 255), font=duration_font)
        else:
            draw.text((x_duration, y_duration - 5 * scale + 1), duration_text, fill=(255, 255, 255, 255), font=duration_font)

    if rating_text and rating_font:
        if mode == "movie":
            rating_x = right - (26 if dynamic_range == "DV" else 30) * scale - rating_width
            rating_y = y_rating - 23 * scale
        else:
            rating_x = right - 30 * scale - rating_width
            rating_y = y_rating - 23 * scale + 2
        draw.text((rating_x, rating_y), rating_text, fill=(255, 155, 21, 255), font=rating_font)

    output = poster.convert("RGB")
    exif_tags = output.getexif()
    exif_tags[OVERLAY_EXIF_TAG] = OVERLAY_EXIF_VALUE
    out_path = poster_path.parent / "overlay.jpg"
    output.save(out_path, quality=99, exif=exif_tags)
    return out_path


def build_overlay_poster(
    poster_path: Path,
    asset_root: Path,
    title: str,
    resolution: str,
    dynamic_range: str,
    duration_text: str,
    rating_text: str,
) -> Path:
    return _build_reference_overlay(
        poster_path=poster_path,
        asset_root=asset_root,
        resolution=resolution,
        dynamic_range=dynamic_range,
        duration_text=duration_text,
        rating_text=rating_text,
    )
