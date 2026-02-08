import base64
import logging
from pathlib import Path

from PIL import Image

logger = logging.getLogger(__name__)

MAX_DIMENSION = 1568  # Claude Vision recommended max


def resize_if_needed(image_path: Path, max_size_kb: int = 1024) -> Path:
    file_size_kb = image_path.stat().st_size / 1024
    if file_size_kb <= max_size_kb:
        return image_path

    with Image.open(image_path) as img:
        ratio = min(MAX_DIMENSION / max(img.size), 1.0)
        if ratio < 1.0:
            new_size = (int(img.width * ratio), int(img.height * ratio))
            img = img.resize(new_size, Image.LANCZOS)

        # Save as JPEG for smaller size
        resized_path = image_path.with_suffix(".resized.jpg")
        img.convert("RGB").save(resized_path, "JPEG", quality=85)
        logger.debug(
            f"Resized {image_path.name}: {file_size_kb:.0f}KB -> "
            f"{resized_path.stat().st_size / 1024:.0f}KB"
        )
        return resized_path


def image_to_base64(image_path: Path) -> tuple[str, str]:
    """Returns (base64_data, media_type)."""
    suffix = image_path.suffix.lower()
    media_type_map = {
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".gif": "image/gif",
        ".webp": "image/webp",
    }
    media_type = media_type_map.get(suffix, "image/jpeg")
    data = base64.standard_b64encode(image_path.read_bytes()).decode("utf-8")
    return data, media_type


def cleanup_resized(image_dir: Path):
    for f in image_dir.glob("*.resized.jpg"):
        f.unlink(missing_ok=True)
