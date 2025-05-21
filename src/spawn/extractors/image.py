"""
Image metadata extractor for SPAwn.

This module provides functionality for extracting metadata from image files.
"""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from spawn.metadata import MetadataExtractor

logger = logging.getLogger(__name__)


class ImageMetadataExtractor(MetadataExtractor):
    """Extract metadata from image files."""

    supported_extensions = [
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".bmp",
        ".tiff",
        ".tif",
        ".webp",
        ".svg",
        ".ico",
        ".heic",
        ".heif",
    ]
    supported_mime_types = [
        "image/",
    ]

    def __init__(self, extract_exif: bool = True, extract_colors: bool = True):
        """
        Initialize the image metadata extractor.

        Args:
            extract_exif: Whether to extract EXIF metadata.
            extract_colors: Whether to extract color information.
        """
        self.extract_exif = extract_exif
        self.extract_colors = extract_colors

    def extract(self, file_path: Path) -> Dict[str, Any]:
        """
        Extract metadata from an image file.

        Args:
            file_path: Path to the file.

        Returns:
            Dictionary of metadata.
        """
        metadata = {}

        try:
            # Try to import PIL, which is required for image processing
            try:
                from PIL import Image, ExifTags

                has_pil = True
            except ImportError:
                has_pil = False
                metadata["error"] = (
                    "PIL/Pillow library not available for image processing"
                )
                return metadata

            if not has_pil:
                return metadata

            # Open the image
            with Image.open(file_path) as img:
                # Basic image properties
                metadata["format"] = img.format
                metadata["mode"] = img.mode
                metadata["width"] = img.width
                metadata["height"] = img.height
                metadata["aspect_ratio"] = (
                    round(img.width / img.height, 3) if img.height > 0 else None
                )
                metadata["resolution"] = img.info.get("dpi", None)

                # Calculate image size in pixels
                metadata["pixel_count"] = img.width * img.height

                # Extract color information if requested
                if self.extract_colors and img.mode in ("RGB", "RGBA"):
                    metadata["color_info"] = self._extract_color_info(img)

                # Extract EXIF data if available and requested
                if (
                    self.extract_exif
                    and hasattr(img, "_getexif")
                    and callable(img._getexif)
                ):
                    exif_data = img._getexif()
                    if exif_data:
                        metadata["exif"] = self._process_exif(exif_data, ExifTags)

                # Extract other image metadata from info dict
                for key, value in img.info.items():
                    if key not in ("exif",) and isinstance(
                        value, (str, int, float, bool)
                    ):
                        metadata[f"info_{key}"] = value

        except Exception as e:
            logger.error(f"Error extracting image metadata from {file_path}: {e}")
            metadata["error"] = str(e)

        return metadata

    def _extract_color_info(
        self, img, max_colors: int = 5, sample_size: int = 100
    ) -> Dict[str, Any]:
        """
        Extract color information from an image.

        Args:
            img: PIL Image object.
            max_colors: Maximum number of dominant colors to extract.
            sample_size: Size of the sample image for color analysis.

        Returns:
            Dictionary of color information.
        """
        color_info = {}

        try:
            # Resize image for faster processing
            img_small = img.copy()
            img_small.thumbnail((sample_size, sample_size))

            # Check if we can get color distribution
            try:
                # Convert to RGB if needed
                if img_small.mode != "RGB":
                    img_small = img_small.convert("RGB")

                # Get color distribution
                from collections import Counter

                pixels = list(img_small.getdata())
                color_counts = Counter(pixels)
                total_pixels = len(pixels)

                # Get most common colors
                dominant_colors = []
                for color, count in color_counts.most_common(max_colors):
                    percentage = round((count / total_pixels) * 100, 2)
                    hex_color = "#{:02x}{:02x}{:02x}".format(*color)
                    dominant_colors.append(
                        {"rgb": color, "hex": hex_color, "percentage": percentage}
                    )

                color_info["dominant_colors"] = dominant_colors

                # Calculate average color
                r_sum = g_sum = b_sum = 0
                for (r, g, b), count in color_counts.items():
                    r_sum += r * count
                    g_sum += g * count
                    b_sum += b * count

                avg_r = round(r_sum / total_pixels)
                avg_g = round(g_sum / total_pixels)
                avg_b = round(b_sum / total_pixels)

                color_info["average_color"] = {
                    "rgb": (avg_r, avg_g, avg_b),
                    "hex": "#{:02x}{:02x}{:02x}".format(avg_r, avg_g, avg_b),
                }

                # Calculate brightness
                brightness = (0.299 * avg_r + 0.587 * avg_g + 0.114 * avg_b) / 255
                color_info["brightness"] = round(brightness, 2)

            except Exception as e:
                logger.debug(f"Error extracting color information: {e}")

        except Exception as e:
            logger.debug(f"Error in color analysis: {e}")

        return color_info

    def _process_exif(self, exif_data, ExifTags) -> Dict[str, Any]:
        """
        Process EXIF data into a readable format.

        Args:
            exif_data: Raw EXIF data from PIL.
            ExifTags: ExifTags module from PIL.

        Returns:
            Dictionary of processed EXIF data.
        """
        exif = {}

        # Create a mapping of EXIF tags
        try:
            exif_tags = {v: k for k, v in ExifTags.TAGS.items()}
        except AttributeError:
            exif_tags = {}

        # Process each EXIF tag
        for tag_id, value in exif_data.items():
            # Skip undefined tags
            if tag_id not in exif_tags:
                continue

            tag_name = exif_tags[tag_id]

            # Handle different types of EXIF data
            if isinstance(value, bytes):
                try:
                    value = value.decode("utf-8").strip("\x00")
                except:
                    value = str(value)
            elif (
                isinstance(value, tuple)
                and len(value) == 2
                and all(isinstance(x, int) for x in value)
            ):
                # Handle rational numbers
                if value[1] != 0:
                    value = value[0] / value[1]
                else:
                    value = value[0]

            # Store the EXIF value
            exif[tag_name] = value

        # Extract common EXIF fields into a more accessible format
        common_exif = {}

        # Camera information
        if "Make" in exif:
            common_exif["camera_make"] = exif["Make"]
        if "Model" in exif:
            common_exif["camera_model"] = exif["Model"]

        # Capture information
        if "DateTimeOriginal" in exif:
            common_exif["date_taken"] = exif["DateTimeOriginal"]
        if "ExposureTime" in exif:
            common_exif["exposure_time"] = exif["ExposureTime"]
        if "FNumber" in exif:
            common_exif["f_number"] = f"f/{exif['FNumber']}"
        if "ISOSpeedRatings" in exif:
            common_exif["iso"] = exif["ISOSpeedRatings"]
        if "FocalLength" in exif:
            common_exif["focal_length"] = f"{exif['FocalLength']}mm"

        # GPS information
        gps_info = {}
        if "GPSInfo" in exif and isinstance(exif["GPSInfo"], dict):
            gps_data = exif["GPSInfo"]

            # Extract latitude
            if 2 in gps_data and 1 in gps_data:
                lat = gps_data[2]
                lat_ref = gps_data[1]
                if isinstance(lat, tuple) and len(lat) == 3:
                    lat_value = lat[0] + lat[1] / 60 + lat[2] / 3600
                    if lat_ref == "S":
                        lat_value = -lat_value
                    gps_info["latitude"] = round(lat_value, 6)

            # Extract longitude
            if 4 in gps_data and 3 in gps_data:
                lon = gps_data[4]
                lon_ref = gps_data[3]
                if isinstance(lon, tuple) and len(lon) == 3:
                    lon_value = lon[0] + lon[1] / 60 + lon[2] / 3600
                    if lon_ref == "W":
                        lon_value = -lon_value
                    gps_info["longitude"] = round(lon_value, 6)

            # Extract altitude
            if 6 in gps_data and 5 in gps_data:
                alt = gps_data[6]
                alt_ref = gps_data[5]
                if isinstance(alt, tuple) and len(alt) == 2:
                    alt_value = alt[0] / alt[1]
                    if alt_ref == 1:
                        alt_value = -alt_value
                    gps_info["altitude"] = round(alt_value, 2)

        if gps_info:
            common_exif["gps"] = gps_info

        # Return both the processed common EXIF and the full EXIF data
        return {"common": common_exif, "raw": exif}
