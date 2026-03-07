"""
AWS S3 Image Uploader
======================
Handles uploading extracted question images to AWS S3 and generating
public CDN URLs for use in the API response.

Configuration is read from environment variables:
    AWS_ACCESS_KEY_ID       - AWS access key
    AWS_SECRET_ACCESS_KEY   - AWS secret key
    AWS_REGION              - AWS region (default: ap-south-1)
    S3_BUCKET_NAME          - S3 bucket name (default: examsqa-question-images)
    CDN_BASE_URL            - CDN URL prefix (default: https://cdn.examsqa.in/question-images)
"""

from __future__ import annotations

import logging
import mimetypes
import os
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def _get_s3_client():
    """Create and return a boto3 S3 client using environment variables."""
    return boto3.client(
        "s3",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_REGION", "ap-south-1"),
    )


def _get_bucket_name() -> str:
    """Get the S3 bucket name from environment."""
    return os.environ.get("S3_BUCKET_NAME", "examsqa-question-images")


def _get_cdn_base_url() -> str:
    """Get the CDN base URL for constructing public image URLs."""
    return os.environ.get(
        "CDN_BASE_URL", "https://cdn.examsqa.in/question-images"
    ).rstrip("/")


def upload_image_to_s3(
    file_path: str,
    s3_key: str,
    content_type: Optional[str] = None,
) -> str:
    """
    Upload a single image file to S3.

    Args:
        file_path: Absolute path to the local image file.
        s3_key: The S3 object key (path within the bucket).
        content_type: Optional MIME type. Auto-detected if not provided.

    Returns:
        Public CDN URL for the uploaded image.

    Raises:
        ClientError: If the S3 upload fails.
        FileNotFoundError: If the local file doesn't exist.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Image file not found: {file_path}")

    # Auto-detect content type
    if not content_type:
        content_type, _ = mimetypes.guess_type(file_path)
        content_type = content_type or "image/png"

    bucket = _get_bucket_name()
    s3 = _get_s3_client()

    logger.info(f"Uploading to S3: s3://{bucket}/{s3_key}")

    with open(file_path, "rb") as f:
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=f.read(),
            ContentType=content_type,
            CacheControl="public, max-age=31536000",  # 1 year cache
        )

    cdn_url = f"{_get_cdn_base_url()}/{s3_key}"
    logger.info(f"Uploaded: {cdn_url}")
    return cdn_url


def upload_image_bytes_to_s3(
    image_bytes: bytes,
    s3_key: str,
    content_type: str = "image/png",
) -> str:
    """
    Upload raw image bytes to S3.

    Args:
        image_bytes: Raw image data.
        s3_key: The S3 object key.
        content_type: MIME type of the image.

    Returns:
        Public CDN URL for the uploaded image.
    """
    bucket = _get_bucket_name()
    s3 = _get_s3_client()

    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=image_bytes,
        ContentType=content_type,
        CacheControl="public, max-age=31536000",
    )

    cdn_url = f"{_get_cdn_base_url()}/{s3_key}"
    logger.info(f"Uploaded: {cdn_url}")
    return cdn_url


def upload_directory_to_s3(
    local_dir: str,
    s3_prefix: str,
) -> dict[str, str]:
    """
    Upload all image files from a local directory to S3.

    Args:
        local_dir: Path to the local directory containing images.
        s3_prefix: S3 key prefix (e.g., "pdf_abc123").

    Returns:
        Mapping of local filename → public CDN URL.
    """
    image_extensions = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tiff"}
    local_path = Path(local_dir)

    if not local_path.exists():
        logger.warning(f"Image directory not found: {local_dir}")
        return {}

    url_map: dict[str, str] = {}

    for file_path in sorted(local_path.iterdir()):
        if not file_path.is_file():
            continue
        if file_path.suffix.lower() not in image_extensions:
            continue

        s3_key = f"{s3_prefix}/{file_path.name}"

        try:
            cdn_url = upload_image_to_s3(str(file_path), s3_key)
            url_map[file_path.name] = cdn_url
        except Exception as e:
            logger.error(f"Failed to upload {file_path.name}: {e}")
            # Continue with remaining files
            url_map[file_path.name] = ""

    logger.info(
        f"Uploaded {len(url_map)} images to S3 under prefix: {s3_prefix}"
    )
    return url_map


def verify_s3_config() -> dict:
    """
    Verify that S3 configuration is valid and credentials work.

    Returns:
        Dict with status and details.
    """
    try:
        s3 = _get_s3_client()
        bucket = _get_bucket_name()

        # Try to check if bucket exists
        s3.head_bucket(Bucket=bucket)

        return {
            "status": "ok",
            "bucket": bucket,
            "cdn_base_url": _get_cdn_base_url(),
            "region": os.environ.get("AWS_REGION", "ap-south-1"),
        }
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        return {
            "status": "error",
            "error": f"S3 access failed: {error_code}",
            "bucket": _get_bucket_name(),
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
        }
