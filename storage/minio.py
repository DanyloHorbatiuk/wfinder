import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from core.setting import get_settings
from utils.logger import get_logger


logger = get_logger(__name__)

load_dotenv()

logger.info(f"file '.env' loaded")


aws_access_key_id = get_settings().minio_root_user
aws_secret_access_key = get_settings().minio_root_password
minio_endpoint = get_settings().minio_endpoint
BUCKET = get_settings().minio_bucket_name

_s3_client = None


def get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client(
            "s3",
            endpoint_url=minio_endpoint,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        logger.info(f"s3-boto3 client created")
        _ensure_bucket_exists(_s3_client)
    else:
        logger.info(f"s3-boto3 client already exists")
    return _s3_client


def _ensure_bucket_exists(s3_client):
    try:
        s3_client.head_bucket(Bucket=BUCKET)
        logger.info(f"bucket '{BUCKET}' exists")
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in ("404", "NoSuchBucket"):
            s3_client.create_bucket(Bucket=BUCKET)
            logger.info(f"bucket '{BUCKET}' did not exist, created new one")
        else:
            logger.error(f"error checking bucket '{BUCKET}': {e}")
            raise


def put_object(key: str, content: bytes, content_type: str = "application/json") -> dict:
    s3_client = get_s3_client()
    logger.info(f"saving object '{key}' to minio bucket '{BUCKET}'...")
    try:
        response = s3_client.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=content,
            ContentType=content_type,
        )
        logger.info(f"saving '{key}' is finished")
        return {
            "bucket": BUCKET,
            "key": key,
            "etag": response["ETag"].strip('"'),
            "size_bytes": len(content),
        }
    except ClientError as e:
        logger.error(f"failed saving '{key}': {e}")
        raise


def save_object(name: str, content: str) -> dict:
    return put_object(f"raw/{name}.json", content.encode("utf-8"))