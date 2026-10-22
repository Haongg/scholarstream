"""Writes raw OpenAlex pages to the bronze bucket, one partition per publication date."""

import gzip
import json
import os
from datetime import date

import boto3

BRONZE_BUCKET = "bronze"


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1",
    )


class BronzeWriter:
    def __init__(self, s3=None, bucket: str = BRONZE_BUCKET):
        self.s3 = s3 or s3_client()
        self.bucket = bucket

    @staticmethod
    def prefix(day: date) -> str:
        return f"works/publication_date={day.isoformat()}/"

    def clear_day(self, day: date) -> None:
        """Remove existing pages so a re-run replaces the partition instead of mixing with it."""
        paginator = self.s3.get_paginator("list_objects_v2")
        for listing in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix(day)):
            keys = [{"Key": o["Key"]} for o in listing.get("Contents", [])]
            if keys:
                self.s3.delete_objects(Bucket=self.bucket, Delete={"Objects": keys})

    def write_page(self, day: date, page_number: int, page: dict) -> str:
        key = f"{self.prefix(day)}page-{page_number:05d}.json.gz"
        body = gzip.compress(json.dumps(page).encode("utf-8"))
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body)
        return key
