from src.config.settings import get_settings
import boto3


def list_prefix(prefix: str):
    settings = get_settings()
    s3 = boto3.client("s3", region_name=settings.AWS_REGION)
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=settings.S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            print(obj["Key"], obj["Size"]) 


if __name__ == "__main__":
    # adjust prefix as needed
    list_prefix("raw/hubspot/contacts/")
