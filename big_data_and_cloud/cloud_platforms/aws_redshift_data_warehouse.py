import boto3
from typing import Optional, Self
from pathlib import Path


class S3Client:
    """AWS S3 Client with type-safe operations."""
    
    def __init__(self: Self, region: str = "eu-central-1") -> None:
        self.client = boto3.client('s3', region_name=region)
    
    def upload_file(
        self: Self,
        file_path: Path,
        bucket: str,
        key: str,
        metadata: Optional[dict[str, str]] = None
    ) -> bool:
        """Upload a file to S3 with optional metadata."""
        try:
            extra_args = {}
            if metadata:
                extra_args['Metadata'] = metadata
            
            self.client.upload_file(
                str(file_path),
                bucket,
                key,
                ExtraArgs=extra_args
            )
            return True
        except Exception as e:
            print(f"Upload failed: {e}")
            return False
    
    def download_file(
            self: Self,
            bucket: str,
            key: str,
            local_path: Path
    ) -> bool:
        """Download file from S3 to local path."""
        try:
            self.client.download_file(bucket, key, str(local_path))
            return True
        except Exception as e:
            print(f"Download failed: {e}")
            return False
    
    def list_objects(
            self: Self,
            bucket: str,
            prefix: str = ""
    ) -> list[dict[str, str]]:
        """List objects in S3 bucket with optional prefix filter."""
        response = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return [
            {
                'key': obj['Key'],
                'size': obj['Size'],
                'last_modified': obj['LastModified'].isoformat()
            }
            for obj in response.get('Contents', [])
        ]


# Usage Example
if __name__ == "__main__":
    s3 = S3Client()

    # Upload a file
    s3.upload_file(
        file_path=Path("data/transactions.json"),
        bucket="my-data-lake",
        key="raw/transactions/2023-10-01.json",
        metadata={"source": "API", "processed": "false"}
    )

    # List files
    objects = s3.list_objects("my-data-lake", prefix="raw/transactions/")
    print(f"Found {len(objects)} objects in S3")
