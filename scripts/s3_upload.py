import boto3
def upload_file_to_s3(local_path, s3_path, bucket):
    s3 = boto3.client("s3")
    s3.upload_file(
        local_path,
        bucket,
        s3_path
    )

