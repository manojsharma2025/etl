import boto3
from botocore.exceptions import ClientError
from pathlib import Path
from datetime import datetime


class SpacesUploader:
    """Upload files to DigitalOcean Spaces (S3-compatible storage)"""
    
    def __init__(self, logger, config):
        self.logger = logger
        self.config = config
        
        self.access_key = config.get('access_key')
        self.secret_key = config.get('secret_key')
        self.region = config.get('region', 'sfo3')
        self.bucket_name = config.get('bucket_name')
        self.endpoint_url = config.get('endpoint_url', f'https://{self.region}.digitaloceanspaces.com')
        
        if not self.access_key or not self.secret_key:
            raise ValueError("Spaces access_key and secret_key must be provided")
        
        if not self.bucket_name:
            raise ValueError("Spaces bucket_name must be provided")
        
        self.s3_client = boto3.client(
            's3',
            region_name=self.region,
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        )
        
        self.logger.info(f"Initialized Spaces uploader for bucket: {self.bucket_name}")
    
    def upload_file(self, local_file_path, remote_path=None, make_public=False):
        """
        Upload a file to DigitalOcean Spaces
        
        Args:
            local_file_path: Path to local file
            remote_path: Remote path in bucket (optional, uses date-based structure if not provided)
            make_public: Whether to make the file publicly accessible
        
        Returns:
            Public URL of uploaded file
        """
        local_file = Path(local_file_path)
        
        if not local_file.exists():
            raise FileNotFoundError(f"Local file not found: {local_file_path}")
        
        if not remote_path:
            today = datetime.now().strftime('%Y-%m-%d')
            remote_path = f"{today}/{local_file.name}"
        
        self.logger.info(f"Uploading {local_file.name} to Spaces: {remote_path}")
        
        try:
            extra_args = {}
            if make_public:
                extra_args['ACL'] = 'public-read'
            
            self.s3_client.upload_file(
                str(local_file),
                self.bucket_name,
                remote_path,
                ExtraArgs=extra_args
            )
            
            public_url = f"{self.endpoint_url}/{self.bucket_name}/{remote_path}"
            
            self.logger.log_upload_progress(local_file.name, "Success")
            self.logger.info(f"Upload complete: {public_url}")
            
            return public_url
            
        except ClientError as e:
            self.logger.error(f"Failed to upload {local_file.name}: {e}")
            raise
    
    def upload_multiple_files(self, file_paths, folder_prefix=None, make_public=False):
        """
        Upload multiple files to Spaces
        
        Args:
            file_paths: List of local file paths
            folder_prefix: Optional folder prefix (uses date if not provided)
            make_public: Whether to make files publicly accessible
        
        Returns:
            List of public URLs
        """
        if not folder_prefix:
            folder_prefix = datetime.now().strftime('%Y-%m-%d')
        
        uploaded_urls = []
        
        for file_path in file_paths:
            try:
                local_file = Path(file_path)
                remote_path = f"{folder_prefix}/{local_file.name}"
                url = self.upload_file(file_path, remote_path, make_public)
                uploaded_urls.append(url)
            except Exception as e:
                self.logger.error(f"Failed to upload {file_path}: {e}")
        
        return uploaded_urls
    
    def list_files(self, prefix=''):
        """
        List files in the bucket
        
        Args:
            prefix: Optional prefix to filter files
        
        Returns:
            List of file keys
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            else:
                return []
                
        except ClientError as e:
            self.logger.error(f"Failed to list files: {e}")
            return []
    
    def delete_file(self, remote_path):
        """
        Delete a file from Spaces
        
        Args:
            remote_path: Remote file path in bucket
        """
        try:
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=remote_path
            )
            self.logger.info(f"Deleted from Spaces: {remote_path}")
        except ClientError as e:
            self.logger.error(f"Failed to delete {remote_path}: {e}")
            raise
    
    def upload_log_file(self, log_file_path):
        """
        Upload log file to Spaces
        
        Args:
            log_file_path: Path to log file
        
        Returns:
            Public URL of uploaded log
        """
        today = datetime.now().strftime('%Y-%m-%d')
        log_folder = f"logs/{today}"
        
        return self.upload_file(log_file_path, f"{log_folder}/{Path(log_file_path).name}")
