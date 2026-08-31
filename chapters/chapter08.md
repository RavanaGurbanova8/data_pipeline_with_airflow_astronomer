## Communicating with external systems

#### Installing additional operators

You can connect with additional services and operators with Airflow. For example, you can connect to a client and send requests with API on several services. To connect to AWS, we use boto3, for Google Cloud Platform we use Cloud SDK, and for Microsoft Azure we use Azure SDK. To use these operators, we should install additional packages with pip.

```bash
pip install apache-airflow-providers-amazon
pip install apache-airflow-providers-google
pip install apache-airflow-providers-microsoft-azure
```

To perform an action on AWS let's use the S3CopyObjectOperator for example. Let's copy objects from one bucket to other bucket.

```python
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
S3CopyObjectOperator(
   task_id="...",
   source_bucket_name="databucket",                
   source_bucket_key="/data/{{ ds }}.json",        
   dest_bucket_name="backupbucket",                
   dest_bucket_key="/data/{{ ds }}-backup.json",   
)
```

