import os
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

load_dotenv()

# Environment variable-lardan oxu (yoxdursa, default dəyər istifadə et)
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin_user")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "strong_password_123")

ALIASES = {
    "local": {
        "endpoint": MINIO_ENDPOINT,
        "access_key": MINIO_ACCESS_KEY,
        "secret_key": MINIO_SECRET_KEY,
        "secure": False,
        "buckets": ["spray-time", "crop-variety", "crop-health"]
    },
    "general": {
            "endpoint": MINIO_ENDPOINT,
            "access_key": MINIO_ACCESS_KEY,
            "secret_key": MINIO_SECRET_KEY,
            "secure": False,
            "buckets": ["weather", "grass", "crop-pickup", "water-quality"]
        }
}


def get_client(config: dict) -> Minio:
    return Minio(
        config["endpoint"],
        access_key=config["access_key"],
        secret_key=config["secret_key"],
        secure=config["secure"]
    )


def create_buckets_for_alias(alias_name: str, config: dict):
    print(f"\n--- Alias: {alias_name} ({config['endpoint']}) ---")
    client = get_client(config)

    for bucket_name in config["buckets"]:
        try:
            if client.bucket_exists(bucket_name):
                print(f"  '{bucket_name}' artıq mövcuddur, keçilir.")
            else:
                client.make_bucket(bucket_name)
                print(f"  '{bucket_name}' uğurla yaradıldı.")
        except S3Error as e:
            print(f"  Xəta ({bucket_name}): {e}")


def main():
    for alias_name, config in ALIASES.items():
        create_buckets_for_alias(alias_name, config)


if __name__ == "__main__":
    main()

print("\nBütün əməliyyatlar tamamlandı.")