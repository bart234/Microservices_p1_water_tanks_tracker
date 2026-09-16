import os

def get_secrets(secret_name):
    try:
        with open(f"{secret_name}",'r') as file:
            return file.read().strip()
    except IOError:
        return os.getenv(secret_name)