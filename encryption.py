# encryption.py
from cryptography.fernet import Fernet
import base64
from dotenv import load_dotenv
import os

load_dotenv()

# Generate a secret key (it must be the same for encryption and decryption)
SECRET_KEY = os.getenv('SECRET_KEY')

# Make sure the key is 32 bytes
if len(SECRET_KEY) != 32:
    raise ValueError("The secret key must have 32 bytes.")

# Convert the key to base64 format for Fernet
fernet_key = base64.urlsafe_b64encode(SECRET_KEY.encode())
cipher_suite = Fernet(fernet_key)


def encrypt_token(token):
    """
    Encrypt a token with AES.
    """
    if not token:
        return None
    return cipher_suite.encrypt(token.encode()).decode()

def decrypt_token(encrypted_token):
    """
    Dencrypt a token with AES.
    """
    if not encrypted_token:
        return None
    return cipher_suite.decrypt(encrypted_token.encode()).decode()
