from cryptography.fernet import Fernet
import base64

from config import SECRET_KEY

# The secret key must be 32 bytes (it must be the same for encryption and decryption)
if not SECRET_KEY or len(SECRET_KEY) < 32:
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
