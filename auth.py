# auth.py
import uuid
import jwt
import time
import hashlib

def generate_jwt_token(access_key, secret_key, params=None):
    payload = {
        'access_key': access_key,
        'nonce': str(uuid.uuid4()),
    }

    if params:
        query_string = '&'.join([f"{key}={value}" for key, value in params.items()])
        m = hashlib.sha512()
        m.update(query_string.encode())
        query_hash = m.hexdigest()
        payload.update({
            'query_hash': query_hash,
            'query_hash_alg': 'SHA512',
        })

    #jwt_token = jwt.encode(payload, secret_key)
    jwt_token = jwt.encode(payload, secret_key, algorithm='HS256')
    return f"Bearer {jwt_token}"