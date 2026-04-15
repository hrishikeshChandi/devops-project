from slowapi import Limiter
from slowapi.util import get_remote_address
from fastapi import Request


limiter = Limiter(key_func=get_remote_address)
