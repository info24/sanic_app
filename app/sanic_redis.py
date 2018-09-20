import redis

def create_redis(host="127.0.0.1", port=6379, password=""):
    if password:
        return redis.Redis(host, port, password)
    else:
        return redis.Redis(host, port)