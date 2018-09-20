from qiniu import Auth, put_file, etag, urlsafe_base64_encode
import qiniu.config

class Qiniu(object):
    def __init__(self, bucket, url, access_key, secret_key):
        self.qiniu_client = Auth(access_key, secret_key)
        self.url = url
        self.bucket = bucket

    def get_url(self, image_name, image_style=None, expired=7200):
        if isinstance(image_style, str):
            image_name += "-%s"%image_style

        return self.qiniu_client.private_download_url("%s/%s"%(self.url, image_name), expires=expired)
