from sanic import Sanic

from app.ad.view import ad_blueprint
from clients import client_blueprint

app = Sanic()

app.config.update(dict(MYSQL=dict(host="172.16.1.251", port=3306, user='root', password='password', db='db')))

app.register_blueprint(ad_blueprint)
app.register_blueprint(client_blueprint)

if __name__ == '__main__':
    app.run(host="0.0.0.0")