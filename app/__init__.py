# 플라스크 APP 전체의 구조와 설정들을 주입해 주는 곳
# create_app(App Factory), 확장 초기화 지점

from flask import Flask
from .routes import register_routes

def create_app():
    app = Flask(__name__)

    # 한글 JSON 깨짐 방지
    app.config["JSON_AS_ASCII"] = False

    # 라우트(블루프린트) 등록
    register_routes(app)
    return app