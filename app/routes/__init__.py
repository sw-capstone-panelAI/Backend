# 블루프린트(도메인별 라우팅)
# 라우터들을 한 곳에서 등록

from flask import Flask
from .search import bp as search_bp

def register_routes(app: Flask):
    app.register_blueprint(search_bp, url_prefix="/api/search")
