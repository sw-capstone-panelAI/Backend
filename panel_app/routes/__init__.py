# 블루프린트(도메인별 라우팅)
# 라우터들을 한 곳에서 등록
from .api import bp_api #search로 시작하는 주소
# --으로 시작하는 주소

def register_routes(flask_app):
    """앱에 모든 블루프린트를 등록"""
    flask_app.register_blueprint(bp_api) # search 블루프린터 등록
    # 추가될 다른 주소의 블루프린터
