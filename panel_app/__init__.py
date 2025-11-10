# 플라스크 APP 전체의 구조와 설정들을 주입해 주는 곳
# create_app(App Factory), 확장 초기화 지점

from flask import Flask
from flask_cors import CORS
import config
from .models import db
from .routes import register_routes

def create_app():
    flask_app = Flask(__name__)
    CORS(flask_app) # CORS에러 방지를 위해

    # 한글 JSON 깨짐 방지
    flask_app.config["JSON_AS_ASCII"] = False

    # Flask 설정(config.py)에 작성된 DB 연결 정보 입력
    flask_app.config["SQLALCHEMY_DATABASE_URI"] = (config.DATABASE_CONNECTION_URI)
    flask_app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False # 객체 변경사항을 추적하는 기능을 끄는 옵션
    flask_app.app_context().push() # 지금부터 이 코드 블록 안에서는 Flask 앱이 실행 중인 것처럼 간주해라

    # 여기서 Flask 앱과 db를 연결(초기화) 
    db.init_app(flask_app) # DB와 실제로 연결되는 시점이 이 부분
    db.create_all() # SQLAlchemy에 등록된 **모든 모델 클래스(테이블 구조)**를 실제 DB에 생성
    
    # 라우트(블루프린트) 등록
    register_routes(flask_app) # routes폴더에 있는 모든 블루프린터 한번에 등록

    return flask_app