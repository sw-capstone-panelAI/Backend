# DB 테이블 구조 생성하는 부분
from flask_sqlalchemy import SQLAlchemy

# 전역에서 쓸 수 있는 db 객체 생성
db = SQLAlchemy() # gpt왈 db를(sql쿼리 사용하지 않고) 쉽게 사용할 수 있게 도와주는 역할?