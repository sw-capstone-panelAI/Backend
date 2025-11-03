
from app import create_app
from flask_cors import CORS

app = create_app()
CORS(app)

if __name__ == "__main__":
    # 개발 서버 (운영에서는 gunicorn/waitress 사용 권장)
    app.run(host="127.0.0.1", port=5000, debug=True)
