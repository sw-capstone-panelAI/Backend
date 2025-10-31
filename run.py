from flask import Flask, jsonify, request

# Flask객체생성 이걸 보고 어디서 실행되는지 판단
app = Flask(__name__)

# 기본 라우팅 페이지
@app.route('/')
def home():
    return jsonify({"message": "Hello world"})



# 파이썬 파일을 직접 실행했을 때만 아래 코드가 작동하게 하는 조건문
# (다른 파일에서 import될 때는 실행되지 않게 함)
if __name__ == '__main__':
    app.run(debug=True)  # 개발모드, 기본포트 5000