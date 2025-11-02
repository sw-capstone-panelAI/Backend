#pip install sentence-transformers scikit-learn 실행해야함

from flask import Flask, request, jsonify
from flask_cors import CORS
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import re

app = Flask(__name__)
CORS(app)

# 임베딩 모델 로드 (예: BAAI/bge-m3)
embed_model = SentenceTransformer('BAAI/bge-m3')

# 예시 패널 데이터 5개
panels = [
    {"id": "패널001", "age": 22, "gender": "남성", "occupation": "학생", "residence": "서울", "income": 1000, "reliability": 85, "vehicle": {"hasVehicle": False, "type": ""}, "surveyProvider": "설문사 A"},
    {"id": "패널002", "age": 27, "gender": "여성", "occupation": "디자이너", "residence": "부산", "income": 3200, "reliability": 100, "vehicle": {"hasVehicle": False, "type": ""}, "surveyProvider": "설문사 B"},
    {"id": "패널003", "age": 31, "gender": "남성", "occupation": "개발자", "residence": "경기", "income": 4800, "reliability": 95, "vehicle": {"hasVehicle": True, "type": "세단"}, "surveyProvider": "설문사 C"},
    {"id": "패널004", "age": 44, "gender": "여성", "occupation": "교사", "residence": "인천", "income": 5200, "reliability": 88, "vehicle": {"hasVehicle": True, "type": "SUV"}, "surveyProvider": "설문사 A"},
    {"id": "패널005", "age": 36, "gender": "남성", "occupation": "마케터", "residence": "제주", "income": 4000, "reliability": 75, "vehicle": {"hasVehicle": False, "type": ""}, "surveyProvider": "설문사 B"},
    {"id": "패널006", "age": 29, "gender": "여성", "occupation": "간호사", "residence": "서울", "income": 3900, "reliability": 100, "vehicle": {"hasVehicle": True, "type": "소형차"}, "surveyProvider": "설문사 C"},
    {"id": "패널007", "age": 41, "gender": "남성", "occupation": "영업", "residence": "대구", "income": 5100, "reliability": 91, "vehicle": {"hasVehicle": True, "type": "SUV"}, "surveyProvider": "설문사 A"},
    {"id": "패널008", "age": 25, "gender": "여성", "occupation": "마케터", "residence": "광주", "income": 3100, "reliability": 77, "vehicle": {"hasVehicle": False, "type": ""}, "surveyProvider": "설문사 B"},
    {"id": "패널009", "age": 54, "gender": "남성", "occupation": "자영업", "residence": "울산", "income": 6500, "reliability": 94, "vehicle": {"hasVehicle": True, "type": "트럭"}, "surveyProvider": "설문사 A"},
    {"id": "패널010", "age": 33, "gender": "여성", "occupation": "연구원", "residence": "경북", "income": 4700, "reliability": 89, "vehicle": {"hasVehicle": False, "type": ""}, "surveyProvider": "설문사 C"},
    {"id": "패널011", "age": 46, "gender": "남성", "occupation": "교수", "residence": "세종", "income": 8000, "reliability": 93, "vehicle": {"hasVehicle": True, "type": "세단"}, "surveyProvider": "설문사 B"},
    {"id": "패널012", "age": 39, "gender": "여성", "occupation": "회계사", "residence": "경기", "income": 6700, "reliability": 90, "vehicle": {"hasVehicle": True, "type": "SUV"}, "surveyProvider": "설문사 A"},
    {"id": "패널013", "age": 50, "gender": "남성", "occupation": "관리직", "residence": "충남", "income": 7200, "reliability": 88, "vehicle": {"hasVehicle": True, "type": "세단"}, "surveyProvider": "설문사 C"},
    {"id": "패널014", "age": 24, "gender": "여성", "occupation": "학생", "residence": "서울", "income": 1200, "reliability": 20, "vehicle": {"hasVehicle": False, "type": ""}, "surveyProvider": "설문사 A"},
    {"id": "패널015", "age": 62, "gender": "남성", "occupation": "자영업", "residence": "부산", "income": 5000, "reliability": 87, "vehicle": {"hasVehicle": True, "type": "SUV"}, "surveyProvider": "설문사 B"},
    {"id": "패널016", "age": 37, "gender": "여성", "occupation": "공무원", "residence": "전북", "income": 5400, "reliability": 92, "vehicle": {"hasVehicle": True, "type": "소형차"}, "surveyProvider": "설문사 C"},
    {"id": "패널017", "age": 48, "gender": "남성", "occupation": "개발자", "residence": "강원", "income": 6800, "reliability": 85, "vehicle": {"hasVehicle": True, "type": "SUV"}, "surveyProvider": "설문사 A"},
    {"id": "패널018", "age": 30, "gender": "여성", "occupation": "디자이너", "residence": "인천", "income": 3500, "reliability": 81, "vehicle": {"hasVehicle": False, "type": ""}, "surveyProvider": "설문사 B"},
    {"id": "패널019", "age": 43, "gender": "남성", "occupation": "마케터", "residence": "충북", "income": 6000, "reliability": 95, "vehicle": {"hasVehicle": True, "type": "SUV"}, "surveyProvider": "설문사 A"},
    {"id": "패널020", "age": 52, "gender": "여성", "occupation": "강사", "residence": "제주", "income": 4800, "reliability": 90, "vehicle": {"hasVehicle": True, "type": "소형차"}, "surveyProvider": "설문사 C"},
    {"id": "패널021", "age": 28, "gender": "남성", "occupation": "학생", "residence": "서울", "income": 2100, "reliability": 10, "vehicle": {"hasVehicle": False, "type": ""}, "surveyProvider": "설문사 B"},
    {"id": "패널022", "age": 34, "gender": "여성", "occupation": "간호사", "residence": "대전", "income": 4200, "reliability": 86, "vehicle": {"hasVehicle": True, "type": "세단"}, "surveyProvider": "설문사 C"},
    {"id": "패널023", "age": 59, "gender": "남성", "occupation": "자영업", "residence": "전남", "income": 6100, "reliability": 100, "vehicle": {"hasVehicle": True, "type": "트럭"}, "surveyProvider": "설문사 A"},
    {"id": "패널024", "age": 45, "gender": "여성", "occupation": "마케터", "residence": "경남", "income": 4900, "reliability": 83, "vehicle": {"hasVehicle": True, "type": "SUV"}, "surveyProvider": "설문사 B"},
    {"id": "패널025", "age": 26, "gender": "남성", "occupation": "학생", "residence": "울산", "income": 1800, "reliability": 75, "vehicle": {"hasVehicle": False, "type": ""}, "surveyProvider": "설문사 C"},
    {"id": "패널026", "age": 33, "gender": "여성", "occupation": "개발자", "residence": "서울", "income": 5200, "reliability": 38, "vehicle": {"hasVehicle": False, "type": ""}, "surveyProvider": "설문사 A"},
    {"id": "패널027", "age": 40, "gender": "남성", "occupation": "공무원", "residence": "경기", "income": 6100, "reliability": 92, "vehicle": {"hasVehicle": True, "type": "세단"}, "surveyProvider": "설문사 C"},
    {"id": "패널028", "age": 49, "gender": "여성", "occupation": "영업", "residence": "충북", "income": 5600, "reliability": 90, "vehicle": {"hasVehicle": True, "type": "SUV"}, "surveyProvider": "설문사 B"},
    {"id": "패널029", "age": 31, "gender": "남성", "occupation": "디자이너", "residence": "부산", "income": 3700, "reliability": 84, "vehicle": {"hasVehicle": False, "type": ""}, "surveyProvider": "설문사 A"},
    {"id": "패널030", "age": 27, "gender": "여성", "occupation": "마케터", "residence": "광주", "income": 3300, "reliability": 79, "vehicle": {"hasVehicle": False, "type": ""}, "surveyProvider": "설문사 C"},
    {"id": "패널031", "age": 42, "gender": "남성", "occupation": "교사", "residence": "전북", "income": 5800, "reliability": 91, "vehicle": {"hasVehicle": True, "type": "SUV"}, "surveyProvider": "설문사 B"},
    {"id": "패널032", "age": 38, "gender": "여성", "occupation": "연구원", "residence": "서울", "income": 6400, "reliability": 43, "vehicle": {"hasVehicle": True, "type": "세단"}, "surveyProvider": "설문사 A"},
    {"id": "패널033", "age": 24, "gender": "남성", "occupation": "학생", "residence": "경기", "income": 1600, "reliability": 70, "vehicle": {"hasVehicle": False, "type": ""}, "surveyProvider": "설문사 C"},
    {"id": "패널034", "age": 57, "gender": "여성", "occupation": "자영업", "residence": "대구", "income": 7000, "reliability": 96, "vehicle": {"hasVehicle": True, "type": "SUV"}, "surveyProvider": "설문사 B"},
    {"id": "패널035", "age": 44, "gender": "남성", "occupation": "마케터", "residence": "서울", "income": 5600, "reliability": 89, "vehicle": {"hasVehicle": True, "type": "SUV"}, "surveyProvider": "설문사 A"},
    {"id": "패널036", "age": 53, "gender": "여성", "occupation": "교사", "residence": "경남", "income": 6300, "reliability": 92, "vehicle": {"hasVehicle": True, "type": "세단"}, "surveyProvider": "설문사 C"},
    {"id": "패널037", "age": 29, "gender": "남성", "occupation": "개발자", "residence": "인천", "income": 4700, "reliability": 50, "vehicle": {"hasVehicle": False, "type": ""}, "surveyProvider": "설문사 B"},
    {"id": "패널038", "age": 48, "gender": "여성", "occupation": "회계사", "residence": "서울", "income": 7100, "reliability": 95, "vehicle": {"hasVehicle": True, "type": "SUV"}, "surveyProvider": "설문사 A"},
    {"id": "패널039", "age": 39, "gender": "남성", "occupation": "공무원", "residence": "충남", "income": 5800, "reliability": 88, "vehicle": {"hasVehicle": True, "type": "소형차"}, "surveyProvider": "설문사 C"},
    {"id": "패널040", "age": 26, "gender": "여성", "occupation": "학생", "residence": "부산", "income": 2200, "reliability": 73, "vehicle": {"hasVehicle": False, "type": ""}, "surveyProvider": "설문사 B"},
    {"id": "패널041", "age": 34, "gender": "남성", "occupation": "마케터", "residence": "광주", "income": 4200, "reliability": 85, "vehicle": {"hasVehicle": True, "type": "세단"}, "surveyProvider": "설문사 A"},
    {"id": "패널042", "age": 60, "gender": "여성", "occupation": "자영업", "residence": "경북", "income": 6200, "reliability": 10, "vehicle": {"hasVehicle": True, "type": "SUV"}, "surveyProvider": "설문사 C"},
    {"id": "패널043", "age": 32, "gender": "남성", "occupation": "디자이너", "residence": "경기", "income": 4300, "reliability": 82, "vehicle": {"hasVehicle": False, "type": ""}, "surveyProvider": "설문사 B"},
    {"id": "패널044", "age": 55, "gender": "여성", "occupation": "교수", "residence": "세종", "income": 7500, "reliability": 94, "vehicle": {"hasVehicle": True, "type": "SUV"}, "surveyProvider": "설문사 A"},
    {"id": "패널045", "age": 46, "gender": "남성", "occupation": "관리직", "residence": "서울", "income": 6900, "reliability": 92, "vehicle": {"hasVehicle": True, "type": "세단"}, "surveyProvider": "설문사 C"},
    {"id": "패널046", "age": 28, "gender": "여성", "occupation": "연구원", "residence": "경기", "income": 3900, "reliability": 79, "vehicle": {"hasVehicle": False, "type": ""}, "surveyProvider": "설문사 B"},
    {"id": "패널047", "age": 36, "gender": "남성", "occupation": "개발자", "residence": "대구", "income": 5100, "reliability": 100, "vehicle": {"hasVehicle": True, "type": "SUV"}, "surveyProvider": "설문사 A"},
    {"id": "패널048", "age": 30, "gender": "여성", "occupation": "간호사", "residence": "인천", "income": 4200, "reliability": 87, "vehicle": {"hasVehicle": False, "type": ""}, "surveyProvider": "설문사 C"},
    {"id": "패널049", "age": 41, "gender": "남성", "occupation": "마케터", "residence": "전남", "income": 5300, "reliability": 89, "vehicle": {"hasVehicle": True, "type": "세단"}, "surveyProvider": "설문사 B"},
    {"id": "패널050", "age": 58, "gender": "여성", "occupation": "공무원", "residence": "서울", "income": 6400, "reliability": 93, "vehicle": {"hasVehicle": True, "type": "SUV"}, "surveyProvider": "설문사 A"},
]

# 추천 검색어 후보 리스트
candidate_keywords = [
    "서울 30대 남성",
    "고소득 40대",
    "마케팅 프리랜서",
    "디자이너 부산",
    "프리랜서 차량 보유",
    "학생 인천",
    "교사 광주",
    "SUV 보유",
    "20대 여성",
    "30대 개발자"
]

# 임베딩 함수
def get_embedding(texts):
    embeddings = embed_model.encode(texts, convert_to_numpy=True)
    return embeddings

# 추천어 줌?
@app.route('/api/related-searches', methods=['POST'])
def related_searches():
    data = request.get_json()
    query = data.get('query', '').strip()

    if not query:
        return jsonify([])

    query_emb = get_embedding([query])[0]
    keywords_emb = get_embedding(candidate_keywords)
    sims = cosine_similarity([query_emb], keywords_emb)[0]

    related = []
    for idx in sims.argsort()[::-1][:5]:
        related.append({
            "text": candidate_keywords[idx],
            "similarity": float(sims[idx])
        })

    return jsonify(related)

def filter_panels_by_query(query, panels):
    # 연령대 추출 (예: '20대' -> 20~29)
    age_min, age_max = None, None
    age_match = re.search(r"(\d{2})대", query)
    if age_match:
        age_start = int(age_match.group(1))
        age_min, age_max = age_start * 10, age_start * 10 + 9

    # 성별 추출
    gender = None
    if re.search(r"남[자성]", query):
        gender = "남성"
    elif re.search(r"여[자성]", query):
        gender = "여성"

    # 거주지 추출
    residences = ['서울', '부산', '제주', '인천', '대구', '광주']
    residence = next((city for city in residences if city in query), None)

    # 직업 추출
    occupations = ['개발자', '디자이너', '마케터', '프리랜서', '학생', '교사']
    occupation = next((job for job in occupations if job in query), None)

    def match(panel):
        if age_min is not None and not (age_min <= panel['age'] <= age_max):
            return False
        if gender is not None and panel['gender'] != gender:
            return False
        if residence is not None and panel['residence'] != residence:
            return False
        if occupation is not None and panel['occupation'] != occupation:
            return False
        return True

    return [p for p in panels if match(p)]

@app.route('/api/search', methods=['POST'])
def search():
    data = request.get_json()               # axios.post()로 보낸 json 데이터를 파싱
    query = data.get('query', '').strip()   # 딕셔너리처럼 접근 가능
    print(f"쿼리 수신: {query}")

    # 연관어 로직 함수 사용해야함 (지금은 임의로 연관어 목 데이터 사용)
    words = ["남성", "서울", "회사원", "고소득", "차량", "20대", "30대", "40대"] 

    filtered_panels = filter_panels_by_query(query, panels)  # 이거 뭔지 모르겠음
    return jsonify({
        "panels" : filtered_panels,  # 추출 패널 데이터
        "words": words,   # 추천 검색어 
    }) 



# 파이썬 파일을 직접 실행했을 때만 아래 코드가 작동하게 하는 조건문
# (다른 파일에서 import될 때는 실행되지 않게 함)
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)  # 개발모드, 기본포트 5000
