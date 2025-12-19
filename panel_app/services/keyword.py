# ============================================================
# 추천 검색어 기능
# ============================================================
from config import antropicLLM # 클로드 llm 가져오기
import json

def get_schema_info_from_db():
    """DB에서 테이블 스키마 JSON 정보를 조회합니다."""
    try:
        # category가 'panel_schema'인 데이터를 조회
        sql = text("SELECT content FROM schema_info WHERE category = 'panel_schema' LIMIT 1")
        result = db.session.execute(sql).fetchone()
        
        if result:
            return result[0]  # JSON 객체(dict) 반환
        return {}
    except Exception as e:
        print(f"DB 스키마 조회 오류: {e}")
        return {}

# 테이블 스키마 설명서
# with open("./tabel_schema_info.json", "r", encoding="utf-8") as f:
#     jsonFile = json.load(f)

# 추천어 생성 함수
def makeKeyword(user_query:str):

    # DB에서 최신 스키마 정보 가져오기
    jsonFile = get_schema_info_from_db()

    # 입력할 프롬프트
    content = f"""
        [역할]
        너는 자연어 쿼리를 SQL쿼리문으로 변환하는 과정에서 사용자가 입력한 자연어 쿼리를 
        보강하기 위해 문장에 추가되면 좋을 키워드를 생성하는 '추천 키워드 생성기'이다.
        
        제공되는 테이블 스키마 가이드 json파일의 컬럼과 응답을 참고하여 사용자가 입력한 
        자연어 쿼리가 적합한 SQL 쿼리문으로 변환될 수 있게 자연어 쿼리에 추가되면 
        좋을 것 같은 '추천 키워드'를 생성하라.

        [테이블 스키마 가이드 json]
        {jsonFile}
        
        [사용자 입력 자연어 쿼리]
        {user_query}

        [역할]
        - 사용자의 자연어 쿼리와 의미적으로 연관된 검색 키워드 또는 문장에 추가되면 
        SQL쿼리문 변환을 증강시킬 수 있는 키워드를 생성한다.
        - 키워드는 정확하게 6개만 생성한다.
        - 각 키워드는 1~2단어 정도의 짧은 명사구로 작성한다.
        - 문장은 작성하지 말고, 키워드만 출력한다.
        - (매우 중요) 아래 출력형식에 맞게 출력한다.

        [출력 형식]
        - 아래 예시처럼 **콤마( , )로만 구분된 한 줄**로 출력한다.
        - 불릿, 번호, 설명, 따옴표를 절대 넣지 않는다.

        [조건]
        1. 키워드는 무조건 10자를 초과하지 않는다.
        2. 사용자가 입력한 자연어 문장내의 단어와 동일한 키워드는 제외한다.

        예시: 키워드1, 키워드2, 키워드3, 키워드4, 키워드5, 키워드6
    """

    # llm을 활용하여 추천어 6개 생성

    # 클로드 불러와서 프롬프트 입력
    message = antropicLLM.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        messages=[
            {"role": "user", "content": content}
        ]
    )

    # LLM 응답 텍스트 추출
    raw_text = message.content[0].text.strip()

    # 리턴받은 문자열을 6개의 단어 배열로 파싱
    # 예: "여름철 더위, 땀 냄새, 데오드란트, 샤워, 탈취제, 땀 관리"
    keywords = [kw.strip() for kw in raw_text.split(",") if kw.strip()]

    # 혹시 6개보다 많거나 적게 오는 경우 방어 로직
    if len(keywords) > 6:
        keywords = keywords[:6]

    # JSON 형태로 변환
    json_keywords = [{"text": kw} for kw in keywords]

    return json_keywords



# 추천어 기반 자연어 쿼리 생성 함수
def makeNewQuery(user_query: str, keywords: list[str]) -> str:

    content = f"""
        [역할]
        너는 자연어 쿼리를 SQL쿼리문으로 변환하는 과정에서 자연어 쿼리가 SQL문으로 
        잘 변환 될 수 있게 자연어 쿼리를 증강 시켜주는 '문장 증강기'이다. 
        
        주어진 키워드와 자연어 쿼리를 조합하여, SQL쿼리문으로 변환하기에 더 쉬운 구조로 
        자연어 쿼리를 재생성한다.
      
        제공되는 테이블 스키마 가이드 json파일을 참고하여 사용자가 입력한 자연어 쿼리가 
        적합한 SQL 쿼리문으로 변환될 수 있게 자연어 쿼리를 재구성하라.
        결과를 출력할때 문장 생성 규칙에 맞게 출력한다.

        [테이블 스키마 가이드 json]
        {jsonFile}

        [입력 자연어 쿼리]
        {user_query}

        [추천 키워드 목록]
        {keywords}

        [문장 생성 규칙]
        - 사용자의 원래 자연어 쿼리의 의미를 유지하면서, 
        위의 추천 키워드들을 자연스럽게 섞어 하나의 문장으로 확장·보강한다.
        - 자연어 문장을 보강하기 위해 테이블 구조를 참고하여도 된다.
        - 사용자는 한국어 화자이므로, 결과도 자연스러운 한국어 문장으로 작성한다.
        - 문장은 어떠한 집단, 패널 혹은 사람으로 만들어야한다.
        - 문장과 같은 특징을 가진 집단, 패널을 찾는 것이 목표이기 때문에 문장 생성에 참고한다.
        
        [출력 형식]
        - 한 줄짜리 문장 **1개만** 출력한다.
        - 앞뒤에 따옴표("), 번호, 불릿(-, •) 등을 붙이지 않는다.
        - 불필요한 설명 문장은 절대 넣지 않는다.
    """

    # llm을 활용하여 추천어 + 기존쿼리 기반 새로운 문장 생성

    # 클로드 불러와서 프롬프트 입력
    message = antropicLLM.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        messages=[
            {"role": "user", "content": content}
        ]
    )

    # llm이 생성한 결과를 받아옴
    newQuery = message.content[0].text.strip()

    # 조합한 새로운 문장 리턴
    return newQuery