from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
from anthropic import Anthropic
import json
import logging
import traceback
import re

app = Flask(__name__)
CORS(app)

DB_CONFIG = {
    "host": "rds-postgresql-pgvector.cbq4y662mptt.ap-northeast-2.rds.amazonaws.com",
    "user": "root",
    "password": "rkdtmdwls123",
    "dbname": "postgres",
    "port": 5432
}

client = Anthropic(api_key="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")

# ============================================================
# 신뢰도 계산 관련 상수 및 유틸리티
# ============================================================

NULL_TOKENS = {"", ",", ";", "/", "-", "모름", "무응답", "모름/무응답", "해당없음", None}

INCOME_RANK = {
    "월 100만원 미만": 1,
    "월 100~199만원": 2,
    "월 200~299만원": 3,
    "월 300~399만원": 4,
    "월 400~499만원": 5,
    "월 500~599만원": 6,
    "월 600~699만원": 7,
    "월 700~799만원": 8,
    "월 800~899만원": 9,
    "월 900~999만원": 10,
    "월 1000만원 이상": 11,
    "모름/무응답": None,
    "": None    
}

RULE_MESSAGES = {
    "age_married_under18": "18세 미만인데 결혼 상태",
    "age_child_under18": "18세 미만인데 자녀 있음",
    "age_college_under18": "18세 미만인데 대학 재학/졸업 이상",
    "age_car_under18_hascar": "만 18세 미만 차량 보유",
    "age_car_under18_maker_filled": "만 18세 미만인데 자동차 제조사 기입",
    "age_car_under18_model_filled": "만 18세 미만인데 자동차 모델 기입",
    "old_student_80plus": "80세 이상인데 학생",
    "born_before_1990_secondary_student": "1990년 이전 출생인데 중/고등학생",
    "teen_smoker": "미성년 흡연 경험",
    "teen_drink": "미성년 음주 경험",
    "brand_without_smoke": "흡연경험 없음인데 담배브랜드 선택",
    "brand_etc_without_smoke": "흡연경험 없음인데 담배브랜드 기타내용 입력",
    "heat_e_cig_without_smoke": "흡연경험 없음인데 가열식/전자담배 선택",
    "alcohol_memo_without_drink": "음용경험 없음인데 술 기타내용 입력",
    "lowedu_projob": "고졸 이하인데 전문직",
    "personal_gt_household": "월 개인소득 > 월 가구소득",
    "phone_brand_model_mismatch": "휴대폰 브랜드/모델 불일치",
    "old_student_flag": "50세 이상인데 대학생/대학원생",
    "car_brand_but_no_model": "차량 브랜드 있음 + 차종 없음",
    "car_model_but_no_brand": "차량 브랜드 없음 + 차종 있음",
    "car_have_N_but_brand_or_model": "차량 보유 '없다'인데 브랜드/모델 기재",
    "car_brand_model_mismatch_heuristic": "차량 브랜드/모델 불일치(휴리스틱)",
}

# 차량 브랜드 정규화 매핑
_CAR_BRAND_ALIASES = {
    "현대": {"현대", "현대자동차", "hyundai"},
    "기아": {"기아", "기아자동차", "kia"},
    "제네시스": {"제네시스", "genesis"},
    "르노코리아": {"르노코리아", "르노삼성", "르노", "renault", "renault samsung", "renault korea"},
    "쉐보레": {"쉐보레", "chevrolet", "gm", "gm대우", "대우"},
    "쌍용": {"쌍용", "쌍용자동차", "kg모빌리티", "kg mobility", "ssangyong"},
    "메르세데스-벤츠": {"메르세데스-벤츠", "벤츠", "mercedes", "mercedes-benz", "m-benz"},
    "BMW": {"bmw", "비엠더블유", "비엠"},
    "아우디": {"audi", "아우디"},
    "폭스바겐": {"vw", "volkswagen", "폭스바겐"},
    "토요타": {"toyota", "토요타"},
    "혼다": {"honda", "혼다"},
    "닛산": {"nissan", "닛산"},
    "렉서스": {"lexus", "렉서스"},
    "포르쉐": {"porsche", "포르쉐"},
}

# 차량 모델 패턴
_CAR_BRAND_MODEL_PATTERNS = {
    "현대": [
        r"\b(아반떼|쏘나타|그랜저|투싼|싼타페|팰리세이드|베뉴|캐스퍼|코나|아이오닉|넥쏘)\b",
        r"\b(IONIQ|NEXO|SANTA\s?FE|PALISADE|TUCSON|KONA|AVANTE|SONATA|GRANDEUR)\b",
    ],
    "기아": [
        r"\b(K[3-9]|K\d{1,2}|쏘렌토|스포티지|모닝|레이|니로|카니발|EV\d+|EV[36])\b",
        r"\b(SORENTO|SPORTAGE|CARNIVAL|MORNING|RAY|NIRO)\b",
    ],
    "제네시스": [r"\b(GV?\d{2}|G70|G80|G90|GV60|GV70|GV80|GV90)\b"],
    "르노코리아": [r"\b(QM\d|XM3|SM\d|SM6)\b"],
    "쉐보레": [
        r"\b(스파크|말리부|임팔라|트랙스|이쿼녹스|트래버스|콜로라도|타호|카마로|트레일블레이저)\b",
        r"\b(SPARK|MALIBU|IMPALA|TRAX|EQUINOX|TRAVERSE|COLORADO|TAHOE|CAMARO|TRAILBLAZER)\b",
    ],
    "쌍용": [r"\b(렉스턴|코란도|티볼리|토레스)\b", r"\b(REXTON|KORANDO|TIVOLI|TORRES)\b"],
    "메르세데스-벤츠": [r"\b([CES]\s?-?\d{2,3}|GL[ABC]?|GLE|GLS|S\s?CLASS|E\s?CLASS|C\s?CLASS|AMG|EQ[BS]?\d*)\b"],
    "BMW": [r"\b([1-8]\s?시리즈|[1-8]\s?Series|X[1-7]|M\d{1,3}|i\d|i3|i4|i5|i7|320|520)\b"],
    "아우디": [r"\b(A[1-8]|Q[2-8]|e-?tron)\b"],
    "폭스바겐": [r"\b(티구안|골프|아테온|파사트|TIGUAN|GOLF|ARTEON|PASSAT)\b"],
    "토요타": [r"\b(캠리|코롤라|라브4|프리우스|GR86|CAMRY|COROLLA|RAV4|PRIUS)\b"],
    "혼다": [r"\b(어코드|시빅|CR-?V|HR-?V|ACCORD|CIVIC|CRV|HRV)\b"],
    "닛산": [r"\b(알티마|로그|캐시카이|노트|리프|ALTIMA|ROGUE|QASHQAI|LEAF)\b"],
    "렉서스": [r"\b(ES\d*|RX\d*|NX\d*|UX\d*|IS\d*|LS\d*)\b"],
    "포르쉐": [r"\b(911|카이엔|마칸|파나메라|타이칸|CAYENNE|MACAN|PANAMERA|TAYCAN)\b"],
}

def norm_str(x):
    """문자열 정규화 (int/float 타입도 처리)"""
    if x is None:
        return ""
    # int나 float인 경우 문자열로 변환
    if isinstance(x, (int, float)):
        return str(x)
    # 문자열인 경우 strip
    s = str(x).strip()
    if all(ch in {',',';','/','-','·','.'} for ch in s):
        return ""
    return s

def is_meaningful_text(x):
    """의미있는 텍스트인지 확인"""
    s = norm_str(x)
    return s not in NULL_TOKENS and s != ""

def norm_list(xs):
    """리스트 정규화"""
    if xs is None: return []
    if isinstance(xs, str): xs = [xs]
    out = []
    for x in xs:
        s = norm_str(x)
        if is_meaningful_text(s):
            out.append(s)
    return out

def _norm_text_none(x):
    """None-safe 텍스트 정규화"""
    s = str(x).strip() if x is not None else ""
    return s if s else None

def _canonical_car_brand(brand_text):
    """차량 브랜드 정규화"""
    b = _norm_text_none(brand_text)
    if b is None:
        return None
    bl = b.lower()
    for canon, aliases in _CAR_BRAND_ALIASES.items():
        for a in aliases:
            if bl == a.lower():
                return canon
    return b

def _guess_brands_from_model(model_text):
    """모델명에서 브랜드 추정"""
    m = _norm_text_none(model_text)
    if m is None:
        return set()
    u = m.upper()
    hits = set()
    for brand, pats in _CAR_BRAND_MODEL_PATTERNS.items():
        if any(re.search(p, u, flags=re.IGNORECASE) for p in pats):
            hits.add(brand)
    return hits

def _car_model_matches_brand(brand_text, model_text):
    """차량 브랜드-모델 일치 확인"""
    brand = _canonical_car_brand(brand_text)
    model = _norm_text_none(model_text)
    if not brand or not model:
        return None
    cand = _guess_brands_from_model(model)
    if len(cand) == 0:
        return None
    if len(cand) == 1:
        return (brand == next(iter(cand)))
    return None

def _brand_group_from_text(brand):
    """휴대폰 브랜드 그룹 판별"""
    b = brand
    if not b: return None
    if "애플" in b or "Apple" in b or "아이폰" in b: return "apple"
    if "삼성" in b or "갤럭시" in b or "Galaxy" in b or "노트" in b: return "samsung"
    if "LG" in b: return "lg"
    if "샤오미" in b or "Xiaomi" in b or "포코" in b or "홍미" in b or "레드미" in b: return "xiaomi"
    if "기타" in b: return "etc"
    return None

def _model_group_from_text(model):
    """휴대폰 모델 그룹 판별"""
    m = model
    if not m: return None
    if "폴더폰" in m or "보유 X" in m or ("기타" in m and "아이폰" not in m and "갤럭시" not in m and "LG" not in m and "샤오미" not in m):
        return "special"
    if "아이폰" in m: return "apple"
    if "갤럭시" in m or "Galaxy" in m or "노트" in m or "Z Fold" in m or "Z Flip" in m: return "samsung"
    if "LG" in m or "V 시리즈" in m or "G 시리즈" in m: return "lg"
    if "샤오미" in m or "포코" in m or "홍미" in m or "레드미" in m: return "xiaomi"
    return None

def _any_smoke_selected(smoke_set):
    """흡연 경험 체크"""
    items = norm_list(smoke_set)
    NEG_SMOKE = "담배를 피워본 적이 없다"
    for s in items:
        if NEG_SMOKE in s:
            continue
        return True
    return False

def _is_under(n, limit):
    """나이 미만 체크"""
    return (n is not None) and (n < limit)

def _is_overeq(n, limit):
    """나이 이상 체크"""
    return (n is not None) and (n >= limit)

def _get(row, key, default=None):
    """안전한 딕셔너리 접근"""
    return row.get(key, default)

# ============================================================
# 패널 데이터 전처리
# ============================================================

def preprocess_panel(row):
    """패널 데이터 전처리 및 메타데이터 생성"""
    r = dict(row)

    # 나이 계산
    birth = _get(r, "출생년도")
    try:
        if isinstance(birth, int):
            birth_int = birth
        else:
            birth_int = int(str(birth).strip())
        r["age"] = 2025 - birth_int
    except Exception:
        r["age"] = None

    # 가족수 정규화
    fam_text = norm_str(_get(r, "가족수"))
    fam_map = {"1명(혼자 거주)": 1, "2명": 2, "3명": 3, "4명": 4, "5명 이상": 5}
    r["_가족수_수치"] = fam_map.get(fam_text, None)
    
    # 자녀수 안전 처리 (이미 int일 수 있음)
    children = _get(r, "자녀수")
    if isinstance(children, int):
        r["_자녀수"] = children
    elif children:
        try:
            r["_자녀수"] = int(norm_str(children))
        except:
            r["_자녀수"] = 0
    else:
        r["_자녀수"] = 0

    # 기본 정보 정규화
    r["_학력"] = norm_str(_get(r, "최종학력"))
    r["_직업"] = norm_str(_get(r, "직업"))
    r["_결혼"] = norm_str(_get(r, "결혼여부"))

    # 소득 랭크
    r["_개인소득_랭크"] = INCOME_RANK.get(norm_str(_get(r, "월평균_개인소득")), None)
    r["_가구소득_랭크"] = INCOME_RANK.get(norm_str(_get(r, "월평균_가구소득")), None)

    # 멀티선택 정규화
    r["_흡연_set"] = norm_list(_get(r, "흡연경험", []))
    r["_담배브랜드_set"] = norm_list(_get(r, "흡연경험_담배브랜드", []))
    r["_가열식_set"] = norm_list(_get(r, "전자담배_이용경험", []))
    r["_주류_set"] = norm_list(_get(r, "음용경험_술", []))

    # ETC 플래그
    r["_담배브랜드_ETC"] = is_meaningful_text(_get(r, "흡연경험_담배브랜드_기타"))
    r["_가열식_ETC"] = is_meaningful_text(_get(r, "흡연경험_담배_기타내용"))
    r["_술_ETC"] = is_meaningful_text(_get(r, "음용경험_술_기타내용"))

    # 휴대폰 정보
    r["_폰브랜드"] = norm_str(_get(r, "휴대폰_브랜드"))
    r["_폰모델"] = norm_str(_get(r, "휴대폰_모델"))

    # 차량 정보
    r["_차량보유"] = norm_str(_get(r, "차량여부"))
    r["_제조사"] = _norm_text_none(_get(r, "자동차_제조사"))
    r["_차모델"] = _norm_text_none(_get(r, "자동차_모델"))

    return r

# ============================================================
# 신뢰도 규칙 정의
# ============================================================

def get_reliability_rules():
    """신뢰도 검증 규칙 리스트 반환"""
    return [
        # 연령 기반
        ("age_married_under18",
         lambda r: _is_under(r.get("age"), 18) and (r["_결혼"] in ["기혼", "기타(사별/이혼 등)"])),
        
        ("age_child_under18",
         lambda r: _is_under(r.get("age"), 18) and (r.get("_자녀수", 0) > 0)),
        
        ("age_college_under18",
         lambda r: _is_under(r.get("age"), 18) and (r["_학력"] in ["대학교 재학(휴학 포함)", "대학교 졸업", "대학원 재학/졸업 이상"])),

        ("old_student_80plus",
         lambda r: _is_overeq(r.get("age"), 80) and (r["_직업"] in ["중/고등학생", "대학생/대학원생"])),

        # 차량 (만 18세 미만으로 변경)
        ("age_car_under18_hascar",
         lambda r: _is_under(r.get("age"), 18) and (r["_차량보유"] == "있다")),
        
        ("age_car_under18_maker_filled",
         lambda r: _is_under(r.get("age"), 18) and bool(r["_제조사"])),
        
        ("age_car_under18_model_filled",
         lambda r: _is_under(r.get("age"), 18) and bool(r["_차모델"])),

        # 흡연/음주 (미성년)
        ("teen_smoker",
         lambda r: _is_under(r.get("age"), 19) and _any_smoke_selected(r["_흡연_set"])),
        
        ("teen_drink",
         lambda r: _is_under(r.get("age"), 19) and any(a for a in r["_주류_set"] if "최근 1년 이내 술을 마시지 않음" not in a)),

        # 흡연경험 없음인데 관련 기입
        ("brand_without_smoke",
         lambda r: (len(r["_흡연_set"]) == 0 or not _any_smoke_selected(r["_흡연_set"])) and len(r["_담배브랜드_set"]) > 0),
        
        ("brand_etc_without_smoke",
         lambda r: (len(r["_흡연_set"]) == 0 or not _any_smoke_selected(r["_흡연_set"])) and r["_담배브랜드_ETC"]),
        
        ("heat_e_cig_without_smoke",
         lambda r: (len(r["_흡연_set"]) == 0 or not _any_smoke_selected(r["_흡연_set"])) and (len(r["_가열식_set"]) > 0 or r["_가열식_ETC"])),
        
        ("alcohol_memo_without_drink",
         lambda r: (len(r["_주류_set"]) == 0 or all("최근 1년 이내 술을 마시지 않음" in a for a in r["_주류_set"])) and r["_술_ETC"]),

        # 학력/직업
        ("lowedu_projob",
         lambda r: (r["_학력"] in ["고등학교 졸업 이하"]) and (r["_직업"] == "전문직 (의사, 간호사, 변호사, 회계사, 예술가, 종교인, 엔지니어, 프로그래머, 기술사 등)")),

        # 소득(서열)
        ("personal_gt_household",
         lambda r: (r["_개인소득_랭크"] is not None and r["_가구소득_랭크"] is not None) and
                   (r["_개인소득_랭크"] > r["_가구소득_랭크"])),

        # 휴대폰 브랜드/모델 불일치
        ("phone_brand_model_mismatch",
         lambda r: (lambda bg, mg: (
             False if (bg is None or mg is None or mg == "special")
             else ((bg == "apple"   and mg != "apple") or
                   (bg == "samsung" and mg != "samsung") or
                   (bg == "lg"      and mg != "lg") or
                   (bg == "xiaomi"  and mg != "xiaomi"))
         ))(_brand_group_from_text(r["_폰브랜드"]),
            _model_group_from_text(r["_폰모델"]))),

        # 선택 규칙
        ("old_student_flag",
         lambda r: (r.get("age") is not None) and (r["age"] >= 50) and (r["_직업"] == "대학생/대학원생")),

        ("born_before_1990_secondary_student",
         lambda r: (
             (_get(r, "출생년도") and (
                 (isinstance(_get(r, "출생년도"), int) and _get(r, "출생년도") < 1990) or
                 (isinstance(_get(r, "출생년도"), str) and int(_get(r, "출생년도")) < 1990)
             )) and (r["_직업"] == "중/고등학생")
         )),

        # 차량 불일치/누락 규칙 (car_have_Y_but_missing_brand_or_model 삭제)
        ("car_brand_but_no_model",
         lambda r: bool(_norm_text_none(r.get("_제조사"))) and not _norm_text_none(r.get("_차모델"))),

        ("car_model_but_no_brand",
         lambda r: not _norm_text_none(r.get("_제조사")) and bool(_norm_text_none(r.get("_차모델")))),

        ("car_have_N_but_brand_or_model",
         lambda r: (r.get("_차량보유") == "없다") and (bool(_norm_text_none(r.get("_제조사"))) or bool(_norm_text_none(r.get("_차모델"))))),

        ("car_brand_model_mismatch_heuristic",
         lambda r: (lambda ok: (ok is False))(_car_model_matches_brand(r.get("_제조사"), r.get("_차모델")))),
    ]

def calculate_reliability_score(row):
    """
    신뢰도 점수 계산 및 위반 규칙 반환
    Returns: (score, hit_rules, hit_messages)
    """
    rr = preprocess_panel(row)
    rules = get_reliability_rules()
    
    detail = {name: bool(fn(rr)) for name, fn in rules}
    hit_rules = [k for k, v in detail.items() if v]
    hit_messages = [RULE_MESSAGES.get(k, k) for k in hit_rules]
    
    # 신뢰도 점수: 100점에서 위반 규칙당 5점씩 감점
    score = max(0, 100 - 5 * len(hit_rules))
    
    return score, hit_rules, hit_messages

# ============================================================
# 패널 텍스트화
# ============================================================

def panel_to_text(r):
    """패널 데이터를 자연어 텍스트로 변환"""
    parts = []
    
    # 1) 성별 + 연령
    gender = r.get("성별")
    if gender:
        parts.append(f"{gender}이다.")
    
    birth = r.get("출생년도")
    age = r.get("age")
    if age:
        parts.append(f"{birth}년생으로 {age}세이다.")
    elif birth:
        parts.append(f"{birth}년생이다.")
    
    # 2) 거주지역
    region1 = r.get("지역")
    region2 = r.get("지역구")
    if region1 and region2:
        parts.append(f"{region1} {region2} 거주자이다.")
    elif region1:
        parts.append(f"{region1} 거주자이다.")
    
    # 3) 개인소득 / 가구소득
    personal = r.get("월평균_개인소득")
    household = r.get("월평균_가구소득")
    if personal:
        parts.append(f"월 개인소득은 {personal} 수준이다.")
    if household:
        parts.append(f"월 가구소득은 {household} 수준이다.")
    
    # 4) 직업 / 학력
    job = r.get("직업")
    edu = r.get("최종학력")
    if job:
        parts.append(f"직업은 {job}이다.")
    if edu:
        parts.append(f"최종학력은 {edu}이다.")
    
    # 5) 차량 / 휴대폰
    car = r.get("차량여부")
    if car:
        parts.append(f"차량 보유 여부는 {car}이다.")
    
    phone_brand = r.get("휴대폰_브랜드")
    phone_model = r.get("휴대폰_모델")
    if phone_brand and phone_model:
        parts.append(f"{phone_brand}의 {phone_model}을 사용하고 있다.")
    elif phone_brand:
        parts.append(f"{phone_brand} 스마트폰을 사용하고 있다.")
    
    # 6) 흡연 / 음주
    smokes = r.get("흡연경험") or []
    if smokes:
        smoke_str = ", ".join(smokes) if isinstance(smokes, list) else str(smokes)
        parts.append(f"흡연경험으로는 {smoke_str} 경험이 있다.")
    
    drinks = r.get("음용경험_술") or []
    if drinks:
        drink_str = ", ".join(drinks) if isinstance(drinks, list) else str(drinks)
        parts.append(f"음주 경험으로는 {drink_str} 경험이 있다.")
    
    return " ".join(parts)

# ============================================================
# SQL 생성 프롬프트
# ============================================================

def create_sql_generation_prompt(user_query: str) -> str:
    return f"""당신은 PostgreSQL SQL 쿼리 생성 전문가입니다.

테이블 이름: welcome_cb_scored

테이블 스키마 (정확한 컬럼명):
- 패널id (VARCHAR, PRIMARY KEY) ⚠️ 소문자 'id' 주의!
- 성별 (VARCHAR) - 예: '남성', '여성'
- 출생년도 (VARCHAR) ⚠️ 문자열이므로 숫자 비교시 반드시 ::INTEGER 캐스팅 필요!
- 지역 (VARCHAR) - 예: '서울', '부산', '경기', '인천' 등
- 지역구 (VARCHAR)
- 결혼여부 (VARCHAR) - 예: '기혼', '미혼'
- 자녀수 (INTEGER) - 이미 숫자형이므로 캐스팅 불필요
- 가족수 (VARCHAR) - 숫자 비교시 ::INTEGER 캐스팅 필요
- 최종학력 (VARCHAR)
- 직업 (VARCHAR)
- 직무 (VARCHAR)
- 월평균_개인소득 (VARCHAR)
- 월평균_가구소득 (VARCHAR)
- 보유전제품 (JSONB)
- 휴대폰_브랜드 (VARCHAR)
- 휴대폰_모델 (VARCHAR)
- 차량여부 (VARCHAR) - 예: '있음', '없음'
- 자동차_제조사 (VARCHAR)
- 자동차_모델 (VARCHAR)
- 흡연경험 (JSONB)
- 흡연경험_담배브랜드 (JSONB)
- 흡연경험_담배브랜드_기타 (VARCHAR)
- 전자담배_이용경험 (JSONB)
- 흡연경험_담배_기타내용 (VARCHAR)
- 음용경험_술 (JSONB)
- 음용경험_술_기타내용 (VARCHAR)

사용자 요청: "{user_query}"

쿼리 생성 규칙 (매우 중요!):
1. 기본 형식: SELECT * FROM welcome_cb_scored
2. 컬럼명 정확히 사용: 패널id (대문자 ID 아님!)
3. 출생년도로 나이 계산 시 반드시 ::INTEGER 캐스팅:
   ✅ 올바른 예: 출생년도::INTEGER BETWEEN 1985 AND 1994
   ❌ 틀린 예: 출생년도 BETWEEN 1985 AND 1994
4. 나이대별 출생년도 (2025년 기준):
   - 10대: 출생년도::INTEGER BETWEEN 2006 AND 2015
   - 20대: 출생년도::INTEGER BETWEEN 1995 AND 2005
   - 30대: 출생년도::INTEGER BETWEEN 1985 AND 1994
   - 40대: 출생년도::INTEGER BETWEEN 1975 AND 1984
   - 50대: 출생년도::INTEGER BETWEEN 1965 AND 1974
   - 60대: 출생년도::INTEGER BETWEEN 1955 AND 1964
5. 자녀수는 이미 INTEGER이므로: 자녀수 >= 2 (캐스팅 불필요)
6. 가족수는 VARCHAR이므로: 가족수::INTEGER >= 4 (캐스팅 필요)
7. JSONB 존재 확인: 흡연경험 IS NOT NULL
8. 텍스트 검색: 휴대폰_브랜드 LIKE '%삼성%'
9. 차량 소유: 차량여부 = '있음'
10. 순수 SQL만 반환 (설명, 코드블록 없이)
11. LIMIT 처리 규칙:
    - 사용자가 인원수를 명시한 경우: LIMIT [인원수]를 반드시 추가
    - 인원수 표현: "10명", "50명", "100명", "10개", "50개", "100개 패널" 등
    - 사용자가 인원수를 명시하지 않은 경우: LIMIT 없이 전체 결과 반환
12. NULL 값 처리:
    - NULL 값은 백엔드에서 '무응답'으로 자동 변환됨
    - WHERE 조건에서 NULL 체크: 컬럼명 IS NOT NULL

좋은 예시:
- "서울 30대 남성 자녀 2명 이상"
  → SELECT * FROM welcome_cb_scored WHERE 지역 = '서울' AND 성별 = '남성' AND 출생년도::INTEGER BETWEEN 1985 AND 1994 AND 자녀수 >= 2

- "서울 30대 남성 50명"
  → SELECT * FROM welcome_cb_scored WHERE 지역 = '서울' AND 성별 = '남성' AND 출생년도::INTEGER BETWEEN 1985 AND 1994 LIMIT 50

나쁜 예시 (절대 이렇게 하지 마세요):
- 출생년도 BETWEEN... (❌ 캐스팅 없음)
- 패널ID (❌ 대문자 ID)
- 인원수가 명시되었는데 LIMIT 없음 (❌)

지금 SQL 쿼리를 생성하세요 (순수 SQL만):"""

# ============================================================
# API 엔드포인트
# ============================================================

@app.route('/api/search', methods=['POST'])
def search():
    try:
        data = request.get_json()
        query = data.get('query', '').strip()

        if not query:
            return jsonify({"error": "쿼리를 입력해주세요."}), 400

        logging.info(f"🔍 검색 쿼리: {query}")

        # Claude API로 SQL 쿼리 생성
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1024,
            messages=[
                {"role": "user", "content": create_sql_generation_prompt(query)}
            ]
        )
        
        sql_query = message.content[0].text.strip()
        
        # SQL 쿼리 정제
        if sql_query.startswith("```sql"):
            sql_query = sql_query[6:]
        if sql_query.startswith("```"):
            sql_query = sql_query[3:]
        if sql_query.endswith("```"):
            sql_query = sql_query[:-3]
        sql_query = sql_query.strip()
        
        logging.info(f"📝 생성된 SQL: {sql_query}")
        
        # DB 조회 실행
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql_query)
        results = cur.fetchall()
        cur.close()
        conn.close()
        
        if not results:
            logging.info("❌ 검색 결과 없음")
            return jsonify({
                "panels": [],
                "words": []
            })
        
        logging.info(f"✅ DB 조회 완료: {len(results)}개 패널")
        
        # 결과 변환 및 신뢰도 계산
        panels = []
        for idx, row in enumerate(results, start=1):  # 1부터 시작하는 인덱스
            panel_dict = dict(row)
            
            # 신뢰도 계산 (새로운 로직 사용)
            score, hit_rules, hit_messages = calculate_reliability_score(panel_dict)
            
            # 나이 계산
            birth_year = panel_dict.get('출생년도')
            age = None
            if birth_year:
                try:
                    age = 2025 - int(birth_year)
                except:
                    age = None
            
            # NULL 값을 '무응답'으로 변환하는 헬퍼 함수
            def convert_null(value, default='무응답'):
                if value is None or value == '' or value == '-' or value == 'null':
                    return default
                return value
            
            # 프론트엔드 형식으로 변환
            panel = {
                "id": f"패널{idx}",  # 패널1, 패널2, 패널3...
                "reliability": score,
                "reliabilityReasons": hit_messages,
                "age": age,
                "gender": convert_null(panel_dict.get('성별')),
                "occupation": convert_null(panel_dict.get('직업')),
                "residence": convert_null(panel_dict.get('지역')),
                "district": convert_null(panel_dict.get('지역구')),
                "maritalStatus": convert_null(panel_dict.get('결혼여부')),
                "education": convert_null(panel_dict.get('최종학력')),
                "job": convert_null(panel_dict.get('직업')),
                "role": convert_null(panel_dict.get('직무')),
                "personalIncome": convert_null(panel_dict.get('월평균_개인소득')),
                "householdIncome": convert_null(panel_dict.get('월평균_가구소득')),
                "children": panel_dict.get('자녀수') if panel_dict.get('자녀수') is not None else 0,
                "familySize": convert_null(panel_dict.get('가족수')),
                "phoneModel": convert_null(panel_dict.get('휴대폰_모델')),
                "phoneBrand": convert_null(panel_dict.get('휴대폰_브랜드')),
                "carOwnership": convert_null(panel_dict.get('차량여부'), '없음'),
                "carBrand": convert_null(panel_dict.get('자동차_제조사')),
                "carModel": convert_null(panel_dict.get('자동차_모델')),
                "smokingExperience": panel_dict.get('흡연경험') or [],
                "drinkingExperience": panel_dict.get('음용경험_술') or [],
                "ownedProducts": panel_dict.get('보유전제품') or [],
                "birthYear": birth_year,
                "_text_description": panel_to_text(panel_dict),  # 텍스트화된 설명
            }
            panels.append(panel)
        
        # 신뢰도 높은 순으로 정렬
        panels.sort(key=lambda x: x['reliability'], reverse=True)
        
        # 검색어에서 키워드 추출
        words = []
        keywords = query.split()
        for keyword in keywords:
            if len(keyword) > 1:
                words.append({"text": keyword, "value": 10})
        
        logging.info(f"🎉 검색 완료: {len(panels)}개 패널 (평균 신뢰도: {sum(p['reliability'] for p in panels) / len(panels):.1f}%)")
        
        return jsonify({
            "panels": panels,
            "words": words
        })
        
    except Exception as e:
        logging.error(f"💥 검색 오류: {str(e)}")
        traceback.print_exc()
        return jsonify({
            "error": "검색 중 오류가 발생했습니다.",
            "detail": str(e)
        }), 500

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    app.run(host='0.0.0.0', port=5000, debug=True)