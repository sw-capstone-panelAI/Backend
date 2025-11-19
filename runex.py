from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
from anthropic import Anthropic
import json
import logging
import traceback
import re
from sentence_transformers import SentenceTransformer, util
import numpy as np
import torch



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
# 임베딩 모델 로드
# ============================================================
kure_model = SentenceTransformer('nlpai-lab/KURE-v1')

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
    "required_birth_year_missing": "필수정보 누락 : 나이",
    "required_occupation_missing": "필수정보 누락 : 직업",
    "required_income_missing": "필수정보 누락 : 개인소득",
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

# 생활패턴 칼럼 리스트
LIFESTYLE_COLUMNS = [
    "체력_관리를_위한_활동",
    "이용_중인_OTT_서비스",
    "전통시장_방문_빈도",
    "선호하는_설_선물_유형",
    "초등학생_시절_겨울방학_때_기억에_남는_일",
    "반려동물을_키우거나_키웠던_경험",
    "이사할_때_스트레스_받는_부분",
    "본인을_위해_소비하는_것_중_기분_좋아지는_소비",
    "요즘_많이_사용하는_앱",
    "스트레스를_많이_느끼는_상황",
    "스트레스를_해소하는_방법",
    "본인_피부_상태에_대한_만족도",
    "한_달_기준으로_스킨케어_제품에_소비하는_정도",
    "스킨케어_제품을_구매할_때_중요하게_고려하는_요소",
    "사용해_본_AI_챗봇_서비스",
    "사용해_본_AI_챗봇_서비스_중_주로_사용하는_것",
    "AI_챗봇_서비스를_활용한_용도나_앞으로의_활용_여부",
    "두_서비스_중_더_호감이_가는_서비스",
    "해외여행을_간다면_가고싶은_곳",
    "빠른_배송(당일·새벽·직진_배송)_서비스를_어떤_제품을_구매할_때_이용하는지",
    "여름철_가장_걱정되는_점",
    "버리기_아까운_물건이_있을_때_어떻게_하는지",
    "아침에_기상하기_위해_알람을_설정해두는_방식",
    "외부_식당에서_혼자_식사하는_빈도",
    "가장_중요하다고_생각하는_행복한_노년의_조건",
    "여름철_땀_때문에_겪는_불편함",
    "가장_효과_있었던_다이어트_방법",
    "야식을_먹는_방법",
    "여름철_최애_간식",
    "최근_지출을_많이_한_곳",
    "AI_서비스를_활용하는_분야",
    "본인이_미니멀리스트와_맥시멀리스트_중_어느_쪽에_가까운지",
    "여행_갈_때의_스타일",
    "일회용_비닐봉투_사용을_줄이기_위한_노력",
    "할인,_캐시백,_멤버십_등_포인트_적립_혜택을_신경_쓰는_정도",
    "초콜릿을_먹는_때",
    "개인정보_보호를_위한_습관",
    "절대_포기할_수_없는_여름_패션_필수템",
    "갑작스런_비가_오는데_우산이_없는_경우_취하는_행동",
    "휴대폰_갤러리에_가장_많이_저장되어_있는_사진",
    "여름철_물놀이_장소로_선호하는_곳"
]

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
    if isinstance(x, (int, float)):
        return str(x)
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

    # 나이 계산 (일반 나이)
    birth = _get(r, "출생년도")
    try:
        if isinstance(birth, int):
            birth_int = birth
        else:
            birth_int = int(str(birth).strip())
        r["age"] = 2025 - birth_int - 1  # 만 나이
    except Exception:
        r["age"] = None

    # 가족수 정규화
    fam_text = norm_str(_get(r, "가족수"))
    fam_map = {"1명(혼자 거주)": 1, "2명": 2, "3명": 3, "4명": 4, "5명 이상": 5}
    r["_가족수_수치"] = fam_map.get(fam_text, None)
    
    # 자녀수 안전 처리
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
        ("required_birth_year_missing",
         lambda r: not r.get("출생년도") or r.get("출생년도") in ["", "-", None, "무응답"]),
        
        ("required_occupation_missing",
         lambda r: not r.get("직업") or r.get("직업") in ["", "-", None, "무응답"]),
        
        ("required_income_missing",
         lambda r: not r.get("월평균_개인소득") or r.get("월평균_개인소득") in ["", "-", None, "무응답"]),
        
        ("age_married_under18",
         lambda r: _is_under(r.get("age"), 18) and (r["_결혼"] in ["기혼", "기타(사별/이혼 등)"])),
        
        ("age_child_under18",
         lambda r: _is_under(r.get("age"), 18) and (r.get("_자녀수", 0) > 0)),
        
        ("age_college_under18",
         lambda r: _is_under(r.get("age"), 18) and (r["_학력"] in ["대학교 재학(휴학 포함)", "대학교 졸업", "대학원 재학/졸업 이상"])),

        ("old_student_80plus",
         lambda r: _is_overeq(r.get("age"), 80) and (r["_직업"] in ["중/고등학생", "대학생/대학원생"])),

        ("age_car_under18_hascar",
         lambda r: _is_under(r.get("age"), 18) and (r["_차량보유"] == "있다")),
        
        ("age_car_under18_maker_filled",
         lambda r: _is_under(r.get("age"), 18) and bool(r["_제조사"])),
        
        ("age_car_under18_model_filled",
         lambda r: _is_under(r.get("age"), 18) and bool(r["_차모델"])),

        ("teen_smoker",
         lambda r: _is_under(r.get("age"), 19) and _any_smoke_selected(r["_흡연_set"])),
        
        ("teen_drink",
         lambda r: _is_under(r.get("age"), 19) and any(a for a in r["_주류_set"] if "최근 1년 이내 술을 마시지 않음" not in a)),

        ("brand_without_smoke",
         lambda r: (len(r["_흡연_set"]) == 0 or not _any_smoke_selected(r["_흡연_set"])) and len(r["_담배브랜드_set"]) > 0),
        
        ("brand_etc_without_smoke",
         lambda r: (len(r["_흡연_set"]) == 0 or not _any_smoke_selected(r["_흡연_set"])) and r["_담배브랜드_ETC"]),
        
        ("heat_e_cig_without_smoke",
         lambda r: (len(r["_흡연_set"]) == 0 or not _any_smoke_selected(r["_흡연_set"])) and (len(r["_가열식_set"]) > 0 or r["_가열식_ETC"])),
        
        ("alcohol_memo_without_drink",
         lambda r: (len(r["_주류_set"]) == 0 or all("최근 1년 이내 술을 마시지 않음" in a for a in r["_주류_set"])) and r["_술_ETC"]),

        ("lowedu_projob",
         lambda r: (r["_학력"] in ["고등학교 졸업 이하"]) and (r["_직업"] == "전문직 (의사, 간호사, 변호사, 회계사, 예술가, 종교인, 엔지니어, 프로그래머, 기술사 등)")),

        ("personal_gt_household",
         lambda r: (r["_개인소득_랭크"] is not None and r["_가구소득_랭크"] is not None) and
                   (r["_개인소득_랭크"] > r["_가구소득_랭크"])),

        ("phone_brand_model_mismatch",
         lambda r: (lambda bg, mg: (
             False if (bg is None or mg is None or mg == "special")
             else ((bg == "apple"   and mg != "apple") or
                   (bg == "samsung" and mg != "samsung") or
                   (bg == "lg"      and mg != "lg") or
                   (bg == "xiaomi"  and mg != "xiaomi"))
         ))(_brand_group_from_text(r["_폰브랜드"]),
            _model_group_from_text(r["_폰모델"]))),

        ("old_student_flag",
         lambda r: (r.get("age") is not None) and (r["age"] >= 50) and (r["_직업"] == "대학생/대학원생")),

        ("born_before_1990_secondary_student",
         lambda r: (
             (_get(r, "출생년도") and (
                 (isinstance(_get(r, "출생년도"), int) and _get(r, "출생년도") < 1990) or
                 (isinstance(_get(r, "출생년도"), str) and int(_get(r, "출생년도")) < 1990)
             )) and (r["_직업"] == "중/고등학생")
         )),

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
    rr = preprocess_panel(row)
    rules = get_reliability_rules()
    detail = {name: bool(fn(rr)) for name, fn in rules}
    hit_rules = [k for k, v in detail.items() if v]
    hit_messages = [RULE_MESSAGES.get(k, k) for k in hit_rules]

    required_missing_count = sum(1 for rule in ["required_birth_year_missing", "required_occupation_missing", "required_income_missing"] if rule in hit_rules)
    other_rules = [rule for rule in hit_rules if rule not in ["required_birth_year_missing", "required_occupation_missing", "required_income_missing"]]
    
    score = 100 - (26 * required_missing_count) - (5 * len(other_rules))
    score = max(0, score)
    
    return score, hit_rules, hit_messages

# ============================================================
# 패널 텍스트화
# ============================================================

def panel_to_text(r):
    """패널 데이터를 자연어 텍스트로 변환"""
    parts = []
    
    gender = r.get("성별")
    if gender:
        parts.append(f"{gender}이다.")
    
    birth = r.get("출생년도")
    age = r.get("age")
    if age:
        parts.append(f"{birth}년생으로 만 {age}세이다.")
    elif birth:
        parts.append(f"{birth}년생이다.")
    
    region1 = r.get("지역")
    region2 = r.get("지역구")
    if region1 and region2:
        parts.append(f"{region1} {region2} 거주자이다.")
    elif region1:
        parts.append(f"{region1} 거주자이다.")
    
    personal = r.get("월평균_개인소득")
    household = r.get("월평균_가구소득")
    if personal:
        parts.append(f"월 개인소득은 {personal} 수준이다.")
    if household:
        parts.append(f"월 가구소득은 {household} 수준이다.")
    
    job = r.get("직업")
    edu = r.get("최종학력")
    if job:
        parts.append(f"직업은 {job}이다.")
    if edu:
        parts.append(f"최종학력은 {edu}이다.")
    
    car = r.get("차량여부")
    if car:
        parts.append(f"차량 보유 여부는 {car}이다.")
    
    phone_brand = r.get("휴대폰_브랜드")
    phone_model = r.get("휴대폰_모델")
    if phone_brand and phone_model:
        parts.append(f"{phone_brand}의 {phone_model}을 사용하고 있다.")
    elif phone_brand:
        parts.append(f"{phone_brand} 스마트폰을 사용하고 있다.")
    
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
# 생활패턴 임베딩 기반 패널ID 추출
# ============================================================

def is_lifestyle_query(query: str) -> bool:
    """쿼리가 생활패턴 관련인지 판단"""
    lifestyle_keywords = [
        '운동', '체력', 'OTT', '넷플릭스', '디즈니', '전통시장', '시장', '설선물', '선물',
        '방학', '겨울방학', '추억', '반려동물', '강아지', '고양이', '애완동물', '이사', '스트레스',
        '소비', '쇼핑', '앱', '어플', '피부', '스킨케어', '화장품', 'AI', '챗봇', '여행',
        '해외여행', '배송', '당일배송', '여름', '더위', '물건', '알람', '혼밥', '노년', '땀',
        '다이어트', '야식', '간식', '지출', '미니멀', '맥시멀', '비닐봉투', '환경', '할인',
        '포인트', '멤버십', '초콜릿', '개인정보', '패션', '우산', '갤러리', '사진', '물놀이'
    ]
    return any(keyword in query for keyword in lifestyle_keywords)

def get_lifestyle_based_panel_ids(query: str, top_k: int = 100):
    """생활패턴 임베딩 기반으로 패널 ID 리스트 추출"""
    try:
        # 1. 전체 패널 데이터 가져오기
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # 생활패턴 칼럼만 선택하여 쿼리
        lifestyle_cols_quoted = ', '.join([f'"{col}"' for col in LIFESTYLE_COLUMNS])
        query_sql = f'SELECT "패널id", {lifestyle_cols_quoted} FROM panel_cb_all'
        
        cur.execute(query_sql)
        all_panels = cur.fetchall()
        cur.close()
        conn.close()
        
        if not all_panels:
            return []
        
        logging.info(f"🔍 생활패턴 임베딩 분석: 전체 {len(all_panels)}개 패널")
        
        # 2. 쿼리 임베딩
        query_embedding = kure_model.encode(query, convert_to_tensor=True)
        
        # 3. 각 패널의 생활패턴 텍스트 생성 및 유사도 계산
        panel_scores = []
        for panel in all_panels:
            lifestyle_texts = []
            for col in LIFESTYLE_COLUMNS:
                value = panel.get(col)
                if value and value not in NULL_TOKENS and str(value).strip():
                    lifestyle_texts.append(str(value))
            
            if lifestyle_texts:
                # 생활패턴 텍스트 결합
                combined_text = " ".join(lifestyle_texts)
                panel_embedding = kure_model.encode(combined_text, convert_to_tensor=True)
                
                # 코사인 유사도 계산
                similarity = util.pytorch_cos_sim(query_embedding, panel_embedding).item()
                panel_scores.append((panel.get('패널id'), similarity))
        
        # 4. 유사도 순으로 정렬하여 상위 top_k개 패널 ID 반환
        panel_scores.sort(key=lambda x: x[1], reverse=True)
        top_panel_ids = [panel_id for panel_id, score in panel_scores[:top_k]]
        
        logging.info(f"✅ 생활패턴 기반 패널 ID 추출 완료: {len(top_panel_ids)}개")
        
        return top_panel_ids
        
    except Exception as e:
        logging.error(f"💥 생활패턴 임베딩 분석 오류: {str(e)}")
        traceback.print_exc()
        return []

# ============================================================
# SQL 생성 프롬프트
# ============================================================

def create_sql_generation_prompt(user_query: str, lifestyle_panel_ids: list = None) -> str:
    """SQL 쿼리 생성 프롬프트 (생활패턴 기반 필터링 포함)"""
    
    # 생활패턴 기반 패널 ID가 있으면 IN 절 추가
    lifestyle_filter = ""
    if lifestyle_panel_ids:
        # SQL Injection 방지를 위해 패널 ID를 이스케이프 처리
        escaped_ids = [f"'{pid}'" for pid in lifestyle_panel_ids[:100]]  # 상위 100개만
        lifestyle_filter = f"\n⚠️ 중요: 생활패턴 기반 필터링이 적용됩니다. WHERE 절에 반드시 다음 조건을 추가하세요:\n\"패널id\" IN ({', '.join(escaped_ids)})\n"
    
    return f"""당신은 PostgreSQL SQL 쿼리 생성 전문가입니다.

테이블 이름: panel_cb_all

테이블 스키마 (정확한 컬럼명):
- 패널id (VARCHAR, PRIMARY KEY)
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
- 차량여부 (VARCHAR) - 예: '있다', '없다'
- 자동차_제조사 (VARCHAR)
- 자동차_모델 (VARCHAR)
- 흡연경험 (JSONB)
- 음용경험_술 (JSONB)
{lifestyle_filter}
사용자 요청: "{user_query}"

쿼리 생성 규칙:
1. 기본 형식: SELECT * FROM panel_cb_all
2. 출생년도로 나이 계산 시 반드시 ::INTEGER 캐스팅
3. 나이대별 출생년도 (2025년 기준, 만 나이):
   - 10대 (만 10~19세): 출생년도::INTEGER BETWEEN 2005 AND 2014
   - 20대 (만 20~29세): 출생년도::INTEGER BETWEEN 1995 AND 2004
   - 30대 (만 30~39세): 출생년도::INTEGER BETWEEN 1985 AND 1994
   - 40대 (만 40~49세): 출생년도::INTEGER BETWEEN 1975 AND 1984
   - 50대 (만 50~59세): 출생년도::INTEGER BETWEEN 1965 AND 1974
   - 60대 (만 60~69세): 출생년도::INTEGER BETWEEN 1955 AND 1964
4. 인원수 명시시 LIMIT 추가
5. 고소득자는 월평균_개인소득 400만원 이상
6. 생활패턴 필터가 있으면 WHERE 절에 \"패널id\" IN (...) 조건을 반드시 포함
7. 여러 조건이 있을 때는 AND로 연결

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

        # 생활패턴 관련 쿼리인지 확인 후 패널 ID 추출
        lifestyle_panel_ids = None
        if is_lifestyle_query(query):
            logging.info("🎯 생활패턴 임베딩 분석 시작")
            lifestyle_panel_ids = get_lifestyle_based_panel_ids(query, top_k=100)
            
            if not lifestyle_panel_ids:
                logging.info("❌ 생활패턴 기반 검색 결과 없음")
                return jsonify({
                    "panels": [],
                    "words": []
                })

        # Claude API로 SQL 쿼리 생성 (생활패턴 패널 ID 포함)
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2048,
            messages=[
                {"role": "user", "content": create_sql_generation_prompt(query, lifestyle_panel_ids)}
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
        
        # 결과 변환
        panels = []
        for idx, row in enumerate(results, start=1):
            panel_dict = dict(row)
            
            score, hit_rules, hit_messages = calculate_reliability_score(panel_dict)
            
            birth_year = panel_dict.get('출생년도')
            age = None
            if birth_year:
                try:
                    age = 2025 - int(birth_year) -1
                except:
                    age = None
            
            def convert_null(value, default='무응답'):
                if value is None or value == '' or value == '-' or value == 'null':
                    return default
                return value
            
            lifestyle_dict = {}
            for f in LIFESTYLE_COLUMNS:
                lifestyle_dict[f] = convert_null(panel_dict.get(f))

            panel = {
                "id": f"패널{idx}",
                "mbSn": convert_null(panel_dict.get('패널id'), f"MB{idx}"),
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
                "lifestylePatterns": lifestyle_dict,
                "birthYear": birth_year,
                "_text_description": panel_to_text(panel_dict),
            }
            panels.append(panel)
        
        panels.sort(key=lambda x: x['reliability'], reverse=True)
        
        words = []
        keywords = query.split()
        for keyword in keywords:
            if len(keyword) > 1:
                words.append({"text": keyword, "value": 10})
        
        logging.info(f"🎉 검색 완료: {len(panels)}개 패널")
        
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

@app.route('/api/common-characteristics', methods=['POST'])
def common_characteristics():
    """패널들의 공통 특성 분석"""
    try:
        data = request.get_json()
        panels = data.get('panels', [])
        
        if not panels or len(panels) == 0:
            return jsonify({"error": "분석할 패널 데이터가 없습니다."}), 400
        
        logging.info(f"🔍 공통 특성 분석: {len(panels)}개 패널")
        
        keyword_counter = {}
        
        for panel in panels:
            gender = panel.get('gender')
            if gender and gender != '무응답':
                keyword_counter[gender] = keyword_counter.get(gender, 0) + 1
            
            residence = panel.get('residence')
            if residence and residence != '무응답':
                keyword_counter[residence] = keyword_counter.get(residence, 0) + 1
            
            job = panel.get('job')
            if job and job != '무응답':
                keyword_counter[job] = keyword_counter.get(job, 0) + 1
            
            age = panel.get('age')
            if age:
                if age < 20:
                    age_group = '10대'
                elif age < 30:
                    age_group = '20대'
                elif age < 40:
                    age_group = '30대'
                elif age < 50:
                    age_group = '40대'
                elif age < 60:
                    age_group = '50대'
                else:
                    age_group = '60대 이상'
                keyword_counter[age_group] = keyword_counter.get(age_group, 0) + 1
            
            income = panel.get('personalIncome')
            if income and income != '무응답':
                keyword_counter[income] = keyword_counter.get(income, 0) + 1
        
        top_keywords = sorted(
            keyword_counter.items(), 
            key=lambda x: x[1], 
            reverse=True
        )[:5]
        
        keywords = [
            {"keyword": k, "count": v} 
            for k, v in top_keywords
        ]
        
        total_count = len(panels)
        avg_age = sum(p.get('age', 0) for p in panels) / total_count if total_count > 0 else 0
        
        gender_dist = {}
        for p in panels:
            g = p.get('gender', '무응답')
            gender_dist[g] = gender_dist.get(g, 0) + 1
        
        residence_dist = {}
        for p in panels:
            r = p.get('residence', '무응답')
            if r != '무응답':
                residence_dist[r] = residence_dist.get(r, 0) + 1
        
        summary_prompt = f"""다음은 {total_count}명의 패널 데이터 분석 결과입니다:

공통 특성 상위 5개:
{chr(10).join([f'- {k["keyword"]}: {k["count"]}명' for k in keywords])}

평균 나이: {avg_age:.1f}세
성별 분포: {', '.join([f'{k} {v}명' for k, v in gender_dist.items()])}
주요 거주지: {', '.join([f'{k} {v}명' for k, v in sorted(residence_dist.items(), key=lambda x: x[1], reverse=True)[:3]])}

이 패널 집단의 특징을 2-3문장으로 자연스럽게 요약해주세요. 
마케팅이나 타겟팅 관점에서 유용한 인사이트를 포함해주세요."""

        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=512,
            messages=[
                {"role": "user", "content": summary_prompt}
            ]
        )
        
        summary = message.content[0].text.strip()
        
        logging.info(f"✅ 공통 특성 분석 완료: {len(keywords)}개 키워드")
        
        return jsonify({
            "keywords": keywords,
            "summary": summary
        })
        
    except Exception as e:
        logging.error(f"💥 공통 특성 분석 오류: {str(e)}")
        traceback.print_exc()
        return jsonify({
            "error": "공통 특성 분석 중 오류가 발생했습니다.",
            "detail": str(e)
        }), 500

# 키워드 후보군
KEYWORD_POOL = [
    '20대 여성', '서울 거주', '직장인', '월소득 300만원', '미혼', '대졸', 'IT업계',
    '베이비붐 세대', '프리랜서', '운동 좋아함', '30대 남성', '부동산 투자', '경기도',
    '여행', '육아', '부모'
]

@app.route('/api/related_keywords', methods=['POST'])
def related_keywords():
    data = request.get_json()
    user_query = data.get('query', '').strip()
    top_n = int(data.get('top_n', 7))
    if not user_query:
        return jsonify({'keywords': []})
    
    cand_emb = kure_model.encode(KEYWORD_POOL, convert_to_tensor=True)
    q_emb = kure_model.encode(user_query, convert_to_tensor=True)
    sims = util.pytorch_cos_sim(q_emb, cand_emb).cpu().numpy().flatten()
    indices = sims.argsort()[::-1][:top_n]
    related = [
        {'text': KEYWORD_POOL[i], 'similarity': float(sims[i])} for i in indices
    ]
    return jsonify({'keywords': related})

@app.route('/api/export-csv', methods=['POST'])
def export_csv():
    """패널 데이터를 CSV로 내보내기"""
    try:
        import csv
        from io import StringIO
        from flask import make_response
        
        data = request.get_json()
        panels = data.get('panels', [])
        
        if not panels:
            return jsonify({"error": "내보낼 패널 데이터가 없습니다."}), 400
        
        output = StringIO()
        
        headers = [
            'MB_SN', '패널번호', '신뢰도', '감점사유',
            '성별', '나이', '출생년도', '거주지', '지역구',
            '결혼여부', '자녀수', '가족수', '최종학력', '직업', '직무',
            '월평균_개인소득', '월평균_가구소득',
            '휴대폰_브랜드', '휴대폰_모델',
            '차량여부', '자동차_제조사', '자동차_모델',
            '흡연경험', '음주경험', '보유제품'
        ]
        
        writer = csv.DictWriter(
            output, 
            fieldnames=headers,
            quoting=csv.QUOTE_ALL,
            lineterminator='\n'
        )
        writer.writeheader()
        
        for panel in panels:
            def format_list(value):
                if value is None:
                    return '-'
                if isinstance(value, list):
                    if len(value) == 0:
                        return '-'
                    return ' / '.join(str(v) for v in value)
                return str(value) if value else '-'
            
            writer.writerow({
                'MB_SN': panel.get('mbSn', '-'),
                '패널번호': panel.get('id', '-'),
                '신뢰도': f"{panel.get('reliability', 0)}%",
                '감점사유': ' / '.join(panel.get('reliabilityReasons', [])) if panel.get('reliabilityReasons') else '-',
                '성별': panel.get('gender', '-'),
                '나이': f"만 {panel.get('age', '-')}세" if panel.get('age') else '-',
                '출생년도': panel.get('birthYear', '-'),
                '거주지': panel.get('residence', '-'),
                '지역구': panel.get('district', '-'),
                '결혼여부': panel.get('maritalStatus', '-'),
                '자녀수': panel.get('children', 0),
                '가족수': panel.get('familySize', '-'),
                '최종학력': panel.get('education', '-'),
                '직업': panel.get('job', '-'),
                '직무': panel.get('role', '-'),
                '월평균_개인소득': panel.get('personalIncome', '-'),
                '월평균_가구소득': panel.get('householdIncome', '-'),
                '휴대폰_브랜드': panel.get('phoneBrand', '-'),
                '휴대폰_모델': panel.get('phoneModel', '-'),
                '차량여부': panel.get('carOwnership', '-'),
                '자동차_제조사': panel.get('carBrand', '-'),
                '자동차_모델': panel.get('carModel', '-'),
                '흡연경험': format_list(panel.get('smokingExperience')),
                '음주경험': format_list(panel.get('drinkingExperience')),
                '보유제품': format_list(panel.get('ownedProducts')),
            })
        
        csv_content = output.getvalue()
        output.close()
        
        csv_bytes = '\ufeff' + csv_content
        
        response = make_response(csv_bytes.encode('utf-8'))
        response.headers['Content-Type'] = 'text/csv; charset=utf-8'
        response.headers['Content-Disposition'] = 'attachment; filename*=UTF-8\'\'%ED%8C%A8%EB%84%90%EB%8D%B0%EC%9D%B4%ED%84%B0.csv'
        
        logging.info(f"✅ CSV 내보내기 완료: {len(panels)}개 패널")
        
        return response
        
    except Exception as e:
        logging.error(f"💥 CSV 내보내기 오류: {str(e)}")
        traceback.print_exc()
        return jsonify({
            "error": "CSV 내보내기 중 오류가 발생했습니다.",
            "detail": str(e)
        }), 500

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    app.run(host='0.0.0.0', port=5000, debug=True)