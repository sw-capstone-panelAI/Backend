
# ============================================================
# 신뢰도 계산 관련 상수 및 유틸리티
# ============================================================
import re

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
    "car_brand_model_mismatch_heuristic": "차량 브랜드/모델 불일치",
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
        r["age"] = 2025 - birth_int  # 만 나이
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


# ============================================================
# 신뢰도 점수 계산
# ============================================================

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
