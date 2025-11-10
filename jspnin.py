import os
import json
import psycopg2
from psycopg2.extras import execute_batch

# JSON 파일 경로 설정
json_path = r'C:\LLM_pyt\최종json\welcome_cb_fin.json'

# PostgreSQL 접속 정보
DB_HOST = 'rds-postgresql-pgvector.cbq4y662mptt.ap-northeast-2.rds.amazonaws.com'
DB_NAME = 'postgres'      # 본인 DB명
DB_USER = 'root'
DB_PASS = 'rkdtmdwls123'
DB_PORT = 5432

# 테이블 생성 쿼리 (최초 1회 실행)
create_table_query = """
CREATE TABLE IF NOT EXISTS panel_responses (
    패널ID VARCHAR PRIMARY KEY,
    성별 VARCHAR,
    출생년도 VARCHAR,
    지역 VARCHAR,
    지역구 VARCHAR,
    결혼여부 VARCHAR,
    자녀수 INTEGER,
    가족수 VARCHAR,
    최종학력 VARCHAR,
    직업 VARCHAR,
    직무 VARCHAR,
    월평균_개인소득 VARCHAR,
    월평균_가구소득 VARCHAR,
    보유전제품 JSONB,
    휴대폰_브랜드 VARCHAR,
    휴대폰_모델 VARCHAR,
    차량여부 VARCHAR,
    자동차_제조사 VARCHAR,
    자동차_모델 VARCHAR,
    흡연경험 JSONB,
    흡연경험_담배브랜드 JSONB,
    흡연경험_담배브랜드_기타 VARCHAR,
    전자담배_이용경험 JSONB,
    흡연경험_담배_기타내용 VARCHAR,
    음용경험_술 JSONB,
    음용경험_술_기타내용 VARCHAR
);
"""

def parse_none(value):
    if value in ["", ",", "null", None]:
        return None
    return value

def connect_db():
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT
    )

def insert_data_batch(cur, data_list):
    insert_query = """
    INSERT INTO panel_responses (
        패널ID, 성별, 출생년도, 지역, 지역구, 결혼여부, 자녀수, 가족수,
        최종학력, 직업, 직무, 월평균_개인소득, 월평균_가구소득,
        보유전제품, 휴대폰_브랜드, 휴대폰_모델, 차량여부,
        자동차_제조사, 자동차_모델,
        흡연경험, 흡연경험_담배브랜드, 흡연경험_담배브랜드_기타,
        전자담배_이용경험, 흡연경험_담배_기타내용,
        음용경험_술, 음용경험_술_기타내용
    ) VALUES (
        %(패널ID)s, %(성별)s, %(출생년도)s, %(지역)s, %(지역구)s, %(결혼여부)s, %(자녀수)s, %(가족수)s,
        %(최종학력)s, %(직업)s, %(직무)s, %(월평균_개인소득)s, %(월평균_가구소득)s,
        %(보유전제품)s, %(휴대폰_브랜드)s, %(휴대폰_모델)s, %(차량여부)s,
        %(자동차_제조사)s, %(자동차_모델)s,
        %(흡연경험)s, %(흡연경험_담배브랜드)s, %(흡연경험_담배브랜드_기타)s,
        %(전자담배_이용경험)s, %(흡연경험_담배_기타내용)s,
        %(음용경험_술)s, %(음용경험_술_기타내용)s
    )
    ON CONFLICT (패널ID) DO NOTHING;
    """
    execute_batch(cur, insert_query, data_list, page_size=1000)

def prepare_entry(entry):
    import json as pyjson
    panel_id = entry.get('패널ID')
    if not panel_id:
        # 패널ID가 없으면 None 반환하여 건너뜀 처리용
        return None
    return {
        '패널ID': panel_id,
        '성별': parse_none(entry.get('귀하의 성별은')),
        '출생년도': parse_none(entry.get('귀하의 출생년도는 어떻게 되십니까?')),
        '지역': parse_none(entry.get('회원님께서 현재 살고 계신 지역은 어디인가요?')),
        '지역구': parse_none(entry.get('그렇다면, 현재 살고 계신 지역의 어느 구에 살고 계신가요?')),
        '결혼여부': parse_none(entry.get('결혼여부')),
        '자녀수': entry.get('자녀수'),
        '가족수': parse_none(entry.get('가족수')),
        '최종학력': parse_none(entry.get('최종학력')),
        '직업': parse_none(entry.get('직업')),
        '직무': parse_none(entry.get('직무')),
        '월평균_개인소득': parse_none(entry.get('월평균 개인소득')),
        '월평균_가구소득': parse_none(entry.get('월평균 가구소득')),
        '보유전제품': pyjson.dumps(entry.get('보유전제품')) if entry.get('보유전제품') else None,
        '휴대폰_브랜드': parse_none(entry.get('보유 휴대폰 단말기 브랜드')),
        '휴대폰_모델': parse_none(entry.get('보유 휴대폰 모델명')),
        '차량여부': parse_none(entry.get('보유차량여부')),
        '자동차_제조사': parse_none(entry.get('자동차 제조사')),
        '자동차_모델': parse_none(entry.get('자동차 모델')),
        '흡연경험': pyjson.dumps(entry.get('흡연경험')) if entry.get('흡연경험') else None,
        '흡연경험_담배브랜드': pyjson.dumps(entry.get('흡연경험 담배브랜드')) if entry.get('흡연경험 담배브랜드') else None,
        '흡연경험_담배브랜드_기타': parse_none(entry.get('흡연경험 담배브랜드(기타브랜드)')),
        '전자담배_이용경험': pyjson.dumps(entry.get('궐련형 전자담배/가열식 전자담배 이용경험')) if entry.get('궐련형 전자담배/가열식 전자담배 이용경험') else None,
        '흡연경험_담배_기타내용': parse_none(entry.get('흡연경험 담배 브랜드(기타내용)')),
        '음용경험_술': pyjson.dumps(entry.get('음용경험 술')) if entry.get('음용경험 술') else None,
        '음용경험_술_기타내용': parse_none(entry.get('음용경험 술(기타내용)')),
    }

def main():
    if not os.path.isfile(json_path):
        print(f'Error: JSON 파일이 존재하지 않습니다: {json_path}')
        return

    print('JSON 파일 로딩 중...')
    with open(json_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
    print(f'JSON 로드 완료, 총 항목 수: {len(json_data)}')

    conn = connect_db()
    cur = conn.cursor()
    cur.execute(create_table_query)
    conn.commit()

    print('데이터 삽입 준비 중...')
    batch_size = 1000
    batch_list = []
    skipped_count = 0
    for i, entry in enumerate(json_data):
        prepared = prepare_entry(entry)
        if prepared is None:
            skipped_count += 1
            continue
        batch_list.append(prepared)
        if len(batch_list) >= batch_size:
            insert_data_batch(cur, batch_list)
            conn.commit()
            print(f'{i+1}건 입력 완료...')
            batch_list.clear()

    if batch_list:
        insert_data_batch(cur, batch_list)
        conn.commit()
        print(f'총 {len(json_data)-skipped_count}건 입력 완료.')

    cur.close()
    conn.close()
    print(f'작업 완료. 누락된 패널ID 항목: {skipped_count}건')

if __name__ == '__main__':
    main()
