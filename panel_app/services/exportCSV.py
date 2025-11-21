# ============================================================
# export csv 생성
# ============================================================
import csv
from io import StringIO
from flask import make_response, current_app


def makeCsv(panels):

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

    current_app.logger.info(f"✅ CSV 내보내기 완료: {len(panels)}개 패널")

    return response

