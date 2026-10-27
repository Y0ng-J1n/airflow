import pandas as pd
import xmltodict
import requests
import os

from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook


class SeoulApiToCsvOperator(BaseOperator):

    template_fields = ('endpoint', 'path', 'file_name', 'base_dt')

    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)

        self.http_conn_id = 'openapi.seoul.go.kr'
        self.path = path
        self.file_name = file_name

        # JSON → XML 변경
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/xml/' + dataset_nm

        self.base_dt = base_dt

    def execute(self, context):

        connection = BaseHook.get_connection(self.http_conn_id)

        self.base_url = (
            f'http://{connection.host}:{connection.port}/{self.endpoint}'
        )

        total_row_df = pd.DataFrame()

        start_row = 1
        end_row = 1000

        while True:

            self.log.info(f'시작: {start_row}')
            self.log.info(f'끝: {end_row}')

            row_df = self._call_api(
                self.base_url,
                start_row,
                end_row
            )

            total_row_df = pd.concat(
                [total_row_df, row_df],
                ignore_index=True
            )

            if len(row_df) < 1000:
                break

            start_row = end_row + 1
            end_row += 1000

        os.makedirs(self.path, exist_ok=True)

        total_row_df.to_csv(
            os.path.join(self.path, self.file_name),
            encoding='utf-8-sig',
            index=False
        )

        self.log.info(f'최종 저장 건수: {len(total_row_df)}')

    def _call_api(self, base_url, start_row, end_row):

        request_url = f'{base_url}/{start_row}/{end_row}/'

        if self.base_dt is not None:
            request_url = f'{base_url}/{start_row}/{end_row}/{self.base_dt}'

        response = requests.get(request_url, timeout=30)
        response.raise_for_status()

        # XML 문자열 → Python 딕셔너리
        contents = xmltodict.parse(response.text)

        # API 오류 확인
        if 'RESULT' in contents:
            result = contents['RESULT']

            raise ValueError(
                f"서울시 API 오류: {result.get('CODE')} - "
                f"{result.get('MESSAGE')}"
            )

        key_nm = list(contents.keys())[0]
        dataset = contents[key_nm]

        # 정상 응답의 RESULT 코드 확인
        result = dataset.get('RESULT', {})

        if result.get('CODE') not in (None, 'INFO-000'):
            raise ValueError(
                f"서울시 API 오류: {result.get('CODE')} - "
                f"{result.get('MESSAGE')}"
            )

        row_data = dataset.get('row', [])

        if isinstance(row_data, dict):
            row_data = [row_data]

        row_df = pd.DataFrame(row_data)

        return row_df