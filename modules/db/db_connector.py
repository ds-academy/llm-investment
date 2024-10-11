import pymysql
from abc import ABC, abstractmethod
from modules.db.db_config import db_config

class DBConnector(ABC):

    def __init__(self):
        self.connection = pymysql.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            db=db_config['database'],
            port=db_config['port'],
            cursorclass=pymysql.cursors.DictCursor,
            # 다음 줄을 추가하여 쿼리 로그를 활성화합니다.
            client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS
        )

    def execute_query(self, query, params=None):
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            self.connection.commit()
            return cursor.fetchall()

    @abstractmethod
    def select(self):
        pass

    @abstractmethod
    def update(self, data: dict, where: str):
        pass

    @abstractmethod
    def delete(self, where: str):
        pass

    def close(self):
        if self.connection:
            self.connection.close()
