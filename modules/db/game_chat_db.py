from modules.db.db_connector import DBConnector

class GameChatDBConnector(DBConnector):
    def select(self, query, params=None):
        return self.execute_query(query, params)

    def update(self, query, params=None):
        self.execute_query(query, params)
        self.connection.commit()

    def delete(self, query, params=None):
        self.execute_query(query, params)
        self.connection.commit()

    def insert(self, query, params=None):
        self.execute_query(query, params)
        self.connection.commit()

    # 게임 ID 조회
    def select_game_idx(self, user_id):
        query = "SELECT GAME_IDX FROM USERS_TB WHERE USER_ID = %(user_id)s"
        result = self.select(query, {"user_id": user_id})
        return result[0]['GAME_IDX'] if result else None

    def get_details(self, tip_name):
        query = """
            SELECT TIPS_DETAIL_NAME
            FROM TIPS_DETAILS_TB
            WHERE TIP_IDX = (
                SELECT TIPS_IDX FROM TIPS_TB WHERE TIP_WORD = %(tip_name)s
            )
        """
        result = self.select(query, {"tip_name": tip_name})
        return [row['TIPS_DETAIL_NAME'] for row in result]

    def get_answer(self, tip_name, detail_name):
        query = """
            SELECT TIPS_ANSWER
            FROM TIPS_ANSWERS_TB
            WHERE TIPS_DETAIL_IDX = (
                SELECT TIPS_DETAIL_IDX
                FROM TIPS_DETAILS_TB
                WHERE TIPS_DETAIL_NAME = %(detail_name)s
                AND TIP_IDX = (
                    SELECT TIPS_IDX FROM TIPS_TB WHERE TIP_WORD = %(tip_name)s
                )
            )
        """
        result = self.select(query, {"tip_name": tip_name, "detail_name": detail_name})
        return result[0]['TIPS_ANSWER'] if result else "해당 항목에 대한 답변이 없습니다."
    
    def save_conversation(self, game_idx, sender, message):
        if isinstance(message, list):
            message = ", ".join(message)
        query = """
            INSERT INTO CONVERSATIONS_TB (GAME_IDX, SENDER, MESSAGE)
            VALUES (%s, %s, %s)
        """
        self.insert(query, (game_idx, sender, message))
    

