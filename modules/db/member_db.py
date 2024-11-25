from modules.db.db_connector import DBConnector

class MemberDBConnector(DBConnector):
    def insert(self, query, params=None):
        self.execute_query(query, params)
        return self.connection.insert_id()

    def select(self, query, params=None):
        return self.execute_query(query, params)

    def update(self, query, params=None):
        self.execute_query(query, params)

    def delete(self, query, params=None):
        self.execute_query(query, params)

    def insert_user(self, user_data):
        query = """
            INSERT INTO USERS_TB (user_id, user_pw, user_nickname, user_name, user_phone, user_email, join_date)
            VALUES (%(user_id)s, %(user_pw)s, %(user_nickname)s, %(user_name)s, %(user_phone)s, %(user_email)s, %(join_date)s)
        """
        self.insert(query, user_data)

    def get_user_by_id(self, user_id):
        query = "SELECT user_id, user_pw FROM USERS_TB WHERE user_id = %(user_id)s"
        result = self.select(query, {"user_id": user_id})
        return result[0] if result else None
    
    def select_my_profile(self, user_id):
        query = """
            WITH RankedUsers AS (
                SELECT
                    USER_ID,
                    USER_NICKNAME,
                    USER_EMAIL, 
                    USER_PROFILE, 
                    RANK() OVER (ORDER BY USER_SCORE DESC) AS user_rank, 
                    WIN_COUNT, 
                    LOSE_COUNT, 
                    USER_SCORE,
                    USER_NAME
                FROM USERS_TB
            )
            SELECT *
            FROM RankedUsers
            WHERE USER_ID = %(user_id)s;
        """
        try:
            # 쿼리 실행
            user_info = self.select(query, {"user_id": user_id})
            print(f"Query result: {user_info}")

            # 결과가 없을 경우 None 반환
            if not user_info or len(user_info) == 0:
                return None
            return user_info[0]
        except Exception as e:
            print(f"Database query error: {e}")
            return None
        
    def update_profile_image(self, user_id, profile_image_url):
        query = """
            UPDATE USERS_TB
            SET USER_PROFILE = %(profile_image_url)s
            WHERE USER_ID = %(user_id)s
        """
        self.update(query, {"user_id":user_id, "profile_image_url":profile_image_url})

    def get_user_rank_list(self):
        # 유저 스코어 내림차순
        query = """
            SELECT USER_ID, USER_NICKNAME, USER_PROFILE, USER_SCORE, WIN_COUNT, LOSE_COUNT 
            FROM USERS_TB ORDER BY USER_SCORE DESC
        """
        rankList = self.select(query)
        return rankList
    
    def get_stock_words(self):
        query = """
            SELECT STOCK_WORD, STOCK_WORD_DESCRIPTION 
            FROM STOCK_WORDS_TB ORDER BY RAND() LIMIT 1
        """
        stockWords = self.select(query)
        return stockWords
    
    def get_stock_tips(self):
        query = """
            SELECT STOCK_TIP_DESCRIPTION
            FROM STOCK_TIPS_TB ORDER BY RAND() LIMIT 1
        """
        stockTips = self.select(query)
        return stockTips
    
    def get_wise_says(self):
        query = """
            SELECT WISE_SAY, WISE_SAY_DESCRIPTION 
            FROM WISE_SAYS_TB ORDER BY RAND() LIMIT 1
        """
        wiseSays = self.select(query)
        return wiseSays
