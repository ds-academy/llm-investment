from modules.db.db_connector import DBConnector


class GameDBConnector(DBConnector):
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

    def select_user_nickname(self, idToken):
        query = "SELECT USER_ID FROM USERS_TB WHERE USER_ID = %(idToken)s"
        nickname = self.select(query, {"idToken": idToken})
        return nickname

    def select_sector_aliases(self, sector):
        query = "SELECT * FROM STOCK_LIST_TB WHERE SECTOR = %(sector)s"
        results = self.select(query, {"sector": sector})
        print("쿼리 결과 :", results)
        # 각 종목별로 COMPANY_ALIAS의 값 추출
        aliases = [row['COMPANY_ALIAS'] for row in results] if results else []
        return aliases
    
    def select_game_company(self, alias):
        query = """
            SELECT COMPANY_CODE
            FROM STOCK_LIST_TB
            WHERE COMPANY_ALIAS = %(alias)s
        """
        company_code = self.select(query, {"alias":alias})
        return company_code[0]['COMPANY_CODE']
    
    def select_game_generate(self, company_code):
        query = """
            SELECT *
            FROM GAME_INFO_TB
            WHERE USER_ID = 'UNNAMED' AND COMPANY_CODE = %(company_code)s
            ORDER BY GAME_IDX ASC
            LIMIT 1
        """
        # WHERE USER_ID = 'UNNAMED' AND CURRENT_TURN = 1 AND COMPANY_CODE = %(company_code)s
        game_idx = self.select(query, {"company_code":company_code})
        return game_idx[0]['GAME_IDX']
    
    def update_game_idx_user(self, user_id, game_idx):
        query = """
            UPDATE USERS_TB
            SET GAME_IDX = %(game_idx)s
            WHERE USER_ID = %(user_id)s
        """
        self.update(query, {"user_id":user_id, "game_idx":game_idx})
    
    def update_game_user(self, user_id, game_idx):
        query = """
            UPDATE GAME_INFO_TB
            SET USER_ID = %(user_id)s
            WHERE GAME_IDX = %(game_idx)s
        """
        self.update(query, {"user_id":user_id, "game_idx":game_idx})

    def select_game_idx(self, user_id):
        query = """
            SELECT GAME_IDX
            FROM USERS_TB
            WHERE USER_ID = %(user_id)s
        """
        result = self.select(query, {"user_id": user_id})
        if not result:
            print(f"No game_idx found for user: {user_id}")
            return None  # 결과가 없으면 None 반환
        return result[0]["GAME_IDX"]

    
    def select_game_info_all(self, game_idx):
        query = """
            SELECT *
            FROM GAME_INFO_TB
            WHERE GAME_IDX = %(game_idx)s
        """
        game_info_all = self.select(query, {"game_idx": game_idx})
        return game_info_all
    
    def select_stock_list_all(self, company_code):
        query = """
            SELECT *
            FROM STOCK_LIST_TB
            WHERE COMPANY_CODE = %(company_code)s
        """
        stock_list_all = self.select(query, {"company_code": company_code})
        return stock_list_all

    def select_game_detail_all(self, game_idx):
        query = """
            SELECT *
            FROM GAME_DETAIL_TB
            WHERE GAME_IDX = %(game_idx)s
            ORDER BY GAME_DETAIL_IDX ASC
        """
        game_detail_all = self.select(query, {"game_idx": game_idx})
        return game_detail_all
    
    def select_chart_detail_all(self, game_detail_idx):
        query = """
            SELECT *
            FROM CHART_DETAIL_TB
            WHERE GAME_DETAIL_IDX = %(game_detail_idx)s
            ORDER BY CHART_DETAIL_IDX ASC
        """
        chart_detail_all = self.select(query, {"game_detail_idx": game_detail_idx})
        return chart_detail_all
    
    def update_next_detail_turn(self, game_detail_idx):
        query = """
            UPDATE GAME_DETAIL_TB
            SET CURRENT_DETAIL_TURN = CURRENT_DETAIL_TURN + 1
            WHERE GAME_DETAIL_IDX = %(game_detail_idx)s
        """
        self.update(query, {"game_detail_idx":game_detail_idx})

    def update_next_turn(self, game_idx):
        query = """
            UPDATE GAME_INFO_TB
            SET CURRENT_TURN = CURRENT_TURN + 1
            WHERE GAME_IDX = %(game_idx)s
        """
        self.update(query, {"game_idx": game_idx})

    def select_news_list(self, game_idx ,current_turn):
        query = """
            SELECT INNEWS_TITLE, INNEWS_INFO, OUTNEWS_TITLE, OUTNEWS_INFO
            FROM GAME_DETAIL_TB
            WHERE GAME_IDX = %(game_idx)s
            ORDER BY GAME_DETAIL_IDX ASC
            LIMIT %(current_turn)s
        """
        results = self.select(query, {"game_idx": game_idx, "current_turn": current_turn})
        return results
    
    def select_report_list(self, game_idx ,current_turn):
        query = """
            SELECT REPORT_TITLE, REPORT_INFO
            FROM GAME_DETAIL_TB
            WHERE GAME_IDX = %(game_idx)s
            ORDER BY GAME_DETAIL_IDX ASC
            LIMIT %(current_turn)s
        """
        results = self.select(query, {"game_idx": game_idx, "current_turn": current_turn})
        return results
    
    def select_financial_list(self, game_idx ,current_turn):
        query = """
            SELECT FINANCIAL_STATEMENTS_TITLE, FINANCIAL_STATEMENTS_INFO
            FROM GAME_DETAIL_TB
            WHERE GAME_IDX = %(game_idx)s
            ORDER BY GAME_DETAIL_IDX ASC
            LIMIT %(current_turn)s
        """
        results = self.select(query, {"game_idx": game_idx, "current_turn": current_turn})
        return results
    
    def update_buy(self, game_detail_idx, current_money, position_money):
        query = """
            UPDATE GAME_DETAIL_TB
            SET CURRENT_MONEY = CURRENT_MONEY-%(current_money)s, POSITION_MONEY = POSITION_MONEY + %(position_money)s
            WHERE GAME_DETAIL_IDX = %(game_detail_idx)s
        """
        self.update(query, {"game_detail_idx": game_detail_idx, "current_money": current_money, "position_money": position_money})

    def update_sell(self, game_detail_idx, current_money, position_money):
        query = """
            UPDATE GAME_DETAIL_TB
            SET CURRENT_MONEY = CURRENT_MONEY+%(current_money)s, POSITION_MONEY = POSITION_MONEY - %(position_money)s
            WHERE GAME_DETAIL_IDX = %(game_detail_idx)s
        """
        self.update(query, {"game_detail_idx": game_detail_idx, "current_money": current_money, "position_money": position_money})

    def update_money(self, game_detail_idx, current_money, position_money):
        query = """
            UPDATE GAME_DETAIL_TB
            SET CURRENT_MONEY = %(current_money)s, POSITION_MONEY = %(position_money)s
            WHERE GAME_DETAIL_IDX = %(game_detail_idx)s
        """
        self.update(query, {"game_detail_idx": game_detail_idx, "current_money":current_money, "position_money":position_money})

    def select_chart_detail_range(self, game_idx, max_turn):
        query = """
            WITH ranked_game_details AS (
                SELECT 
                    GAME_DETAIL_TB.GAME_DETAIL_IDX,
                    ROW_NUMBER() OVER (ORDER BY GAME_DETAIL_TB.GAME_DETAIL_IDX ASC) AS row_num
                FROM GAME_DETAIL_TB
                INNER JOIN GAME_INFO_TB 
                ON GAME_DETAIL_TB.GAME_IDX = GAME_INFO_TB.GAME_IDX
                WHERE GAME_INFO_TB.GAME_IDX = %(game_idx)s
            ),
            limited_game_details AS (
                SELECT GAME_DETAIL_IDX
                FROM ranked_game_details
                WHERE row_num <= %(max_turn)s
            )
            SELECT 
                CHART_DETAIL_TB.CHART_CURRENT, 
                CHART_DETAIL_TB.CHART_HIGH, 
                CHART_DETAIL_TB.CHART_LOW, 
                CHART_DETAIL_TB.CHART_OPEN, 
                CHART_DETAIL_TB.CHART_TIME, 
                CHART_DETAIL_TB.GAME_DETAIL_IDX
            FROM CHART_DETAIL_TB
            INNER JOIN limited_game_details
            ON CHART_DETAIL_TB.GAME_DETAIL_IDX = limited_game_details.GAME_DETAIL_IDX
            ORDER BY CHART_DETAIL_TB.GAME_DETAIL_IDX, CHART_DETAIL_TB.CHART_DETAIL_IDX ASC;
        """
        results = self.select(query, {"game_idx": game_idx, "max_turn": max_turn})

        # 디버깅 출력 추가
        print(f"쿼리 결과 (game_idx: {game_idx}, max_turn: {max_turn}): {results}")
        if not results:
            print("쿼리 결과가 없습니다. 데이터베이스를 확인하세요.")

        return results
    
    def select_money_turn(self, game_idx, turn, close_price):
        query = """
        WITH ranked_game_details AS (
            SELECT 
                GAME_DETAIL_TB.GAME_DETAIL_IDX,
                GAME_DETAIL_TB.CURRENT_MONEY,
                GAME_DETAIL_TB.POSITION_MONEY,
                ROW_NUMBER() OVER (ORDER BY GAME_DETAIL_TB.GAME_DETAIL_IDX ASC) AS row_num
            FROM GAME_DETAIL_TB
            WHERE GAME_DETAIL_TB.GAME_IDX = %(game_idx)s
        )
        SELECT 
            row_num AS turn,
            CURRENT_MONEY,
            POSITION_MONEY,
            (CURRENT_MONEY + POSITION_MONEY * %(close_price)s) AS total_money
        FROM ranked_game_details
        WHERE row_num = %(turn)s
        ORDER BY turn ASC;
        """
        results = self.select(query, {"game_idx": game_idx, "turn": turn, "close_price": close_price})

        # 디버깅용 출력
        print(f"select_money_turn 쿼리 결과 (game_idx: {game_idx}, turn: {turn}, close_price: {close_price}): {results}")

        if not results:
            print("select_money_turn 결과가 없습니다. 데이터베이스를 확인하세요.")

        return results

    
    def update_game_success(self, user_id, score):
        query = """
            UPDATE USERS_TB
            SET USER_SCORE = USER_SCORE + %(score)s, WIN_COUNT = WIN_COUNT + 1
            WHERE USER_ID = %(user_id)s
        """
        self.update(query, {"user_id":user_id, "score":score})
    
    def update_game_fail(self, user_id, score):
        query = """
            UPDATE USERS_TB
            SET USER_SCORE = USER_SCORE + %(score)s, LOSE_COUNT = LOSE_COUNT + 1
            WHERE USER_ID = %(user_id)s
        """
        self.update(query, {"user_id":user_id, "score":score})

    def update_game_reset(self, user_id):
        query = """
            UPDATE USERS_TB
            SET GAME_IDX = 0
            WHERE USER_ID = %(user_id)s
        """
        self.update(query, {"user_id": user_id})

    def select_user_game_idx(self, idToken):
        query = "SELECT GAME_IDX FROM USERS_TB WHERE USER_ID = %(idToken)s"
        idx = self.select(query, {"idToken": idToken})
        return idx