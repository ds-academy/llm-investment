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
