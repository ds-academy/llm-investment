from modules.db.db_connector import DBConnector


class ChatDBConnector(DBConnector):
    def select(self, query, params=None):
        return self.execute_query(query, params)

    def update(self, query, params=None):
        self.execute_query(query, params)

    def delete(self, query, params=None):
        self.execute_query(query, params)

    def create_chatroom(self, user_id):
        try:
            # 채팅방 생성 쿼리 실행
            cursor = self.connection.cursor()
            cursor.execute("INSERT INTO chat_rooms (user_id) VALUES (%s)", (user_id,))
            room_id = cursor.lastrowid  # 마지막 삽입된 row의 ID를 가져옴
            self.connection.commit()
            return room_id
        except Exception as e:
            print(f"Error creating chatroom: {e}")
            return None
        finally:
            cursor.close()

    def get_chatroom_count_by_userid(self, userid):
        query = "SELECT COUNT(*) as count FROM chat_rooms WHERE user_id = %s"
        result = self.execute_query(query, (userid,))
        return result[0]["count"] if result else 0

    def get_last_active_chatroom_by_userid(self, userid):
        query = """
        SELECT cr.chatroom_id
        FROM chat_rooms cr
        LEFT JOIN chat_history ch ON cr.chatroom_id = ch.room_id
        WHERE cr.user_id = %s
        ORDER BY ch.timestamp DESC
        LIMIT 1
        """
        result = self.execute_query(query, (userid,))
        return result[0]["chatroom_id"] if result else None

    def get_chat_history(self, chatroom_id):
        query = "SELECT speaker, message, timestamp FROM chat_history WHERE room_id = %s ORDER BY timestamp ASC"
        return self.execute_query(query, (chatroom_id,))

    def save_chat_history(self, chatroom_id, speaker, message):
        query = """
            INSERT INTO chat_history (room_id, speaker, message, timestamp)
            VALUES (%s, %s, %s, NOW())
        """
        self.execute_query(query, (chatroom_id, speaker, message))

    def get_user_chatrooms(self, userid):
        query = "SELECT chatroom_id FROM chat_rooms WHERE user_id = %s"
        return self.execute_query(query, (userid,))

    def delete_chatroom(self, chatroom_id):
        query = "DELETE FROM chat_rooms WHERE chatroom_id = %s"
        self.execute_query(query, (chatroom_id,))
