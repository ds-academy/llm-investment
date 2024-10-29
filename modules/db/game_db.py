from modules.db.db_connector import DBConnector


class GameDBConnector(DBConnector):
    def select(self, query, params=None):
        return self.execute_query(query, params)

    def update(self, query, params=None):
        self.execute_query(query, params)

    def delete(self, query, params=None):
        self.execute_query(query, params)