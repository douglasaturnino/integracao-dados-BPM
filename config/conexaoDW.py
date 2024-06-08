import os
from config.conexao import DBConnection


class DBConnectionDW(DBConnection):
    def __init__(self) -> None:
        super().__init__()
        self.path = os.path.join(os.getcwd(), "data")
        self.namedb = os.path.join(self.path, "pascoaDW.db")
