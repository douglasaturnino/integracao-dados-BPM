import os
from config.conexao import DBConnection


class DBConnectionODS(DBConnection):
    def __init__(self) -> None:
        super().__init__()
        self.path = os.path.join(os.getcwd(), "data")
        self.namedb = os.path.join(self.path, "pascoaODS.db")

    def insert(self, dadosIBGE):
        dadosIBGE.to_sql("tbLogMunic", self.conexao, if_exists="append")
