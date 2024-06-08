import sqlite3 as sql


class DBConnection:
    def __init__(self) -> None:
        self.namedb = None
        self.conexao = None

    def __enter__(self):
        conexaoODS = sql.connect(self.namedb)
        self.conexao = conexaoODS
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conexao.commit()
        self.conexao.close()
