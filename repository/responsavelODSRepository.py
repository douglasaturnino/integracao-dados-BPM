from config.conexaoODS import DBConnectionODS


class DBConnectionResponsavelODS(DBConnectionODS):
    def insert(self, tbLogRespDP):
        self.conexao.executemany(
            """INSERT INTO tbLogRespDP (codDP, nmResponsavel, dtCarga) VALUES (?,?,?)""",
            tbLogRespDP.values.tolist(),
        )
