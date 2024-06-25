from config.conexaoODS import DBConnectionODS


class DBConnectionResponsavelODS(DBConnectionODS):
    def create(self):
        qry_dRespDP = """
            CREATE TABLE IF NOT EXISTS tbLogRespDP(
            
            codDP INTEGER,
            nmResponsavel VARCHAR(100),
            dtCarga DATETIME
        )
        """
        self.conexao.execute(qry_dRespDP)

        tbLogRespDP_codRespDP_IDX = (
            "CREATE INDEX IF NOT EXISTS tbLogRespDP_codDP_IDX ON tbLogRespDP (codDP)"
        )
        self.conexao.execute(tbLogRespDP_codRespDP_IDX)

    def insert(self, tbLogRespDP):
        self.conexao.executemany(
            """INSERT INTO tbLogRespDP (codDP, nmResponsavel, dtCarga) VALUES (?,?,?)""",
            tbLogRespDP.values.tolist(),
        )
