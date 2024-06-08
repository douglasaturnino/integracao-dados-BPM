from config.conexaoODS import DBConnectionODS


class DBConnectionAreaBPMODS(DBConnectionODS):
    def create(self):
        qry_tbLogAreaBPM = """
            CREATE TABLE IF NOT EXISTS tbLogAreaBPM(
            
            codBPM INTEGER,
            areaBPM REAL (5,2),
            dtCarga DATETIME
        )
        """
        self.conexao.execute(qry_tbLogAreaBPM)

        tbLogAreaBPM_codBPM_IDX = "CREATE INDEX IF NOT EXISTS tbLogAreaBPM_codBPM_IDX ON tbLogAreaBPM (codBPM)"
        self.conexao.execute(tbLogAreaBPM_codBPM_IDX)

    def insert(self, tbLogAreaBPM):
        self.conexao.executemany(
            """ INSERT INTO tbLogAreaBPM (codBPM, areaBPM, dtCarga) VALUES (?,?,?)""",
            tbLogAreaBPM.values.tolist(),
        )
