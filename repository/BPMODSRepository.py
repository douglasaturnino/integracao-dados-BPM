from config.conexaoODS import DBConnectionODS
import pandas as pd


class DBConnectionBPMODS(DBConnectionODS):
    def create(self):
        qry_dBPM = """
            CREATE TABLE IF NOT EXISTS tbLogBPM(
            
            codBPM INTEGER,
            nmBPM VARCHAR (7),
            enderecoBPM VARCHAR (200),
            dtCarga DATETIME
        )
        """
        self.conexao.execute(qry_dBPM)

        tbLogBPM_codBPM_IDX = (
            "CREATE INDEX IF NOT EXISTS tbLogBPM_codBPM_IDX ON tbLogBPM (codBPM)"
        )
        self.conexao.execute(tbLogBPM_codBPM_IDX)

    def select(self):
        qry_dBPM = """
            SELECT
                codBPM,
                nmBPM,
                enderecoBPM,
                areaBPM
            FROM (
                SELECT 
                    a.codBPM,
                    a.nmBPM,
                    a.enderecoBPM,
                    b.areaBPM,
                    MAX(a.dtCarga)
                FROM tbLogBPM a
                JOIN tbLogAreaBPM b ON a.codBPM = b.codBPM
                WHERE a.dtCarga = (
                    SELECT MAX(x.dtCarga)
                    FROM tbLogBPM x
                )
                GROUP BY
                    a.codBPM,
                    a.nmBPM,
                    a.enderecoBPM,
                    b.areaBPM
            )
        """
        dBPM = pd.read_sql(qry_dBPM, self.conexao)
        return dBPM

    def insert(self, tbLogBPM):
        self.conexao.executemany(
            """ INSERT INTO tbLogBPM (codBPM, nmBPM, enderecoBPM, dtCarga) VALUES (?,?,?,?)""",
            tbLogBPM.values.tolist(),
        )
