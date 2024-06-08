from config.conexaoDW import DBConnectionDW
import pandas as pd


class DBConnectionAreaBPMDW(DBConnectionDW):
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

    def insert(self, dBPM):
        # Inserindo registros na tabela dBPM
        self.conexao.executemany(
            """ INSERT INTO dDP (codDP, nmDP, endereco, nmResponsavel) VALUES (?,?,?,?)""",
            dBPM.values.tolist(),
        )

    def delete(self):
        self.conexao.execute("DELETE FROM dBPM")
        self.conexao.execute("UPDATE sqlite_sequence SET seq=0 WHERE name='dBPM'")
