from config.conexaoODS import DBConnectionODS
import pandas as pd


class DBConnectionDPODS(DBConnectionODS):
    def select(self):
        qry_dDP = """
            SELECT
                codDP,
                nmDP,
                endereco,
                nmResponsavel
            FROM
            (
                SELECT
                    a.codDP,
                    a.nmDP,
                    a.endereco,
                    b.nmResponsavel,
                    MAX(a.dtCarga)
                FROM  tbLogDP a
                JOIN tbLogRespDP b ON (a.codDP  = b.codDP)
                WHERE a.dtCarga = (
                    SELECT MAX(x.dtCarga)
                    FROM tbLogDP x
                )
                GROUP BY 
                    a.codDP,
                    a.nmDP,
                    a.endereco,
                    b.nmResponsavel
            ) a
        """

        dDP = pd.read_sql(qry_dDP, self.conexao)

        return dDP

    def create(self):
        qry_dDP = """
            CREATE TABLE IF NOT EXISTS tbLogDP(
            
            codDP INTEGER,
            nmDP VARCHAR(100),
            endereco VARCHAR(200),
            dtCarga DATETIME

        )
        """
        self.conexao.execute(qry_dDP)

        tbLogDP_codDP_IDX = (
            "CREATE INDEX IF NOT EXISTS tbLogDP_codDP_IDX ON tbLogDP (codDP);"
        )
        self.conexao.execute(tbLogDP_codDP_IDX)

    def insert(self, tbLogDP):
        self.conexao.executemany(
            """ INSERT INTO tbLogDP (codDP, nmDP, endereco, dtCarga) VALUES (?,?,?,?)""",
            tbLogDP.values.tolist(),
        )
