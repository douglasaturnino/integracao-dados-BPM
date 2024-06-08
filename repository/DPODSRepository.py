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

    def insert(self, tbLogDP):
        self.conexao.executemany(
            """ INSERT INTO tbLogDP (codDP, nmDP, endereco, dtCarga) VALUES (?,?,?,?)""",
            tbLogDP.values.tolist(),
        )
