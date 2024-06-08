from config.conexaoODS import DBConnectionODS
import pandas as pd


class DBConnectionOcorrenciasODS(DBConnectionODS):
    def create(self):
        qry_tbLogOcorrencias = """
            CREATE TABLE IF NOT EXISTS tbLogOcorrencias(
            
            codDP INTEGER,
            codBPM INTEGER,
            ano INTEGER,
            mes INTEGER,
            mes_ano CHAR(7),
            regiao INTEGER,
            codMunicIBGE INTEGER,
            ocorrencias VARCHAR(100),
            qtde INTEGER,
            dtCarga DATETIME
        )
        """
        self.conexao.execute(qry_tbLogOcorrencias)

        tbLogOcorrencias_IDX = "CREATE INDEX IF NOT EXISTS tbLogOcorrencias_IDX ON tbLogOcorrencias (codDP, codBPM, codMunicIBGE)"
        self.conexao.execute(tbLogOcorrencias_IDX)

    def select(self):
        # Definição da query de carga de dados
        qry_fOcorrencias = """

            SELECT
                a.codDP,
                a.codBPM,
                a.ano,
                a.mes,
                a.regiao,
                a.codMunicIBGE,
                a.ocorrencias,
                a.qtde
            FROM tbLogOcorrencias a
            WHERE a.dtCarga = (
                                SELECT MAX(dtCarga)
                                FROM tbLogOcorrencias
                                )
        """

        # popular o dataframe
        fOcorrencias = pd.read_sql(qry_fOcorrencias, self.conexao)
        return fOcorrencias

    def insert(self, tbLogOcorrencias):
        self.conexao.executemany(
            """ 
            INSERT INTO tbLogOcorrencias (codDP, codBPM, ano, mes, mes_ano, regiao, codMunicIBGE, ocorrencias, qtde,  dtCarga) 
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            tbLogOcorrencias.values.tolist(),
        )
