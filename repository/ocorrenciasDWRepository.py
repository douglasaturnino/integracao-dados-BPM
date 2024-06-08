from config.conexaoDW import DBConnectionDW
import pandas as pd


class DBConnectionOcorrenciasDW(DBConnectionDW):
    def create(self):
        qry_fOcorrencias = """
            CREATE TABLE IF NOT EXISTS fOcorrencias(
            
            idDP INTEGER REFERENCES dDP (idDP) ON UPDATE NO ACTION ON DELETE NO ACTION,
            idBPM INTEGER REFERENCES dBPM (idBPM) ON UPDATE NO ACTION ON DELETE NO ACTION,
            idPeriodo INTEGER REFERENCES dPeriodo (idPeriodo) ON UPDATE NO ACTION ON DELETE NO ACTION,
            regiao INTEGER,
            idMunic INTEGER REFERENCES dMunicipio (idMunic) ON UPDATE NO ACTION ON DELETE NO ACTION,
            ocorrencias VARCHAR(100),
            qtde INTEGER
        )
        """

        self.conexao.execute(qry_fOcorrencias)

        # definição da qry de criação de index fOcorrencias
        fOcorrencias_IDX = "CREATE INDEX IF NOT EXISTS fOcorrencias_IDX ON fOcorrencias (idDP, idBPM, idPeriodo, idMunic )"

        # executar a criação do index fOcorrencias
        self.conexao.execute(fOcorrencias_IDX)

    def select(self):
        # definindo a query de carga incremental
        qry_carga_incremental_focorrencias = """ 
            SELECT
                b.idDP,
                c.idBPM,
                d.idPeriodo,
                a.regiao,
                e.idMunic,
                a.ocorrencias,
                a.qtde 
            FROM  tempOcorrencias a
            JOIN dDP b ON a.codDP = b.codDP
            JOIN dBPM c ON a.codBPM  = c.codBPM
            JOIN dPeriodo d ON (a.ano = d.ano) AND (a.mes = d.mes)
            JOIN dMunicipio e ON a.codMunicIBGE = e.codMunic
            LEFT JOIN fOcorrencias g ON (b.idDP  = g.idDP) 
                                    AND (c.idBPM = g.idBPM) 
                                    AND (d.idPeriodo =g.idPeriodo)
                                    AND (e.idMunic = g.idMunic)
            WHERE g.idDP IS NULL 
                AND g.idBPM  IS NULL
                AND g.idPeriodo IS NULL
                AND g.idMunic IS NULL
        """
        # criando um DF Incremental
        carga_fOcorrencias = pd.read_sql(
            qry_carga_incremental_focorrencias, self.conexao
        )
        return carga_fOcorrencias

    def insert(self, fOcorrencias):
        self.conexao.executemany(
            """ 
                   INSERT INTO fOcorrencias (idDP, idBPM, idPeriodo, regiao, idMunic, ocorrencias, qtde) 
                   VALUES (?,?,?,?,?,?,?)""",
            fOcorrencias.values.tolist(),
        )

    def update(self):
        # definindo a qry de verificação da existência de atualizações retroativas
        qry_atualizacao_fOcorrencias = """
            SELECT
                b.idDP,
                c.idBPM,
                d.idPeriodo,
                a.regiao,
                e.idMunic,
                a.ocorrencias,
                a.qtde 
            FROM  tempOcorrencias a
            JOIN dDP b ON a.codDP = b.codDP
            JOIN dBPM c ON a.codBPM  = c.codBPM
            JOIN dPeriodo d ON (a.ano = d.ano) AND (a.mes = d.mes)
            JOIN dMunicipio e ON a.codMunicIBGE = e.codMunic
            LEFT JOIN fOcorrencias g ON (b.idDP  = g.idDP) 
                                    AND (c.idBPM = g.idBPM) 
                                    AND (d.idPeriodo =g.idPeriodo)
                                    AND (e.idMunic = g.idMunic)
                                    AND (a.regiao = g.regiao)
                                    AND (a.ocorrencias = g.ocorrencias)
            WHERE a.qtde <> g.qtde
        """

        # Criando um DF de atualização
        atualiza_fOcorrencias = pd.read_sql(qry_atualizacao_fOcorrencias, self.conexao)

        # deletar a tabela temporaria
        self.conexao.execute("DROP TABLE tempOcorrencias")
        return atualiza_fOcorrencias
