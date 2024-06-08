from config.conexaoDW import DBConnectionDW
import pandas as pd


class DBConnectionPeriodoDW(DBConnectionDW):
    def create(self):
        qry_dPeriodo = """
            CREATE TABLE IF NOT EXISTS dPeriodo(
            
            idPeriodo INTEGER PRIMARY KEY AUTOINCREMENT,
            data DATETIME,
            mes INTERGER,
            ano INTEGER,
            trimestre INTEGER,
            semestre INTEGER
        )
        """
        # executar a criação da dPeriodo
        self.conexao.execute(qry_dPeriodo)

        # definição da qry de criação de index idPeriodo
        dPeriodo_idPeriodo_IDX = (
            "CREATE INDEX IF NOT EXISTS dPeriodo_idPeriodo_IDX ON dPeriodo (idPeriodo)"
        )

        # definição da qry de criação de index codPeriodo
        dPeriodo_IDX = "CREATE INDEX IF NOT EXISTS dPeriodo_IDX ON dPeriodo (mes, ano, trimestre,semestre)"

        # executar a criação do index idPeriodo
        self.conexao.execute(dPeriodo_idPeriodo_IDX)

        # executar a criação do index dPeriodo_IDX
        self.conexao.execute(dPeriodo_IDX)

    def select(self):
        qry_dados_dPeriodo = """
            WITH data(d) AS
            (
                VALUES('2018-01-01')
                UNION ALL 
                SELECT date(d,'+1 month')
                FROM DATA
                WHERE d < date('now')
            )
            SELECT 
                strftime('%d/%m/%Y', d) AS DATA,
                cast(strftime('%m',d) AS integer) AS mes,
                cast(strftime('%Y',d) AS integer) AS ano,
                CASE 
                    WHEN cast(strftime('%m',d) AS integer) IN (1,2,3) THEN 1
                    WHEN cast(strftime('%m',d) AS integer) IN (4,5,6) THEN 2
                    WHEN cast(strftime('%m',d) AS integer) IN (7,8,9) THEN 3
                                                                    ELSE 4
                END AS trimestre,
                CASE
                    WHEN cast(strftime('%m',d) AS integer) IN (1,2,3,4,5,6) THEN 1
                                                                            ELSE 2
                END AS semestre
            FROM data;
            """
        # Carregando dados no DF dPeriodo
        dPeriodo = pd.read_sql(qry_dados_dPeriodo, self.conexao)
        return dPeriodo

    def insert(self, dPeriodo):
        # Executar a carga de dados na dPeriodo
        self.conexao.executemany(
            """ INSERT INTO dPeriodo (data, mes, ano, trimestre, semestre) VALUES (?,?,?,?,?)""",
            dPeriodo.values.tolist(),
        )

    def delete(self):
        # Deletar os dados atuais
        self.conexao.execute("DELETE FROM dPeriodo")

        # Reiniciando o autoincremental da Chave
        self.conexao.execute("UPDATE sqlite_sequence SET seq=0 WHERE name='dPeriodo'")
