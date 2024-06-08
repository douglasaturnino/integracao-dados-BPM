from config.conexaoDW import DBConnectionDW
import pandas as pd


class DBConnectionBPMDW(DBConnectionDW):
    def select(self):
        qry_dBPM = """
            CREATE TABLE IF NOT EXISTS dBPM(
            
            idBPM INTEGER PRIMARY KEY AUTOINCREMENT,
            codBPM INTEGER,
            nmBPM VARCHAR (7),
            enderecoBPM VARCHAR (200),
            areaBPM REAL (5,2)
        )
        """
        dBPM = pd.read_sql(qry_dBPM, self.conexao)

        # definição da qry de criação de index idBPM
        dBPM_idBPM_IDX = "CREATE INDEX IF NOT EXISTS dBPM_idBPM_IDX ON dBPM (idBPM)"

        # definição da qry de criação de index codBPM
        dBPM_codBPM_IDX = "CREATE INDEX IF NOT EXISTS dBPM_codBPM_IDX ON dBPM (codBPM)"

        # executar a criação do index idBPM
        self.conexao.execute(dBPM_idBPM_IDX)

        # executar a criação do index codBPM
        self.conexao.execute(dBPM_codBPM_IDX)
        return dBPM

    def insert(self, dBPM):
        # Inserindo registros na tabela dBPM
        self.conexao.executemany(
            """INSERT INTO dBPM (codBPM, nmBPM, enderecoBPM, areaBPM) VALUES (?,?,?,?)""",
            dBPM.values.tolist(),
        )

    def delete(self):
        # Deletar os dados atuais
        self.conexao.execute("DELETE FROM dBPM")

        # Reiniciando o autoincremental da Chave
        self.conexao.execute("UPDATE sqlite_sequence SET seq=0 WHERE name='dBPM'")
