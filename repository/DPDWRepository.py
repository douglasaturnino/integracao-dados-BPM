from config.conexaoDW import DBConnectionDW


class DBConnectionDPDW(DBConnectionDW):
    def create(self):
        qry_dDP = """
            CREATE TABLE IF NOT EXISTS dDP (
            idDP INTEGER PRIMARY KEY AUTOINCREMENT,
            codDP INTEGER,
            nmDP VARCHAR (100),
            endereco VARCHAR (200),
            nmResponsavel VARCHAR(100)

            )
        """
        self.conexao.execute(qry_dDP)
        dDP_codDP_IDX = "CREATE INDEX IF NOT EXISTS dDP_codDP_IDX ON dDP (codDP)"
        self.conexao.execute(dDP_codDP_IDX)

    def insert(self, dDP):
        # Inserindo registros na tabela dDP
        self.conexao.executemany(
            """ INSERT INTO dDP (codDP, nmDP, endereco, nmResponsavel) VALUES (?,?,?,?)""",
            dDP.values.tolist(),
        )

    def delete(self):
        self.conexao.execute("DELETE FROM dDP")
        self.conexao.execute("UPDATE sqlite_sequence SET seq=0 WHERE name='dDP'")
