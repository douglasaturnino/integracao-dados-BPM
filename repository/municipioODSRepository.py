from config.conexaoODS import DBConnectionODS


class DBConnectionMunicipioODS(DBConnectionODS):
    def insert(self, dadosIBGE):
        dadosIBGE.to_sql("tbLogMunic", self.conexao, if_exists="append")
