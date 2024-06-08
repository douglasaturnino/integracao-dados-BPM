from config.conexaoDW import DBConnectionDW


class DBConnectionMunicipioDW(DBConnectionDW):
    def insert(self, dadosIBGE):
        dadosIBGE.to_sql("dMunicipio", self.conexao, if_exists="replace")
