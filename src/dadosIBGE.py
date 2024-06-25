from datetime import datetime

from repository.municipioODSRepository import DBConnectionMunicipioODS
from repository.municipioDWRepository import DBConnectionMunicipioDW
import pandas as pd


class dadosIBGE:
    def __init__(self) -> None:
        self.url = "https://www.ibge.gov.br/explica/codigos-dos-municipios.php#RJ"
        self.dadosIBGE = self.getDados()
        self.add_date()

    def getDados(self) -> pd.DataFrame:
        dadosIBGE = pd.read_html(self.url, match="Municípios do Rio de Janeiro")[0]

        # Renomear colunas do df
        dadosIBGE = dadosIBGE.rename(
            columns={"Municípios do Rio de Janeiro": "nmMunic", "Códigos": "codMunic"}
        )

        # Alterando o nome do index
        dadosIBGE.index.name = "idMunic"

        # Alterando o index para comelar em 1
        dadosIBGE.index = dadosIBGE.index + 1
        return dadosIBGE

    def add_date(self) -> None:
        # Adiciona uma coluna de data e hora decarga
        dtCarga = datetime.today().strftime("%d/%m/%Y %H:%M")
        self.dadosIBGE["dtCarga"] = dtCarga

    def create_dbODS(self):
        with DBConnectionMunicipioODS() as db:
            db.insert(self.dadosIBGE)
            print("----------------------------------------------------")
            print(
                f"Carga Finalizada! {len(self.dadosIBGE )} registros inseridos na tbLogMunic\n"
            )

    def create_dbDW(self):
        # Selecionar somente as colunas para criação da dMunicipio
        dadosIBGE = self.dadosIBGE[["nmMunic", "codMunic"]].copy()

        with DBConnectionMunicipioDW() as db:
            db.insert(dadosIBGE)
            print(
                f"Carga Finalizada! {len(self.dadosIBGE )} registros inseridos na dMunicipio\n"
            )

    def run(self):
        self.add_date()
        self.create_dbODS()
        self.create_dbDW()


if __name__ == "__main__":
    dados = dadosIBGE()

    dados.run()
