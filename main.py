from src.dadosIBGE import dadosIBGE
from src.dadosDP import dadosDP
from src.dadosResponsavel import dadosResponsavel
from src.dadosBPM import dadosBPM
from src.dadosAreaBPM import dadosAreaBPM
from src.dadosOcorrencia import dadosOcorrencia
from src.dadosPeriodo import dadosPeriodo


dadosIBGE().create_dbODS()
dadosDP().create_dbODS()
dadosResponsavel().create_dbODS()
dadosBPM().create_dbODS()
dadosAreaBPM().create_dbODS()
dadosOcorrencia().create_dbODS()

dadosDP().create_dbDW()
dadosBPM().create_dbDW()
dadosPeriodo().create_dbDW()
dadosOcorrencia().create_dbDW()
