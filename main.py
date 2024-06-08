from dadosIBGE import dadosIBGE
from dadosDP import dadosDP
from dadosResponsavel import dadosResponsavel
from dadosBPM import dadosBPM
from dadosAreaBPM import dadosAreaBPM
from dadosOcorrencia import dadosOcorrencia
from dadosPeriodo import dadosPeriodo


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
