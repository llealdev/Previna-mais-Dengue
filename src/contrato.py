import pandera as pa 
from pandera.typing import Series
import pandas as pd


class ContratosDadosDengue(pa.DataFrameModel):

    id_agravo: Series[str]
    dt_invest: Series[pa.DateTime]
    dt_notific: Series[pa.DateTime]
    dt_sin_pri: Series[pa.DateTime]
    id_ocupa_n: Series[int] = pa.Field(nullable=True) 
    febre: Series[int] = pa.Field(isin=[1, 2]) 
    mialgia: Series[int] = pa.Field(isin=[1,2])    
    cefaleia: Series[int] = pa.Field(isin=[1, 2])  
    exantema: Series[int] = pa.Field(isin=[1, 2])  
    vomito: Series[int] = pa.Field(isin=[1, 2]) 
    nausea: Series[int] = pa.Field(isin=[1, 2])
    dor_costas: Series[int] = pa.Field(isin=[1, 2])  
    conjuntvit: Series[int] = pa.Field(isin=[1, 2]) 
    artrite: Series[int] = pa.Field(isin=[1, 2]) 
    artralgia: Series[int] = pa.Field(isin=[1, 2])  
    petequia_n: Series[int] = pa.Field(isin=[1, 2])  
    leucopenia: Series[int] = pa.Field(isin=[1, 2])  
    laco: Series[int] = pa.Field(isin=[1, 2])  
    dor_retro: Series[int] = pa.Field(isin=[1, 2])  
    diabetes: Series[int] = pa.Field(isin=[1, 2])  
    hematolog: Series[int] = pa.Field(isin=[1, 2])  
    hepatopat: Series[int] = pa.Field(isin=[1, 2])  
    renal: Series[int] = pa.Field(isin=[1, 2])  
    hipertensa: Series[int] = pa.Field(isin=[1, 2])  
    auto_imune: Series[int] = pa.Field(isin=[1, 2])  
    resul_soro: Series[int] = pa.Field(isin=[1, 2], nullable=True)  
    resul_ns1: Series[int] = pa.Field(isin=[1, 2], nullable=True)  
    resul_vi_n: Series[int] = pa.Field(isin=[1, 2], nullable=True)  
    resul_pcr_: Series[int] = pa.Field(isin=[1, 2], nullable=True)  
    sorotipo: Series[str] = pa.Field(nullable=True) 
    hospitaliz: Series[int] = pa.Field(isin=[1, 2])  
    tpautocto: Series[int] = pa.Field(isin=[1, 2, 3, 4])  
    coufinf: Series[str] = pa.Field(nullable=True)  
    comuninf: Series[str] = pa.Field(nullable=True)  
    classi_fin: Series[int] = pa.Field(isin=[1, 2, 3, 4, 5, 10])  
    criterio: Series[int] = pa.Field(isin=[1, 2, 3])  
    evolucao: Series[int] = pa.Field(isin=[1, 2, 3, 4, 5])  
    dt_obito: Series[pa.DateTime] = pa.Field(nullable=True)  
    cs_sexo: Series[str] = pa.Field(isin=["M", "F", "I", ""])  

    class Config:
        strict = True
        coerce = True

    @pa.check(
        "id_agravo",
        name="Checagem do ID do Agravo",
        error= "O ID do agravo deve começar com 'A90' ou 'A91' (dengue)."
    )
    def validar_id_agravo(cls, id_agravo:Series[str]) -> Series[bool]:
        return id_agravo.str.startswith(("A90", "A91"))
    

    @pa.check(
        "dt_notific",
        name="Validação da Data de Notificação",
        error="A data de notificação não pode ser posterior à data atual."
    )
    def valida_data_notificação(cls, dt_notific: Series[pa.DateTime]) -> Series[bool]:
        return dt_notific <= pd.Timestamp.now()

    @pa.check(
        "dt_sin_pri",
        name="Validação da Data de Início  dos Sintomas",
        erro="A data de início dos sintomas não pode ser posterior à data de notificação."
    )
    def valida_data_sintomas(cls, dt_sin_pri: Series[pa.DateTime], dt_notific:Series[pa.DateTime]) ->  Series[bool]:
        dt_sin_pri <= dt_notific