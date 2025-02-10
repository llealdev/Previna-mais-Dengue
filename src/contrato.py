import pandera as pa
from pandera.typing import Series
import pandas as pd

class ContratosDadosDengue(pa.DataFrameModel):
    id_agravo: pa.typing.Series[str] = pa.Field(nullable=True)
    dt_invest: Series[str] = pa.Field(nullable=True)
    dt_notific: Series[str] = pa.Field(nullable=True)
    dt_nasc: Series[str] = pa.Field(nullable=True)
    dt_sin_pri: Series[str] = pa.Field(nullable=True)
    id_ocupa_n: Series[str] = pa.Field(nullable=True)
    febre: Series[str] = pa.Field(isin=["1", "2", "", " "], nullable=True)
    mialgia: Series[str] = pa.Field(nullable=True)
    cefaleia: Series[str] = pa.Field(nullable=True)
    exantema: Series[str] = pa.Field(nullable=True)
    vomito: Series[str] = pa.Field(nullable=True)
    nausea: Series[str] = pa.Field(nullable=True)
    dor_costas: Series[str] = pa.Field(nullable=True)
    conjuntvit: Series[str] = pa.Field(nullable=True)
    artrite: Series[str] = pa.Field(nullable=True)
    artralgia: Series[str] = pa.Field(nullable=True)
    petequia_n: Series[str] = pa.Field(nullable=True)
    leucopenia: Series[str] = pa.Field(nullable=True)
    laco: Series[str] = pa.Field(nullable=True)
    dor_retro: Series[str] = pa.Field(nullable=True)
    diabetes: Series[str] = pa.Field(nullable=True)
    hematolog: Series[str] = pa.Field(nullable=True)
    hepatopat: Series[str] = pa.Field(nullable=True)
    renal: Series[str] = pa.Field(nullable=True)
    hipertensa: Series[str] = pa.Field(nullable=True)
    auto_imune: Series[str] = pa.Field(nullable=True)
    resul_soro: Series[str] = pa.Field(nullable=True)
    resul_ns1: Series[str] = pa.Field(nullable=True)
    resul_pcr_: Series[str] = pa.Field(nullable=True)  
    sorotipo: Series[str] = pa.Field(nullable=True)  
    hospitaliz: Series[str] = pa.Field(nullable=True)  
    tpautocto: Series[str] = pa.Field(nullable=True)  
    coufinf: Series[str] = pa.Field(nullable=True)  
    comuninf: Series[str] = pa.Field(nullable=True)  
    classi_fin: Series[str] = pa.Field(nullable=True)  
    criterio: Series[str] = pa.Field(nullable=True)  
    evolucao: Series[str] = pa.Field(nullable=True)  
    dt_obito: Series [str] = pa.Field(nullable=True) 
    cs_sexo: Series[str] = pa.Field(nullable=True)  

    class Config:
        strict = True
        coerce = True

@pa.check("dt_invest", "dt_notific", "dt_nasc", "dt_sin_pri")
def check_data_timezone(self, series: Series[pa.DateTime]) -> Series[pa.DateTime]:
    return series.dt.tz_localize(None)  

@pa.check(
    "id_agravo",
    name="Checagem do ID do Agravo",
    error="O ID do agravo deve começar com 'A90' ou 'A91' (dengue)."
)
def validar_id_agravo(cls, id_agravo: Series[str]) -> Series[bool]:
    return id_agravo.str.startswith(("A90", "A91"), na=True)

@pa.check(
    "dt_notific",
    name="Validação da Data de Notificação",
    error="A data de notificação não pode ser posterior à data atual."
)
def valida_data_notificacao(cls, dt_notific: Series[pa.DateTime]) -> Series[bool]:
    return dt_notific <= pd.Timestamp.now(tz='UTC')

@pa.dataframe_check(name="Validação da Data de Início dos Sintomas")
def valida_data_sintomas(cls, df: pd.DataFrame) -> Series[bool]:
    if "dt_sin_pri" in df.columns and "dt_notific" in df.columns:
        return df["dt_sin_pri"] <= df["dt_notific"]
    return True  

@pa.dataframe_check(name="Cálculo da Idade do Paciente")
def calcular_idade(cls, df: pd.DataFrame) -> pd.DataFrame:
    if "dt_nasc" in df.columns and "dt_notific" in df.columns:

        df["dt_nasc"] = pd.to_datetime(df["dt_nasc"], errors='coerce')
        df["dt_notific"] = pd.to_datetime(df["dt_notific"], errors='coerce')

        idade = (df["dt_notific"] - df["dt_nasc"]).dt.days // 365
        df["pac_idade"] = idade
    else:
        df["pac_idade"] = None
    return df