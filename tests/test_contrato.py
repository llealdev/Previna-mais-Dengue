import pandas as pd
import numpy as np
import pandera as pa
import pytest

from src.contrato import ContratosDadosDengue  

def test_contratos_dados_dengue_valido():
    df_test = pd.DataFrame({
        "id_agravo": ["A90X123", "A91Y456"],
        "dt_invest": [pd.Timestamp("2023-01-01"), pd.Timestamp("2023-01-02")],
        "dt_notific": [pd.Timestamp("2023-01-01"), pd.Timestamp("2023-01-02")],
        "dt_sin_pri": [pd.Timestamp("2023-01-01"), pd.Timestamp("2023-01-02")],
        "id_ocupa_n": [1, 2],
        "febre": [1, 2],
        "mialgia": [1, 2],
        "cefaleia": [1, 2],
        "exantema": [1, 2],
        "vomito": [1, 2],
        "nausea": [1, 2],
        "dor_costas": [1, 2],
        "conjuntvit": [1, 2],
        "artrite": [1, 2],
        "artralgia": [1, 2],
        "petequia_n": [1, 2],
        "leucopenia": [1, 2],
        "laco": [1, 2],
        "dor_retro": [1, 2],
        "diabetes": [1, 2],
        "hematolog": [1, 2],
        "hepatopat": [1, 2],
        "renal": [1, 2],
        "hipertensa": [1, 2],
        "auto_imune": [1, 2],
        "resul_soro": [1, np.nan], 
        "resul_ns1": [np.nan, 2], 
        "resul_vi_n": [np.nan, 2], 
        "resul_pcr_": [np.nan, 2], 
        "sorotipo": ["Dengue", np.nan], 
        "hospitaliz": [1, 2],
        "tpautocto": [1, 3], 
        "coufinf": ["info", np.nan], 
        "comuninf": ["info", np.nan], 
        "classi_fin": [1, 3], 
        "criterio": [1, 3], 
        "evolucao": [1, 4], 
        "dt_obito": [np.nan, pd.Timestamp("2023-01-02")], 
        "cs_sexo": ["M", "F"]
    })

    ContratosDadosDengue.validate(df_test)


def test_id_agravo_invalido():
    df_test = pd.DataFrame({
        "id_agravo": ["B90X123", "C91Y456"],
    })

    with pytest.raises(pa.errors.SchemaError):
        ContratosDadosDengue.validate(df_test)

def test_data_notificacao_futura():
    df_test = pd.DataFrame({
        "id_agravo": ["A90X123"],
        "dt_notific": [pd.Timestamp("2100-01-01")], 
    })

    with pytest.raises(pa.errors.SchemaError):
        ContratosDadosDengue.validate(df_test)

def test_data_sintomas_invalida():
    df_test = pd.DataFrame({
        "id_agravo": ["A90X123"],
        "dt_notific": [pd.Timestamp("2023-01-01")],
        "dt_sin_pri": [pd.Timestamp("2023-01-02")] 
    })

    with pytest.raises(pa.errors.SchemaError):
        ContratosDadosDengue.validate(df_test)
