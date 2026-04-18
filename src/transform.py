import pandas as pd
 
from clean import run_cleaning
from dictionaries import Column_translations
 
 
# -----------------------------
# TRANSLATE
# -----------------------------
def translate_columns(df):
    for column, mapping in Column_translations.items():
        if column in df.columns:
            df[column] = df[column].map(mapping)
    return df
 
 
# -----------------------------
# REEMPLAZAR NA
# -----------------------------
def replace_na(df):
    """
    Reemplaza los NA del dataframe por cadenas vacías en columnas de texto
    y deja pd.NA en columnas numéricas, para evitar que NA sea tratado
    como una categoría en las dimensiones.
    """
    numeric_cols = ["edad", "factor_de_expansion", "cuantas_semanas_busco_trabajo", "anio"]
 
    for col in df.columns:
        if col in numeric_cols:
            continue
        df[col] = df[col].fillna("")
 
    return df
 
 
# -----------------------------
# DIMENSION DEMOGRAFIA
# -----------------------------
def transform_demografia(df):
 
    dim_demografia = (
        df[[
            "sexo",
            "edad",
            "rango_edad",
            "nivel_parentesco_jefe_hogar"
        ]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
 
    dim_demografia["demografia_sk"] = dim_demografia.index + 1
 
    return dim_demografia
 
 
# -----------------------------
# DIMENSION EDUCACION
# -----------------------------
def transform_educacion(df):
 
    dim_educacion = (
        df[[
            "alfabetizado",
            "actualmente_estudia",
            "nivel_educativo_alcanzado",
            "nivel_educativo_aprobado",
            "nivel_educativo_matriculado",
            "nivel_educativo_en_curso",
            "recibe_plantel_edu_alimentos",
            "pensiones_modalidad_presencial",
            "pensiones_modalidad_virtual",
            "pensiones_modalidad_mixta",
            "comunicacion_maestros_sp"
        ]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
 
    dim_educacion["educacion_sk"] = dim_educacion.index + 1
 
    return dim_educacion
 
 
# -----------------------------
# DIMENSION TECNOLOGIA
# -----------------------------
def transform_tecnologia(df):
 
    dim_tecnologia = (
        df[["celular_inteligente"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
 
    dim_tecnologia["tecnologia_sk"] = dim_tecnologia.index + 1
 
    return dim_tecnologia
 
 
# -----------------------------
# DIMENSION SALUD
# -----------------------------
def transform_salud(df):
 
    dim_salud = (
        df[[
            "afiliado_seguridad_social_de_salud",
            "problema_de_salud_ultimos_30d",
            "que_hizo_problema_salud"
        ]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
 
    dim_salud["salud_sk"] = dim_salud.index + 1
 
    return dim_salud
 
 
# -----------------------------
# DIMENSION TRABAJO
# -----------------------------
def transform_trabajo(df):
 
    trabajo_cols = [
        "actividad_de_mayor_tiempo_sp",
        "actividad_pagada_sp",
        "trabajo_externo_por_ingresos_sp",
        "trabajo_sin_pago_sp",
        "disponibilidad_posible_trabajo_sp",
        "cotiza_fondo_de_pensiones"
    ]
    if "actividad_dedicada_del_negocio" in df.columns:
        trabajo_cols.append("actividad_dedicada_del_negocio")
 
    dim_trabajo = (
        df[trabajo_cols]
        .drop_duplicates()
        .reset_index(drop=True)
    )
 
    dim_trabajo["trabajo_sk"] = dim_trabajo.index + 1
 
    return dim_trabajo
 
 
# -----------------------------
# DIMENSION TIEMPO
# -----------------------------
def transform_tiempo(df):
 
    dim_tiempo = (
        df[["anio"]]
        .drop_duplicates()
        .sort_values("anio")
        .reset_index(drop=True)
    )

    dim_tiempo["tiempo_sk"] = dim_tiempo.index + 1
    dim_tiempo["periodo"] = dim_tiempo["anio"].astype(str)
    dim_tiempo["descripcion"] = dim_tiempo["anio"].apply(lambda x: f"Año {x}")
 
    return dim_tiempo
 
 
# -----------------------------
# FACT TABLE
# -----------------------------
def fact_persona(
        df,
        dim_demografia,
        dim_educacion,
        dim_tecnologia,
        dim_salud,
        dim_trabajo,
        dim_tiempo):
 
    df = df.merge(
        dim_demografia,
        on=["sexo", "edad", "rango_edad", "nivel_parentesco_jefe_hogar"],
        how="left"
    )
 
    df = df.merge(
        dim_educacion,
        on=[
            "alfabetizado",
            "actualmente_estudia",
            "nivel_educativo_alcanzado",
            "nivel_educativo_aprobado",
            "nivel_educativo_matriculado",
            "nivel_educativo_en_curso",
            "recibe_plantel_edu_alimentos",
            "pensiones_modalidad_presencial",
            "pensiones_modalidad_virtual",
            "pensiones_modalidad_mixta",
            "comunicacion_maestros_sp"
        ],
        how="left"
    )
 
    df = df.merge(
        dim_tecnologia,
        on=["celular_inteligente"],
        how="left"
    )
 
    df = df.merge(
        dim_salud,
        on=[
            "afiliado_seguridad_social_de_salud",
            "problema_de_salud_ultimos_30d",
            "que_hizo_problema_salud"
        ],
        how="left"
    )
 
    trabajo_on = [
        "actividad_de_mayor_tiempo_sp",
        "actividad_pagada_sp",
        "trabajo_externo_por_ingresos_sp",
        "trabajo_sin_pago_sp",
        "disponibilidad_posible_trabajo_sp",
        "cotiza_fondo_de_pensiones"
    ]
    if "actividad_dedicada_del_negocio" in df.columns:
        trabajo_on.append("actividad_dedicada_del_negocio")
 
    df = df.merge(
        dim_trabajo,
        on=trabajo_on,
        how="left"
    )
 
    df = df.merge(
        dim_tiempo,
        on=["anio"],
        how="left"
    )
 
    fact_table = df[[
        "demografia_sk",
        "educacion_sk",
        "tecnologia_sk",
        "salud_sk",
        "trabajo_sk",
        "tiempo_sk",
        "factor_de_expansion",
        "poverty_rate",
        "unemployment_rate",
        "gdp_per_capita",
        "gini_index"
    ]].copy()
 
    fact_table["persona_fact_id"] = fact_table.index + 1
 
    return fact_table
 
 
# -----------------------------
# ORQUESTADOR
# -----------------------------
def transform_data(df, df_api):
 
    print("\n=== TRANSFORMING ===")
 
    # Limpieza y validaciones
    df = run_cleaning(df)
 
    # Traducir códigos numéricos a sus etiquetas
    df = translate_columns(df)
    
    df = df.merge(df_api, left_on="anio", right_on="year", how="left")
 
    # Columna derivada: rango de edad
    def rango_edad(x):
        if pd.isna(x):
            return pd.NA
        elif x <= 18:
            return "0-18"
        elif x <= 30:
            return "19-30"
        elif x <= 60:
            return "31-60"
        else:
            return "61+"
 
    df["rango_edad"] = df["edad"].apply(rango_edad)
 
    # Columna derivada: tiene trabajo (1/0)
    df["tiene_trabajo"] = df["actividad_pagada_sp"].apply(
        lambda x: 1 if x == "Sí" else 0
    )
 
    # Reemplazar NA por "" en columnas categóricas
    df = replace_na(df)
 
    # Construir dimensiones
    dim_demografia  = transform_demografia(df)
    dim_educacion   = transform_educacion(df)
    dim_tecnologia  = transform_tecnologia(df)
    dim_salud       = transform_salud(df)
    dim_trabajo     = transform_trabajo(df)
    dim_tiempo      = transform_tiempo(df)
 
    # Construir tabla de hechos
    fact_table = fact_persona(
        df,
        dim_demografia,
        dim_educacion,
        dim_tecnologia,
        dim_salud,
        dim_trabajo,
        dim_tiempo
    )
 
    print("=== TRANSFORM FINISHED ===\n")
 
    return {
        "dim_demografia":  dim_demografia,
        "dim_educacion":   dim_educacion,
        "dim_tecnologia":  dim_tecnologia,
        "dim_tiempo":      dim_tiempo,
        "dim_salud":       dim_salud,
        "dim_trabajo":     dim_trabajo,
        "fact_persona":    fact_table
    }
 