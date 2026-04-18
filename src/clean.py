import pandas as pd
 
 
REQUIRED_COLUMNS = {
    "directorio",
    "secuencia_encuesta",
    "secuencia_p",
    "orden",
    "nivel_parentesco_jefe_hogar",
    "sexo",
    "edad",
    "factor_de_expansion",
    "afiliado_seguridad_social_de_salud",
    "problema_de_salud_ultimos_30d",
    "que_hizo_problema_salud",
    "personas_permanece_entre_semana",
    "consume_desayuno",
    "paga_por_alimentacion",
    "alfabetizado",
    "actualmente_estudia",
    "nivel_educativo_alcanzado",
    "nivel_educativo_aprobado",
    "nivel_educativo_matriculado",
    "nivel_educativo_en_curso",
    "recibe_plantel_edu_alimentos",
    "actividad_de_mayor_tiempo_sp",
    "actividad_pagada_sp",
    "trabajo_externo_por_ingresos_sp",
    "trabajo_sin_pago_sp",
    "disponibilidad_posible_trabajo_sp",
    "cuantas_semanas_busco_trabajo",
    "cotiza_fondo_de_pensiones",
    "pensiones_modalidad_presencial",
    "pensiones_modalidad_virtual",
    "pensiones_modalidad_mixta",
    "comunicacion_maestros_sp",
    "celular_inteligente"
}

OPTIONAL_COLUMNS = {
    "actividad_dedicada_del_negocio"
}

EXPECTED_COLUMNS = REQUIRED_COLUMNS | OPTIONAL_COLUMNS
 
 
def normalize_columns(df):
    """
    Normaliza los nombres de columnas: strip, lowercase, sin espacios ni acentos.
    Luego verifica que estén presentes todas las columnas esperadas.
    """
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
    )
 
    # Renombrar columna duplicada que pandas agrega automáticamente
    if "nivel_educativo_aprobado.1" in df.columns:
        df = df.rename(columns={"nivel_educativo_aprobado.1": "nivel_educativo_matriculado"})
 
    missing_required = REQUIRED_COLUMNS - set(df.columns)
    if missing_required:
        raise ValueError(f"Columnas faltantes en el dataset: {missing_required}")

    for optional in OPTIONAL_COLUMNS:
        if optional not in df.columns:
            df[optional] = pd.NA

    # Limpieza de texto en columna libre
    df["actividad_dedicada_del_negocio"] = (
        df["actividad_dedicada_del_negocio"]
        .astype(str)
        .str.strip()
    )
 
    return df
 
 
# Rangos válidos por columna según el diccionario de respuestas
 
VALID_RANGES = {
    "nivel_parentesco_jefe_hogar":    (1, 14),
    "sexo":                           (1, 2),
    "afiliado_seguridad_social_de_salud": (1, 9),
    "problema_de_salud_ultimos_30d":  (1, 2),
    "que_hizo_problema_salud":        (1, 8),
    "personas_permanece_entre_semana":(1, 8),
    "consume_desayuno":               (1, 2),
    "paga_por_alimentacion":          (1, 4),
    "alfabetizado":                   (1, 2),
    "actualmente_estudia":            (1, 2),
    "nivel_educativo_alcanzado":      (1, 13),
    "nivel_educativo_aprobado":       (1, 13),
    "nivel_educativo_matriculado":    (1, 8),
    "nivel_educativo_en_curso":       (1, 13),
    "recibe_plantel_edu_alimentos":   (1, 2),
    "actividad_de_mayor_tiempo_sp":   (1, 6),
    "actividad_pagada_sp":            (1, 2),
    "trabajo_externo_por_ingresos_sp":(1, 2),
    "trabajo_sin_pago_sp":            (1, 2),
    "disponibilidad_posible_trabajo_sp": (1, 2),
    "cotiza_fondo_de_pensiones":      (1, 3),
    "pensiones_modalidad_presencial": (1, 2),
    "pensiones_modalidad_virtual":    (1, 2),
    "pensiones_modalidad_mixta":      (1, 2),
    "comunicacion_maestros_sp":       (1, 2),
    "celular_inteligente":            (1, 2),
}
 
 
def validate_column_ranges(df):
    """
    Invalida valores fuera del rango permitido por el diccionario de respuestas.
    Los valores inválidos se reemplazan con pd.NA.
    """
    for col, (min_val, max_val) in VALID_RANGES.items():
        if col not in df.columns:
            continue
        mask = df[col].notna() & ~df[col].between(min_val, max_val)
        invalid_count = mask.sum()
        if invalid_count > 0:
            print(f"  [WARN] {col}: {invalid_count} valor(es) fuera de rango [{min_val}-{max_val}] → reemplazados con NA")
        df.loc[mask, col] = pd.NA
    return df
 
 
def validate_numeric_columns(df):
    """
    Valida columnas numéricas continuas:
    - edad: entre 0 y 110
    - cuantas_semanas_busco_trabajo: no negativo
    - factor_de_expansion: mayor que 0
    """
    if "edad" in df.columns:
        mask = df["edad"].notna() & ~df["edad"].between(0, 110)
        if mask.sum() > 0:
            print(f"  [WARN] edad: {mask.sum()} valor(es) fuera de rango [0-110] → reemplazados con NA")
        df.loc[mask, "edad"] = pd.NA
 
    if "cuantas_semanas_busco_trabajo" in df.columns:
        mask = df["cuantas_semanas_busco_trabajo"].notna() & (df["cuantas_semanas_busco_trabajo"] < 0)
        if mask.sum() > 0:
            print(f"  [WARN] cuantas_semanas_busco_trabajo: {mask.sum()} valor(es) negativos → reemplazados con NA")
        df.loc[mask, "cuantas_semanas_busco_trabajo"] = pd.NA
 
    if "factor_de_expansion" in df.columns:
        mask = df["factor_de_expansion"].notna() & (df["factor_de_expansion"] <= 0)
        if mask.sum() > 0:
            print(f"  [WARN] factor_de_expansion: {mask.sum()} valor(es) <= 0 → reemplazados con NA")
        df.loc[mask, "factor_de_expansion"] = pd.NA
 
    return df
 
 
def validate_no_duplicates(df):
    """
    Detecta filas completamente duplicadas y las elimina.
    Una fila duplicada se define como igual en directorio + secuencia_encuesta + orden.
    """
    key_cols = ["directorio", "secuencia_encuesta", "orden"]
    if "anio" in df.columns:
        key_cols.append("anio")
    key_cols = [c for c in key_cols if c in df.columns]
 
    if key_cols:
        dupes = df.duplicated(subset=key_cols, keep="first").sum()
        if dupes > 0:
            print(f"  [WARN] {dupes} fila(s) duplicadas por clave {key_cols} -> eliminadas")
            df = df.drop_duplicates(subset=key_cols, keep="first")
    return df
 
 
def run_cleaning(df):
    """
    Orquestador del proceso de limpieza. Ejecuta en orden:
    1. normalize_columns     — normaliza nombres y verifica columnas esperadas
    2. validate_column_ranges — invalida valores fuera del rango del diccionario
    3. validate_numeric_columns — valida edad, semanas y factor de expansión
    4. validate_no_duplicates — elimina filas duplicadas por clave
    """
    print("\n=== CLEANING ===")
 
    df = normalize_columns(df)
    df = validate_column_ranges(df)
    df = validate_numeric_columns(df)
    df = validate_no_duplicates(df)
 
    print("=== CLEANING FINISHED ===\n")
 
    return df
 