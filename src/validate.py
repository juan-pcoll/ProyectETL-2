import great_expectations as gx
import pandas as pd
 
 
def validate_data(tables):
 
    print("\n=== VALIDATION ===")
 
    context = gx.get_context(mode="ephemeral")
 
    _validate_fact_persona(context, tables["fact_persona"])
    _validate_dim_demografia(context, tables["dim_demografia"])
    _validate_dim_educacion(context, tables["dim_educacion"])
    _validate_dim_salud(context, tables["dim_salud"])
    _validate_dim_trabajo(context, tables["dim_trabajo"])
    _validate_dim_tiempo(context, tables["dim_tiempo"])
 
    print("=== VALIDATION FINISHED ===\n")
 
    return tables
 
 
def _run_suite(context, df, suite_name, expectations, critical_columns):
    """
    Corre un suite de expectations sobre un DataFrame.
    - Fallas en columnas críticas → detienen el pipeline (raise)
    - Fallas en columnas no críticas → solo loguean un warning
    """
    data_source = context.data_sources.add_pandas(name=suite_name)
    asset = data_source.add_dataframe_asset(name=suite_name)
    batch = asset.add_batch_definition_whole_dataframe(suite_name)
    batch_parameters = {"dataframe": df}
 
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    for exp in expectations:
        suite.add_expectation(exp)
 
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name=suite_name,
            data=batch,
            suite=suite
        )
    )
 
    results = validation_definition.run(batch_parameters=batch_parameters)
 
    critical_failures = []
    non_critical_failures = []
 
    for result in results.results:
        if not result.success:
            col = result.expectation_config.kwargs.get("column", "N/A")
            exp_type = result.expectation_config.type
            msg = f"  [FAIL] {suite_name} | {exp_type} | column: {col}"
            if col in critical_columns or col == "N/A":
                critical_failures.append(msg)
            else:
                non_critical_failures.append(msg)
 
    for msg in non_critical_failures:
        print(f"[WARN] {msg}")
 
    if critical_failures:
        for msg in critical_failures:
            print(f"[CRITICAL] {msg}")
        raise ValueError(
            f"Validación crítica fallida en {suite_name}. Pipeline detenido."
        )
 
    print(f" {suite_name} validado correctamente")
 
 
# -------------------------
# FACT PERSONA
# -------------------------
def _validate_fact_persona(context, df):
 
    expectations = [
        # CRÍTICAS
        gx.expectations.ExpectColumnValuesToNotBeNull(column="persona_fact_id"),
        gx.expectations.ExpectColumnValuesToBeUnique(column="persona_fact_id"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="demografia_sk"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="educacion_sk"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="salud_sk"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="trabajo_sk"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="tecnologia_sk"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="tiempo_sk"),
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=50000),
 
        # NO CRÍTICAS
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="factor_de_expansion",
            min_value=0,
            mostly=0.95
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="poverty_rate",
            min_value=0,
            max_value=100,
            mostly=0.8
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="unemployment_rate",
            min_value=0,
            max_value=100,
            mostly=0.8
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="gdp_per_capita",
            min_value=0,
            mostly=0.8
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="gini_index",
            min_value=0,
            max_value=100,
            mostly=0.8
        ),
    ]
 
    critical_columns = {
        "persona_fact_id",
        "demografia_sk",
        "educacion_sk",
        "salud_sk",
        "trabajo_sk",
        "tecnologia_sk",
        "tiempo_sk"
    }
 
    _run_suite(context, df, "fact_persona", expectations, critical_columns)
 
 
# -------------------------
# DIM DEMOGRAFIA
# -------------------------
def _validate_dim_demografia(context, df):
 
    expectations = [
        # CRÍTICAS
        gx.expectations.ExpectColumnValuesToNotBeNull(column="demografia_sk"),
        gx.expectations.ExpectColumnValuesToBeUnique(column="demografia_sk"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="sexo"),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="sexo",
            value_set=["Hombre", "Mujer", ""]
        ),
 
        # NO CRÍTICAS
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="rango_edad",
            value_set=["0-18", "19-30", "31-60", "61+", ""]
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="edad",
            min_value=0,
            max_value=110,
            mostly=0.95
        ),
    ]
 
    critical_columns = {"demografia_sk", "sexo"}
 
    _run_suite(context, df, "dim_demografia", expectations, critical_columns)
 
 
# -------------------------
# DIM EDUCACION
# -------------------------
def _validate_dim_educacion(context, df):
 
    expectations = [
        # CRÍTICAS
        gx.expectations.ExpectColumnValuesToNotBeNull(column="educacion_sk"),
        gx.expectations.ExpectColumnValuesToBeUnique(column="educacion_sk"),
 
        # NO CRÍTICAS
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="alfabetizado",
            value_set=["Sí", "No", ""]
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="actualmente_estudia",
            value_set=["Sí", "No", ""]
        ),
    ]
 
    critical_columns = {"educacion_sk"}
 
    _run_suite(context, df, "dim_educacion", expectations, critical_columns)
 
 
# -------------------------
# DIM SALUD
# -------------------------
def _validate_dim_salud(context, df):
 
    expectations = [
        # CRÍTICAS
        gx.expectations.ExpectColumnValuesToNotBeNull(column="salud_sk"),
        gx.expectations.ExpectColumnValuesToBeUnique(column="salud_sk"),
 
        # NO CRÍTICAS
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="afiliado_seguridad_social_de_salud",
            value_set=["Sí", "No", "No sabe, No informa", ""]
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="problema_de_salud_ultimos_30d",
            value_set=["Sí", "No", ""]
        ),
    ]
 
    critical_columns = {"salud_sk"}
 
    _run_suite(context, df, "dim_salud", expectations, critical_columns)
 
 
# -------------------------
# DIM TRABAJO
# -------------------------
def _validate_dim_trabajo(context, df):
 
    expectations = [
        # CRÍTICAS
        gx.expectations.ExpectColumnValuesToNotBeNull(column="trabajo_sk"),
        gx.expectations.ExpectColumnValuesToBeUnique(column="trabajo_sk"),
 
        # NO CRÍTICAS
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="actividad_pagada_sp",
            value_set=["Sí", "No", ""]
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="cotiza_fondo_de_pensiones",
            value_set=["Sí", "No", "Ya es pensionado", ""]
        ),
    ]
 
    critical_columns = {"trabajo_sk"}
 
    _run_suite(context, df, "dim_trabajo", expectations, critical_columns)
 
 
# -------------------------
# DIM TIEMPO
# -------------------------
def _validate_dim_tiempo(context, df):
 
    expectations = [
        # CRÍTICAS
        gx.expectations.ExpectColumnValuesToNotBeNull(column="tiempo_sk"),
        gx.expectations.ExpectColumnValuesToBeUnique(column="tiempo_sk"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="anio"),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="anio",
            value_set=[2023, 2024]
        ),
    ]
 
    critical_columns = {"tiempo_sk", "anio"}
 
    _run_suite(context, df, "dim_tiempo", expectations, critical_columns)