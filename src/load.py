import os
import pandas as pd


def load_data(tables, use_postgres=False):

    print("\n=== LOADING DATA ===")

    # -------------------------
    # Guardar siempre en CSV
    # -------------------------
    output_path = "data/processed"

    os.makedirs(output_path, exist_ok=True)

    for name, df in tables.items():

        file_path = os.path.join(output_path, f"{name}.csv")

        df.to_csv(file_path, index=False)

        print(f"  Saved CSV: {file_path} ({len(df)} filas, {len(df.columns)} columnas)")

    # -------------------------
    # Intentar cargar a PostgreSQL
    # -------------------------
    if use_postgres:

        try:

            from sqlalchemy import create_engine

            db_user     = os.getenv("DB_USER", "postgres")
            db_password = os.getenv("DB_PASSWORD", "postgres")
            db_host     = os.getenv("DB_HOST", "host.docker.internal")
            db_port     = os.getenv("DB_PORT", "5432")
            db_name     = os.getenv("DB_NAME", "proyecto_etl_cep")

            engine = create_engine(
                f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            )

            load_order = [
                "dim_demografia",
                "dim_educacion",
                "dim_tecnologia",
                "dim_tiempo",
                "dim_salud",
                "dim_trabajo",
                "fact_persona"
            ]

            for name in load_order:

                if name not in tables:
                    print(f"  [WARN] Tabla '{name}' no encontrada en tables, se omite")
                    continue

                df = tables[name]

                df.to_sql(
                    name,
                    engine,
                    if_exists="replace",
                    index=False
                )

                print(f"  Loaded to PostgreSQL: {name} ({len(df)} filas)")

            print("\n  PostgreSQL load completado")

        except Exception as e:

            print("\n  [WARN] PostgreSQL no disponible")
            print("  Solo se generaron archivos CSV")
            print(f"  Error: {e}")

    print("\n=== LOAD FINISHED ===")