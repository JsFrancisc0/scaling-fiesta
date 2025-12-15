#IMPORTANTE usar esto si no funciona / aparece la pagina en blanco

#import streamlit as st
#import os

#st.title("🚨 DEBUG STREAMLIT")
#st.write("Archivo ejecutado:", os.path.abspath(__file__))
#st.write("Directorio actual:", os.getcwd())

####################################
import time
import streamlit as st
import pandas as pd
import os
import jwt
import time
from sqlalchemy import create_engine, text
from selenium import webdriver
# Usar estos para Chrome (хдх)
#from selenium.webdriver.chrome.service import Service
#from webdriver_manager.chrome import ChromeDriverManager
#from selenium.webdriver.chrome.options import Options
# Estos para Firefox
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from webdriver_manager.firefox import GeckoDriverManager
from PIL import Image
import asyncio
from groq import AsyncGroq
from fpdf import FPDF

# ==================
# CONFIGURACIÓN
# Tiene que estar declaradas las variables de entorno para que funcione!

METABASE_SITE_URL = "http://localhost:3000"
DASHBOARD_ID = 2
METABASE_SECRET_KEY = os.getenv("METABASE_SECRET_KEY")
POSTGRES_URL = os.getenv("POSTGRES_URL")

if not METABASE_SECRET_KEY or not POSTGRES_URL:
    st.error("Faltan variables de entorno")
    st.stop()

engine = create_engine(POSTGRES_URL)

# =============================
# Esto son la columnas que tiene que tener el csv
# =============================

EXPECTED_COLUMNS = [
    "service_name", "type", "category", "countries_available",
    "monthly_price_usd", "annual_price_usd", "launch_year",
    "subscribers_millions", "content_type", "platforms",
    "is_free", "parent_company",
    "age_group_18_24_pct", "age_group_25_34_pct",
    "age_group_35_44_pct", "age_group_45_54_pct",
    "age_group_55_64_pct", "age_group_65_plus_pct",
    "device_android_pct", "device_ios_pct", "device_web_pct",
    "device_smart_tv_pct", "device_gaming_console_pct",
    "device_other_pct", "engagement_cluster", "arpu_usd",
    "churn_rate_pct", "subscribers_2020_millions",
    "subscribers_2021_millions", "subscribers_2022_millions",
    "subscribers_2023_millions", "subscribers_2024_millions"
]

# =============================
# FUNCIONES
# =============================

def generar_embed_dashboard(dashboard_id):
    payload = {
        "resource": {"dashboard": dashboard_id},
        "params": {},
        "exp": round(time.time()) + (60 * 10)
    }
    token = jwt.encode(payload, METABASE_SECRET_KEY, algorithm="HS256")
    return f"{METABASE_SITE_URL}/embed/dashboard/{token}#bordered=true&titled=true"

def limpiar_tabla():
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE streaming_services"))

def cargar_csv(df):
    with engine.begin() as conn:
        # Esto hace que se limpie la tabla antes de hacer una insercion
        #conn.execute(text("TRUNCATE TABLE streaming_services"))
        df.to_sql(
            "streaming_services",
            conn,
            if_exists="append",
            index=False
        )

def capturar_dashboard(iframe_url: str, out_dir: str = "exports", timeout: int = 12):
    """
    Abre el iframe del dashboard embebido y captura:
    - dashboard completo
    - cada tarjeta (gráfico) individualmente si son detectables
    Devuelve lista de rutas de imágenes PNG.
    """
    os.makedirs(out_dir, exist_ok=True)

    options = FirefoxOptions()
    options.add_argument("-headless")
    options.add_argument("--width=1600")
    options.add_argument("--height=1200")

    driver = webdriver.Firefox(
        service=FirefoxService(GeckoDriverManager().install()),
        options=options
    )

    try:
        driver.get(iframe_url)

        time.sleep(timeout)

        dashboard_png = os.path.join(out_dir, "dashboard_full.png")
        driver.save_screenshot(dashboard_png)

        img_paths = [dashboard_png]

        try:
            cards = driver.find_elements(
                By.CSS_SELECTOR,
                ".DashCard, .Card, .visualization-root"
            )

            for idx, card in enumerate(cards, start=1):
                driver.execute_script(
                    "arguments[0].scrollIntoView(true);",
                    card
                )
                time.sleep(0.8)

                location = card.location
                size = card.size

                temp_full = os.path.join(out_dir, f"temp_full_{idx}.png")
                driver.save_screenshot(temp_full)

                img = Image.open(temp_full)

                left = int(location["x"])
                top = int(location["y"])
                right = left + int(size["width"])
                bottom = top + int(size["height"])

                cropped = img.crop((left, top, right, bottom))

                out_path = os.path.join(out_dir, f"card_{idx}.png")
                cropped.save(out_path)

                img_paths.append(out_path)
                os.remove(temp_full)

        except Exception:
            # Si no se detectan tarjetas, al menos devolvemos el dashboard completo
            pass

        return img_paths

    finally:
        driver.quit()




# =============================
# FUNCIÓN PARA LLAMAR A GROQ
# =============================
async def generate_report_with_groq(prompt: str) -> str:
    client = AsyncGroq(api_key=os.getenv("GROQ_API_KEY"))

    messages = [
        {"role": "system", "content": "Eres un asistente que genera reportes claros y estructurados."},
        {"role": "user", "content": prompt}
    ]

    response = await client.chat.completions.create(
        messages=messages,
        model="meta-llama/llama-4-scout-17b-16e-instruct",
        temperature=0.3,
        max_tokens=1500,
        top_p=1,
        stream=False
    )

    return response.choices[0].message.content

# =============================
# FUNCIÓN PARA GENERAR PDF
# =============================
def generar_pdf(texto_reporte: str, image_paths: list[str], out_path: str = "reporte.pdf"):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)

    # Texto del reporte
    pdf.multi_cell(0, 10, texto_reporte)

    pdf.ln(10)
    pdf.set_font("Arial", "B", 14)
    pdf.cell(0, 10, "Gráficos del Dashboard", ln=True)

    # Insertar imágenes
    for img in image_paths:
        pdf.image(img, w=150)
        pdf.ln(10)

    pdf.output(out_path)
    return out_path

# =============================
# INTERFAZ STREAMLIT
# =============================

st.set_page_config(layout="wide")
st.title("📊 Streaming Services Dashboard")

st.markdown(
    "Carga un archivo CSV para actualizar los datos del dashboard."
)

# =============================
# UPLOAD CSV
# =============================

uploaded_file = st.file_uploader(
    "Sube tu archivo CSV",
    type=["csv"]
)

if uploaded_file:
    df = pd.read_csv(uploaded_file)

    if set(df.columns) != set(EXPECTED_COLUMNS):
        st.error("❌ El CSV no tiene las columnas correctas")
        st.write("Columnas esperadas:")
        st.code(EXPECTED_COLUMNS)
        st.stop()

    if st.button("📥 Cargar datos a Postgres"):
        cargar_csv(df)
        st.success("✅ Datos cargados correctamente")
        st.rerun()

# =============================
# BOTÓN LIMPIAR DATOS
# =============================

st.divider()

if st.button("🧹 Limpiar todos los datos"):
    limpiar_tabla()
    st.warning("⚠️ Tabla limpiada")
    st.rerun()

# =============================
# DASHBOARD EMBEBIDO
# =============================

st.divider()
st.subheader("📈 Dashboard")

iframe_url = generar_embed_dashboard(DASHBOARD_ID)

st.components.v1.iframe(
    iframe_url,
    height=850,
    scrolling=True
)


# =============================
# PROMPT BASE PARA LA IA
# =============================
if st.button("📝 Generar reporte con IA"):
    with st.spinner("Capturando gráficos y generando reporte..."):
        try:
            # 1) Capturar imágenes del dashboard embebido
            iframe_url = generar_embed_dashboard(DASHBOARD_ID)
            img_paths = capturar_dashboard(iframe_url, out_dir="exports")

            # 2) Consultas SQL a la base de datos
            df_rentabilidad = pd.read_sql("SELECT monthly_price_usd, churn_rate_pct FROM streaming_services", engine)

            df_segmentacion = pd.read_sql("""
                SELECT content_type,
                       AVG(age_group_18_24_pct) AS jovenes,
                       AVG(age_group_65_plus_pct) AS adultos
                FROM streaming_services
                GROUP BY content_type
            """, engine)

            df_ingresos = pd.read_sql("""
                SELECT service_name, monthly_price_usd, arpu_usd
                FROM streaming_services
            """, engine)

            df_perfil = pd.read_sql("""
                SELECT is_free, AVG(age_group_18_24_pct) AS promedio_jovenes
                FROM streaming_services
                GROUP BY is_free
            """, engine)

            df_tecnologia = pd.read_sql("""
                SELECT AVG(device_smart_tv_pct) AS smart_tv,
                       AVG(device_android_pct + device_ios_pct) AS mobile,
                       AVG(device_web_pct) AS web
                FROM streaming_services
            """, engine)

            df_boom = pd.read_sql("""
                SELECT launch_year, COUNT(*) AS cantidad
                FROM streaming_services
                GROUP BY launch_year
                ORDER BY launch_year
            """, engine)

            # 3) Construir prompt dinámico con los datos
            prompt = f"""
            Genera un reporte ejecutivo sobre el mercado de streaming basándote en las siguientes preguntas y datos:

            1. Rentabilidad (precio vs churn):
            {df_rentabilidad.head(10).to_string(index=False)}

            2. Segmentación de mercado (contenido vs edades):
            {df_segmentacion.to_string(index=False)}

            3. Eficiencia de ingresos (ARPU vs precio):
            {df_ingresos.head(10).to_string(index=False)}

            4. Perfil del usuario (gratis vs pago):
            {df_perfil.to_string(index=False)}

            5. Tecnología (dispositivos dominantes):
            {df_tecnologia.to_string(index=False)}

            6. Boom del streaming (lanzamientos por año):
            {df_boom.to_string(index=False)}

            Por favor:
            - Resume hallazgos clave de cada pregunta.
            - Explica tendencias y correlaciones.
            - Da una conclusion sobre las metricas.
            """

            # 4) Llamar a Groq con el prompt dinámico
            report_text = asyncio.run(generate_report_with_groq(prompt))

            # 5) Generar PDF con texto + imágenes
            pdf_file = generar_pdf(report_text, img_paths)

            # 6) Mostrar resultado y permitir descarga
            st.subheader("Reporte generado por IA")
            st.write(report_text)

            with open(pdf_file, "rb") as f:
                st.download_button("📄 Descargar PDF", f, file_name="reporte.pdf")

            # Opcional: mostrar imágenes capturadas
            for i, img in enumerate(img_paths, start=1):
                st.image(img, caption=f"Gráfico {i}")

        except Exception as e:
            st.error(f"Ocurrió un error generando el reporte: {e}")
