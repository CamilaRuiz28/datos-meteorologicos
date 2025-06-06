import pandas as pd
import re
from pathlib import Path
import streamlit as st
from io import StringIO, BytesIO
import requests
from base64 import b64encode

# ───── 1. Analiza una línea ─────────────────────────────────────────
def parse_line(line: str):
    if not re.match(r'^[A-Za-z]{3} \d{2} [A-Za-z]{3} \d{4} \d{2}:\d{2}:\d{2}', line):
        return None

    rest = line.split(",", 1)[1].lstrip()
    parts = [p.replace('\x02', '').replace('\x03', '').strip() for p in rest.split(",") if p != ""]

    if parts and parts[0] == "Q":
        parts.pop(0)

    if parts and re.fullmatch(r'[0-9A-Fa-f]{1,2}', parts[-1]):
        crc = parts.pop()

    if len(parts) != 13:
        return None

    parts[5] = parts[5].lstrip("+")
    parts[6] = parts[6].lstrip("+")
    parts.pop(-2)
    return parts

# ───── 2. Lee el archivo completo ───────────────────────────────────
def parse_file(path):
    with open(path, encoding="utf-8", errors="ignore") as f:
        return [row for l in f if (row := parse_line(l))]

# ───── 3. Procesa y exporta promedios horarios ──────────────────────
def procesar(path_txt):
    filas = parse_file(path_txt)
    if not filas:
        print("⚠️  No se encontraron registros válidos.")
        return

    cols = ["DirViento", "VelViento", "DirVientoCorr", "Presion", "Humedad",
            "Temp", "PuntoRocio", "PrecipTotal", "IntensidadPrec",
            "Irradiancia", "FechaISO", "Flag"]

    df = pd.DataFrame(filas, columns=cols)
    num = [c for c in cols if c not in ("FechaISO", "Flag")]
    df[num] = df[num].apply(pd.to_numeric, errors="coerce")
    df["FechaISO"] = pd.to_datetime(df["FechaISO"], errors="coerce")
    df = df.dropna(subset=["FechaISO"])
    df["FechaHora"] = df["FechaISO"].dt.floor("h")
    df_h = df.groupby("FechaHora")[num].mean().reset_index()

    base = Path(path_txt)
    csv_out = base.with_name(base.stem + "_promedios.csv")
    xlsx_out = base.with_name(base.stem + "_promedios.xlsx")

    df_h.to_csv(csv_out, index=False)
    try:
        import openpyxl
        df_h.to_excel(xlsx_out, index=False)
        print(f"✅ Exportados: {csv_out.name} y {xlsx_out.name}")
    except ImportError:
        print(f"✅ Exportado: {csv_out.name} (instala openpyxl para Excel)")

def procesar_buffer(uploaded_file):
    filas = [parse_line(l.decode('utf-8', 'ignore')) for l in uploaded_file.readlines()]
    filas = [f for f in filas if f]
    if not filas:
        raise ValueError("Sin registros válidos.")

    cols = ["DirViento", "VelViento", "DirVientoCorr", "Presion", "Humedad",
            "Temp", "PuntoRocio", "PrecipTotal", "IntensidadPrec",
            "Irradiancia", "FechaISO", "Flag"]
    df = pd.DataFrame(filas, columns=cols)
    num = [c for c in cols if c not in ("FechaISO", "Flag")]
    df[num] = df[num].apply(pd.to_numeric, errors="coerce")
    df["FechaISO"] = pd.to_datetime(df["FechaISO"], errors="coerce")
    df = df.dropna(subset=["FechaISO"])
    df["FechaHora"] = df["FechaISO"].dt.floor("h")
    return df.groupby("FechaHora")[num].mean().reset_index()

# ---------- Interfaz Streamlit ----------
st.set_page_config(page_title="Promedios horarios", page_icon="☁️")
st.title("☁️ Procesador de datos meteorológicos")

archivo = st.file_uploader("Sube el archivo .txt", type="txt")
if archivo:
    try:
        df = procesar_buffer(archivo)
        st.success(f"Procesadas {len(df)} horas de datos.")
        st.dataframe(df, use_container_width=True)

        csv = StringIO()
        df.to_csv(csv, index=False, sep=";")
        st.session_state['csv_data'] = csv.getvalue()

        st.download_button("⬇️ Descargar CSV", csv.getvalue(),
                           "promedios_horarios.csv", "text/csv")

        bio = BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)
        st.download_button("⬇️ Descargar Excel", bio.getvalue(),
                           "promedios_horarios.xlsx",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    except Exception as e:
        st.error(f"Error: {e}")
else:
    st.info("Cargar un .txt para comenzar.")

# ───── Subida a GitHub ─────────────────────────────────────────────
def subir_a_github(usuario, repo, token, nombre_archivo, contenido_csv):
    url_api = f"https://api.github.com/repos/{usuario}/{repo}/contents/{nombre_archivo}"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json"
    }

    get_response = requests.get(url_api, headers=headers)
    sha = get_response.json().get("sha") if get_response.status_code == 200 else None

    data = {
        "message": "📤 Subida automática desde Streamlit",
        "content": b64encode(contenido_csv.encode()).decode(),
        "branch": "main"
    }
    if sha:
        data["sha"] = sha

    response = requests.put(url_api, headers=headers, json=data)
    return response

with st.expander("☁️ Subir archivo a GitHub"):
    usuario = st.text_input("Usuario GitHub")
    repo = st.text_input("Repositorio", placeholder="ej. datos-meteorologicos")
    token = st.text_input("Token de acceso", type="password")
    nombre_archivo = st.text_input("Nombre del archivo", value="Datos_public/promedios_horarios.csv")

    if st.button("📤 Subir a GitHub"):
        if 'csv_data' not in st.session_state:
            st.warning("⚠️ Primero carga y procesa un archivo .txt")
        else:
            respuesta = subir_a_github(usuario, repo, token, nombre_archivo, st.session_state['csv_data'])
            if respuesta.status_code in (200, 201):
                url_descarga = f"https://raw.githubusercontent.com/{usuario}/{repo}/main/{nombre_archivo}"
                st.success("✅ Archivo subido correctamente a GitHub")
                st.code(url_descarga)
                st.markdown("🔗 Puedes usar esta URL directamente en Power BI como fuente de datos")
            else:
                st.error(f"❌ Error al subir: {respuesta.status_code}")
                st.json(respuesta.json())
