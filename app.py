import streamlit as st
import pandas as pd
import base64
from io import BytesIO
from PIL import Image
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
import random
import time

# Generate random number to force rerun and clear cache
CHART_VERSION = str(random.randint(1, 10000))
import folium
from streamlit_folium import st_folium

from sheets_utils import (
    append_or_update_data,
    read_master_data,
    cached_read_master_data,
    read_log,
    get_filter_options_fast,
    filter_data_efficiently,
    get_data_statistics_fast,
)

# Cached data loader with TTL so external sheet edits get picked up periodically
@st.cache_data(ttl=60, show_spinner=False)
def load_dashboard_data():
    # Use cached reader to avoid repeated Google Sheets hits on page switches
    df = cached_read_master_data()
    # Normalize columns once here
    df.columns = df.columns.str.strip().str.replace("\u200b", "", regex=False).str.replace("\xa0", "", regex=False).str.upper()
    filters = get_filter_options_fast(df)
    return df, filters

st.set_page_config(
    page_title="Dashboard Inspeksi PT. PLN UID Lampung",
    page_icon="⚡",
    layout="wide",
)

# Simple dataframe signature for change detection (shape + columns + row hash sum)
def compute_df_signature(df: pd.DataFrame) -> int:
    try:
        hashed = pd.util.hash_pandas_object(df.fillna("").astype(str), index=True).sum()
        return hash((df.shape, tuple(df.columns), int(hashed)))
    except Exception:
        return hash((df.shape, tuple(df.columns)))

# Tambahkan CSS untuk styling divider
st.markdown("""
<style>
    /* Style untuk horizontal divider */
    hr {
        height: 3px !important;
        background-color: #5e5e5e !important;
        border: none !important;
        margin: 0 !important;
    }
</style>
""", unsafe_allow_html=True)

if "page" not in st.session_state:
    st.session_state.page = "upload"

def set_page(page_name: str):
    st.session_state.page = page_name

def image_to_base64(image: Image.Image) -> str:
    buffered = BytesIO()
    image.save(buffered, format="PNG")
    return base64.b64encode(buffered.getvalue()).decode()

try:
    logo_dinantara = Image.open("assets/LOGO DANANTARA.png")
    logo_pln = Image.open("assets/LOGO PLN.png")
    b64_logo_dinantara = image_to_base64(logo_dinantara)
    b64_logo_pln = image_to_base64(logo_pln)
except FileNotFoundError:
    st.error("File logo tidak ditemukan.")
    b64_logo_dinantara = b64_logo_pln = None

st.markdown(
    f"""
    <style>
    .block-container {{ padding: 1.5rem 2rem 2rem 2rem; }}
    .header-container {{
        background-color: #007C8F;
        padding: 5px 30px;
        border-radius: 10px;
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 25px;
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
    }}
    .logo-container {{ flex: 1; display: flex; align-items: center; }}
    .logo-left {{ justify-content: flex-start; }}
    .logo-right {{ justify-content: flex-end; }}
    .logo-img-dinantara {{ height: 70px; object-fit: contain; }}
    .logo-img-pln {{ height: 85px; object-fit: contain; }}
    .title-container {{ flex: 3; text-align: center; }}
    .main-title {{
        color: white;
        font-size: 15px;
        font-weight: 600;
        margin: 0;
        line-height: 1.3;
        letter-spacing: 0.5px;
    }}
    .info-container {{
        border: 1.5px solid #007C8F;
        background-color: transparent;
        padding: 25px;
        border-radius: 10px;
        margin-top: 20px;
    }}
    .info-container p {{ margin-bottom: 10px; }}
    .info-container ul {{ list-style-position: inside; padding-left: 5px; }}
    </style>
    """,
    unsafe_allow_html=True,
)

if b64_logo_dinantara and b64_logo_pln:
    st.markdown(
        f"""
        <div class="header-container">
            <div class="logo-container logo-left">
                <img src="data:image/png;base64,{b64_logo_dinantara}" class="logo-img-dinantara" />
            </div>
            <div class="title-container">
                <h1 class="main-title">DASHBOARD MONITORING TEMUAN HASIL INSPEKSI JARINGAN DISTRIBUSI</h1>
            </div>
            <div class="logo-container logo-right">
                <img src="data:image/png;base64,{b64_logo_pln}" class="logo-img-pln" />
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# Sidebar Navigasi
st.sidebar.markdown("<h1 style='text-align: center; font-size: 24px;'>Navigasi Aplikasi</h1>", unsafe_allow_html=True)
st.sidebar.button("📁 Upload Data", on_click=set_page, args=("upload",), use_container_width=True)
st.sidebar.button("📊 Dashboard Utama", on_click=set_page, args=("dashboard",), use_container_width=True)
st.sidebar.button("📋 Rekapitulasi Data", on_click=set_page, args=("rekap",), use_container_width=True)
st.sidebar.button("📝 Log Aktivitas", on_click=set_page, args=("log",), use_container_width=True)
st.sidebar.markdown("---")

# Halaman Upload
if st.session_state.page == "upload":
    st.header("📁 Upload Data Inspeksi", divider="rainbow")
    st.markdown(
        """
        <div class="info-container">
            <p><strong>Silakan unggah file inspeksi dari hasil lapangan.</strong></p>
            <ul>
                <li>File boleh berisi 1–4 sheet: <b>TGK, PSW, KTB, MTR</b></li>
                <li>Header di baris ke-1, data mulai dari baris ke-2</li>
            </ul>
        </div>
        """,
        unsafe_allow_html=True,
    )

    uploaded_file = st.file_uploader(
        "Pilih file:",
        type=["xlsx", "xls", "xlsm", "csv"]
    )

    if uploaded_file is not None:
        progress_bar = st.progress(0, text="Memulai...")
        all_sheets = {}
        try:
            progress_bar.progress(10, text="Validasi file 10%...")
            if uploaded_file.name.endswith(".csv"):
                df = pd.read_csv(uploaded_file, dtype=str, na_filter=False)
                all_sheets["CSV"] = df
            else:
                xls = pd.ExcelFile(uploaded_file)
                for idx, sheet in enumerate(xls.sheet_names):
                    try:
                        progress_bar.progress(10 + int(15 * (idx+1)/len(xls.sheet_names)), text=f"Membaca sheet {sheet}...")
                        df = pd.read_excel(
                            xls,
                            sheet_name=sheet,
                            header=0,
                            dtype=str,
                            na_filter=False
                        )
                        df.columns = (
                            df.columns.str.strip()
                            .str.replace("\u200b", "", regex=False)
                            .str.replace("\xa0", "", regex=False)
                            .str.upper()
                        )
                        all_sheets[sheet] = df
                    except Exception as e:
                        st.warning(f"Gagal membaca sheet {sheet}: {str(e)}")
                        continue
            progress_bar.progress(30, text="Gabung semua sheet 30%...")
            if not all_sheets:
                st.error(" Tidak ada sheet yang dapat dibaca!")
                progress_bar.empty()
                st.stop()
            combined_df = pd.concat(all_sheets.values(), ignore_index=True)
            if "ID SURVEY" not in combined_df.columns:
                st.error(" Kolom 'ID SURVEY' tidak ditemukan!")
                progress_bar.empty()
                st.stop()
            combined_df = combined_df.fillna("")
            progress_bar.progress(50, text="Proses data & validasi 50%...")
            try:
                success, msg = append_or_update_data(combined_df)
                progress_bar.progress(70, text="Simpan ke database 70%...")
                progress_bar.progress(100, text="Selesai 100%!")
                progress_bar.empty()
                # ===== CACHE INVALIDATION =====
                if success:
                    # Clear manual caches and TTL cache
                    cache_keys = ['dashboard_data_cache', 'master_data_cache', 'filter_options_cache']
                    for key in cache_keys:
                        if key in st.session_state:
                            del st.session_state[key]
                    try:
                        load_dashboard_data.clear()  # invalidate TTL cache immediately after upload
                        # Also clear low-level cached reader to force fresh fetch
                        from sheets_utils import cached_read_master_data
                        cached_read_master_data.clear()
                    except Exception:
                        pass
                    # Reset last known signature to force recompute and recognition
                    st.session_state.dashboard_data_signature = None
                    st.session_state.data_updated = True
                    st.session_state.last_upload_time = datetime.now()
                    st.success(f" Upload berhasil: {uploaded_file.name}")
                    st.info(msg)
                    st.write(" **Preview data (10 baris pertama):**")
                    st.dataframe(combined_df.head(10), use_container_width=True)
                else:
                    st.error(f" Upload gagal: {msg}")
            except Exception as upload_error:
                progress_bar.empty()
                st.error(f" Error saat upload: {str(upload_error)}")
                st.write(" **Debug info:**", str(upload_error))
        except Exception as file_error:
            progress_bar.empty()
            st.error(f" Gagal memproses file: {str(file_error)}")

# Halaman Dashboard
elif st.session_state.page == "dashboard":
    # Header (tombol refresh dihapus sesuai permintaan)
    st.header("📊 Dashboard Utama", divider="rainbow")

    # CSS untuk styling dashboard yang lebih modern
    st.markdown("""
    <style>
    /* Eliminate spacing between elements */
    .css-10trblm {
        margin-top: 0 !important;
        margin-bottom: 0 !important;
    }
    /* Pull content closer to dividers */
    .element-container {
        margin-top: -5px !important;
        margin-bottom: -5px !important;
    }
    /* Make all chart containers more compact */
    .stPlotlyChart {
        margin-top: 0 !important;
        margin-bottom: 0 !important;
    }
    .slicer-row {
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
        margin-bottom: 18px;
    }
    .slicer-box {
        background: #f8f9fa;
        border-radius: 10px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.07);
        padding: 1rem 1.2rem;
        min-width: 180px;
        flex: 1;
        display: flex;
        flex-direction: column;
        align-items: flex-start;
    }
    .slicer-label {
        font-weight: 600;
        color: #2c3e50;
        margin-bottom: 0.5rem;
        font-size: 0.95rem;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    .dashboard-section {
        background: #fff;
        border-radius: 12px;
        box-shadow: 0 2px 12px rgba(0,0,0,0.08);
        padding: 1.5rem;
        margin-bottom: 24px;
    }
    /* NEW METRIC CARDS WITH WHITE BACKGROUND */
    .metric-card {
        background: white;
        border-radius: 12px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        padding: 20px;
        text-align: center;
        border: 1px solid #e1e5e9;
        margin-bottom: 10px;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(0,0,0,0.15);
    }
    .metric-number {
        font-size: 2.5em;
        font-weight: bold;
        margin-bottom: 8px;
        color: #2c3e50;
    }
    .metric-label {
        font-size: 0.9em;
        color: #7f8c8d;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    /* Color scheme untuk charts dan metrics */
    .color-primary { color: #3498db; }
    .color-success { color: #27ae60; }
    .color-warning { color: #f39c12; }
    .color-danger { color: #e74c3c; }
    .color-info { color: #8e44ad; }
    </style>
    """, unsafe_allow_html=True)
    
    # Auto-refresh info caption removed per request

    # Load data untuk dashboard via TTL cache (hindari spinner jika sudah ada cache)
    df_dashboard = None
    filter_options_dashboard = {}
    if (
        st.session_state.get("dashboard_ready")
        and st.session_state.get("dashboard_data_cache") is not None
        and not getattr(st.session_state, "force_dashboard_reload", False)
    ):
        # Pakai cache session agar pindah halaman instan
        df_dashboard = st.session_state.dashboard_data_cache
        filter_options_dashboard = st.session_state.dashboard_filter_cache or {}
    else:
        # Hanya tampilkan spinner saat first-load atau forced reload
        with st.spinner("Memuat data dashboard..."):
            try:
                df_dashboard, filter_options_dashboard = load_dashboard_data()
                st.session_state.dashboard_data_cache = df_dashboard
                st.session_state.dashboard_filter_cache = filter_options_dashboard
                st.session_state.dashboard_ready = True
                st.session_state.force_dashboard_reload = False
            except Exception as e:
                st.error(f"❌ Error loading dashboard data: {str(e)}")
                df_dashboard = pd.DataFrame()
                filter_options_dashboard = {}

    # Deteksi perubahan data: simpan dan bandingkan signature
    if 'dashboard_data_signature' not in st.session_state:
        st.session_state.dashboard_data_signature = None
    current_sig = compute_df_signature(df_dashboard) if not df_dashboard.empty else None
    data_changed = (current_sig is not None and current_sig != st.session_state.dashboard_data_signature)
    if data_changed:
        st.session_state.dashboard_data_signature = current_sig
        # Tandai bahwa data baru terdeteksi dari sumber eksternal (bukan upload)
        st.session_state.external_data_changed = True
    else:
        st.session_state.external_data_changed = False

    # ===== FILTER DATA DASHBOARD (MENGIKUTI STYLE REKAPITULASI) =====
    if not df_dashboard.empty:
        # CSS untuk styling filter (sama seperti di Rekapitulasi)
        st.markdown("""
        <style>
        .filter-header {
            color: #007C8F;
            font-weight: bold;
            font-size: 16px;
            margin-bottom: 0px;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        div[data-testid="stSelectbox"] {
            margin-top: -8px !important;
            margin-bottom: 8px !important;
        }
        div[data-testid="stButton"] > button {
            height: 38px !important;
            padding: 8px 16px !important;
            margin-top: 0px !important;
        }
        div[data-testid="stDateInput"] {
            margin-top: -8px !important;
            margin-bottom: 8px !important;
        }
        /* Kompak layout untuk filter */
        .stColumn {
            padding: 0 6px !important;
        }
        .stColumn:first-child {
            padding-left: 0 !important;
        }
        .stColumn:last-child {
            padding-right: 0 !important;
        }
        /* Alignment untuk button container - sejajar TEPAT dengan selectbox */
        .button-container {
            margin-top: 8px;
            height: 38px;
            display: flex;
            align-items: flex-start;
        }
        /* Enhanced styling for ALL dropdown searches */
        div[data-testid="stSelectbox"] div[role="listbox"] {
            max-height: 300px !important;
            overflow-y: auto;
        }
        div[data-testid="stSelectbox"] input {
            background-color: rgba(0, 124, 143, 0.1);
            border-radius: 4px;
            padding: 4px 8px !important;
        }
        div[data-testid="stSelectbox"] input:focus {
            background-color: rgba(0, 124, 143, 0.15);
            border-color: #007C8F !important;
        }
        </style>
        """, unsafe_allow_html=True)
        
        # Inisialisasi session state untuk filter dashboard
        if 'dashboard_filter_state' not in st.session_state:
            st.session_state.dashboard_filter_state = {
                'role': 'Semua',
                'up3': 'Semua',
                'ulp': 'Semua', 
                'tanggal_survey': None,
                'tanggal_har_range': None,
                'status_eksekusi': 'Semua',
                'jenis_temuan': 'Semua',
                'equipment': 'Semua',
                'nama_penyulang': 'Semua'
            }
            
        # Inisialisasi temporary filter state untuk dashboard (untuk pilihan yang belum diaplikasikan)
        if 'temp_dashboard_filter' not in st.session_state:
            st.session_state.temp_dashboard_filter = {
                'role': st.session_state.dashboard_filter_state['role'],
                'up3': st.session_state.dashboard_filter_state['up3'],
                'ulp': st.session_state.dashboard_filter_state['ulp'], 
                'tanggal_survey': st.session_state.dashboard_filter_state['tanggal_survey'],
                'tanggal_har_range': st.session_state.dashboard_filter_state['tanggal_har_range'],
                'status_eksekusi': st.session_state.dashboard_filter_state['status_eksekusi'],
                'jenis_temuan': st.session_state.dashboard_filter_state['jenis_temuan'],
                'equipment': st.session_state.dashboard_filter_state['equipment'],
                'nama_penyulang': st.session_state.dashboard_filter_state['nama_penyulang']
            }
        
        # ===== FILTER LAYOUT BARU - 2 BARIS SESUAI PERMINTAAN =====
        with st.container():
            # ROW 1: Role, UP3, ULP, Equipment, Apply Filter (5 kolom)
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.markdown('<div class="filter-header">👤 Role</div>', unsafe_allow_html=True)
                role_options = ['Semua'] + list(df_dashboard['ROLE'].dropna().unique()) if 'ROLE' in df_dashboard.columns else ['Semua']
                selected_role = st.selectbox(
                    "",
                    role_options,
                    key="dash_filter_role",
                    index=role_options.index(st.session_state.temp_dashboard_filter['role']) if st.session_state.temp_dashboard_filter['role'] in role_options else 0
                )
                # Store selection in temporary filter state
                st.session_state.temp_dashboard_filter['role'] = selected_role
            
            with col2:
                st.markdown('<div class="filter-header">🏢 UP3</div>', unsafe_allow_html=True)
                up3_options = ['Semua'] + filter_options_dashboard.get('UP3', [])
                selected_up3 = st.selectbox(
                    "",
                    up3_options,
                    key="dash_filter_up3",
                    index=up3_options.index(st.session_state.temp_dashboard_filter['up3']) if st.session_state.temp_dashboard_filter['up3'] in up3_options else 0
                )
                # Store selection in temporary filter state
                st.session_state.temp_dashboard_filter['up3'] = selected_up3
            
            with col3:
                st.markdown('<div class="filter-header">🏪 ULP</div>', unsafe_allow_html=True)
                ulp_options = ['Semua'] + filter_options_dashboard.get('ULP', [])
                selected_ulp = st.selectbox(
                    "",
                    ulp_options,
                    key="dash_filter_ulp",
                    index=ulp_options.index(st.session_state.temp_dashboard_filter['ulp']) if st.session_state.temp_dashboard_filter['ulp'] in ulp_options else 0
                )
                # Store selection in temporary filter state
                st.session_state.temp_dashboard_filter['ulp'] = selected_ulp
            
            with col4:
                st.markdown('<div class="filter-header">⚙ Equipment</div>', unsafe_allow_html=True)
                equipment_options = ['Semua'] + filter_options_dashboard.get('EQUIPMENT', [])
                selected_equipment = st.selectbox(
                    "",
                    equipment_options,
                    key="dash_filter_equipment",
                    index=equipment_options.index(st.session_state.temp_dashboard_filter['equipment']) if st.session_state.temp_dashboard_filter['equipment'] in equipment_options else 0
                )
                # Store selection in temporary filter state
                st.session_state.temp_dashboard_filter['equipment'] = selected_equipment
            
            with col5:
                st.markdown('<div class="button-container">', unsafe_allow_html=True)
                if st.button("✅ Apply Filter", help="Terapkan filter yang dipilih", use_container_width=True, key="dash_apply_filter"):
                    # Ambil nilai dari temp_dashboard_filter ke dalam dashboard_filter_state
                    st.session_state.dashboard_filter_state = st.session_state.temp_dashboard_filter.copy()
                    st.success("✅ Filter berhasil diterapkan!")
                    st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)
            
            # ROW 2: Penyulang, Jenis Temuan, Status Eksekusi, Tanggal HAR, Reset Filter (5 kolom)
            # ROW 2: Penyulang, Jenis Temuan, Status Eksekusi, PROGRAM HAR, Reset Filter (5 kolom)
            col6, col7, col8, col9, col10 = st.columns(5)
            
            with col6:
                st.markdown('<div class="filter-header">🔌 Penyulang</div>', unsafe_allow_html=True)
                
                # Get ALL penyulang options with no limit
                penyulang_options = ['Semua'] + filter_options_dashboard.get('NAMA PENYULANG', [])
                
                selected_penyulang = st.selectbox(
                    "",
                    options=penyulang_options,
                    key="dash_filter_penyulang",
                    index=penyulang_options.index(st.session_state.temp_dashboard_filter['nama_penyulang']) if st.session_state.temp_dashboard_filter['nama_penyulang'] in penyulang_options else 0
                )
                # Store selection in temporary filter state
                st.session_state.temp_dashboard_filter['nama_penyulang'] = selected_penyulang
            
            with col7:
                st.markdown('<div class="filter-header">🎯 Jenis Temuan</div>', unsafe_allow_html=True)
                temuan_options = ['Semua'] + filter_options_dashboard.get('JENIS TEMUAN', [])
                selected_jenis = st.selectbox(
                    "",
                    temuan_options,
                    key="dash_filter_jenis",
                    index=temuan_options.index(st.session_state.temp_dashboard_filter['jenis_temuan']) if st.session_state.temp_dashboard_filter['jenis_temuan'] in temuan_options else 0
                )
                # Store selection in temporary filter state
                st.session_state.temp_dashboard_filter['jenis_temuan'] = selected_jenis
            
            with col8:
                st.markdown('<div class="filter-header">⚡ Status Eksekusi</div>', unsafe_allow_html=True)
                status_options = ['Semua'] + filter_options_dashboard.get('STATUS EKSEKUSI', [])
                selected_status = st.selectbox(
                    "",
                    status_options,
                    key="dash_filter_status",
                    index=status_options.index(st.session_state.temp_dashboard_filter['status_eksekusi']) if st.session_state.temp_dashboard_filter['status_eksekusi'] in status_options else 0
                )
                # Store selection in temporary filter state
                st.session_state.temp_dashboard_filter['status_eksekusi'] = selected_status
            
            with col9:
                st.markdown('<div class="filter-header">📅 PROGRAM HAR</div>', unsafe_allow_html=True)
                # Ambil data PROGRAM HAR yang valid dari dataset
                if 'PROGRAM HAR' in df_dashboard.columns:
                    program_har_options = df_dashboard['PROGRAM HAR'].fillna('').replace('', '(blank)').unique().tolist()
                    option_order = ['AGRESSI', 'HAR RUTIN / 4DX', 'HAR PDKB', '(blank)']
                    program_har_options = [opt for opt in option_order if opt in program_har_options] + [opt for opt in program_har_options if opt not in option_order]
                    program_har_options = ['Semua'] + program_har_options
                    selected_program_har = st.selectbox("", program_har_options, key="dash_filter_program_har",
                        index=program_har_options.index(st.session_state.temp_dashboard_filter.get('program_har', 'Semua')) if st.session_state.temp_dashboard_filter.get('program_har', 'Semua') in program_har_options else 0)
                    st.session_state.temp_dashboard_filter['program_har'] = selected_program_har
                else:
                    st.info("Data PROGRAM HAR tidak tersedia")
            
            with col10:
                st.markdown('<div class="button-container">', unsafe_allow_html=True)
                if st.button("🔄 Reset Filter", help="Reset semua filter", use_container_width=True, key="dash_reset_filter_new"):
                    # Reset default values
                    default_filter = {
                        'role': 'Semua',
                        'up3': 'Semua',
                        'ulp': 'Semua', 
                        'tanggal_survey': None,
                        'program_har': 'Semua',
                        'status_eksekusi': 'Semua',
                        'jenis_temuan': 'Semua',
                        'equipment': 'Semua',
                        'nama_penyulang': 'Semua'
                    }
                    # Reset both actual and temporary filter states
                    st.session_state.dashboard_filter_state = default_filter.copy()
                    st.session_state.temp_dashboard_filter = default_filter.copy()
                    st.success("🔄 Filter berhasil direset!")
                    st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)
            
    # ===== SUPER FAST LOADING DENGAN SMART CACHE =====
    if "dashboard_data_cache" not in st.session_state:
        st.session_state.dashboard_data_cache = None
        st.session_state.dashboard_filter_cache = None
        st.session_state.dashboard_ready = False
    
    # Pre-load indicator untuk UX yang lebih baik
    if not st.session_state.dashboard_ready:
        # Background loading tanpa UI yang mengganggu
        pass
    else:
        # Data sudah ready - langsung tampilkan
        pass
    
    # (Pengambilan data kedua dihapus agar tidak duplikat)
    
    # ===== CHECK FOR RECENT DATA UPDATES =====
    if hasattr(st.session_state, 'data_updated') and st.session_state.data_updated:
        if hasattr(st.session_state, 'last_upload_time'):
            time_since_upload = datetime.now() - st.session_state.last_upload_time
            if time_since_upload.total_seconds() < 300:  # Show alert for 5 minutes
                st.success(f"🆕 **Data baru berhasil diupload!** Dashboard telah ter-update otomatis. "
                          f"(Diupload {int(time_since_upload.total_seconds())} detik yang lalu)")
        # Reset flag setelah ditampilkan
        st.session_state.data_updated = False

    # Jika ada perubahan eksternal terdeteksi, tampilkan info singkat (tanpa rerun paksa)
    if st.session_state.get('external_data_changed'):
        st.info("🔁 Perubahan terbaru pada database terdeteksi. Tampilan sudah memuat data terbaru.")
    
    # Gunakan cached data untuk INSTANT ACCESS
    if st.session_state.dashboard_data_cache is not None and st.session_state.dashboard_ready:
        df_dashboard = st.session_state.dashboard_data_cache
        filter_options_dashboard = st.session_state.dashboard_filter_cache or {}
        
        # DEBUG: Print data info (REMOVED FOR INSTANT LOADING)
        # Auto-load data if cache is empty or problematic
        if df_dashboard.empty or 'STATUS EKSEKUSI' not in df_dashboard.columns:
            try:
                df_dashboard, filter_options_dashboard = load_dashboard_data()
                # Update cache
                st.session_state.dashboard_data_cache = df_dashboard
                st.session_state.dashboard_filter_cache = filter_options_dashboard
                st.session_state.dashboard_ready = True
            except Exception as e:
                st.error(f"❌ Error loading data: {str(e)}")
                df_dashboard = pd.DataFrame()
                filter_options_dashboard = {}
        
        if not df_dashboard.empty:
            
            # ===== INSTANT FILTERING WITHOUT PROGRESS INDICATORS =====
            
            # Build filter conditions efficiently
            filter_conditions = {}
            
            # Build filter dictionary
            if st.session_state.dashboard_filter_state['role'] != 'Semua' and 'ROLE' in df_dashboard.columns:
                filter_conditions['ROLE'] = st.session_state.dashboard_filter_state['role']
            if st.session_state.dashboard_filter_state['up3'] != 'Semua':
                filter_conditions['UP3'] = st.session_state.dashboard_filter_state['up3']
            if st.session_state.dashboard_filter_state['ulp'] != 'Semua':
                filter_conditions['ULP'] = st.session_state.dashboard_filter_state['ulp']
            if st.session_state.dashboard_filter_state['equipment'] != 'Semua':
                filter_conditions['EQUIPMENT'] = st.session_state.dashboard_filter_state['equipment']
            if st.session_state.dashboard_filter_state['status_eksekusi'] != 'Semua':
                filter_conditions['STATUS EKSEKUSI'] = st.session_state.dashboard_filter_state['status_eksekusi']
            if st.session_state.dashboard_filter_state['jenis_temuan'] != 'Semua':
                filter_conditions['JENIS TEMUAN'] = st.session_state.dashboard_filter_state['jenis_temuan']
            if st.session_state.dashboard_filter_state['nama_penyulang'] != 'Semua':
                filter_conditions['NAMA PENYULANG'] = st.session_state.dashboard_filter_state['nama_penyulang']
            
            # Apply filters efficiently
            df_filtered = filter_data_efficiently(df_dashboard, filter_conditions, limit=None)
            
            # Date filtering
            if st.session_state.dashboard_filter_state['tanggal_survey'] is not None and 'TANGGAL_SURVEY_DT' in df_filtered.columns:
                date_range = st.session_state.dashboard_filter_state['tanggal_survey']
                if len(date_range) == 2:
                    start_date, end_date = date_range
                    start_datetime = pd.to_datetime(start_date)
                    end_datetime = pd.to_datetime(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
                    mask = (df_filtered['TANGGAL_SURVEY_DT'] >= start_datetime) & (df_filtered['TANGGAL_SURVEY_DT'] <= end_datetime)
                    df_filtered = df_filtered[mask]
            
            # PROGRAM HAR filtering
            if 'PROGRAM HAR' in df_filtered.columns:
                selected_program_har = st.session_state.dashboard_filter_state.get('program_har', 'Semua')
                if selected_program_har == '(blank)':
                    df_filtered = df_filtered[df_filtered['PROGRAM HAR'].isna() | (df_filtered['PROGRAM HAR'] == '')]
                elif selected_program_har != 'Semua':
                    df_filtered = df_filtered[df_filtered['PROGRAM HAR'] == selected_program_har]
            
            # Cache filtered result
            st.session_state.cached_filtered_data = df_filtered
            st.session_state.filter_cache_time = time.time()
            
            # ===== INSTANT KPI CALCULATIONS =====
            st.markdown('<div class="main-dashboard">', unsafe_allow_html=True)
            
            # Pre-filter data untuk performa (hanya kolom yang diperlukan)
            essential_cols = ['JENIS TEMUAN', 'STATUS EKSEKUSI', 'STATUS ASET']
            available_cols = [col for col in essential_cols if col in df_filtered.columns]
            df_metrics = df_filtered[available_cols].copy() if available_cols else df_filtered.copy()
            
            # STREAMLINED KPI CALCULATIONS - langsung tanpa debugging
            
            # KPI 1: Total Temuan
            valid_temuan_mask = df_metrics['JENIS TEMUAN'].notna() & (df_metrics['JENIS TEMUAN'].astype(str).str.strip() != '')
            total_temuan = valid_temuan_mask.sum()
            
            # KPI 2 & 3: STATUS EKSEKUSI calculations
            if 'STATUS EKSEKUSI' in df_metrics.columns:
                # Normalisasi STATUS EKSEKUSI
                df_metrics['STATUS_EKSEKUSI_CLEAN'] = df_metrics['STATUS EKSEKUSI'].astype(str).str.strip().str.upper()
                
                # Hitung KPI
                valid_status_mask = df_metrics['STATUS_EKSEKUSI_CLEAN'].isin(['SELESAI', 'BELUM SELESAI'])
                total_status_valid = valid_status_mask.sum()
                
                selesai_count = (df_metrics['STATUS_EKSEKUSI_CLEAN'] == 'SELESAI').sum()
                pct_temuan_selesai = (selesai_count / total_status_valid * 100) if total_status_valid > 0 else 0
                
                temuan_belum_ditindaklanjuti = (df_metrics['STATUS_EKSEKUSI_CLEAN'] == 'BELUM SELESAI').sum()
            else:
                pct_temuan_selesai = 0
                temuan_belum_ditindaklanjuti = 0
                selesai_count = 0
            
            # KPI 4: Total Aset Buruk (menghitung jumlah STATUS ASET == BURUK)
            if 'STATUS ASET' in df_metrics.columns:
                status_aset_norm = df_metrics['STATUS ASET'].astype(str).str.strip().str.upper()
                buruk_count = int((status_aset_norm == 'BURUK').sum())
            else:
                buruk_count = 0
            
            # Display NEW KPI metrics in modern cards with WHITE background
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-number color-primary">{total_temuan:,}</div>
                    <div class="metric-label">Total Temuan</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-number color-success">{selesai_count:,}</div>
                    <div class="metric-label">TOTAL TEMUAN SELESAI</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col3:
                color_class = "color-danger" if temuan_belum_ditindaklanjuti > 0 else "color-success"
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-number {color_class}">{temuan_belum_ditindaklanjuti:,}</div>
                    <div class="metric-label">Temuan Belum Ditindaklanjuti</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col4:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-number" style="color: #2c3e50;">{buruk_count:,}</div>
                    <div class="metric-label">Total Aset Buruk</div>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Add some space before the divider
            st.markdown("<div style='margin-top:20px;'></div>", unsafe_allow_html=True)
            
            st.markdown("---")
            
            # ===== INSTANT CHART VISUALIZATION =====
            
            # Persiapan data untuk visualisasi (menggunakan df_filtered yang sudah ter-filter)
            if len(df_filtered) > 0:
                
                # ===== 1. % TEMUAN SELESAI PER UP3 - COMBINED BAR + LINE CHART =====
                # Filter data yang valid sekali saja
                valid_temuan_mask = df_filtered['JENIS TEMUAN'].notna() & (df_filtered['JENIS TEMUAN'] != '')
                df_valid_temuan = df_filtered[valid_temuan_mask]
                
                # Hitung total temuan per UP3 dengan groupby yang efisien
                total_temuan_per_up3 = df_valid_temuan.groupby('UP3').size().reset_index(name='Total Temuan')
                
                # Hitung temuan selesai per UP3 dengan filter boolean
                selesai_mask = df_valid_temuan['STATUS EKSEKUSI'] == 'SELESAI'
                temuan_selesai_per_up3 = df_valid_temuan[selesai_mask].groupby('UP3').size().reset_index(name='Jumlah Selesai')
                
                # Gabungkan data dan hitung persentase dengan operasi vectorized
                combined_data = total_temuan_per_up3.merge(temuan_selesai_per_up3, on='UP3', how='left')
                combined_data['Jumlah Selesai'] = combined_data['Jumlah Selesai'].fillna(0)
                combined_data['% Selesai'] = (combined_data['Jumlah Selesai'] / combined_data['Total Temuan'] * 100).round(1)
                
                # Urutkan UP3 dengan categorical untuk performa
                up3_order = ['TANJUNG KARANG', 'METRO', 'KOTABUMI', 'PRINGSEWU']
                combined_data['UP3'] = pd.Categorical(combined_data['UP3'], categories=up3_order, ordered=True)
                combined_data = combined_data.sort_values('UP3').reset_index(drop=True)
                
                if not combined_data.empty:
                    # Create chart
                    fig_up3_combo = go.Figure()
                    
                    # Add traces one by one
                    # Bar Chart untuk Total Temuan (Biru)
                    fig_up3_combo.add_trace(go.Bar(
                        x=combined_data['UP3'],
                        y=combined_data['Total Temuan'],
                        name='Total Temuan',
                        marker_color='#3498db',  # Biru
                        text=combined_data['Total Temuan'],
                        textposition='outside',
                        hoverinfo='skip',
                        yaxis='y'
                    ))
                    
                    # Bar Chart untuk Temuan Selesai (Tosca)
                    fig_up3_combo.add_trace(go.Bar(
                        x=combined_data['UP3'],
                        y=combined_data['Jumlah Selesai'],
                        name='Jumlah Selesai',
                        marker_color='#26D0CE',  # Tosca
                        text=combined_data['Jumlah Selesai'],
                        textposition='outside',
                        hoverinfo='skip',
                        yaxis='y'
                    ))
                    
                    # Line Chart untuk % Selesai (Magenta)
                    fig_up3_combo.add_trace(go.Scatter(
                        x=combined_data['UP3'],
                        y=combined_data['% Selesai'],
                        mode='lines+markers',
                        name='% Selesai',
                        line=dict(color='#E91E63', width=3),  # Magenta
                        marker=dict(size=8, color='#E91E63'),
                        text=[f"{pct}%" for pct in combined_data['% Selesai']],
                        textposition='top center',
                        hoverinfo='skip',
                        yaxis='y2'
                    ))
                    
                    # TRACE INVISIBLE UNTUK HOVER TUNGGAL
                    for i, row in combined_data.iterrows():
                        fig_up3_combo.add_trace(go.Scatter(
                            x=[row['UP3']],
                            y=[row['Total Temuan']],
                            mode='markers',
                            marker=dict(size=20, opacity=0),
                            showlegend=False,
                            hovertemplate=f"""
                            <b>{row['UP3']}</b><br>
                            <span style='display:inline-block;width:100px'>Total Temuan:</span> {int(row['Total Temuan'])}<br>
                            <span style='display:inline-block;width:100px'>Jumlah Selesai:</span> {int(row['Jumlah Selesai'])}<br>
                            <span style='display:inline-block;width:100px'>% Penyelesaian:</span> {row['% Selesai']:.1f}%
                            <extra></extra>
                            """,
                            yaxis='y'
                        ))
                    
                    # Update layout dengan dual y-axis
                    fig_up3_combo.update_layout(
                        title="📊 % Temuan Selesai per UP3",
                        xaxis_title="UP3",
                        yaxis=dict(
                            title="Jumlah Temuan",
                            side="left",
                            showgrid=True
                        ),
                        yaxis2=dict(
                            title="Persentase Selesai (%)",
                            side="right",
                            overlaying="y",
                            showgrid=False,
                            ticksuffix="%"
                        ),
                        height=450,
                        barmode='group',
                        showlegend=True,
                        legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=1.02,
                            xanchor="right",
                            x=1
                        ),
                        font=dict(size=11),
                        hovermode='x'
                    )
                    
                    st.plotly_chart(fig_up3_combo, use_container_width=True)
                else:
                    st.info("📊 Data tidak cukup untuk menampilkan grafik UP3")
                
                # Tambahkan divider untuk memisahkan visualisasi
                st.markdown("""<hr style="height:3px;border:none;color:#333;background-color:#5e5e5e;margin:0;"/>""", unsafe_allow_html=True)
                
                # Layout 3 kolom: left chart, divider, right chart
                col_left, col_divider, col_right = st.columns([0.485, 0.03, 0.485])
                
                with col_left:
                    # ===== 2. PROPORSI STATUS EKSEKUSI - DONUT CHART (OPTIMIZED) =====
                    # Gunakan value_counts yang sudah efisien
                    status_counts = df_filtered['STATUS EKSEKUSI'].value_counts().reset_index()
                    status_counts.columns = ['Status', 'Jumlah']
                    
                    if not status_counts.empty:
                        # Pre-process data untuk chart
                        labels = status_counts['Status'].tolist()
                        values = status_counts['Jumlah'].tolist()
                        
                        # Optimized color mapping - PASTIKAN SESUAI DENGAN DATA ASLI
                        color_map = {'SELESAI': '#0068C9', 'BELUM SELESAI': '#83C9FF', 'Selesai': '#0068C9', 'Belum Selesai': '#83C9FF'}
                        colors = [color_map.get(label, '#CCCCCC') for label in labels]
                        
                        # Buat custom pie chart dengan go.Pie langsung
                        fig_donut_status = go.Figure(data=[go.Pie(
                            labels=labels,
                            values=values,
                            hole=0.5,
                            marker=dict(colors=colors),
                            textinfo='percent+label',
                            textposition='inside',
                            textfont_size=10,
                            hovertemplate='<b>%{label}</b><br>Jumlah: %{value}<br>Persentase: %{percent}<extra></extra>'
                        )])
                        
                        # Tambahkan judul
                        fig_donut_status.update_layout(
                            title="🍩 Proporsi Status Eksekusi",
                            title_font=dict(size=13, family="Arial", color="#ffffff")
                        )
                        fig_donut_status.update_traces(
                            textposition='inside', 
                            textinfo='percent+label',
                            textfont_size=10,
                            hovertemplate='<b>%{label}</b><br>Jumlah: %{value}<br>Persentase: %{percent}<extra></extra>'
                        )
                        # Mengatur ukuran donut chart yang lebih seimbang
                        fig_donut_status.update_layout(
                            height=350, 
                            font=dict(size=10),
                            margin=dict(t=30, b=0, l=0, r=0),
                            title_font=dict(size=13, family="Arial", color="#ffffff"),
                            legend=dict(font=dict(size=14))  # Memperbesar ukuran font legenda
                        )
                        # Add unique key with timestamp to force refresh
                        st.plotly_chart(fig_donut_status, use_container_width=True)
                    else:
                        st.info("📊 Data tidak cukup untuk menampilkan grafik status")
                
                with col_divider:
                    # Tambahkan vertical divider antara dua charts
                    st.markdown("""
                    <div style="background-color: #5e5e5e; width: 3px; height: 350px; margin: 0 auto; border-radius: 2px;">
                    </div>
                    """, unsafe_allow_html=True)
                    
                with col_right:
                    # ===== 6. RASIO ASET BURUK VS KURANG - DONUT CHART =====
                    # Header removed
                    
                    if 'STATUS ASET' in df_filtered.columns:
                        # Bersihkan dan standardisasi data STATUS ASET
                        df_filtered['STATUS_ASET_CLEAN'] = df_filtered['STATUS ASET'].astype(str).str.strip().str.upper()
                        
                        # Filter hanya data yang valid (BURUK/KURANG)
                        valid_status_aset = df_filtered[df_filtered['STATUS_ASET_CLEAN'].isin(['BURUK', 'KURANG'])]
                        aset_counts = valid_status_aset['STATUS_ASET_CLEAN'].value_counts().reset_index()
                        aset_counts.columns = ['Status Aset', 'Jumlah']
                        
                        if not aset_counts.empty:
                            fig_donut_aset = px.pie(
                                aset_counts, 
                                values='Jumlah', 
                                names='Status Aset',
                                title="🍩 Rasio Aset Buruk vs Kurang",
                                color_discrete_map={
                                    'BURUK': '#e74c3c',  # Merah
                                    'KURANG': '#f39c12'  # Kuning
                                },
                                hole=0.5
                            )
                            fig_donut_aset.update_traces(
                                textposition='inside', 
                                textinfo='percent+label',
                                textfont_size=10,
                                hovertemplate='<b>%{label}</b><br>Jumlah: %{value}<br>Persentase: %{percent}<extra></extra>'
                            )
                            # Mengatur ukuran donut chart yang lebih seimbang
                            fig_donut_aset.update_layout(
                                height=350, 
                                font=dict(size=10),
                                margin=dict(t=30, b=0, l=0, r=0),
                                title_font=dict(size=13, family="Arial", color="#ffffff"),
                                legend=dict(font=dict(size=14))  # Memperbesar ukuran font legenda
                            )
                            st.plotly_chart(fig_donut_aset, use_container_width=True)
                        else:
                            st.info("📊 Data STATUS ASET tidak tersedia")
                    else:
                        st.info("📊 Kolom STATUS ASET tidak ditemukan")
                
                # Container sections already closed properly
                
                # Tambahkan divider untuk memisahkan visualisasi
                st.markdown("""<hr style="height:3px;border:none;color:#333;background-color:#5e5e5e;margin:0;"/>""", unsafe_allow_html=True)
                
                # ===== 3. % TEMUAN PER KATEGORI TEMUAN - STACKED BAR CHART (OPTIMIZED) =====
                
                temuan_status_data = df_filtered.groupby(['JENIS TEMUAN', 'STATUS EKSEKUSI']).size().reset_index(name='Jumlah')
                
                if not temuan_status_data.empty:
                    # Hitung jumlah temuan per jenis temuan dan status untuk TOP 20
                    temuan_total = temuan_status_data.groupby('JENIS TEMUAN')['Jumlah'].sum().reset_index()
                    temuan_total = temuan_total.sort_values('Jumlah', ascending=False)
                    top_20_temuan = temuan_total.head(20)['JENIS TEMUAN'].tolist()
                    
                    # Filter data hanya untuk TOP 20
                    filtered_temuan_data = temuan_status_data[temuan_status_data['JENIS TEMUAN'].isin(top_20_temuan)]
                    
                    # Pivot data untuk mendapatkan 'Selesai' dan 'Belum Selesai' per jenis temuan
                    pivot_data = filtered_temuan_data.pivot_table(
                        index='JENIS TEMUAN', 
                        columns='STATUS EKSEKUSI', 
                        values='Jumlah', 
                        aggfunc='sum'
                    ).reset_index().fillna(0)
                    
                    # Pastikan kedua kolom status ada (sesuai data asli - HURUF BESAR)
                    if 'SELESAI' not in pivot_data.columns:
                        pivot_data['SELESAI'] = 0
                    if 'BELUM SELESAI' not in pivot_data.columns:
                        pivot_data['BELUM SELESAI'] = 0
                    
                    # Hitung total dan persentase selesai
                    pivot_data['Total'] = pivot_data['SELESAI'] + pivot_data['BELUM SELESAI']
                    pivot_data['Persen_Selesai'] = (pivot_data['SELESAI'] / pivot_data['Total'] * 100).round(1)
                    
                    # Urutkan berdasarkan persentase selesai (descending)
                    pivot_data = pivot_data.sort_values('Persen_Selesai', ascending=False)
                    category_order = pivot_data['JENIS TEMUAN'].tolist()
                    
                    # Buat kombinasi bar chart dan line chart dengan dual-axis menggunakan go.Figure
                    fig_temuan_combo = go.Figure()
                    
                    # 1. Bar chart untuk temuan dengan status "SELESAI" (Biru Tua)
                    fig_temuan_combo.add_trace(go.Bar(
                        x=pivot_data['JENIS TEMUAN'],
                        y=pivot_data['SELESAI'],
                        name='Selesai',
                        marker_color='#0052CC',  # Biru Tua
                        text=pivot_data['SELESAI'],
                        textposition='inside',
                        hovertemplate='<b>%{x}</b><br>Status: Selesai<br>Jumlah: %{y}<extra></extra>'
                    ))
                    
                    # 2. Bar chart untuk temuan dengan status "BELUM SELESAI" (Tosca)
                    fig_temuan_combo.add_trace(go.Bar(
                        x=pivot_data['JENIS TEMUAN'],
                        y=pivot_data['BELUM SELESAI'],
                        name='Belum Selesai',
                        marker_color='#26D0CE',  # Tosca
                        text=pivot_data['BELUM SELESAI'],
                        textposition='inside',
                        hovertemplate='<b>%{x}</b><br>Status: Belum Selesai<br>Jumlah: %{y}<extra></extra>'
                    ))
                    
                    # 3. Line chart untuk persentase selesai (Magenta) dengan sumbu Y kedua
                    fig_temuan_combo.add_trace(go.Scatter(
                        x=pivot_data['JENIS TEMUAN'],
                        y=pivot_data['Persen_Selesai'],
                        mode='lines+markers',
                        name='% Selesai',
                        yaxis='y2',
                        line=dict(color='#E91E63', width=3),  # Magenta
                        marker=dict(size=8, color='#E91E63'),
                        text=[f"{pct:.1f}%" for pct in pivot_data['Persen_Selesai']],
                        textposition='top center',
                        hovertemplate='<b>%{x}</b><br>Persentase Selesai: %{y:.1f}%<extra></extra>'
                    ))
                    
                    # 4. Tambahkan invisible trace untuk hover yang lebih lengkap
                    for i, row in pivot_data.iterrows():
                        fig_temuan_combo.add_trace(go.Scatter(
                            x=[row['JENIS TEMUAN']],
                            y=[row['Total'] * 0.5],  # Posisi di tengah bar
                            mode='markers',
                            marker=dict(size=20, opacity=0),  # Marker transparan
                            showlegend=False,
                            hovertemplate=f"""
                            <b>{row['JENIS TEMUAN']}</b><br>
                            Jumlah Selesai: {int(row['SELESAI'])}<br>
                            Jumlah Belum Selesai: {int(row['BELUM SELESAI'])}<br>
                            Total Temuan: {int(row['Total'])}<br>
                            Persentase Selesai: {row['Persen_Selesai']:.1f}%
                            <extra></extra>
                            """,
                            yaxis='y1'
                        ))
                    
                    # Update layout dengan dual y-axis dan pengaturan lainnya
                    fig_temuan_combo.update_layout(
                        title="📊 Top 20 % Temuan per Kategori Temuan",
                        barmode='group',
                        xaxis=dict(
                            title="Jenis Temuan",
                            tickangle=45,
                            categoryorder='array',
                            categoryarray=category_order
                        ),
                        yaxis=dict(
                            title="Jumlah Temuan",
                            side="left",
                            showgrid=True
                        ),
                        yaxis2=dict(
                            title="Persentase Selesai (%)",
                            side="right",
                            overlaying="y",
                            showgrid=False,
                            ticksuffix="%",
                            range=[0, 100]
                        ),
                        height=550,
                        legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=1.02,
                            xanchor="center",
                            x=0.5
                        ),
                        hovermode='closest',
                        margin=dict(b=150, l=80, r=80),
                        font=dict(size=12)
                    )
                    # Tambahkan style untuk scroll horizontal tanpa border dan background
                    st.markdown("""
                    <style>
                    .scroll-container {
                        overflow-x: auto;
                        padding: 0;
                    }
                    </style>
                    """, unsafe_allow_html=True)
                    
                    # Langsung menampilkan chart tanpa container tambahan
                    st.markdown('<div class="scroll-container">', unsafe_allow_html=True)
                    st.plotly_chart(fig_temuan_combo, use_container_width=True)
                    st.markdown('</div>', unsafe_allow_html=True)
                else:
                    st.info("📊 Data tidak cukup untuk menampilkan grafik jenis temuan")
                
                # Tambahkan divider untuk memisahkan visualisasi
                st.markdown("""<hr style="height:3px;border:none;color:#333;background-color:#5e5e5e;margin:0;"/>""", unsafe_allow_html=True)
                
                # ===== 4. % TEMUAN SELESAI PER ULP - DUAL AXIS CHART (OPTIMIZED) =====
                
                # Ambil data sesuai STATUS EKSEKUSI per ULP
                ulp_status_data = df_filtered.groupby(['ULP', 'STATUS EKSEKUSI']).size().reset_index(name='Jumlah')
                
                if not ulp_status_data.empty:
                    # Pivot data untuk memudahkan perhitungan persentase
                    pivot_ulp = ulp_status_data.pivot_table(
                        index='ULP', 
                        columns='STATUS EKSEKUSI', 
                        values='Jumlah',
                        fill_value=0
                    ).reset_index()
                    
                    # Pastikan kolom 'SELESAI' dan 'BELUM SELESAI' ada (HURUF BESAR sesuai data asli)
                    if 'SELESAI' not in pivot_ulp.columns:
                        pivot_ulp['SELESAI'] = 0
                    if 'BELUM SELESAI' not in pivot_ulp.columns:
                        pivot_ulp['BELUM SELESAI'] = 0
                    
                    # Hitung Total dan Persentase Selesai
                    pivot_ulp['Total'] = pivot_ulp['SELESAI'] + pivot_ulp['BELUM SELESAI']
                    pivot_ulp['Persen_Selesai'] = (pivot_ulp['SELESAI'] / pivot_ulp['Total'] * 100).round(1)
                    
                    # Urutkan berdasarkan persentase selesai (descending)
                    pivot_ulp = pivot_ulp.sort_values('Persen_Selesai', ascending=False)
                    
                    # Buat figure dengan dual y-axis
                    fig_ulp = go.Figure()
                    
                    # Tambahkan Bar Chart untuk Status 'SELESAI'
                    fig_ulp.add_trace(go.Bar(
                        x=pivot_ulp['ULP'],
                        y=pivot_ulp['SELESAI'],
                        name='Selesai',
                        marker_color='#4FBCF3',  # Biru Muda
                        text=pivot_ulp['SELESAI'],
                        textposition='outside',
                        yaxis='y'
                    ))
                    
                    # Tambahkan Bar Chart untuk Status 'BELUM SELESAI'
                    fig_ulp.add_trace(go.Bar(
                        x=pivot_ulp['ULP'],
                        y=pivot_ulp['BELUM SELESAI'],
                        name='Belum Selesai',
                        marker_color='#026FA5',  # Biru Tua
                        text=pivot_ulp['BELUM SELESAI'],
                        textposition='outside',
                        yaxis='y'
                    ))
                    
                    # Tambahkan Line Chart untuk Persentase Selesai
                    fig_ulp.add_trace(go.Scatter(
                        x=pivot_ulp['ULP'],
                        y=pivot_ulp['Persen_Selesai'],
                        mode='lines+markers',
                        name='% Selesai',
                        line=dict(color='magenta', width=3),
                        marker=dict(size=8, color='magenta'),
                        yaxis='y2'
                    ))
                    
                    # Tambahkan hover interaktif dengan tooltip informatif
                    for i, row in pivot_ulp.iterrows():
                        fig_ulp.add_trace(go.Scatter(
                            x=[row['ULP']],
                            y=[max(row['SELESAI'], row['BELUM SELESAI'])],  # Atur posisi hover point
                            mode='markers',
                            marker=dict(size=20, opacity=0),  # Marker transparan
                            showlegend=False,
                            hovertemplate=f"""
                            <b>{row['ULP']}</b><br>
                            Jumlah Selesai: {int(row['SELESAI'])}<br>
                            Jumlah Belum Selesai: {int(row['BELUM SELESAI'])}<br>
                            Total Temuan: {int(row['Total'])}<br>
                            Persentase Selesai: {row['Persen_Selesai']:.1f}%
                            <extra></extra>
                            """,
                            yaxis='y'
                        ))
                    
                    # Update layout dengan dual y-axis
                    fig_ulp.update_layout(
                        title="📊 % Temuan Selesai per ULP",
                        barmode='group',
                        xaxis=dict(
                            title="ULP",
                            tickangle=45,
                            categoryorder='array',
                            categoryarray=pivot_ulp['ULP'].tolist()
                        ),
                        yaxis=dict(
                            title="Jumlah Temuan",
                            side="left",
                            showgrid=True
                        ),
                        yaxis2=dict(
                            title="Persentase Selesai (%)",
                            side="right",
                            overlaying="y",
                            showgrid=False,
                            ticksuffix="%",
                            range=[0, 100]
                        ),
                        height=500,
                        legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=1.02,
                            xanchor="center",
                            x=0.5
                        ),
                        hovermode='closest',
                        font=dict(size=12),
                        margin=dict(b=150, l=80, r=80)
                    )
                    
                    # Tambahkan container dengan scroll horizontal untuk ULP yang banyak
                    st.markdown("""
                    <style>
                    .chart-container-ulp {
                        overflow-x: auto;
                        padding: 10px;
                    }
                    </style>
                    """, unsafe_allow_html=True)
                    
                    # Bungkus chart dengan div yang memiliki scroll horizontal
                    st.markdown('<div class="chart-container-ulp">', unsafe_allow_html=True)
                    st.plotly_chart(fig_ulp, use_container_width=True)
                    st.markdown('</div>', unsafe_allow_html=True)
                else:
                    st.info("📊 Data tidak cukup untuk menampilkan grafik ULP")
                
                # Tambahkan divider untuk memisahkan visualisasi
                st.markdown("""<hr style="height:3px;border:none;color:#333;background-color:#5e5e5e;margin:0;"/>""", unsafe_allow_html=True)
                
                # ===== 5. JUMLAH TEMUAN PER PENYULANG - HORIZONTAL BAR CHART (OPTIMIZED) =====
                
                # Gunakan semua data dari df_filtered
                df_penyulang = df_filtered.copy()
                
                # Set default top N value tanpa slider
                top_n = 15
                
                # Set default nilai untuk variabel selected_up3 dan selected_ulp
                selected_up3 = 'Semua UP3'
                selected_ulp = 'Semua ULP'
                
                # Hitung total temuan per penyulang
                penyulang_all_counts = df_penyulang['NAMA PENYULANG'].value_counts().reset_index()
                penyulang_all_counts.columns = ['Nama Penyulang', 'Jumlah Temuan']
                
                # Hitung total keseluruhan untuk persentase
                total_semua_temuan = penyulang_all_counts['Jumlah Temuan'].sum()
                
                if not penyulang_all_counts.empty:
                    # Ambil top N dan gabungkan sisanya sebagai "Lainnya"
                    top_penyulang = penyulang_all_counts.head(top_n)
                    
                    # Tambahkan persentase untuk tooltip
                    top_penyulang['Persentase'] = (top_penyulang['Jumlah Temuan'] / total_semua_temuan * 100).round(1)
                    
                    # Jika ada lebih dari top_n penyulang, tambahkan bar "Lainnya"
                    if len(penyulang_all_counts) > top_n:
                        lainnya_count = penyulang_all_counts.iloc[top_n:]['Jumlah Temuan'].sum()
                        lainnya_percent = (lainnya_count / total_semua_temuan * 100).round(1)
                        
                        # Tambahkan "Lainnya" ke dataframe
                        lainnya_df = pd.DataFrame({
                            'Nama Penyulang': ['Lainnya'],
                            'Jumlah Temuan': [lainnya_count],
                            'Persentase': [lainnya_percent]
                        })
                        top_penyulang = pd.concat([top_penyulang, lainnya_df]).reset_index(drop=True)
                    
                    # Urutkan berdasarkan jumlah temuan (ascending untuk horizontal bar)
                    top_penyulang = top_penyulang.sort_values('Jumlah Temuan', ascending=True)
                    
                    # Identifikasi penyulang dengan risiko tinggi (temuan > 1000)
                    top_penyulang['Risk'] = top_penyulang['Jumlah Temuan'].apply(
                        lambda x: '🔥 High Risk' if x > 1000 else ('⚠️ Medium Risk' if x > 500 else '✓ Normal')
                    )
                    
                    # Buat hover text yang kustomisasi
                    hover_text = []
                    for idx, row in top_penyulang.iterrows():
                        hover_text.append(
                            f"<b>{row['Nama Penyulang']}</b><br>" +
                            f"Jumlah Temuan: {row['Jumlah Temuan']}<br>" +
                            f"Persentase: {row['Persentase']}%<br>" +
                            f"Status: {row['Risk']}"
                        )
                    
                    # Buat figure dengan Plotly Graph Objects untuk kontrol lebih besar
                    fig_penyulang = go.Figure()
                    
                    # Buat colorful palette dengan warna-warna yang menarik untuk setiap bar
                    colorful_palette = [
                        '#FF6B6B', '#4ECDC4', '#FFD166', '#06D6A0', '#118AB2', 
                        '#EF476F', '#FFC43D', '#1B9AAA', '#6A4C93', '#F15BB5',
                        '#00BBF9', '#9B5DE5', '#00F5D4', '#FF9770', '#3A86FF', '#FB5607'
                    ]
                    
                    # Tambahkan bar dengan hover text kustomisasi dan warna berbeda untuk setiap bar
                    fig_penyulang.add_trace(go.Bar(
                        x=top_penyulang['Jumlah Temuan'],
                        y=top_penyulang['Nama Penyulang'],
                        orientation='h',
                        text=top_penyulang['Jumlah Temuan'],
                        textposition='outside',
                        marker=dict(
                            color=colorful_palette[:len(top_penyulang)],  # Satu warna untuk setiap bar
                            line=dict(width=1)
                        ),
                        hovertemplate='%{hovertext}<extra></extra>',
                        hovertext=hover_text
                    ))
                    
                    # Update layout with fixed title
                    fig_penyulang.update_layout(
                        title=f"📊 Top 15 Penyulang dengan Temuan Terbanyak",
                        height=500,
                        xaxis_title="Jumlah Temuan",
                        yaxis_title="Nama Penyulang",
                        yaxis={'categoryorder': 'total ascending'},
                        font=dict(size=10),
                        margin=dict(l=20, r=20, t=40, b=20)
                    )
                    
                    # Tampilkan chart
                    st.plotly_chart(fig_penyulang, use_container_width=True)
                
                # Tambahkan divider untuk memisahkan visualisasi
                st.markdown("""<hr style="height:3px;border:none;color:#333;background-color:#5e5e5e;margin:0;"/>""", unsafe_allow_html=True)
                
                # ===== 7. TREN TEMUAN BULANAN - MULTI-LINE CHART (OPTIMIZED) =====
                
                if 'TANGGAL SURVEY' in df_filtered.columns:
                    # Convert tanggal survey ke datetime dengan pemrosesan yang lebih rapi
                    df_filtered['TANGGAL_SURVEY_DT'] = pd.to_datetime(df_filtered['TANGGAL SURVEY'], errors='coerce')
                    # Buat kolom BULAN_SURVEY dengan format yang lebih baik untuk plotting
                    df_filtered['BULAN_SURVEY'] = df_filtered['TANGGAL_SURVEY_DT'].dt.to_period('M').dt.to_timestamp()
                    
                    # Filter data yang memiliki tanggal valid
                    df_trend = df_filtered[df_filtered['TANGGAL_SURVEY_DT'].notna()]
                    
                    if not df_trend.empty:
                        # Kelompokkan data per bulan dan jenis temuan
                        trend_data = df_trend.groupby(['BULAN_SURVEY', 'JENIS TEMUAN']).size().reset_index(name='Jumlah')
                        
                        if not trend_data.empty:
                            # Ambil 15 jenis temuan teratas untuk clarity di grafik
                            top_jenis_temuan = df_trend['JENIS TEMUAN'].value_counts().nlargest(15).index.tolist()
                            trend_data_top = trend_data[trend_data['JENIS TEMUAN'].isin(top_jenis_temuan)]
                            
                            # Buat grafik dengan go.Figure untuk kontrol lebih besar
                            fig_trend = go.Figure()
                            
                            # Buat color palette yang menarik
                            colors = px.colors.qualitative.Bold + px.colors.qualitative.Vivid
                            
                            # Tambahkan line untuk setiap jenis temuan
                            for i, jenis in enumerate(top_jenis_temuan):
                                df_jenis = trend_data_top[trend_data_top['JENIS TEMUAN'] == jenis]
                                if not df_jenis.empty:
                                    fig_trend.add_trace(go.Scatter(
                                        x=df_jenis['BULAN_SURVEY'],
                                        y=df_jenis['Jumlah'],
                                        mode='lines+markers',
                                        name=jenis,
                                        line=dict(color=colors[i % len(colors)], width=2),
                                        marker=dict(size=8),
                                        hovertemplate='<b>Bulan: %{x|%B %Y}</b><br>Jenis Temuan: ' + jenis + '<br>Jumlah: %{y}<extra></extra>'
                                    ))
                            
                            # Update layout dengan styling yang lebih baik
                            fig_trend.update_layout(
                                title="📈 Tren Temuan Bulanan",
                                height=500,
                                xaxis_title="Bulan",
                                yaxis_title="Jumlah Temuan",
                                xaxis=dict(
                                    tickformat='%b %Y',
                                    tickangle=45,
                                    tickmode='auto',
                                    nticks=12,
                                    gridcolor='rgba(100,100,100,0.2)'
                                ),
                                yaxis=dict(
                                    gridcolor='rgba(100,100,100,0.2)'
                                ),
                                legend=dict(
                                    orientation="v",
                                    yanchor="top",
                                    y=1,
                                    xanchor="right",
                                    x=1.15,
                                    font=dict(size=9),
                                    itemsizing="constant"
                                ),
                                hovermode='closest',
                                plot_bgcolor='rgba(0,0,0,0)',
                                paper_bgcolor='rgba(0,0,0,0)',
                                font=dict(size=11, color='white')
                            )
                            
                            st.plotly_chart(fig_trend, use_container_width=True)
                        else:
                            st.info("📊 Data tidak cukup untuk menampilkan tren bulanan")
                    else:
                        st.info("📊 Data tanggal survey tidak valid")
                else:
                    st.info("📊 Kolom TANGGAL SURVEY tidak ditemukan")
                
                # ===== 8. PETA LOKASI TEMUAN - INTERACTIVE MAP (OPTIMIZED) =====
                st.markdown("#### 🗺️ **Peta Lokasi Temuan**")
                
                # Garis pembatas langsung di bawah header
                st.markdown("""
                <hr style=\"height:3px;border:none;color:#333;background-color:#5e5e5e;margin:10px 0;\"/>
                """, unsafe_allow_html=True)
                # Info jumlah lokasi dan footer akan ditampilkan di bawah peta
                # SESUAI SPESIFIKASI: Hanya gunakan KOORDINAT TEMUAN
                if 'KOORDINAT TEMUAN' in df_filtered.columns:
                    try:
                        # Proses seluruh data untuk info; batasi marker 1000 untuk visualisasi
                        total_data_count = len(df_filtered)
                        map_markers = []  # disimpan maks 1000 untuk ditampilkan
                        valid_coords_count = 0  # hitung total baris dengan koordinat valid
                        for idx, row in df_filtered.iterrows():
                            lat, lon = None, None
                            # HANYA GUNAKAN KOORDINAT TEMUAN sesuai spesifikasi (dengan spasi)
                            koordinat_value = row.get('KOORDINAT TEMUAN', None)
                            if pd.notna(koordinat_value) and str(koordinat_value).strip():
                                coord_str = str(koordinat_value).strip()
                                if ',' in coord_str:
                                    parts = coord_str.split(',')
                                    if len(parts) >= 2:
                                        try:
                                            lat = float(parts[0].strip())
                                            lon = float(parts[1].strip())
                                        except:
                                            continue
                            
                            # Validasi koordinat dalam range Indonesia
                            if lat and lon and -11 <= lat <= 6 and 95 <= lon <= 141:
                                valid_coords_count += 1
                                if len(map_markers) < 1000:
                                    map_markers.append({
                                        'lat': lat,
                                        'lon': lon,
                                        'jenis_temuan': row.get('JENIS TEMUAN', 'Unknown'),
                                        'status_eksekusi': str(row.get('STATUS EKSEKUSI', 'Unknown')).strip().upper(),  # Normalisasi
                                        'ulp': row.get('ULP', 'Unknown'),
                                        'penyulang': row.get('NAMA PENYULANG', 'Unknown')
                                    })
                        # Gunakan hingga 1000 lokasi untuk visualisasi
                        map_data = map_markers
                        if map_data:
                            center_lat = float(np.mean([point['lat'] for point in map_data]))
                            center_lon = float(np.mean([point['lon'] for point in map_data]))
                            m = folium.Map(
                                location=[center_lat, center_lon],
                                zoom_start=8,
                                tiles='OpenStreetMap'
                            )
                            for point in map_data:
                                # HANYA 2 WARNA: Hijau untuk SELESAI, Merah untuk BELUM SELESAI
                                if point['status_eksekusi'] == 'SELESAI':
                                    color = 'green'  # Hijau untuk SELESAI
                                else:  # BELUM SELESAI
                                    color = 'red'    # Merah untuk BELUM SELESAI
                                
                                folium.Marker(
                                    location=[float(point['lat']), float(point['lon'])],
                                    popup=f"""
                                    <b>Jenis Temuan:</b> {point['jenis_temuan']}<br>
                                    <b>Status:</b> {point['status_eksekusi']}<br>
                                    <b>ULP:</b> {point['ulp']}<br>
                                    <b>Penyulang:</b> {point['penyulang']}
                                    """,
                                    tooltip=f"{point['jenis_temuan']} - {point['status_eksekusi']}",  # Sesuai spesifikasi
                                    icon=folium.Icon(color=color, icon='info-sign')
                                ).add_to(m)
                            
                            st.markdown("""
                            <style>
                            .fullwidth-map .stfolium {
                                width: 100%;
                            }
                            .fullwidth-map iframe {
                                width: 100%;
                            }
                            </style>
                            """, unsafe_allow_html=True)
                            st.markdown('<div class="fullwidth-map">', unsafe_allow_html=True)
                            st_folium(m, width=None, height=500, use_container_width=True)
                            st.markdown('</div>', unsafe_allow_html=True)
                            # Garis pembatas, info, dan footer langsung di bawah peta tanpa spasi kosong
                            
                            # Statistik sesuai spesifikasi
                            selesai_count = sum(1 for point in map_data if point['status_eksekusi'] == 'SELESAI')
                            belum_selesai_count = sum(1 for point in map_data if point['status_eksekusi'] == 'BELUM SELESAI')
                            
                            st.info(f"📍 **Peta Lokasi Temuan** | Data: KOORDINAT TEMUAN | "
                                   f"Ditampilkan: {len(map_data)} dari {total_data_count} data | "
                                   f"🟢 Selesai: {selesai_count} | 🔴 Belum Selesai: {belum_selesai_count}")
                            
                            # Validasi data koordinat
                            valid_coords = valid_coords_count
                            invalid_coords = max(total_data_count - valid_coords, 0)
                            if invalid_coords > 0:
                                st.warning(f"⚠️ {invalid_coords} data tidak memiliki KOORDINAT TEMUAN yang valid")
                            st.markdown("""
                            <hr style=\"height:3px;border:none;color:#333;background-color:#5e5e5e;margin:10px 0 0 0;\"/>
                            """, unsafe_allow_html=True)
                            st.caption("© 2025 – Sistem Monitoring Inspeksi • Dibuat untuk Magang MBKM PLN UID Lampung oleh Ganiya Syazwa")
                        else:
                            st.warning("📍 Tidak ditemukan KOORDINAT TEMUAN yang valid untuk ditampilkan di peta")
                            st.caption("© 2025 – Sistem Monitoring Inspeksi • Dibuat untuk Magang MBKM PLN UID Lampung oleh Ganiya Syazwa")
                    except Exception as e:
                        st.error(f"❌ Error dalam membuat peta: {str(e)}")
                        st.caption("© 2025 – Sistem Monitoring Inspeksi • Dibuat untuk Magang MBKM PLN UID Lampung oleh Ganiya Syazwa")
                else:
                    st.info("📍 Kolom KOORDINAT TEMUAN tidak ditemukan dalam data")
                    st.caption("© 2025 – Sistem Monitoring Inspeksi • Dibuat untuk Magang MBKM PLN UID Lampung oleh Ganiya Syazwa")
            
            else:
                st.warning("⚠️ Tidak ada data yang sesuai dengan filter yang dipilih untuk ditampilkan dalam grafik.")
                st.info("💡 Coba ubah filter atau reset untuk melihat visualisasi data.")
            
        else:
            st.warning("📝 Belum ada data dalam sistem. Silakan upload data terlebih dahulu.")
            if st.button("📁 Ke Halaman Upload", key="dash_upload"):
                st.session_state.page = "upload"
                st.rerun()
    
    else:
        st.error("❌ Gagal memuat data dashboard")

# Halaman Rekapitulasi
elif st.session_state.page == "rekap":
    st.header("📋 Rekapitulasi & Integrasi Data", divider="rainbow")
    
    # SUPER FAST LOADING - INSTANT ACCESS untuk Rekapitulasi
    if "master_data_cache" not in st.session_state:
        st.session_state.master_data_cache = None
        st.session_state.filter_options_cache = None
        st.session_state.initial_load_done = False
    
    # INSTANT LOADING tanpa progress bar yang lambat
    if not st.session_state.initial_load_done:
        try:
            # Background loading tanpa UI spinner
            from sheets_utils import cached_read_master_data
            df_master = cached_read_master_data()
            filter_options = get_filter_options_fast(df_master)
            
            # Cache untuk akses instant
            st.session_state.master_data_cache = df_master
            st.session_state.filter_options_cache = filter_options
            st.session_state.initial_load_done = True
            
        except Exception as e:
            df_master = pd.DataFrame()
            filter_options = {}
            st.session_state.initial_load_done = True
    
    # Gunakan cached data jika tersedia
    if st.session_state.master_data_cache is not None:
        df_master = st.session_state.master_data_cache
        filter_options = st.session_state.filter_options_cache or {}
        
        if not df_master.empty:
            
            # ===== KOMPONEN FILTER DATA UI/UX (OPTIMASI UNTUK DATASET BESAR) =====
            st.markdown("### 🎯 Filter Data")
            
            # CSS untuk styling filter
            st.markdown("""
            <style>
            .filter-header {
                color: #007C8F;
                font-weight: bold;
                font-size: 16px;
                margin-bottom: 2px;
                display: flex;
                align-items: center;
                gap: 8px;
            }
            div[data-testid="stSelectbox"] {
                margin-top: -10px !important;
            }
            .action-buttons-wrapper {
                border: 2px solid #C84B37;
                border-radius: 8px;
                padding: 15px;
                margin: 10px 0;
                background-color: rgba(200, 75, 55, 0.05);
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 10px;
            }
            </style>
            """, unsafe_allow_html=True)
            
            # Inisialisasi session state untuk filter
            if 'filter_state' not in st.session_state:
                st.session_state.filter_state = {
                    'up3': 'Semua',
                    'ulp': 'Semua', 
                    'penyulang': 'Semua',
                    'equipment': 'Semua',
                    'jenis_temuan': 'Semua',
                    'status_eksekusi': 'Semua'
                }
                
            # Inisialisasi temporary filter state jika tidak ada (untuk menyimpan pilihan yang belum diterapkan)
            if 'temp_filter' not in st.session_state:
                st.session_state.temp_filter = {
                    'up3': st.session_state.filter_state['up3'],
                    'ulp': st.session_state.filter_state['ulp'], 
                    'penyulang': st.session_state.filter_state['penyulang'],
                    'equipment': st.session_state.filter_state['equipment'],
                    'jenis_temuan': st.session_state.filter_state['jenis_temuan'],
                    'status_eksekusi': st.session_state.filter_state['status_eksekusi']
                }
            
            # ===== FILTER YANG DIOPTIMASI UNTUK DATASET BESAR =====
            with st.container():
                # Row 1: Filter Utama (3 kolom)
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.markdown('<div class="filter-header">🏢 UP3</div>', unsafe_allow_html=True)
                    up3_options = filter_options.get('UP3', ['Semua'])
                    if 'Semua' not in up3_options:
                        up3_options = ['Semua'] + up3_options
                    selected_up3 = st.selectbox(
                        "", 
                        up3_options, 
                        key="temp_filter_up3",
                        index=up3_options.index(st.session_state.temp_filter['up3']) if st.session_state.temp_filter['up3'] in up3_options else 0
                    )
                    # Store selection in temporary filter state
                    st.session_state.temp_filter['up3'] = selected_up3
                
                with col2:
                    st.markdown('<div class="filter-header">🏪 ULP</div>', unsafe_allow_html=True)
                    ulp_options = filter_options.get('ULP', ['Semua'])
                    if 'Semua' not in ulp_options:
                        ulp_options = ['Semua'] + ulp_options
                    selected_ulp = st.selectbox(
                        "", 
                        ulp_options, 
                        key="temp_filter_ulp",
                        index=ulp_options.index(st.session_state.temp_filter['ulp']) if st.session_state.temp_filter['ulp'] in ulp_options else 0
                    )
                    # Store selection in temporary filter state
                    st.session_state.temp_filter['ulp'] = selected_ulp
                
                with col3:
                    st.markdown('<div class="filter-header">🔌 Nama Penyulang</div>', unsafe_allow_html=True)
                    penyulang_options = filter_options.get('NAMA PENYULANG', ['Semua'])
                    if 'Semua' not in penyulang_options:
                        penyulang_options = ['Semua'] + penyulang_options
                    selected_penyulang = st.selectbox(
                        "", 
                        penyulang_options, 
                        key="temp_filter_penyulang",
                        index=penyulang_options.index(st.session_state.temp_filter['penyulang']) if st.session_state.temp_filter['penyulang'] in penyulang_options else 0
                    )
                    # Store selection in temporary filter state
                    st.session_state.temp_filter['penyulang'] = selected_penyulang
                
                st.markdown("<br>", unsafe_allow_html=True)
                
                # Row 2: Filter Lanjutan (3 kolom)
                col4, col5, col6 = st.columns(3)
                
                with col4:
                    st.markdown('<div class="filter-header">⚙ Equipment</div>', unsafe_allow_html=True)
                    equipment_options = filter_options.get('EQUIPMENT', ['Semua'])
                    if 'Semua' not in equipment_options:
                        equipment_options = ['Semua'] + equipment_options
                    selected_equipment = st.selectbox(
                        "", 
                        equipment_options, 
                        key="temp_filter_equipment",
                        index=equipment_options.index(st.session_state.temp_filter['equipment']) if st.session_state.temp_filter['equipment'] in equipment_options else 0
                    )
                    # Store selection in temporary filter state
                    st.session_state.temp_filter['equipment'] = selected_equipment
                
                with col5:
                    st.markdown('<div class="filter-header">🎯 Jenis Temuan</div>', unsafe_allow_html=True)
                    temuan_options = filter_options.get('JENIS TEMUAN', ['Semua'])
                    if 'Semua' not in temuan_options:
                        temuan_options = ['Semua'] + temuan_options
                    selected_temuan = st.selectbox(
                        "", 
                        temuan_options, 
                        key="temp_filter_temuan",
                        index=temuan_options.index(st.session_state.temp_filter['jenis_temuan']) if st.session_state.temp_filter['jenis_temuan'] in temuan_options else 0
                    )
                    # Store selection in temporary filter state
                    st.session_state.temp_filter['jenis_temuan'] = selected_temuan
                
                with col6:
                    st.markdown('<div class="filter-header">⚡ Status Eksekusi</div>', unsafe_allow_html=True)
                    status_options = filter_options.get('STATUS EKSEKUSI', ['Selesai', 'Belum Selesai'])
                    if 'Semua' not in status_options:
                        status_options = ['Semua'] + status_options
                    selected_status = st.selectbox(
                        "", 
                        status_options, 
                        key="temp_filter_status",
                        index=status_options.index(st.session_state.temp_filter['status_eksekusi']) if st.session_state.temp_filter['status_eksekusi'] in status_options else 0
                    )
                    # Store selection in temporary filter state
                    st.session_state.temp_filter['status_eksekusi'] = selected_status
            
            # ===== TOMBOL AKSI DAN FILTERING =====
            st.markdown("<br>", unsafe_allow_html=True)
            
            col_action1, col_action2, col_action3 = st.columns(3)
            
            with col_action1:
                if st.button("🔄 Reset Filter", help="Reset semua filter", use_container_width=True):
                    # Reset both filter states
                    default_filter = {
                        'up3': 'Semua', 'ulp': 'Semua', 'penyulang': 'Semua',
                        'equipment': 'Semua', 'jenis_temuan': 'Semua', 'status_eksekusi': 'Semua'
                    }
                    st.session_state.filter_state = default_filter.copy()
                    st.session_state.temp_filter = default_filter.copy()
                    st.rerun()
            
            with col_action2:
                if st.button("✅ Apply Filter", help="Terapkan filter yang dipilih", use_container_width=True):
                    # Update filter state from temporary filter state
                    st.session_state.filter_state = st.session_state.temp_filter.copy()
                    st.success("✅ Filter berhasil diterapkan!")
                    st.rerun()
            
            # ===== APPLY FILTERING DENGAN OPTIMASI =====
            filter_dict = {}
            active_filters = []
            
            if st.session_state.filter_state['up3'] != 'Semua':
                filter_dict['UP3'] = st.session_state.filter_state['up3']
                active_filters.append(f"UP3: {st.session_state.filter_state['up3']}")
            
            if st.session_state.filter_state['ulp'] != 'Semua':
                filter_dict['ULP'] = st.session_state.filter_state['ulp']
                active_filters.append(f"ULP: {st.session_state.filter_state['ulp']}")
                
            if st.session_state.filter_state['penyulang'] != 'Semua':
                filter_dict['NAMA PENYULANG'] = st.session_state.filter_state['penyulang']
                active_filters.append(f"Penyulang: {st.session_state.filter_state['penyulang']}")
                
            if st.session_state.filter_state['equipment'] != 'Semua':
                filter_dict['EQUIPMENT'] = st.session_state.filter_state['equipment']
                active_filters.append(f"Equipment: {st.session_state.filter_state['equipment']}")
                
            if st.session_state.filter_state['jenis_temuan'] != 'Semua':
                filter_dict['JENIS TEMUAN'] = st.session_state.filter_state['jenis_temuan']
                active_filters.append(f"Jenis Temuan: {st.session_state.filter_state['jenis_temuan']}")
                
            if st.session_state.filter_state['status_eksekusi'] != 'Semua':
                filter_dict['STATUS EKSEKUSI'] = st.session_state.filter_state['status_eksekusi']
                active_filters.append(f"Status: {st.session_state.filter_state['status_eksekusi']}")
            
            # Filter data dengan optimasi untuk tampilan (limit untuk performa)
            filtered_df_display = filter_data_efficiently(df_master, filter_dict, limit=2000)
            
            # Filter data LENGKAP untuk export (tanpa limit)
            filtered_df_full = filter_data_efficiently(df_master, filter_dict, limit=None)
            
            # Export button dengan data LENGKAP (tidak di-limit)
            with col_action3:
                if not filtered_df_full.empty:
                    csv = filtered_df_full.to_csv(index=False)
                    export_count = len(filtered_df_full)
                    display_count = len(filtered_df_display)
                    
                    # Info untuk user tentang perbedaan tampilan vs download
                    if export_count > display_count:
                        help_text = f"Export SEMUA {export_count:,} data hasil filter (di layar hanya tampil {display_count:,})"
                    else:
                        help_text = f"Export semua {export_count:,} data hasil filter"
                    
                    st.download_button(
                        label=f"📥 Export Data ({export_count:,})",
                        data=csv,
                        file_name=f"inspeksi_filtered_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        help=help_text,
                        use_container_width=True
                    )
                else:
                    st.button("📥 Export Data", disabled=True, help="Tidak ada data untuk export", use_container_width=True)
            
            # ===== DISPLAY HASIL FILTER =====
            st.markdown("---")
            st.markdown("### 📊 *Hasil Data*")
            
            if active_filters:
                st.info(f"🎯 *Filter Aktif*: {' • '.join(active_filters)}")
            
            if not filtered_df_display.empty:
                # Info summary dengan informasi lengkap
                display_count = len(filtered_df_display)
                total_filtered = len(filtered_df_full)
                total_all = len(df_master)
                
                if total_filtered > display_count:
                    st.warning(f"📊 *Data Summary: Menampilkan **{display_count:,}* dari *{total_filtered:,}* hasil filter (Total database: {total_all:,} records)")
                    st.info(f"💡 *Tip: Untuk mendapat semua {total_filtered:,} data, gunakan tombol **Export Data* di atas")
                else:
                    st.success(f"✅ Menampilkan *{display_count:,}* dari *{total_all:,}* total data")
                
                # Konfigurasi kolom untuk tampilan yang lebih baik
                column_config = {
                    "NO": st.column_config.NumberColumn("No.", width="small", format="%d"),
                    "ID SURVEY": st.column_config.TextColumn("🆔 ID Survey", width="medium"),
                    "UP3": st.column_config.TextColumn("🏢 UP3", width="small"),
                    "ULP": st.column_config.TextColumn("🏪 ULP", width="medium"),
                    "NAMA PENYULANG": st.column_config.TextColumn("🔌 Penyulang", width="medium"),
                    "EQUIPMENT": st.column_config.TextColumn("⚙ Equipment", width="medium"),
                    "JENIS TEMUAN": st.column_config.TextColumn("🎯 Jenis Temuan", width="medium"),
                    "STATUS EKSEKUSI": st.column_config.TextColumn("⚡ Status Eksekusi", width="medium"),
                    "TANGGAL SURVEY": st.column_config.DateColumn("📅 Tanggal Survey", width="medium", format="YYYY-MM-DD"),
                    "TANGGAL WO": st.column_config.DateColumn("📅 Tanggal WO", width="medium", format="YYYY-MM-DD"),
                    "TANGGAL HAR": st.column_config.DateColumn("📅 Tanggal HAR", width="medium", format="YYYY-MM-DD"),
                    "NAMA ASET": st.column_config.TextColumn("⚡ Aset", width="medium")
                }
                
                st.dataframe(
                    filtered_df_display,
                    use_container_width=True,
                    column_config=column_config,
                    hide_index=True,
                    height=500
                )
                
            else:
                st.warning("⚠ Tidak ada data yang sesuai dengan filter yang dipilih.")
                st.info("💡 Coba ubah kombinasi filter atau klik Reset untuk melihat semua data.")
        
        else:
            st.warning("📝 Belum ada data dalam sistem. Silakan upload data terlebih dahulu.")
            if st.button("📁 Ke Halaman Upload"):
                st.session_state.page = "upload"
                st.rerun()
                
    else:
        st.error("❌ Gagal memuat data dashboard")

# Halaman Log
elif st.session_state.page == "log":
    st.header("📝 Log Aktivitas", divider="rainbow")
    try:
        log_df = read_log()
        if not log_df.empty:
            # Konfigurasi kolom untuk tampilan yang lebih baik
            log_column_config = {
                "NO": st.column_config.NumberColumn("No.", width="small", format="%d"),
                "Tanggal & Waktu": st.column_config.TextColumn("📅 Tanggal & Waktu", width="large"),
                "Jenis Aktivitas": st.column_config.TextColumn("⚡ Jenis Aktivitas", width="medium"),
                "Jumlah Data": st.column_config.NumberColumn("📊 Jumlah Data", width="medium", format="%d")
            }
            
            st.dataframe(
                log_df, 
                use_container_width=True,
                column_config=log_column_config,
                hide_index=True,
                height=400
            )
            st.success(f"✅ Total log aktivitas: *{len(log_df)}* entri")
        else:
            st.info("📝 Belum ada aktivitas yang tercatat dalam sistem.")
            st.markdown("💡 *Tip*: Log akan otomatis tercatat setiap kali Anda melakukan upload data.")
    except Exception as e:
        st.error(f"❌ Gagal membaca log aktivitas: {e}")
        if st.button("🔄 Coba Lagi"):
            st.rerun()

# Footer
st.markdown("---")
st.caption("© 2025 – Sistem Monitoring Inspeksi • Dibuat untuk Magang MBKM PLN UID Lampung oleh Ganiya Syazwa")