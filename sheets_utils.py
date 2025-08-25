import pandas as pd
import gspread
from datetime import datetime
from google.oauth2.service_account import Credentials
import json
from typing import List, Tuple, Dict, Any, Set
import re
import logging
import os
import traceback
import streamlit as st

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Track newly uploaded data IDs
recently_uploaded_ids: Set[str] = set()

# Konfigurasi Google Sheets - SIMPLE VERSION
SPREADSHEET_ID = "1BUFojSbcnXCCDOZJ5oB0uvmDFvhWO69HvdcxhSoQwtM"
MASTER_SHEET_NAME = "MasterData"
LOG_SHEET_NAME = "LogAktivitas"

SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]

# Sheet dan Header yang Valid
VALID_SHEETS = ["TGK", "PSW", "KTB", "MTR"]
VALID_COLUMNS = [
    "NO", "ID SURVEY", "ROLE", "NAMA INSPEKTOR", "UP3", "ID ULP", "ULP", 
    "NAMA PENYULANG", "ID JTM", "ID ASET", "NAMA ASET", "JENIS INSPEKSI", 
    "FASILITAS", "EQUIPMENT", "JENIS TEMUAN", "KONDISI", "KETERANGAN", 
    "JENIS ASET", "PENUNJUK LOC", "STATUS ASET", "TANGGAL SURVEY", 
    "TANGGAL WO", "KOORDINAT X", "KOORDINAT Y", "TANGGAL HAR", 
    "NAMA_INSPEKTOR HAR", "TINDAKAN", "DETIL KETERANGAN TEMUAN HAR", 
    "STATUS HAR", "KETERANGAN HAR", "KODE TO", "KODE TO HAR", "PROGRAM HAR", 
    "FOTO SURVEY", "FOTO HAR", "STATUS EKSEKUSI", "KOORDINAT TEMUAN"
]

# Kolom untuk validasi duplikasi (31 kolom utama - excludes NO, FOTO SURVEY, FOTO HAR, KOORDINAT)
VALIDATION_COLUMNS = [
    "ID SURVEY", "ROLE", "NAMA INSPEKTOR", "UP3", "ID ULP", "ULP", 
    "NAMA PENYULANG", "ID JTM", "ID ASET", "NAMA ASET", "JENIS INSPEKSI", 
    "FASILITAS", "EQUIPMENT", "JENIS TEMUAN", "KONDISI", "KETERANGAN", 
    "JENIS ASET", "PENUNJUK LOC", "STATUS ASET", "TANGGAL SURVEY", 
    "TANGGAL WO", "TANGGAL HAR", 
    "NAMA_INSPEKTOR HAR", "TINDAKAN", "DETIL KETERANGAN TEMUAN HAR", 
    "STATUS HAR", "KETERANGAN HAR", "KODE TO", "KODE TO HAR", "PROGRAM HAR", 
    "STATUS EKSEKUSI"
]

# Kolom tanggal yang perlu standardisasi format
DATE_COLUMNS = ["TANGGAL SURVEY", "TANGGAL WO", "TANGGAL HAR"]

# Format tanggal standard untuk dashboard (YYYY-MM-DD)
STANDARD_DATE_FORMAT = "%Y-%m-%d"

# ===== KAMUS NORMALISASI TEKS UNTUK MENGHINDARI DUPLIKASI =====
# Kamus untuk standarisasi kata-kata yang sering typo atau variasi penulisan

NORMALIZATION_DICTIONARY = {
    # Kata negasi dan affirmasi
    'TIDAK': ['TDK', 'TIAK', 'TIDKA', 'TIDAK', 'TDK.', 'TIDK', 'TDAK', 'ENGGAK', 'ENGGA', 'GA', 'NGGAK', 'NDAK'],
    'YA': ['YES', 'IYA', 'IYAH', 'Y', 'OK', 'OKE', 'BETUL', 'BENAR'],
    
    # Status kondisi
    'BAIK': ['BAEK', 'BAK', 'BAGUS', 'OK', 'AMAN'],
    'BURUK': ['BRUK', 'BURU', 'JELEK', 'RUSAK', 'RUSAKK', 'RUSK', 'BAD', 'POOR'],
    'KURANG': ['KRNG', 'KURNG', 'KRG', 'KURANG BAIK', 'KURANG BAGUS', 'MINUS'],
    
    # Status eksekusi
    'SELESAI': ['SLSAI', 'SELESAI', 'SELESAAI', 'SLSE', 'DONE', 'FINISH', 'SUDAH', 'SUDAH SELESAI', 'COMPLETE', 'OK', 'FINISHED'],
    'BELUM SELESAI': ['BLM SELESAI', 'BELUM', 'BLM', 'BLUM', 'PENDING', 'PROSES', 'ON PROGRESS', 'PROGRESS', 'ONGOING', 'TIDAK SELESAI', 'TDK SELESAI'],
    
    # Status aset
    'NORMAL': ['NORML', 'BIASA', 'STANDAR', 'REGULAR'],
    'RUSAK': ['RUSK', 'RUSAKK', 'BRUK', 'DAMAGE', 'BROKEN'],
    
    # Equipment/Peralatan
    'TRAFO': ['TRAVO', 'TRANSFORMATOR', 'TRANSFORMER', 'TRAFO DISTRIBUSI', 'TRFO'],
    'TIANG': ['TNG', 'TIANG LISTRIK', 'POLE', 'TG'],
    'KABEL': ['KBL', 'CABLE', 'KBEL'],
    'KWH METER': ['KWH', 'KWHMETER', 'KWHM', 'METER LISTRIK', 'METERAN'],
    'PANEL': ['PNL', 'PANELL'],
    'SWITCH': ['SAKLAR', 'SAKELAR', 'SW'],
    
    # Jenis temuan umum
    'POHON': ['PHN', 'TREES', 'KAYU'],
    'PAGAR': ['PGR', 'FENCE'],
    'BANGUNAN': ['BGNN', 'GEDUNG', 'RUMAH', 'BUILDING'],
    'JALAN': ['JL', 'JL.', 'JALAN RAYA', 'ROAD'],
    
    # Lokasi umum
    'KAMPUNG': ['KP', 'KP.', 'KAMPONG'],
    'DESA': ['DS', 'DS.', 'VILLAGE'],
    'KELURAHAN': ['KEL', 'KEL.'],
    'KECAMATAN': ['KEC', 'KEC.'],
    'DUSUN': ['DSN', 'DSN.'],
    
    # Singkatan teknis PLN
    'JARINGAN TEGANGAN MENENGAH': ['JTM', 'JTM 20KV', '20KV'],
    'JARINGAN TEGANGAN RENDAH': ['JTR', 'JTR 380V', '380V'],
    'SALURAN UDARA TEGANGAN MENENGAH': ['SUTM'],
    'SALURAN UDARA TEGANGAN RENDAH': ['SUTR'],
    'GARDU DISTRIBUSI': ['GD', 'GARDU'],
    'RECLOSER': ['RC', 'RECLOSER OTOMATIS'],
    'SECTIONALIZER': ['SC', 'SECTION'],
    
    # Variasi UP3
    'TANJUNG KARANG': ['TANJUNGKARANG', 'TJG KARANG', 'TJK', 'TANKAR'],
    'KOTABUMI': ['KOTA BUMI', 'KTB', 'KOTABUMI'],
    'PRINGSEWU': ['PRINGSEU', 'PSW', 'PRINGS'],
    'METRO': ['MTR', 'METRO CITY'],
    
    # Variasi boolean/status
    'ADA': ['ADA', 'TERSEDIA', 'AVAILABLE', 'EXIST', 'DITEMUKAN'],
    'TIDAK ADA': ['TDK ADA', 'TIDAK ADA', 'KOSONG', 'EMPTY', 'NULL', 'NONE', 'TIDAK DITEMUKAN'],
    
    # Program HAR
    'PEMELIHARAAN': ['PEMELIHRAAN', 'MAINTENANCE', 'MAINT', 'PERAWATAN'],
    'PERBAIKAN': ['REPAIR', 'FIXING', 'PERBAIKN'],
    'PENGGANTIAN': ['REPLACEMENT', 'GANTI', 'TUKAR'],
    
    # Koordinat placeholder
    'TIDAK DIKETAHUI': ['TDK DIKETAHUI', 'UNKNOWN', 'NOT FOUND', 'NULL', 'KOSONG'],
}

def normalize_text_advanced(text: str) -> str:
    """
    Normalisasi teks advanced dengan kamus untuk menghindari duplikasi data.
    
    Fungsi ini mengatasi:
    - Typo dan variasi penulisan
    - Singkatan vs kata lengkap
    - Case sensitivity
    - Spasi berlebih
    - Karakter khusus
    
    Args:
        text: Teks yang akan dinormalisasi
        
    Returns:
        str: Teks yang sudah dinormalisasi
    """
    if pd.isna(text) or text == "":
        return ""
    
    # Konversi ke string dan bersihkan
    text = str(text).strip().upper()
    
    # Hapus karakter khusus yang tidak perlu
    import re
    text = re.sub(r'[^\w\s.-]', ' ', text)
    
    # Normalisasi spasi berlebih
    text = ' '.join(text.split())
    
    # KHUSUS: Handle singkatan lokasi dulu untuk menghindari konflik
    location_abbreviations = {
        'JL. ': 'JALAN ',
        'JL.': 'JALAN',
        'KP. ': 'KAMPUNG ',
        'KP.': 'KAMPUNG',
        'DS. ': 'DESA ',
        'DS.': 'DESA',
        'KEL. ': 'KELURAHAN ',
        'KEL.': 'KELURAHAN',
        'KEC. ': 'KECAMATAN ',
        'KEC.': 'KECAMATAN',
        'DSN. ': 'DUSUN ',
        'DSN.': 'DUSUN',
    }
    
    # Apply location abbreviations first (at the beginning of text)
    for abbrev, full in location_abbreviations.items():
        if text.startswith(abbrev):
            text = text.replace(abbrev, full, 1)
            break
    
    # Cari di kamus normalisasi - exact match dulu
    for standard_form, variations in NORMALIZATION_DICTIONARY.items():
        if text in variations or text == standard_form:
            return standard_form
    
    # Cari partial match untuk frasa yang mengandung kata kunci (skip location abbreviations)
    for standard_form, variations in NORMALIZATION_DICTIONARY.items():
        # Skip JALAN, KAMPUNG etc yang sudah dihandle di atas
        if standard_form in ['JALAN', 'KAMPUNG', 'DESA', 'KELURAHAN', 'KECAMATAN', 'DUSUN']:
            continue
            
        for variant in variations:
            if variant in text:
                # Replace hanya jika kata utuh (word boundary)
                pattern = r'\b' + re.escape(variant) + r'\b'
                if re.search(pattern, text):
                    text = re.sub(pattern, standard_form, text)
                break
    
    return text

def normalize_equipment_name(equipment: str) -> str:
    """Normalisasi nama equipment dengan fokus pada peralatan listrik PLN."""
    if pd.isna(equipment) or equipment == "":
        return ""
    
    # Gunakan normalisasi advanced dulu
    normalized = normalize_text_advanced(equipment)
    
    # Tambahan khusus untuk equipment
    equipment_specific = {
        'RECLOSER': ['RC', 'RECLOSER OTOMATIS', 'AUTO RECLOSER'],
        'SECTIONALIZER': ['SC', 'SECTION SWITCH'],
        'LBS': ['LOAD BREAK SWITCH', 'SWITCH PEMISAH'],
        'ARRESTER': ['ARESTER', 'PENANGKAL PETIR'],
        'KAPASITOR': ['CAPASITOR', 'CAPACITOR', 'CAP BANK'],
        'ISOLATOR': ['SWITCH ISOLASI', 'PEMISAH'],
        'BUSHING': ['BUSHING TRAFO', 'ISOLATOR BUSHING'],
        'GROUNDING': ['PEMBUMIAN', 'EARTHING', 'TANAH'],
    }
    
    for standard, variants in equipment_specific.items():
        for variant in variants:
            if variant in normalized:
                normalized = normalized.replace(variant, standard)
    
    return normalized

def normalize_status_execution(status: str) -> str:
    """Normalisasi khusus untuk STATUS EKSEKUSI."""
    if pd.isna(status) or status == "":
        return ""
    
    normalized = normalize_text_advanced(status)
    
    # Mapping khusus untuk status eksekusi dengan prioritas "BELUM SELESAI" lebih tinggi
    belum_keywords = ['BELUM', 'PENDING', 'PROSES', 'PROGRESS', 'ONGOING', 'BLM', 'BLUM']
    selesai_keywords = ['SELESAI', 'DONE', 'FINISH', 'COMPLETE', 'SUDAH', 'FINISHED']
    
    # Cek BELUM SELESAI dulu (prioritas tinggi)
    for keyword in belum_keywords:
        if keyword in normalized:
            return 'BELUM SELESAI'
    
    # Baru cek SELESAI
    for keyword in selesai_keywords:
        if keyword in normalized:
            return 'SELESAI'
    
    return normalized

def normalize_asset_status(status: str) -> str:
    """Normalisasi khusus untuk STATUS ASET."""
    if pd.isna(status) or status == "":
        return ""
    
    # Gunakan normalisasi advanced dulu
    normalized = normalize_text_advanced(status)
    
    # Mapping khusus untuk status aset - cek pola kata
    if any(word in normalized for word in ['BURUK', 'RUSAK', 'JELEK', 'BAD', 'POOR']):
        return 'BURUK'
    elif any(word in normalized for word in ['KURANG']):
        return 'KURANG'
    elif any(word in normalized for word in ['BAIK', 'BAGUS', 'AMAN', 'NORMAL']):
        return 'BAIK'
    # Jangan map 'OK' dan 'YA' ke BAIK karena bisa ambigu
    
    return normalized

# Inisialisasi koneksi Google Sheets - SIMPLE VERSION
def get_google_sheet_connection():
    """
    Koneksi sederhana ke Google Sheet menggunakan credentials.json
    """
    try:
        # Mulai tanpa kredensial; akan diisi dari st.secrets atau credentials.json
        credentials = None
        # 1) Coba dari Streamlit secrets (untuk deployment)
        try:
            import streamlit as st  # local import to avoid hard dep in non-app contexts
            if 'gcp_service_account' in st.secrets:
                info = dict(st.secrets['gcp_service_account'])
                credentials = Credentials.from_service_account_info(info, scopes=SCOPE)
        except Exception:
            pass

        # 2) Fallback ke  credentials.json (untuk lokal)
        if credentials is None:
            credentials_path = os.path.join(os.path.dirname(__file__), "credentials.json")
            if not os.path.exists(credentials_path):
                raise FileNotFoundError(f"File credentials.json tidak ditemukan di: {credentials_path}")
            credentials = Credentials.from_service_account_file(credentials_path, scopes=SCOPE)
        gc = gspread.authorize(credentials)
        
        # Buka spreadsheet
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        logger.info("✅ Berhasil koneksi ke Google Sheet")
        
        return spreadsheet
        
    except Exception as e:
        logger.error(f"❌ Error connecting to Google Sheet: {str(e)}")
        raise Exception(f"Gagal koneksi ke Google Sheet: {str(e)}")

# Initialize connection
try:
    sh = get_google_sheet_connection()
    logger.info("✅ Koneksi Google Sheet berhasil diinisialisasi")
except Exception as e:
    sh = None
    logger.warning(f"⚠️ Google Sheet connection failed: {str(e)}")
    logger.info("ℹ️ Koneksi akan dicoba ulang saat dibutuhkan")

def standardize_date_format(date_val: Any) -> str:
    """
    Standardisasi format tanggal ke format YYYY-MM-DD untuk konsistensi dashboard.
    
    Mendukung berbagai format input:
    - DD/MM/YYYY, DD-MM-YYYY, DD.MM.YYYY
    - MM/DD/YYYY, MM-DD-YYYY, MM.DD.YYYY  
    - YYYY/MM/DD, YYYY-MM-DD, YYYY.MM.DD
    - DD/MM/YY, DD-MM-YY, DD.MM.YY
    - Excel date serial numbers
    - ISO format dengan time
    
    Args:
        date_val: Nilai tanggal dalam berbagai format
        
    Returns:
        str: Tanggal dalam format YYYY-MM-DD atau string kosong jika invalid
    """
    if pd.isna(date_val) or date_val == "" or str(date_val).strip() == "":
        return ""
    
    date_str = str(date_val).strip()
    
    # Skip jika sudah kosong atau placeholder
    if date_str.lower() in ['', 'nan', 'none', 'null', '-', 'n/a']:
        return ""
    
    try:
        # Case 1: Sudah dalam format standard YYYY-MM-DD
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            # Validate date
            datetime.strptime(date_str, '%Y-%m-%d')
            return date_str
        
        # Case 2: Excel date serial number (numeric)
        if date_str.replace('.', '').replace('-', '').isdigit():
            try:
                excel_date = float(date_str)
                if 1 <= excel_date <= 100000:  # Reasonable range for Excel dates
                    # Excel epoch is 1900-01-01 (but Excel wrongly treats 1900 as leap year)
                    # Use pandas for more accurate Excel date conversion
                    result_date = pd.to_datetime(excel_date, origin='1899-12-30', unit='D')
                    return result_date.strftime(STANDARD_DATE_FORMAT)
            except (ValueError, OverflowError):
                pass
        
        # Case 3: ISO format with time (YYYY-MM-DD HH:MM:SS)
        if 'T' in date_str or len(date_str) > 10:
            # Try parsing ISO format first
            for fmt in ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f']:
                try:
                    parsed_date = datetime.strptime(date_str, fmt)
                    return parsed_date.strftime(STANDARD_DATE_FORMAT)
                except ValueError:
                    continue
        
        # Case 4: Common date formats
        date_formats = [
            # DD/MM/YYYY variants
            '%d/%m/%Y', '%d-%m-%Y', '%d.%m.%Y',
            # MM/DD/YYYY variants  
            '%m/%d/%Y', '%m-%d-%Y', '%m.%d.%Y',
            # YYYY/MM/DD variants
            '%Y/%m/%d', '%Y-%m-%d', '%Y.%m.%d',
            # DD/MM/YY variants (2-digit year)
            '%d/%m/%y', '%d-%m-%y', '%d.%m.%y',
            # MM/DD/YY variants
            '%m/%d/%y', '%m-%d-%y', '%m.%d.%y',
            # Indonesian format variants
            '%d %m %Y', '%d %m %y',
            # Other common formats
            '%Y%m%d', '%d%m%Y'
        ]
        
        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                
                # Handle 2-digit years (assume 20xx for years 00-50, 19xx for 51-99)
                if parsed_date.year < 1950:  # This means it was 2-digit year 00-49
                    parsed_date = parsed_date.replace(year=parsed_date.year + 2000)
                elif 1950 <= parsed_date.year < 2000:  # This means it was 2-digit year 50-99
                    # Already correct, do nothing
                    pass
                
                return parsed_date.strftime(STANDARD_DATE_FORMAT)
                
            except ValueError:
                continue
        
        # Case 5: Try pandas to_datetime as fallback
        try:
            parsed_date = pd.to_datetime(date_str, dayfirst=True, errors='raise')
            return parsed_date.strftime(STANDARD_DATE_FORMAT)
        except:
            pass
        
        # If all parsing fails, log warning and return empty
        print(f"⚠ Warning: Tidak dapat parsing tanggal '{date_str}', menggunakan nilai kosong")
        return ""
        
    except Exception as e:
        print(f"⚠ Warning: Error parsing tanggal '{date_str}': {str(e)}")
        return ""


def are_rows_identical(row1: pd.Series, row2: pd.Series) -> bool:
    """
    CASE 4: Membandingkan SEMUA kolom validasi apakah identik persis.
    Menggunakan normalisasi comprehensive untuk mengatasi duplikasi akibat typo.
    Hanya membandingkan kolom yang ada di kedua rows.
    """
    try:
        # Ambil kolom yang ada di kedua rows
        common_columns = set(row1.index) & set(row2.index)
        
        # Jika tidak ada kolom yang sama, anggap tidak identik
        if not common_columns:
            return False
        
        for col in common_columns:
            # Ambil nilai raw
            raw_val1 = row1[col] if pd.notna(row1[col]) else ""
            raw_val2 = row2[col] if pd.notna(row2[col]) else ""
            
            # Tentukan fungsi normalisasi yang sesuai berdasarkan nama kolom
            col_upper = col.upper()
            if 'LOKASI' in col_upper or 'ALAMAT' in col_upper:
                val1 = normalize_location_name(raw_val1)
                val2 = normalize_location_name(raw_val2)
            elif 'INSPEKTUR' in col_upper or 'PETUGAS' in col_upper:
                val1 = normalize_inspector_name(raw_val1)
                val2 = normalize_inspector_name(raw_val2)
            elif 'EQUIPMENT' in col_upper or 'ASET' in col_upper or 'PERALATAN' in col_upper:
                val1 = normalize_equipment_name(raw_val1)
                val2 = normalize_equipment_name(raw_val2)
            elif 'STATUS EKSEKUSI' in col_upper or 'STATUS PEKERJAAN' in col_upper:
                val1 = normalize_status_execution(raw_val1)
                val2 = normalize_status_execution(raw_val2)
            elif 'STATUS ASET' in col_upper or 'KONDISI' in col_upper:
                val1 = normalize_asset_status(raw_val1)
                val2 = normalize_asset_status(raw_val2)
            else:
                # Gunakan normalisasi umum untuk kolom lainnya
                val1 = normalize_text_advanced(raw_val1)
                val2 = normalize_text_advanced(raw_val2)
            
            if val1 != val2:
                return False  # Ada perbedaan setelah normalisasi
        
        return True  # Semua kolom identik setelah normalisasi
        
    except Exception as e:
        logger.error(f"Error comparing rows with normalization: {str(e)}")
        return False


def has_empty_columns_to_update(existing_row: pd.Series, new_row: pd.Series) -> bool:
    """
    CASE 2: Cek apakah ada kolom kosong di existing yang bisa diisi dari new_row
    """
    try:
        for col in VALIDATION_COLUMNS:
            if col in existing_row.index and col in new_row.index:
                # Nilai existing (kosong/null)
                existing_val = str(existing_row[col]).strip() if pd.notna(existing_row[col]) else ""
                # Nilai baru (ada isi)
                new_val = str(new_row[col]).strip() if pd.notna(new_row[col]) else ""
                
                # Jika existing kosong TAPI new ada isi → bisa di-update
                if existing_val == "" and new_val != "":
                    return True
        
        return False
        
    except Exception as e:
        logger.error(f"Error checking empty columns: {str(e)}")
        return False


def get_update_data(existing_row: pd.Series, new_row: pd.Series) -> Dict[str, str]:
    """
    Ambil data yang perlu di-update (hanya kolom kosong)
    """
    try:
        update_data = {}
        
        for col in VALIDATION_COLUMNS:
            if col in existing_row.index and col in new_row.index:
                existing_val = str(existing_row[col]).strip() if pd.notna(existing_row[col]) else ""
                new_val = str(new_row[col]).strip() if pd.notna(new_row[col]) else ""
                
                # Update hanya jika existing kosong dan new ada nilai
                if existing_val == "" and new_val != "":
                    update_data[col] = new_val
        
        return update_data
        
    except Exception as e:
        logger.error(f"Error getting update data: {str(e)}")
        return {}


def validate_date_consistency(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Validasi konsistensi format tanggal dalam DataFrame.
    
    Args:
        df: DataFrame untuk divalidasi
        
    Returns:
        Tuple[bool, List[str]]: (is_valid, warning_messages)
    """
    warnings = []
    
    for col in DATE_COLUMNS:
        if col in df.columns:
            # Check for inconsistent date formats
            non_empty_dates = df[df[col].astype(str).str.strip() != ''][col]
            
            if not non_empty_dates.empty:
                # Count different date patterns
                patterns = {
                    'YYYY-MM-DD': 0,
                    'DD/MM/YYYY': 0,
                    'MM/DD/YYYY': 0,
                    'DD-MM-YYYY': 0,
                    'Other': 0
                }
                
                for date_val in non_empty_dates:
                    date_str = str(date_val).strip()
                    if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
                        patterns['YYYY-MM-DD'] += 1
                    elif re.match(r'^\d{2}/\d{2}/\d{4}$', date_str):
                        # Could be DD/MM/YYYY or MM/DD/YYYY - ambiguous
                        patterns['DD/MM/YYYY'] += 1
                    elif re.match(r'^\d{2}-\d{2}-\d{4}$', date_str):
                        patterns['DD-MM-YYYY'] += 1
                    else:
                        patterns['Other'] += 1
                
                # Report if multiple formats found
                active_patterns = {k: v for k, v in patterns.items() if v > 0}
                if len(active_patterns) > 1:
                    warnings.append(f"Kolom {col}: Ditemukan {len(active_patterns)} format tanggal berbeda: {active_patterns}")
    
    is_valid = len(warnings) == 0
    return is_valid, warnings


def clean_coordinate(val: str) -> str:
    """
    Membersihkan koordinat dengan MEMPERTAHANKAN FORMAT ASLI sepenuhnya.
    
    PENTING: Koordinat bisa negatif (-) atau positif (+) dalam berbagai sistem:
    - Geographic (WGS84): Latitude -90 to +90, Longitude -180 to +180
    - UTM: Bisa nilai besar seperti 531.639, 10.485.903, dll
    - Sistem lokal: Format bebas sesuai kebutuhan survey
    
    PRINSIP: TIDAK ADA VALIDASI RANGE - Terima semua format koordinat
    """
    if pd.isna(val) or val == "":
        return ""
    
    try:
        # Konversi ke string dan HANYA hapus spasi di awal/akhir
        val_str = str(val).strip()
        
        # Jika nilai kosong setelah strip, return kosong
        if not val_str or val_str == "0":
            return ""
        
        # VALIDASI MINIMAL: Cek apakah bisa dikonversi ke float (untuk deteksi format angka)
        try:
            float(val_str)
            # Jika berhasil dikonversi, berarti format angka valid
            # RETURN NILAI ASLI tanpa modifikasi apapun
            return val_str
        except (ValueError, OverflowError):
            # Jika tidak bisa dikonversi ke float, tetap return nilai asli
            # Mungkin format khusus atau mengandung karakter lain
            return val_str
            
    except Exception:
        # Jika ada error apapun, pertahankan nilai asli sebagai string
        return str(val) if val is not None else ""

def normalize_location_name(text: str) -> str:
    """
    Normalisasi nama lokasi untuk menghindari duplikasi data.
    Menggunakan kamus comprehensive untuk mengatasi typo dan variasi penulisan.
    """
    if pd.isna(text) or text == "":
        return ""
    
    # Gunakan normalisasi advanced sebagai base
    normalized = normalize_text_advanced(text)
    
    # Regional mappings DULU - sebelum singkatan umum
    regional_mappings = {
        'BANDAR LAMPUNG': ['BDL', 'BANDAR LAMPUNG', 'B.LAMPUNG'],
        'LAMPUNG TENGAH': ['LAMTENG'],
        'LAMPUNG SELATAN': ['LAMSEL'], 
        'LAMPUNG UTARA': ['LAMUT'],
        'LAMPUNG TIMUR': ['LAMTIM', 'LTIM'],
        'LAMPUNG BARAT': ['LAMBAR'],
        'PESAWARAN': ['PSW', 'PESAWRAN'],
        'TULANG BAWANG': ['TUBABA'],
        'TANGGAMUS': ['TGS', 'TANGGAMS'],
    }
    
    # Apply regional normalizations dulu
    for standard, variants in regional_mappings.items():
        for variant in variants:
            if variant in normalized:
                normalized = normalized.replace(variant, standard)
    
    # Kemudian baru singkatan lokasi umum - PERHATIKAN URUTAN DAN TITIK
    location_mappings = [
        ('JL. ', 'JALAN '),   # dengan titik dan spasi dulu
        ('JL.', 'JALAN'),     # dengan titik tanpa spasi
        ('JL ', 'JALAN '),    # tanpa titik dengan spasi
        ('KP. ', 'KAMPUNG '),
        ('KP.', 'KAMPUNG'),
        ('KP ', 'KAMPUNG '),
        ('DS. ', 'DESA '),
        ('DS.', 'DESA'),
        ('DS ', 'DESA '),
        ('KEL. ', 'KELURAHAN '),
        ('KEL.', 'KELURAHAN'),
        ('KEL ', 'KELURAHAN '),
        ('KEC. ', 'KECAMATAN '),
        ('KEC.', 'KECAMATAN'),
        ('KEC ', 'KECAMATAN '),
        ('DSN. ', 'DUSUN '),
        ('DSN.', 'DUSUN'),
        ('DSN ', 'DUSUN '),
        ('GG. ', 'GANG '),
        ('GG.', 'GANG'),
        ('GG ', 'GANG '),
    ]
    
    # Apply location-specific normalizations - hanya di awal string
    for variant, standard in location_mappings:
        if normalized.startswith(variant):
            # Replace dan hilangkan titik yang berlebih
            remaining = normalized[len(variant):]
            normalized = standard + remaining
            break  # Stop setelah replacement pertama
    
    return normalized

def normalize_inspector_name(name: str) -> str:
    """
    Normalisasi nama inspektur untuk menghindari duplikasi data.
    Menggunakan kamus comprehensive untuk mengatasi typo dan variasi gelar.
    """
    if pd.isna(name) or name == "":
        return ""
    
    # Gunakan normalisasi advanced sebagai base
    normalized = normalize_text_advanced(name)
    
    # Mapping khusus untuk gelar dan sebutan - STANDARISASI KE BENTUK PENDEK
    title_mappings = {
        'BAPAK': ['BAPAK', 'PAK', 'BP', 'BP.', 'MR', 'MR.'],  # Semua jadi BAPAK
        'IBU': ['IBU', 'BU', 'MRS', 'MRS.', 'MS', 'MS.'],     # Semua jadi IBU
        'SAUDARA': ['SAUDARA', 'SDR', 'SDR.', 'BROTHER'],     # Semua jadi SAUDARA
        'SAUDARI': ['SAUDARI', 'SDRI', 'SDRI.', 'SISTER'],    # Semua jadi SAUDARI
    }
    
    # Apply title normalizations - prioritas exact match dulu
    for standard, variants in title_mappings.items():
        for variant in variants:
            if normalized.startswith(variant + ' '):
                # Replace the title at the beginning
                normalized = standard + normalized[len(variant):]
                break
        else:
            continue
        break  # Stop setelah replacement pertama
    
    # Hapus gelar akademik/profesi (tapi normalisasi dulu)
    academic_titles = ['IR.', 'IR', 'S.T.', 'ST.', 'ST', 'S.KOM.', 'S.KOM', ',S.T', ',ST', ',S.KOM', 'MT', 'M.T.']
    for title in academic_titles:
        import re
        # Remove with word boundary
        pattern = r'\b' + re.escape(title) + r'\b'
        normalized = re.sub(pattern, '', normalized)
    
    # Bersihkan spasi berlebih
    normalized = ' '.join(normalized.split())
    return normalized

def normalize_asset_name(text: str) -> str:
    """
    Normalisasi nama aset menggunakan kamus comprehensive untuk menghindari duplikasi.
    Fokus pada peralatan listrik PLN dan variasi penulisannya.
    """
    if pd.isna(text) or text == "":
        return ""
    
    # Gunakan normalisasi equipment yang sudah comprehensive
    return normalize_equipment_name(text)

def apply_comprehensive_normalization(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplikasikan normalisasi comprehensive ke semua kolom yang relevan.
    Fungsi ini mencegah duplikasi data akibat typo dan variasi penulisan.
    
    Args:
        df: DataFrame yang akan dinormalisasi
        
    Returns:
        pd.DataFrame: DataFrame dengan data yang sudah dinormalisasi
    """
    if df.empty:
        return df
    
    # Buat copy untuk menghindari SettingWithCopyWarning
    df_normalized = df.copy()
    
    # Mapping kolom dengan fungsi normalisasi yang sesuai
    normalization_mapping = {
        # Kolom lokasi
        'NAMA LOKASI ASET': normalize_location_name,
        'LOKASI': normalize_location_name,
        'ALAMAT': normalize_location_name,
        'KOORDINAT LOKASI': normalize_location_name,
        
        # Kolom nama orang
        'NAMA INSPEKTUR': normalize_inspector_name,
        'INSPEKTUR': normalize_inspector_name,
        'PETUGAS': normalize_inspector_name,
        'TEKNISI': normalize_inspector_name,
        
        # Kolom equipment/aset
        'NAMA ASET': normalize_equipment_name,
        'JENIS EQUIPMENT': normalize_equipment_name,
        'EQUIPMENT': normalize_equipment_name,
        'PERALATAN': normalize_equipment_name,
        
        # Kolom status eksekusi
        'STATUS EKSEKUSI': normalize_status_execution,
        'STATUS PEKERJAAN': normalize_status_execution,
        'PROGRESS': normalize_status_execution,
        
        # Kolom status aset
        'STATUS ASET': normalize_asset_status,
        'KONDISI ASET': normalize_asset_status,
        'KONDISI': normalize_asset_status,
        
        # Kolom lainnya yang perlu normalisasi umum
        'JENIS TEMUAN': normalize_text_advanced,
        'KATEGORI': normalize_text_advanced,
        'TINDAKAN': normalize_text_advanced,
        'KETERANGAN': normalize_text_advanced,
        'UP3': normalize_text_advanced,
        'PROGRAM': normalize_text_advanced,
    }
    
    # Apply normalization untuk setiap kolom yang ada di DataFrame
    for column in df_normalized.columns:
        if column in normalization_mapping:
            try:
                print(f"Normalizing column: {column}")
                df_normalized[column] = df_normalized[column].apply(
                    normalization_mapping[column]
                )
            except Exception as e:
                print(f"Warning: Error normalizing column {column}: {e}")
                # Jika error, tetap gunakan nilai asli
                continue
    
    return df_normalized

def normalisasi_teks(val: Any) -> str:
    """Normalisasi nilai teks dengan mempertahankan nilai asli."""
    if pd.isna(val):
        return ""
    
    if isinstance(val, (int, float)):
        return str(val)
    
    if not isinstance(val, str):
        return str(val)
    
    return str(val).strip()  # Hanya menghapus spasi di awal dan akhir

def preprocess_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Pra-proses DataFrame dengan mempertahankan nilai asli data."""
    # Buat copy untuk menghindari SettingWithCopyWarning
    df = df.copy()
    
    # Normalisasi minimal hanya untuk keperluan pencarian duplikat
    if 'NAMA INSPEKTOR' in df.columns:
        df['_NAMA_INSPEKTOR_NORM'] = df['NAMA INSPEKTOR'].apply(normalize_inspector_name)
    if 'NAMA INSPEKTOR HAR' in df.columns:
        df['_NAMA_INSPEKTOR_HAR_NORM'] = df['NAMA INSPEKTOR HAR'].apply(normalize_inspector_name)
    
    if 'NAMA ASET' in df.columns:
        df['_NAMA_ASET_NORM'] = df['NAMA ASET'].apply(normalize_asset_name)
    
    if 'PENUNJUK LOC' in df.columns:
        df['_PENUNJUK_LOC_NORM'] = df['PENUNJUK LOC'].apply(normalize_location_name)
    
    # Bersihkan nilai NaN menjadi string kosong
    return df.fillna("")

def validate_sheet_structure(df: pd.DataFrame) -> Tuple[bool, str]:
    """Validasi struktur DataFrame sesuai template yang ditentukan."""
    if df.empty:
        return False, "DataFrame kosong"
    
    # Validasi kolom wajib
    required_cols = ["ID SURVEY"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        return False, f"Kolom wajib tidak ditemukan: {', '.join(missing_cols)}"
    
    # Validasi nilai ID SURVEY
    if df["ID SURVEY"].isna().all():
        return False, "Semua nilai ID SURVEY kosong"
    
    return True, "Validasi berhasil"

def read_master_data(limit_rows: int | None = None) -> pd.DataFrame:
    """
    Baca data master dari Google Sheets.
    
    Args:
        limit_rows: Batasi jumlah baris yang dibaca (None = semua data)
    """
    try:
        # Initialize connection if needed
        global sh
        if sh is None:
            sh = get_google_sheet_connection()
        
        worksheet = sh.worksheet(MASTER_SHEET_NAME)
        
        # Optimized data reading
        if limit_rows:
            data = worksheet.get_values(f"A1:Z{limit_rows + 1}")
        else:
            data = worksheet.get_all_values()
        
        if not data:
            return pd.DataFrame()
        
        # Buat DataFrame dengan header dari baris pertama
        headers = data[0]
        rows = data[1:]
        
        if not rows:  # Jika tidak ada data setelah header
            return pd.DataFrame(columns=headers)
        
        df = pd.DataFrame(rows, columns=headers)
        
        # Optimasi: Proses hanya kolom yang diperlukan untuk performa
        essential_columns = [
            'NO', 'ID SURVEY', 'UP3', 'ULP', 'NAMA PENYULANG', 'EQUIPMENT', 
            'JENIS TEMUAN', 'STATUS EKSEKUSI', 'TANGGAL SURVEY', 'TANGGAL WO', 
            'TANGGAL HAR', 'NAMA ASET', 'NAMA INSPEKTOR'
        ]
        
        # MINIMAL PROCESSING - PERTAHANKAN FORMAT ASLI DATA
        for col in df.columns:
            if col in essential_columns:
                if col in ['KOORDINAT X', 'KOORDINAT Y', 'KOORDINAT TEMUAN']:
                    # Koordinat: hanya bersihkan tanpa mengubah format
                    df[col] = df[col].astype(str).apply(clean_coordinate)
                elif col in DATE_COLUMNS:
                    # Tanggal: HANYA standardisasi untuk dashboard (diperlukan sistem)
                    df[col] = df[col].apply(standardize_date_format)
                else:
                    # Kolom lain: HANYA strip spasi, TIDAK mengubah konten
                    df[col] = df[col].astype(str).str.strip()
                    # JANGAN replace empty string - biarkan nilai asli
            else:
                # Kolom non-essential: MINIMAL processing - hanya konversi ke string
                df[col] = df[col].astype(str)
        
        # NORMALISASI DATA UNTUK MENCEGAH DUPLIKASI
        # Terapkan normalisasi comprehensive hanya pada kolom yang perlu
        df = apply_targeted_normalization(df)
        
        # Tambahkan kolom NO jika tidak ada
        if not df.empty and "NO" not in df.columns:
            df.insert(0, "NO", range(1, len(df) + 1))
        
        # Hapus baris kosong
        df = df.dropna(how='all')
        df = df[df.apply(lambda x: x.str.strip().ne('').any(), axis=1)]
        
        print(f"📊 Data loaded: {len(df)} records")
        
        return df
        
    except Exception as e:
        raise Exception(f"Gagal membaca data master: {str(e)}")

# Lightweight cache wrapper to avoid repeated Google Sheets fetches across pages/reruns
@st.cache_data(ttl=60, show_spinner=False)
def cached_read_master_data(limit_rows: int | None = None) -> pd.DataFrame:
    """
    Cached wrapper over read_master_data to speed up page switches and initial load.
    TTL ensures external edits are picked up periodically or can be cleared on upload.
    """
    return read_master_data(limit_rows)

def apply_targeted_normalization(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplikasikan normalisasi secara targeted untuk mencegah duplikasi tanpa mengubah data asli.
    Hanya normalisasi kolom yang memang perlu untuk konsistensi data.
    
    Args:
        df: DataFrame yang akan dinormalisasi
        
    Returns:
        pd.DataFrame: DataFrame dengan kolom tertentu yang sudah dinormalisasi
    """
    if df.empty:
        return df
    
    # Buat copy untuk menghindari SettingWithCopyWarning
    df_normalized = df.copy()
    
    # Mapping kolom yang WAJIB dinormalisasi untuk mencegah duplikasi
    targeted_normalization = {
        # Kolom yang sering typo dan menyebabkan duplikasi false
        'STATUS EKSEKUSI': normalize_status_execution,
        'STATUS ASET': normalize_asset_status,
        'KONDISI ASET': normalize_asset_status,
        
        # Kolom equipment yang sering ada singkatan
        'EQUIPMENT': normalize_equipment_name,
        'JENIS EQUIPMENT': normalize_equipment_name,
        'NAMA ASET': normalize_equipment_name,
        
        # Kolom boolean/ya-tidak yang sering typo
        'JENIS TEMUAN': normalize_text_advanced,  # untuk "TIDAK" vs "TDK"
    }
    
    # Apply normalization hanya untuk kolom yang benar-benar perlu
    for column in df_normalized.columns:
        if column in targeted_normalization:
            try:
                print(f"   🔄 Normalizing: {column}")
                df_normalized[column] = df_normalized[column].apply(
                    targeted_normalization[column]
                )
            except Exception as e:
                print(f"   ⚠️ Warning: Error normalizing {column}: {e}")
                # Jika error, tetap gunakan nilai asli
                continue
    
    return df_normalized

def get_filter_options_fast(df: pd.DataFrame | None = None) -> Dict[str, List[str]]:
    """
    Ambil opsi filter dengan optimasi untuk dataset besar.
    
    Args:
        df: DataFrame yang sudah dimuat (optional, jika None akan load subset)
    """
    try:
        # Jika DataFrame tidak diberikan, load subset data untuk filter
        if df is None or df.empty:
            print("📊 Loading subset data untuk filter options...")
            df = read_master_data(limit_rows=5000)  # Load 5k records untuk filter
        
        # Default options untuk STATUS EKSEKUSI jika data kosong
        default_status_options = ['Selesai', 'Belum Selesai']
        
        # Ambil unique values hanya untuk kolom yang sering difilter
        filter_columns = {
            'UP3': 'UP3',
            'ULP': 'ULP', 
            'NAMA PENYULANG': 'NAMA PENYULANG',
            'EQUIPMENT': 'EQUIPMENT',
            'JENIS TEMUAN': 'JENIS TEMUAN',
            'STATUS EKSEKUSI': 'STATUS EKSEKUSI'
        }
        
        options = {}
        
        for key, col in filter_columns.items():
            if col in df.columns:
                # Optimasi: gunakan dropna() dan value_counts() untuk performa
                unique_vals = df[col].dropna().astype(str).str.strip()
                # Remove empty strings and common null representations
                unique_vals = unique_vals[unique_vals != '']
                unique_vals = unique_vals[unique_vals.str.upper() != 'NAN']
                unique_vals = unique_vals[unique_vals.str.upper() != 'NONE']
                unique_vals = unique_vals[unique_vals.str.upper() != 'NULL']
                
                # Don't limit any filter options to ensure all values are available for search
                unique_list = sorted(list(set(unique_vals)))  # No limit for any filters
                
                # Special handling untuk STATUS EKSEKUSI
                if key == 'STATUS EKSEKUSI':
                    if len(unique_list) == 0:
                        print(f"⚠ No valid STATUS EKSEKUSI values found, using defaults")
                        unique_list = default_status_options
                    else:
                        print(f"✅ Found STATUS EKSEKUSI values: {unique_list}")
                
                options[key] = unique_list
            else:
                # Column not found, use defaults for STATUS EKSEKUSI
                if key == 'STATUS EKSEKUSI':
                    print(f"⚠ Column '{col}' not found, using default STATUS EKSEKUSI options")
                    options[key] = default_status_options
                else:
                    print(f"⚠ Column '{col}' not found in dataframe")
                    options[key] = []
        
        print(f"✅ Filter options loaded successfully")
        
        # Final verification
        for key, vals in options.items():
            print(f"📊 {key}: {len(vals)} options - {vals[:5]}{'...' if len(vals) > 5 else ''}")
        
        return options
        
    except Exception as e:
        print(f"❌ Error getting filter options: {str(e)}")
        import traceback
        traceback.print_exc()
        
        # Return safe defaults
        return {
            'UP3': [],
            'ULP': [], 
            'NAMA PENYULANG': [],
            'EQUIPMENT': [],
            'JENIS TEMUAN': [],
            'STATUS EKSEKUSI': ['Selesai', 'Belum Selesai']  # Default options
        }

def filter_data_efficiently(df: pd.DataFrame, filters: Dict[str, Any], limit: int | None = 1000) -> pd.DataFrame:
    """
    Filter data dengan optimasi untuk dataset besar.
    
    Args:
        df: DataFrame untuk difilter
        filters: Dictionary filter conditions
        limit: Batasi hasil maksimal untuk performa (None = tidak ada limit)
    """
    try:
        if df.empty:
            return df
        
        filtered_df = df.copy()
        
        # Apply filters dengan optimasi
        for filter_name, filter_value in filters.items():
            if filter_value and filter_name in filtered_df.columns:
                if isinstance(filter_value, list):
                    # Multiple selection filter
                    mask = filtered_df[filter_name].isin(filter_value)
                else:
                    # Single value filter
                    mask = filtered_df[filter_name] == filter_value
                
                filtered_df = filtered_df[mask]
                
                # Early termination jika sudah terlalu sedikit data
                if len(filtered_df) == 0:
                    break
        
        # Limit hasil untuk performa (hanya jika limit diberikan)
        if limit is not None and len(filtered_df) > limit:
            print(f"⚠ Menampilkan {limit} dari {len(filtered_df)} hasil (untuk performa)")
            filtered_df = filtered_df.head(limit)
        
        return filtered_df
        
    except Exception as e:
        print(f"❌ Error filtering data: {str(e)}")
        if limit is not None:
            return df.head(limit) if not df.empty else df
        else:
            return df if not df.empty else df

def get_data_statistics_fast(df: pd.DataFrame | None = None) -> Dict[str, Any]:
    """
    Ambil statistik data dengan cepat tanpa memproses seluruh dataset.
    """
    try:
        if df is None:
            # Load subset untuk statistik
            df = read_master_data(limit_rows=10000)
        
        if df.empty:
            return {"total_records": 0, "last_updated": "N/A"}
        
        stats = {
            "total_records": len(df),
            "up3_count": len(df['UP3'].dropna().unique()) if 'UP3' in df.columns else 0,
            "ulp_count": len(df['ULP'].dropna().unique()) if 'ULP' in df.columns else 0,
            "equipment_types": len(df['EQUIPMENT'].dropna().unique()) if 'EQUIPMENT' in df.columns else 0,
            "finding_types": len(df['JENIS TEMUAN'].dropna().unique()) if 'JENIS TEMUAN' in df.columns else 0,
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M")
        }
        
        return stats
        
    except Exception as e:
        print(f"❌ Error getting statistics: {str(e)}")
        return {"total_records": 0, "last_updated": "Error"}

def process_sheet_data(xls: pd.ExcelFile) -> pd.DataFrame:
    """
    Proses dan gabungkan data dari sheet yang valid dengan MEMPERTAHANKAN FORMAT ASLI.
    
    PRINSIP PRESERVASI DATA:
    - Format asli data DIPERTAHANKAN sepenuhnya
    - Hanya standardisasi tanggal untuk dashboard (diperlukan sistem)
    - Tidak ada perubahan format lainnya
    """
    all_dfs = []
    
    for sheet_name in xls.sheet_names:
        if isinstance(sheet_name, str) and sheet_name.upper() in VALID_SHEETS:
            try:
                # Baca sheet dengan mengatur semua kolom sebagai string untuk mempertahankan format asli
                df = pd.read_excel(
                    xls,
                    sheet_name=sheet_name,
                    header=0,
                    dtype=str,  # Baca semua kolom sebagai string
                    na_filter=False  # Mencegah konversi NaN
                )
                
                # Normalisasi nama kolom
                df.columns = (
                    df.columns.str.strip()
                    .str.replace("\u200b", "", regex=False)
                    .str.replace("\xa0", "", regex=False)
                    .str.upper()
                )
                
                # Filter dan reorder kolom sesuai VALID_COLUMNS yang ada di file
                existing_cols = []
                for col in VALID_COLUMNS:
                    if col in df.columns:
                        existing_cols.append(col)
                
                if existing_cols:
                    df = df[existing_cols]
                    # Tambahkan kolom yang tidak ada dengan nilai kosong
                    for col in VALID_COLUMNS:
                        if col not in df.columns:
                            df[col] = ""
                    # Reorder kolom sesuai urutan VALID_COLUMNS
                    df = df.reindex(columns=VALID_COLUMNS)
                    
                    # HANYA standardisasi tanggal untuk dashboard (diperlukan sistem)
                    # Sisanya TETAP FORMAT ASLI
                    for col in DATE_COLUMNS:
                        if col in df.columns:
                            df[col] = df[col].apply(standardize_date_format)
                    
                    all_dfs.append(df)
                else:
                    print(f"Warning: Tidak ada kolom valid di sheet {sheet_name}")
                    
            except Exception as e:
                print(f"Error membaca sheet {sheet_name}: {str(e)}")
                continue
    
    if not all_dfs:
        raise ValueError("Tidak ada sheet valid yang dapat diproses")
    
    # Gabungkan semua DataFrame
    combined_df = pd.concat(all_dfs, ignore_index=True)
    
    # Pastikan semua kolom ada dan dalam urutan yang benar
    combined_df = combined_df.reindex(columns=VALID_COLUMNS)
    combined_df = combined_df.fillna("")
    
    return combined_df

def validate_and_sync_data(upload_df: pd.DataFrame, sheet_df: pd.DataFrame) -> Tuple[Dict[str, Any], pd.DataFrame]:
    """
    Validasi dan sinkronisasi data upload dengan data existing di Google Sheet.
    
    Logika dengan VALIDASI 31 KOLOM:
    1. ID SURVEY belum ada → Tambah sebagai baris baru
    2. ID SURVEY ada + ada kolom kosong di sheet tapi terisi di upload → Update kolom kosong
    3. ID SURVEY ada + isi 31 kolom berbeda → Tambah sebagai baris baru (allow duplicate ID)
    4. ID SURVEY sama + seluruh 31 kolom identik → Skip (duplikasi absolut)
    
    Args:
        upload_df: DataFrame dari file upload
        sheet_df: DataFrame dari Google Sheet existing
        
    Returns:
        Tuple[dict, DataFrame]: (statistics, final_merged_dataframe)
    """
    
    # Prepare data - pastikan semua data sebagai string dan bersih
    upload_clean = upload_df.copy().astype(str)
    sheet_clean = sheet_df.copy().astype(str)
    
    # Clean data: replace NaN, None, 'nan' dengan string kosong
    for df in [upload_clean, sheet_clean]:
        df.replace(['nan', 'None', 'NaN', 'null'], '', inplace=True)
        df.fillna('', inplace=True)
        # Apply string cleaning to each cell - KECUALI KOORDINAT
        for col in df.columns:
            if col in ['KOORDINAT X', 'KOORDINAT Y', 'KOORDINAT TEMUAN']:
                # Untuk koordinat: hanya bersihkan null/nan, JANGAN strip titik
                df[col] = df[col].apply(lambda x: str(x) if x != '' else '')
            else:
                # Untuk kolom lain: bersihkan normal
                df[col] = df[col].apply(lambda x: str(x).strip() if x != '' else '')
    
    # Ensure all validation columns exist in both DataFrames
    for col in VALIDATION_COLUMNS:
        if col not in upload_clean.columns:
            upload_clean[col] = ''
        if col not in sheet_clean.columns:
            sheet_clean[col] = ''
    
    # Initialize result tracking
    stats = {
        "new_rows": 0,
        "updated_rows": 0, 
        "skipped_duplicates": 0,
        "duplicate_ids_with_diff_content": 0,
        "processed_rows": 0
    }
    
    # Start with existing sheet data - PRESERVE DATA LAMA
    result_df = sheet_clean.copy()
    updated_indices = set()  # Track which existing rows got updated
    
    print(f"🔍 DEBUG: Memulai dengan data existing: {len(result_df)} baris")
    print(f"📊 Data yang akan dipreservasi: {len(sheet_clean)} baris existing")
    print("🔍 Memulai validasi dan sinkronisasi data...")
    
    # Process each row in upload data
    for row_num, (upload_idx, upload_row) in enumerate(upload_clean.iterrows(), 1):
        stats["processed_rows"] += 1
        id_survey = str(upload_row.get('ID SURVEY', '')).strip()
        
        if not id_survey or pd.isna(id_survey):
            logger.warning(f"⚠ Baris {row_num}: ID SURVEY kosong, dilewati")
            continue
            
        # Find existing rows with same ID SURVEY
        matching_rows = sheet_clean[sheet_clean['ID SURVEY'] == id_survey]
        
        if matching_rows.empty:
            # CASE 1: ID SURVEY belum ada → Tambahkan baris baru
            result_df = pd.concat([result_df, pd.DataFrame([upload_row])], ignore_index=True)
            stats["new_rows"] += 1
            logger.info(f"✅ Baris {row_num}: ID {id_survey} - Data baru ditambahkan")
            recently_uploaded_ids.add(id_survey)
            
        else:
            # Ada record dengan ID SURVEY sama, cek lebih detail
            exact_match_found = False
            update_performed = False
            
            for sheet_idx, sheet_row in matching_rows.iterrows():
                # CASE 4: Cek apakah seluruh kolom identik
                if are_rows_identical(upload_row, sheet_row):
                    stats["skipped_duplicates"] += 1
                    exact_match_found = True
                    logger.info(f"⏭ Baris {row_num}: ID {id_survey} - Duplikasi absolut, dilewati")
                    break
                
                # CASE 2: Cek apakah ada kolom kosong yang bisa di-update
                elif has_empty_columns_to_update(sheet_row, upload_row):
                    original_sheet_idx = sheet_clean.index[sheet_clean['ID SURVEY'] == id_survey].tolist()[0]
                    
                    if original_sheet_idx not in updated_indices:
                        update_data = get_update_data(sheet_row, upload_row)
                        for col, new_val in update_data.items():
                            result_df.loc[original_sheet_idx, col] = new_val
                        
                        updated_indices.add(original_sheet_idx)
                        stats["updated_rows"] += 1
                        update_performed = True
                        logger.info(f"🔄 Baris {row_num}: ID {id_survey} - Data diupdate (mengisi kolom kosong)")
                        recently_uploaded_ids.add(id_survey)
                        break
            
            if not exact_match_found and not update_performed:
                # CASE 3: ID sama tapi konten berbeda → Tambah sebagai baris baru
                result_df = pd.concat([result_df, pd.DataFrame([upload_row])], ignore_index=True)
                stats["duplicate_ids_with_diff_content"] += 1
                stats["new_rows"] += 1
                logger.info(f"➕ Baris {row_num}: ID {id_survey} - Konten berbeda, ditambah sebagai baris baru")
                recently_uploaded_ids.add(id_survey)
    
    # Final cleanup and numbering
    result_df = result_df.drop(columns=['NO'], errors='ignore')
    result_df.insert(0, 'NO', range(1, len(result_df) + 1))
    
    print(f"\n� DEBUG FINAL: Total baris result_df: {len(result_df)}")
    print(f"�📊 Breakdown: {len(sheet_clean)} existing + {stats['new_rows']} baru = {len(result_df)} total")
    
    print("\n📊 Ringkasan Validasi 31 Kolom:")
    print(f"   • Data existing dipreservasi: {len(sheet_clean)} baris")
    print(f"   • Data baru: {stats['new_rows']}")
    print(f"   • Data diupdate: {stats['updated_rows']}")
    print(f"   • Duplikasi diabaikan: {stats['skipped_duplicates']}")
    print(f"   • ID ganda dengan konten berbeda: {stats['duplicate_ids_with_diff_content']}")
    print(f"   • Total diproses: {stats['processed_rows']}")
    print(f"   • TOTAL FINAL: {len(result_df)} baris (TIDAK ADA DATA YANG HILANG!)")
    
    return stats, result_df


def is_row_identical(row1: pd.Series, row2: pd.Series, validation_columns: List[str]) -> bool:
    """
    Periksa apakah dua baris identik berdasarkan kolom validasi.
    PENTING: Koordinat TIDAK di-strip untuk preservasi format asli (contoh: -531.639)
    
    Args:
        row1: Series pertama
        row2: Series kedua  
        validation_columns: List kolom yang digunakan untuk perbandingan
        
    Returns:
        bool: True jika identik, False jika berbeda
    """
    for col in validation_columns:
        if col in ['KOORDINAT X', 'KOORDINAT Y', 'KOORDINAT TEMUAN']:
            # Untuk koordinat: jangan strip, hanya konversi ke string
            val1 = str(row1.get(col, ''))
            val2 = str(row2.get(col, ''))
        else:
            # Untuk kolom lain: strip normal
            val1 = str(row1.get(col, '')).strip()
            val2 = str(row2.get(col, '')).strip()
        
        if val1 != val2:
            return False
    
    return True


def find_updatable_fields(upload_row: pd.Series, sheet_row: pd.Series, validation_columns: List[str]) -> Dict[str, str]:
    """
    Temukan field yang bisa diupdate (kosong di sheet, terisi di upload).
    PENTING: Koordinat TIDAK di-strip untuk preservasi format asli (contoh: 10.485.903)
    
    Args:
        upload_row: Baris dari file upload
        sheet_row: Baris dari Google Sheet
        validation_columns: List kolom untuk validasi
        
    Returns:
        Dict: Mapping kolom yang bisa diupdate dengan nilai baru
    """
    updatable = {}
    
    for col in validation_columns:
        if col in ['KOORDINAT X', 'KOORDINAT Y', 'KOORDINAT TEMUAN']:
            # Untuk koordinat: jangan strip, hanya konversi ke string
            upload_val = str(upload_row.get(col, ''))
            sheet_val = str(sheet_row.get(col, ''))
        else:
            # Untuk kolom lain: strip normal
            upload_val = str(upload_row.get(col, '')).strip()
            sheet_val = str(sheet_row.get(col, '')).strip()
        
        # Jika sheet kosong tapi upload terisi
        if sheet_val == '' and upload_val != '':
            updatable[col] = upload_val
    
    return updatable


def debug_row_comparison(upload_row: pd.Series, sheet_row: pd.Series, validation_columns: List[str]) -> Dict[str, Any]:
    """
    Debug helper untuk melihat perbandingan detail antara dua baris.
    
    Args:
        upload_row: Baris dari file upload
        sheet_row: Baris dari Google Sheet
        validation_columns: List kolom untuk validasi
        
    Returns:
        Dict: Detail perbandingan untuk debugging
    """
    comparison = {
        "identical_fields": [],
        "different_fields": [],
        "updatable_fields": [],
        "empty_in_both": [],
        "summary": {}
    }
    
    for col in validation_columns:
        if col in ['KOORDINAT X', 'KOORDINAT Y', 'KOORDINAT TEMUAN']:
            # Untuk koordinat: jangan strip, hanya konversi ke string
            upload_val = str(upload_row.get(col, ''))
            sheet_val = str(sheet_row.get(col, ''))
        else:
            # Untuk kolom lain: strip normal
            upload_val = str(upload_row.get(col, '')).strip()
            sheet_val = str(sheet_row.get(col, '')).strip()
        
        if upload_val == sheet_val:
            if upload_val == '':
                comparison["empty_in_both"].append(col)
            else:
                comparison["identical_fields"].append(col)
        else:
            if sheet_val == '' and upload_val != '':
                comparison["updatable_fields"].append({
                    "column": col,
                    "new_value": upload_val
                })
            else:
                comparison["different_fields"].append({
                    "column": col,
                    "sheet_value": sheet_val,
                    "upload_value": upload_val
                })
    
    comparison["summary"] = {
        "total_columns": len(validation_columns),
        "identical_count": len(comparison["identical_fields"]),
        "different_count": len(comparison["different_fields"]),
        "updatable_count": len(comparison["updatable_fields"]),
        "empty_both_count": len(comparison["empty_in_both"]),
        "is_identical": len(comparison["different_fields"]) == 0,
        "has_updatable": len(comparison["updatable_fields"]) > 0
    }
    
    return comparison


def validate_data_integrity(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Validasi integritas data untuk memastikan konsistensi.
    
    Args:
        df: DataFrame untuk divalidasi
        
    Returns:
        Tuple[bool, List[str]]: (is_valid, error_messages)
    """
    errors = []
    
    # Check for required columns
    missing_required = [col for col in ["ID SURVEY"] if col not in df.columns]
    if missing_required:
        errors.append(f"Kolom wajib tidak ada: {missing_required}")
    
    # Check for empty ID SURVEY
    if 'ID SURVEY' in df.columns:
        empty_ids = df[df['ID SURVEY'].astype(str).str.strip() == '']
        if not empty_ids.empty:
            errors.append(f"Ditemukan {len(empty_ids)} baris dengan ID SURVEY kosong")
    
    # Check for completely empty rows
    empty_rows = df[df.astype(str).apply(lambda x: x.str.strip().eq('').all(), axis=1)]
    if not empty_rows.empty:
        errors.append(f"Ditemukan {len(empty_rows)} baris yang sepenuhnya kosong")
    
    # Check data types consistency
    non_string_cols = []
    for col in df.columns:
        if not df[col].dtype == 'object':
            non_string_cols.append(col)
    
    if non_string_cols:
        errors.append(f"Kolom dengan tipe data non-string: {non_string_cols}")
    
    is_valid = len(errors) == 0
    return is_valid, errors

def append_or_update_data(new_df: pd.DataFrame) -> Tuple[bool, str]:
    """
    Proses upload data dengan VALIDASI 31 KOLOM CANGGIH dan PRESERVASI FORMAT ASLI.
    
    PRINSIP PRESERVASI DATA:
    - Format asli data DIPERTAHANKAN sepenuhnya
    - Hanya standardisasi tanggal untuk dashboard (diperlukan sistem)
    - Tidak ada normalisasi atau perubahan format lainnya
    
    Logika Validasi:
    1. ID SURVEY belum ada → Tambah sebagai baris baru
    2. ID SURVEY ada + ada kolom kosong di sheet tapi terisi di upload → Update kolom kosong
    3. ID SURVEY ada + isi 31 kolom berbeda → Tambah sebagai baris baru (allow duplicate ID)
    4. ID SURVEY sama + seluruh 31 kolom identik → Skip (duplikasi absolut)
    """
    try:
        # Reset recently uploaded IDs
        recently_uploaded_ids.clear()
        
        # VALIDASI STRUKTUR DATA
        is_valid, msg = validate_sheet_structure(new_df)
        if not is_valid:
            return False, msg
        
        # Inisialisasi koneksi jika belum ada
        global sh
        if sh is None:
            sh = get_google_sheet_connection()
        
        # Baca data existing dari Google Sheet
        logger.info("📖 Membaca data existing dari Google Sheet...")
        existing_df = read_master_data()
        
        # Persiapan data upload - PERTAHANKAN FORMAT ASLI
        logger.info("🔧 Mempersiapkan data upload dengan mempertahankan format asli...")
        upload_df = new_df.copy()
        upload_df = upload_df.drop(columns=['NO'], errors='ignore')
        
        # HANYA konversi ke string untuk keperluan validasi, TIDAK mengubah format
        upload_df = upload_df.astype(str)
        
        # HANYA standardisasi tanggal untuk konsistensi sistem dashboard - ini diperlukan
        logger.info("📅 Standardisasi format tanggal untuk dashboard (diperlukan sistem)...")
        for col in DATE_COLUMNS:
            if col in upload_df.columns:
                # Simpan nilai asli sebagai backup
                original_values = upload_df[col].copy()
                upload_df[col] = upload_df[col].apply(standardize_date_format)
                # Log berapa nilai yang diubah
                changed_count = sum(original_values != upload_df[col])
                if changed_count > 0:
                    print(f"   ℹ️ {col}: {changed_count} tanggal distandardisasi untuk dashboard")
        
        # Filter data yang valid (ID SURVEY tidak kosong)
        upload_df = upload_df.loc[upload_df['ID SURVEY'].notna()]
        upload_df = upload_df.loc[upload_df['ID SURVEY'].astype(str).str.strip() != '']
        
        if upload_df.empty:
            return False, "Tidak ada data valid untuk diproses (ID SURVEY kosong semua)"
        
        # FAST READ - baca existing data minimal processing
        print("� Reading existing data...")
        # Persiapan data existing - PRESERVE DATA LAMA
        existing_df = read_master_data()
        
        print(f"🔍 DEBUG: Data existing ditemukan: {len(existing_df)} baris")
        
        if existing_df.empty:
            print("⚠️ DATABASE KOSONG - Akan membuat data baru")
            sheet_df = pd.DataFrame(columns=VALID_COLUMNS)
        else:
            print(f"✅ DATA EXISTING BERHASIL DIBACA: {len(existing_df)} baris")
            print(f"📊 Kolom existing: {list(existing_df.columns)}")
            sheet_df = existing_df.drop(columns=['NO'], errors='ignore')
            print(f"📊 Setelah drop NO: {len(sheet_df)} baris akan dipreservasi")
        
        # STREAMLINED SYNC - tanpa validasi berlebihan
        print("🔄 Syncing data dengan validasi 31 kolom...")
        new_rows = 0
        updated_rows = 0
        processed_rows = len(upload_df)
        
        # VALIDASI DAN SINKRONISASI DENGAN SISTEM 31 KOLOM
        stats, result_df = validate_and_sync_data(upload_df, sheet_df)
        
        # SAFE UPDATE - TIDAK HAPUS DATA LAMA, HANYA APPEND/UPDATE
        print("💾 Menyimpan hasil ke Google Sheet dengan PRESERVASI data lama...")
        worksheet = sh.worksheet(MASTER_SHEET_NAME)
        
        # HANYA UPDATE JIKA ADA PERUBAHAN NYATA
        if stats["new_rows"] > 0 or stats["updated_rows"] > 0:
            # Backup current header
            current_headers = result_df.columns.tolist()
            
            # Clear dan upload dengan preservasi data existing
            worksheet.clear()
            data_to_upload = [current_headers]
            data_to_upload.extend(result_df.values.tolist())
            worksheet.update(data_to_upload)
            
            print(f"✅ Data berhasil disimpan dengan preservasi data existing")
        else:
            print("ℹ️ Tidak ada perubahan data, database existing tetap utuh")
        
        # Catat aktivitas ke log
        total_changes = stats["new_rows"] + stats["updated_rows"]
        if total_changes > 0:
            simpan_log("Upload Data", total_changes)
        
        # Buat pesan ringkasan yang informatif dengan 4 case
        summary_parts = []
        if stats['new_rows'] > 0:
            summary_parts.append(f"✅ {stats['new_rows']} record baru ditambahkan")
        if stats['updated_rows'] > 0:
            summary_parts.append(f"🔄 {stats['updated_rows']} record di-update")
        if stats['duplicate_ids_with_diff_content'] > 0:
            summary_parts.append(f"📋 {stats['duplicate_ids_with_diff_content']} duplikasi ID diizinkan")
        if stats['skipped_duplicates'] > 0:
            summary_parts.append(f"⏭️ {stats['skipped_duplicates']} record identik diabaikan")
        
        summary = " | ".join(summary_parts) if summary_parts else "Tidak ada perubahan data"
        message = f"✅ Upload selesai!\n📊 {summary}\n🎯 Total diproses: {stats['processed_rows']} baris"
        
        logger.info("✅ Sinkronisasi dengan validasi 4 case selesai - DATA LAMA AMAN!")
        return True, message

    except Exception as e:
        recently_uploaded_ids.clear()
        print(f"❌ Error: {str(e)}")
        return False, f"Gagal memproses data: {str(e)}"

def delete_last_rows(count: int | None = None) -> Tuple[bool, str]:
    """Hapus data yang baru diupload dari data master."""
    try:
        if not recently_uploaded_ids:
            return False, "Tidak ada data baru yang dapat dihapus. Silakan upload data terlebih dahulu."

        worksheet = sh.worksheet(MASTER_SHEET_NAME)
        existing_data = worksheet.get_all_records()
        df = pd.DataFrame(existing_data)

        if df.empty:
            return False, "Database kosong"

        # Filter baris yang baru diupload
        mask = df['ID SURVEY'].astype(str).isin(recently_uploaded_ids)
        rows_to_delete = df[mask]
        
        if rows_to_delete.empty:
            return False, "Tidak ditemukan data yang baru diupload"

        # Hapus baris yang baru diupload
        df_new = df[~mask]
        
        # Reset nomor urut
        df_new = df_new.drop(columns=['NO'], errors='ignore')
        df_new.insert(0, 'NO', range(1, len(df_new) + 1))

        # Update worksheet
        worksheet.clear()
        worksheet.update([df_new.columns.tolist()] + df_new.values.tolist())

        # Catat di log dan clear tracking
        deleted_count = len(rows_to_delete)
        simpan_log("Hapus Data Baru", deleted_count)
        recently_uploaded_ids.clear()

        return True, f"Berhasil menghapus {deleted_count} data yang baru diupload"

    except Exception as e:
        return False, f"Gagal menghapus data: {str(e)}"

def read_log() -> pd.DataFrame:
    """Baca log aktivitas dari Google Sheets dengan header yang benar."""
    try:
        # Initialize connection if needed
        global sh
        if sh is None:
            sh = get_google_sheet_connection()
            
        worksheet = sh.worksheet(LOG_SHEET_NAME)
        all_data = worksheet.get_all_values()
        
        if not all_data:
            # Sheet kosong, return DataFrame dengan header yang benar
            return pd.DataFrame(columns=['NO', 'Tanggal & Waktu', 'Jenis Aktivitas', 'Jumlah Data'])
        
        # Cek apakah header sudah benar
        expected_header = ['NO', 'Tanggal & Waktu', 'Jenis Aktivitas', 'Jumlah Data']
        
        if all_data[0] != expected_header:
            # Header tidak sesuai, perbaiki struktur
            print("🔧 Memperbaiki struktur header log...")
            
            # Backup data lama jika ada
            old_data = []
            for i, row in enumerate(all_data):
                if len(row) >= 3:  # Minimal ada data di 3 kolom
                    # Asumsikan format lama: [timestamp, aksi, keterangan]
                    old_data.append([
                        i + 1,  # NO
                        row[0] if len(row) > 0 else "",  # Tanggal & Waktu
                        row[1] if len(row) > 1 else "",  # Jenis Aktivitas
                        row[2] if len(row) > 2 else ""   # Jumlah Data (dari keterangan lama)
                    ])
            
            # Reset sheet dengan header yang benar
            worksheet.clear()
            worksheet.insert_row(expected_header, 1)
            
            # Masukkan kembali data lama dengan penomoran yang benar
            for row_data in old_data:
                worksheet.append_row(row_data)
            
            # Baca ulang data yang sudah diperbaiki
            all_data = worksheet.get_all_values()
        
        # Buat DataFrame dari data (header sudah benar)
        if len(all_data) > 1:
            df = pd.DataFrame(all_data[1:], columns=all_data[0])
            
            # Pastikan kolom NO adalah integer dan berurutan
            try:
                df['NO'] = pd.to_numeric(df['NO'], errors='coerce').fillna(0).astype(int)
            except:
                # Jika gagal konversi, buat penomoran ulang
                df['NO'] = range(1, len(df) + 1)
            
            return df
        else:
            # Hanya ada header
            return pd.DataFrame(columns=expected_header)
            
    except Exception as e:
        print(f"❌ Error membaca log: {str(e)}")
        # Return DataFrame kosong dengan header yang benar sebagai fallback
        return pd.DataFrame(columns=['NO', 'Tanggal & Waktu', 'Jenis Aktivitas', 'Jumlah Data'])

def reset_log_structure() -> bool:
    """Reset struktur log sheet dengan header yang benar."""
    try:
        log_ws = sh.worksheet(LOG_SHEET_NAME)
        
        # Clear semua data
        log_ws.clear()
        
        # Set header yang benar
        header = ['NO', 'Tanggal & Waktu', 'Jenis Aktivitas', 'Jumlah Data']
        log_ws.insert_row(header, 1)
        
        print("✅ Struktur log berhasil direset dengan header yang benar")
        return True
        
    except Exception as e:
        print(f"❌ Gagal reset struktur log: {str(e)}")
        return False

def simpan_log(aksi: str, jumlah: int) -> None:
    """Simpan aktivitas ke log dengan format yang benar dan data terbaru di atas."""
    try:
        # Initialize connection if needed
        global sh
        if sh is None:
            sh = get_google_sheet_connection()
            
        log_ws = sh.worksheet(LOG_SHEET_NAME)
        timestamp = datetime.now().strftime("%A, %d %B %Y %H:%M")
        
        # Cek apakah sheet sudah memiliki header yang benar
        try:
            all_data = log_ws.get_all_values()
            if not all_data or all_data[0] != ['NO', 'Tanggal & Waktu', 'Jenis Aktivitas', 'Jumlah Data']:
                # Reset sheet dengan header yang benar
                log_ws.clear()
                log_ws.insert_row(['NO', 'Tanggal & Waktu', 'Jenis Aktivitas', 'Jumlah Data'], 1)
                existing_data_rows = []
            else:
                # Ambil data existing (tanpa header)
                existing_data_rows = all_data[1:] if len(all_data) > 1 else []
        except:
            # Jika error, reset sheet
            log_ws.clear()
            log_ws.insert_row(['NO', 'Tanggal & Waktu', 'Jenis Aktivitas', 'Jumlah Data'], 1)
            existing_data_rows = []
        
        # Tambahkan data baru di posisi kedua (setelah header) - ini akan jadi nomor 1
        new_row = [1, timestamp, aksi, jumlah]
        log_ws.insert_row(new_row, 2)
        
        # Update nomor urut untuk data lama (geser ke bawah)
        for i, row in enumerate(existing_data_rows, 2):  # Mulai dari row 2 (karena header di row 1)
            if len(row) >= 4:  # Pastikan row memiliki 4 kolom
                # Update nomor urut (kolom A) untuk data lama
                log_ws.update_cell(i + 1, 1, i)  # i + 1 karena sudah ada data baru di row 2
        
        print(f"✅ Log berhasil disimpan: {aksi} - {jumlah} data")
        
    except Exception as e:
        print(f"❌ Gagal menyimpan log: {str(e)}")