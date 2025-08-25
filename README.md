# Dashboard Inspeksi PLN UID Lampung — Panduan Deploy

Dokumen ini menjelaskan cara menjalankan dan men-deploy aplikasi Streamlit untuk monitoring hasil inspeksi jaringan distribusi PLN UID Lampung.

## Struktur Proyek

## Fitur Utama Dashboard
- Filter data multi-level: UP3, ULP, Penyulang, Equipment, Jenis Temuan, Status Eksekusi
- Rekapitulasi & integrasi data: panel filter, tabel rekap, ekspor data
- Visualisasi: grafik top penyulang, tren bulanan, % temuan selesai per ULP
- Peta lokasi temuan: marker interaktif, status eksekusi, kategori temuan
- Log aktivitas: riwayat upload data, jumlah data, waktu aktivitas
- Optimasi performa: caching data, pembaruan otomatis, navigasi cepat antar halaman

## Prasyarat
- Akun Google Service Account yang memiliki akses ke Spreadsheet target.
- ID Spreadsheet sudah dikonfigurasi di `sheets_utils.py` (variabel `SPREADSHEET_ID`).
- Python 3.10+ untuk pengembangan lokal.

## Menjalankan Secara Lokal
1. Buat virtual environment dan install dependensi dari `requirements.txt`.
2. Simpan file kredensial service account sebagai `credentials.json` di direktori yang sama dengan `app.py`.
3. Jalankan aplikasi dari root proyek:
	 - `streamlit run app.py`

### Alur Penggunaan Dashboard
1. Buka website aplikasi dan tampilan awal akan berada di Halaman Upload Data.
2. Setelah Upload Data, users dapat berpindah ke Halaman Dashboard Utama untuk melihat insight (ex: KPI Card, grafik, dan peta lokasi temuan) dari data yang telah di upload. Lalu users dapat memilih fitur filter/slicer data sesuai kebutuhan analisis.
3. Selanjutnya users dapat berpindah ke Halaman Rekapitulasi Data untuk melihat data yang telah di upload sebelumnya dan dapat melakukan ekspor data hasil filter jika diperlukan.
4. Terakhir, users dapat memantau log aktivitas untuk audit dan monitoring.

Catatan: `credentials.json` hanya untuk lokal dan JANGAN di-commit ke Git. File ini sudah diabaikan melalui `.gitignore`.

## Deploy ke Streamlit Community Cloud
1. Push kode ini ke GitHub (file inti: `app.py`, `sheets_utils.py`, `requirements.txt`, `assets/`, `.gitignore`, `README.md`).
2. Buat App baru di Streamlit Cloud dan arahkan ke repo Anda.
3. Main file path: `app.py` (karena file utama berada di root repo).
4. Buka Settings → Secrets dan tambahkan kredensial service account di bawah key `gcp_service_account` sesuai panduan `.streamlit/README_secrets.md`.
5. Bagikan Google Spreadsheet ke email service account agar memiliki akses baca/tulis.
6. Deploy aplikasi.

Perilaku kredensial:
- PRODUKSI: aplikasi membaca kredensial dari Secrets (`gcp_service_account`).
- LOKAL: aplikasi menggunakan `credentials.json` jika Secrets tidak tersedia.

## Keamanan & Praktik Baik
- Jangan pernah meng-commit `credentials.json` atau rahasia lain ke repository publik.
- Pastikan `.gitignore` berisi entri untuk `credentials.json`, `.streamlit/secrets.toml`, direktori virtual env, dan `__pycache__/`.
- Batasi akses Spreadsheet hanya ke service account yang diperlukan.

## Troubleshooting
	- Pastikan Secrets sudah diisi dan valid (format JSON service account lengkap).
	- Spreadsheet sudah di-share ke email service account.
	- `SPREADSHEET_ID` benar dan sheet/tab yang dirujuk ada.

- "Data tidak muncul/tidak ter-update":
	- Pastikan cache sudah di-refresh (upload data otomatis invalidasi cache)
	- Cek koneksi internet dan akses Google Sheets
- "Peta tidak tampil/marker kurang":
	- Pastikan data koordinat valid dan jumlah marker tidak melebihi batas (default 1000)
	- Gunakan fitur filter untuk membatasi data
- "Upload gagal":
	- Pastikan format file sesuai dan service account memiliki akses tulis


## Lisensi
