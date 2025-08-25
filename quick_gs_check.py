import os, sys, time
start = time.time()
try:
    from sheets_utils import get_google_sheet_connection, MASTER_SHEET_NAME
    sh = get_google_sheet_connection()
    ws = sh.worksheet(MASTER_SHEET_NAME)
    rng = ws.get('A1:K6')
    print('[OK] Connected to spreadsheet. Worksheet:', ws.title)
    print('[OK] Sample rows fetched:', len(rng))
    if rng:
        print('[OK] Header preview:', rng[0])
    print('[OK] Elapsed: %.2fs' % (time.time()-start))
    sys.exit(0)
except Exception as e:
    print('[ERR]', e)
    sys.exit(1)
