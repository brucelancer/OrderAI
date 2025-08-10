Order Availability Checker

Run a small local web app to upload two Excel files — a Stock file and an Order file — and see which orders can be processed based on available stock. The app auto-detects common column names and joins by Fish Name and Pack Size. It shows a clear, styled dashboard and lets you download the matched result.

## Quick start

1. Ensure you have Python 3.9+ installed.
2. Create and activate a virtual environment (Windows PowerShell):

```
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

3. Install dependencies:

```
pip install -r requirements.txt
```

4. Run the app:

```
python app.py
```

5. Open the app in your browser at `http://127.0.0.1:5000`.

## Excel format guidelines

The app is tolerant to column naming, but the following help it work best.

- Stock file should contain columns resembling:
  - Fish Name (aliases: Product)
  - Packed Size (aliases: Pack)
  - Total Carton (aliases: Total_CTN, Carton, Cartons, CTN)

- Order file should contain columns resembling:
  - Fish Name (aliases: Product)
  - Packed Size (aliases: Pack)
  - Total Carton (aliases: Total_CTN, Carton, Cartons, CTN)

The app will uppercase and normalize text before matching. It performs an exact normalized match on Fish Name + Pack Size. Items not found in stock are marked "Not Found"; items with insufficient stock are marked "Insufficient" with a shortfall count.

## Downloading results

After processing, use the "Download Result" button to export the detailed result as an Excel file.

## Notes

- Only local processing; no files are uploaded to any remote service.
- The app reads the first sheet by default. If your workbook has named sheets, you can choose them via the options on the page after upload.


