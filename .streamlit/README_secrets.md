How to configure Streamlit Cloud secrets

1) In Streamlit Cloud, open your app -> Settings -> Secrets.
2) Paste your Google service account JSON under the key "gcp_service_account".
   Example:

[gcp_service_account]
project_id = "your-project-id"
private_key_id = "..."
private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
client_email = "svc-account@your-project.iam.gserviceaccount.com"
client_id = "..."
...

3) Ensure the service account has access to the spreadsheet ID in `sheets_utils.py`.
