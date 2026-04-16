import os
import json
import uuid
import tempfile
import shutil

from fastapi import FastAPI, UploadFile, HTTPException
import snowflake.connector

app = FastAPI()

STAGE = os.getenv("SNOWFLAKE_STAGE") or "@DOCUMENT_OCR.PUBLIC.DOC_STAGE"
ALLOWED_EXTENSIONS = {".pdf", ".png", ".jpg", ".jpeg", ".tiff", ".webp", ".docx", ".pptx", ".html", ".txt"}


def get_connection():
    if os.path.isfile("/snowflake/session/token"):
        return snowflake.connector.connect(
            host=os.getenv("SNOWFLAKE_HOST"),
            port=os.getenv("SNOWFLAKE_PORT"),
            protocol="https",
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            authenticator="oauth",
            token=open("/snowflake/session/token").read(),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            client_session_keep_alive=True,
        )
    return snowflake.connector.connect(
        connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "AWS-DEMO-2"
    )


@app.post("/ocr")
async def ocr(file: UploadFile):
    ext = os.path.splitext(file.filename or "")[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail=f"Unsupported file type: {ext}")

    unique_name = f"{uuid.uuid4().hex}_{file.filename}"
    tmp_dir = tempfile.mkdtemp()
    tmp_path = os.path.join(tmp_dir, unique_name)
    staged_path = f"incoming/{unique_name}"

    conn = get_connection()
    try:
        with open(tmp_path, "wb") as f:
            shutil.copyfileobj(file.file, f)

        cur = conn.cursor()
        try:
            cur.execute(
                f"PUT 'file://{tmp_path}' '{STAGE}/incoming/' AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )

            os.remove(tmp_path)
            os.rmdir(tmp_dir)

            cur.execute(
                f"""
                SELECT AI_PARSE_DOCUMENT(
                    TO_FILE('{STAGE}', '{staged_path}'),
                    {{'mode': 'OCR'}}
                ) AS ocr_result
                """
            )
            row = cur.fetchone()
            if not row or not row[0]:
                raise HTTPException(status_code=500, detail="AI_PARSE_DOCUMENT returned no result")

            result = json.loads(row[0]) if isinstance(row[0], str) else row[0]
            text = result.get("content", "")

            return {"text": text}
        finally:
            try:
                cur.execute(f"REMOVE '{STAGE}/{staged_path}'")
            except Exception:
                pass
            cur.close()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        if os.path.exists(tmp_dir):
            os.rmdir(tmp_dir)


@app.get("/health")
async def health():
    return {"status": "ok"}
