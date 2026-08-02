import sys
import os
import json
import hashlib
import sqlite3
import psutil
import gc
import threading
import queue
from itertools import groupby

os.environ["TRANSFORMERS_OFFLINE"] = "1"

def send_ipc(data):
    print(f"ECOFLOW_IPC__{json.dumps(data)}", flush=True)

def reader_thread(q):
    for line in iter(sys.stdin.readline, ''):
        q.put(line)
    q.put(None)

def normalize_text(text: str) -> str:
    return " ".join(text.strip().split()).lower()

def generate_sha256(normalized_text: str, src_lang: str, tgt_lang: str) -> str:
    payload = f"{normalized_text}|{src_lang}|{tgt_lang}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()

class LocalKVCache:
    def __init__(self, db_path: str):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS translations (
                hash_key TEXT PRIMARY KEY,
                source_lang TEXT,
                target_lang TEXT,
                original_text TEXT,
                translated_text TEXT
            )
        """)
        self.conn.commit()

    def get(self, hash_key: str) -> str:
        cursor = self.conn.execute("SELECT translated_text FROM translations WHERE hash_key = ?", (hash_key,))
        row = cursor.fetchone()
        return row[0] if row else None

    def put(self, hash_key: str, src: str, tgt: str, orig: str, trans: str):
        self.conn.execute(
            "INSERT OR REPLACE INTO translations VALUES (?, ?, ?, ?, ?)",
            (hash_key, src, tgt, orig, trans)
        )
        self.conn.commit()

class OfflineTransformersEngine:
    LANGUAGE_CODE_MAP = {"en": "eng_Latn", "hi": "hin_Deva", "mr": "mar_Deva"}

    def __init__(self, model_dir: str):
        from transformers import AutoModelForSeq2SeqLM, AutoTokenizer
        self.tokenizer = AutoTokenizer.from_pretrained(model_dir, local_files_only=True)
        self.model = AutoModelForSeq2SeqLM.from_pretrained(model_dir, local_files_only=True)
        self.device = "cuda" if os.environ.get("USE_CUDA") == "1" else "cpu"
        if self.device == "cuda":
            self.model = self.model.to("cuda")

    def translate_batch(self, texts: list, src_lang: str, tgt_lang: str) -> list:
        src_code = self.LANGUAGE_CODE_MAP.get(src_lang, "eng_Latn")
        tgt_code = self.LANGUAGE_CODE_MAP.get(tgt_lang, "hin_Deva")
        self.tokenizer.src_lang = src_code
        self.tokenizer.tgt_lang = tgt_code
        
        forced_bos_token_id = self.tokenizer.convert_tokens_to_ids(tgt_code)
        
        inputs = self.tokenizer(texts, return_tensors="pt", padding=True, truncation=True)
        if self.device == "cuda":
            inputs = {k: v.to("cuda") for k, v in inputs.items()}
            
        generated_tokens = self.model.generate(**inputs, forced_bos_token_id=forced_bos_token_id, max_length=512)
        return self.tokenizer.batch_decode(generated_tokens, skip_special_tokens=True)

def boot_daemon():
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    model_dir = os.path.join(project_root, "models", "nllb-200-distilled-600M")
    cache_db_path = os.path.join(project_root, "workspace", "cache", "nmt_cache.db")
    
    if not os.path.exists(model_dir) or not os.listdir(model_dir):
        send_ipc({"status": "error", "error": "❌ FATAL: Local NMT model missing."})
        return

    engine = OfflineTransformersEngine(model_dir)
    cache = LocalKVCache(cache_db_path)

    process = psutil.Process(os.getpid())
    send_ipc({"status": "ready", "actual_ram_mb": process.memory_info().rss / (1024 * 1024)})

    input_queue = queue.Queue()
    t = threading.Thread(target=reader_thread, args=(input_queue,))
    t.daemon = True
    t.start()

    IDLE_TIMEOUT_SECONDS = 60

    while True:
        try:
            line = input_queue.get(timeout=IDLE_TIMEOUT_SECONDS)
            if line is None: break
            line = line.strip()
            if not line: continue

            req = json.loads(line)
            raw_input_target = req.get("input")
            chunk_name = os.path.basename(raw_input_target)

            # 🛡️ THE FIX: Dynamically resolve the matching audio bucket path
            curr = raw_input_target
            job_root = None
            while curr and os.path.dirname(curr) != curr:
                if os.path.basename(curr).startswith("JOB-"):
                    job_root = curr
                    break
                curr = os.path.dirname(curr)

            if not job_root:
                job_root = os.path.dirname(os.path.dirname(raw_input_target))

            bucket_prefix = "_".join(chunk_name.split("_")[0:2]) + "_"
            target_bucket_dir = None
            audio_chunker_dir = os.path.join(job_root, "out_AudioChunker")
            
            if os.path.exists(audio_chunker_dir):
                for d in os.listdir(audio_chunker_dir):
                    if d.startswith(bucket_prefix):
                        target_bucket_dir = os.path.join(audio_chunker_dir, d)
                        break
            
            if target_bucket_dir is None:
                send_ipc({"status": "error", "error": f"❌ Cannot find matching audio bucket for {chunk_name}"})
                continue

            transcript_path = os.path.join(target_bucket_dir, "transcript.json")
            if not os.path.exists(transcript_path):
                send_ipc({"status": "error", "error": f"❌ transcript.json missing in {os.path.basename(target_bucket_dir)}"})
                continue

            with open(transcript_path, "r") as f:
                data = json.load(f)

            send_ipc({"status": "progress", "chunk": chunk_name, "pct": 10})

            timeline = data.get("segments", [])
            if not timeline:
                timeline = [{"start": data.get("start", 0), "end": data.get("end", 0), "text": data.get("text", "")}]

            src_lang = data.get("language", "en")
            
            tgt_lang = "hi"
            if job_root:
                config_path = os.path.join(job_root, "job_config.json")
                if os.path.exists(config_path):
                    with open(config_path, "r") as config_file:
                        tgt_lang = json.load(config_file).get("target_language", "hi")

            send_ipc({"status": "warn", "message": f"🔍 Detected Source Language: {src_lang.upper()}"})

            requests = []
            for idx, segment in enumerate(timeline):
                source_text = segment.get("text", "")
                if not source_text:
                    timeline[idx]["translated_text"] = ""
                    continue

                if src_lang == tgt_lang:
                    timeline[idx]["translated_text"] = source_text
                    continue

                norm_text = normalize_text(source_text)
                key = generate_sha256(norm_text, src_lang, tgt_lang)
                requests.append({"idx": idx, "key": key, "normalized_text": norm_text, "source_text": source_text})

            if src_lang == tgt_lang:
                send_ipc({"status": "warn", "message": f"⚠️ Source matches Target ({src_lang}). Bypassing NMT compute."})
            else:
                cache_misses = []
                for r in requests:
                    cached_val = cache.get(r["key"])
                    if cached_val:
                        timeline[r["idx"]]["translated_text"] = cached_val
                    else:
                        cache_misses.append(r)

                if cache_misses:
                    BATCH_SIZE = 8
                    cache_misses.sort(key=lambda x: x["normalized_text"])
                    grouped = [list(g) for _, g in groupby(cache_misses, key=lambda x: x["normalized_text"])]
                    batches = [group[i:i + BATCH_SIZE] for group in grouped for i in range(0, len(group), BATCH_SIZE)]

                    for i, batch in enumerate(batches):
                        texts_to_translate = [item["normalized_text"] for item in batch]
                        translations = engine.translate_batch(texts_to_translate, src_lang, tgt_lang)
                        
                        for item, translated_text in zip(batch, translations):
                            cache.put(item["key"], src_lang, tgt_lang, item["normalized_text"], translated_text)
                            timeline[item["idx"]]["translated_text"] = translated_text
                        
                        progress_pct = 10 + int((float(i + 1) / len(batches)) * 80)
                        send_ipc({"status": "progress", "chunk": chunk_name, "pct": progress_pct})

            with open(os.path.join(target_bucket_dir, "translated.json"), "w", encoding="utf-8") as f:
                json.dump({"language": tgt_lang, "segments": timeline}, f, ensure_ascii=False, indent=2)

            send_ipc({"status": "progress", "chunk": chunk_name, "pct": 100})
            gc.collect()
            send_ipc({"status": "success", "chunk": chunk_name})

        except queue.Empty:
            send_ipc({"status": "warn", "message": f"🧹 NMTTranslator idle for {IDLE_TIMEOUT_SECONDS}s. Self-terminating to release RAM."})
            break
        except Exception as e:
            send_ipc({"status": "error", "error": f"❌ Inference Crash: {str(e)}"})

if __name__ == "__main__":
    boot_daemon()