import sys
import os
import re
import json
import psutil
import gc
import threading
import queue
from faster_whisper import WhisperModel

num_cores = str(min(4, max(1, (os.cpu_count() or 4) - 2)))
os.environ["OMP_NUM_THREADS"] = num_cores
os.environ["OPENBLAS_NUM_THREADS"] = num_cores
os.environ["MKL_NUM_THREADS"] = num_cores

FOREIGN_SCRIPTS_PATTERN = re.compile(
    r"[\u0A00-\u0A7F\u0C00-\u0C7F\u0C80-\u0CFF\u0B80-\u0BFF\u0D00-\u0D7F\u0980-\u09FF\u0600-\u06FF\u0E00-\u0E7F]"
)

DOMAIN_INITIAL_PROMPTS = {
    "mr": "शेळीपालन, योग्य गोठा व्यवस्थापन, शेळ्यांचे आरोग्य, देवी रोग, लसीकरण, परजीवी नियंत्रण, आहार आणि चारा व्यवस्थापन, पैदास, आणि महिला बचत गट प्रशिक्षण.",
    "hi": "बकरी पालन, उचित बाड़ा प्रबंधन, बकरियों का स्वास्थ्य, चेचक रोग, टीकाकरण, परजीवी नियंत्रण, आहार और चारा प्रबंधन, प्रजनन, और महिला स्वयं सहायता समूह प्रशिक्षण.",
    "en": "Goat farming, proper shed management, goat health, pox vaccination, parasite control, feed and fodder management, breeding, and women self-help group training."
}

def is_valid_speech_text(text: str) -> bool:
    if not text or not text.strip():
        return False
    if any(ord(c) == 0xFFFD or (ord(c) < 32 and c not in '\n\r\t') for c in text):
        return False
    if re.search(r'([^\s\d\w])\s*(?:\1\s*){3,}', text):
        return False
    if re.search(r'(.)\1{4,}', text):
        return False
    letters_only = re.findall(r'[\u0904-\u0939\u0958-\u0961a-zA-Z]', text)
    return len(letters_only) >= 2

def sanitize_transcript_script(text: str, lang: str) -> str:
    if not text:
        return ""
    cleaned = text
    if lang in ["mr", "hi", "en"]:
        cleaned = FOREIGN_SCRIPTS_PATTERN.sub("", cleaned)
    
    cleaned = re.sub(r'\b(\w{2,})(?:\s+\1\b){2,}', r'\1', cleaned, flags=re.UNICODE)
    cleaned = re.sub(r'(.{3,}?)\1{3,}', r'\1', cleaned, flags=re.UNICODE)
    cleaned = re.sub(r'^(?:प्रशिक्षण|आजार|योजना|बैठक|पशुसखी)[\s,]+(?:प्रशिक्षण|आजार|योजना|बैठक|पशुसखी)[\s,]+', '', cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned

def send_ipc(data):
    print(f"ECHOFLOW_IPC__{json.dumps(data)}", flush=True)

def reader_thread(q):
    for line in iter(sys.stdin.readline, ''):
        q.put(line)
    q.put(None)

def boot_daemon():
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    local_model_path = os.path.join(project_root, "models", "whisper-medium")
    
    if not os.path.exists(local_model_path) or not os.listdir(local_model_path):
        send_ipc({"status": "error", "error": f"❌ FATAL: Local Whisper model missing at {local_model_path}"})
        return

    try:
        model = WhisperModel(
            local_model_path, 
            device="cpu", 
            compute_type="int8", 
            cpu_threads=4,
            num_workers=1,
            local_files_only=True
        )
    except Exception as e:
        send_ipc({"status": "error", "error": f"❌ FATAL: Whisper Boot crash: {e}"})
        return

    process = psutil.Process(os.getpid())
    send_ipc({"status": "ready", "actual_ram_mb": process.memory_info().rss / (1024 * 1024)})

    input_queue = queue.Queue()
    t = threading.Thread(target=reader_thread, args=(input_queue,))
    t.daemon = True
    t.start()

    while True:
        try:
            line = input_queue.get(timeout=60)
            if line is None: break
            line = line.strip()
            if not line: continue

            req = json.loads(line)
            raw_input_target = req.get("input")
            chunk_name = os.path.basename(raw_input_target)
            bucket_prefix = "_".join(chunk_name.split("_")[0:2]) + "_" 
            
            curr = raw_input_target
            job_root = None
            while curr and os.path.dirname(curr) != curr:
                if os.path.basename(curr).startswith("JOB-"):
                    job_root = curr
                    break
                curr = os.path.dirname(curr)
            if job_root is None:
                job_root = os.path.dirname(os.path.dirname(raw_input_target))

            target_bucket_dir = raw_input_target
            audio_chunker_dir = os.path.join(job_root, "out_AudioChunker")
            if os.path.exists(audio_chunker_dir):
                for d in os.listdir(audio_chunker_dir):
                    if d.startswith(bucket_prefix):
                        target_bucket_dir = os.path.join(audio_chunker_dir, d)
                        break
            
            wav_path = os.path.join(target_bucket_dir, "chunk_audio.wav")
            meta_path = os.path.join(target_bucket_dir, "meta.json")

            if not os.path.exists(wav_path):
                send_ipc({"status": "success", "chunk": raw_input_target})
                continue

            with open(meta_path, "r") as f:
                meta = json.load(f)

            send_ipc({"status": "progress", "chunk": chunk_name, "pct": 10})

            ALLOWED_LANGUAGES = {"en", "hi", "mr"}
            source_lang = None
            transcription_quality = "balanced"
            
            if job_root:
                config_path = os.path.join(job_root, "job_config.json")
                if os.path.exists(config_path):
                    with open(config_path, "r") as cfg:
                        job_cfg = json.load(cfg)
                        raw_src = job_cfg.get("source_language")
                        if raw_src and raw_src.strip().lower() in ALLOWED_LANGUAGES:
                            source_lang = raw_src.strip().lower()
                        transcription_quality = job_cfg.get("transcription_quality", "balanced")

            beam_size, best_of = (4, 4) if transcription_quality == "accurate" else (3, 3)

            target_lang_for_prompt = source_lang or "mr"
            init_prompt = DOMAIN_INITIAL_PROMPTS.get(target_lang_for_prompt, DOMAIN_INITIAL_PROMPTS.get("mr", ""))

            # 🛡️ THE FIX: Relaxed VAD to prevent mid-sentence cuts
            vad_params = dict(
                min_silence_duration_ms=800, 
                min_speech_duration_ms=250,
                speech_pad_ms=300
            )

            # 🛡️ THE FIX: Forgiving thresholds so Whisper stops deleting noisy audio
            segments, info = model.transcribe(
                wav_path,
                beam_size=beam_size,
                best_of=best_of,
                temperature=[0.0, 0.2, 0.4, 0.6],
                language=source_lang,
                initial_prompt=init_prompt,
                vad_filter=True, 
                vad_parameters=vad_params,
                condition_on_previous_text=False,
                repetition_penalty=1.0,
                no_repeat_ngram_size=0,
                no_speech_threshold=0.6,
                compression_ratio_threshold=2.4,
                log_prob_threshold=-1.0,
                hallucination_silence_threshold=2.0
            )

            detected_lang = getattr(info, "language", "mr")
            if detected_lang not in ALLOWED_LANGUAGES:
                detected_lang = "mr"

            effective_lang = source_lang or detected_lang

            raw_segments_data = []
            for seg in segments:
                cleaned_text = sanitize_transcript_script(seg.text, effective_lang)
                if cleaned_text and is_valid_speech_text(cleaned_text):
                    duration = seg.end - seg.start
                    word_count = len(cleaned_text.split())
                    
                    # 🛡️ THE FIX: Only drop true mathematical impossibilities (15 seconds with <= 3 words)
                    if duration > 15.0 and word_count <= 3:
                        continue
                        
                    raw_segments_data.append({
                        "start": round(meta["start"] + seg.start, 2),
                        "end": round(meta["start"] + seg.end, 2),
                        "text": cleaned_text
                    })

            segments_data = []
            for seg in raw_segments_data:
                if not segments_data:
                    segments_data.append(dict(seg))
                    continue
                prev = segments_data[-1]
                pause = seg["start"] - prev["end"]
                combined_dur = seg["end"] - prev["start"]
                prev_ends_sentence = bool(re.search(r'[.!?।॥]\s*$', prev["text"]))

                if (0.0 <= pause <= 0.85 and combined_dur <= 9.0 and 
                    (not prev_ends_sentence or (prev["end"] - prev["start"]) < 3.0 or len(prev["text"].split()) <= 4)):
                    prev["end"] = seg["end"]
                    prev["text"] = f"{prev['text']} {seg['text']}".strip()
                else:
                    segments_data.append(dict(seg))

            chunk_text = " ".join(s["text"] for s in segments_data)
            
            result_payload = {
                "id": meta["id"],
                "start": meta["start"],
                "end": meta["end"],
                "text": chunk_text,
                "segments": segments_data, 
                "language": detected_lang
            }

            with open(os.path.join(target_bucket_dir, "transcript.json"), "w") as f:
                json.dump(result_payload, f, indent=2)

            send_ipc({"status": "progress", "chunk": chunk_name, "pct": 100})
            gc.collect()
            send_ipc({"status": "success", "chunk": raw_input_target})

        except queue.Empty:
            break
        except Exception as e:
            send_ipc({"status": "error", "error": f"❌ Inference Crash: {str(e)}"})

if __name__ == "__main__":
    boot_daemon()