import sys
import os
import json
import psutil
import gc
import threading
import queue
from faster_whisper import WhisperModel

os.environ["OMP_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"

def send_ipc(data):
    print(f"ECOFLOW_IPC__{json.dumps(data)}", flush=True)

def reader_thread(q):
    for line in iter(sys.stdin.readline, ''):
        q.put(line)
    q.put(None)

def boot_daemon():
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    local_model_path = os.path.join(project_root, "models", "whisper-small")
    
    if not os.path.exists(local_model_path) or not os.listdir(local_model_path):
        send_ipc({"status": "error", "error": f"❌ FATAL: Local Whisper model missing."})
        return

    device = os.environ.get("ASR_DEVICE", "cpu")
    compute_type = "int8" if device == "cpu" else "float16"

    model = WhisperModel(
        local_model_path, 
        device=device, 
        compute_type=compute_type, 
        local_files_only=True
    )

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

            bucket_prefix = "_".join(chunk_name.split("_")[0:2]) + "_" 
            
            curr = raw_input_target
            job_root = None
            while curr and os.path.dirname(curr) != curr:
                if os.path.exists(os.path.join(curr, "out_AudioChunker")):
                    job_root = curr
                    break
                if os.path.basename(curr).startswith("JOB-"):
                    job_root = curr
                    break
                curr = os.path.dirname(curr)

            if job_root is None:
                job_root = os.path.dirname(os.path.dirname(raw_input_target))

            target_bucket_dir = None
            audio_chunker_dir = os.path.join(job_root, "out_AudioChunker")
            
            if os.path.exists(audio_chunker_dir):
                for d in os.listdir(audio_chunker_dir):
                    if d.startswith(bucket_prefix):
                        target_bucket_dir = os.path.join(audio_chunker_dir, d)
                        break
            
            if target_bucket_dir is None:
                target_bucket_dir = raw_input_target

            wav_path = os.path.join(target_bucket_dir, "chunk_audio.wav")
            meta_path = os.path.join(target_bucket_dir, "meta.json")

            if not os.path.exists(wav_path):
                send_ipc({"status": "warn", "message": f"⚠️ No audio stream found for timeframe {bucket_prefix}. Skipping."})
                send_ipc({"status": "success", "chunk": raw_input_target})
                continue
            
            if not os.path.exists(meta_path):
                send_ipc({"status": "error", "error": f"❌ Meta file not found at: {meta_path}"})
                continue

            with open(meta_path, "r") as f:
                meta = json.load(f)

            send_ipc({"status": "progress", "chunk": chunk_name, "pct": 10})

            segments, info = model.transcribe(
                wav_path,
                beam_size=1,
                best_of=1,
                temperature=0.0,
                language=os.environ.get("ASR_LANGUAGE", "en"),
                vad_filter=True, 
                vad_parameters=dict(min_silence_duration_ms=1000),
                condition_on_previous_text=False
            )

            # Extract detailed segments and map them to absolute timeline timestamps
            segments_data = []
            for seg in segments:
                if seg.text.strip():
                    segments_data.append({
                        "start": round(meta["start"] + seg.start, 2),
                        "end": round(meta["start"] + seg.end, 2),
                        "text": seg.text.strip()
                    })

            chunk_text = " ".join(s["text"] for s in segments_data)
            
            result_payload = {
                "id": meta["id"],
                "start": meta["start"],
                "end": meta["end"],
                "text": chunk_text,
                "segments": segments_data, 
                "language": getattr(info, "language", "en")
            }

            with open(os.path.join(target_bucket_dir, "transcript.json"), "w") as f:
                json.dump(result_payload, f, indent=2)

            send_ipc({"status": "progress", "chunk": chunk_name, "pct": 100})
            gc.collect()
            send_ipc({"status": "success", "chunk": raw_input_target})

        except queue.Empty:
            send_ipc({"status": "warn", "message": f"🧹 WhisperTranscriber idle for {IDLE_TIMEOUT_SECONDS}s. Self-terminating to release RAM."})
            break
            
        except Exception as e:
            send_ipc({"status": "error", "error": f"❌ Inference Crash: {str(e)}"})

if __name__ == "__main__":
    boot_daemon()