import sys
import os
import json
import psutil
import gc
import threading
import queue
from faster_whisper import WhisperModel

# Enforce strict CPU limits for CTranslate2 (Faster-Whisper backend)
num_cores = str(max(1, os.cpu_count() - 2))
os.environ["OMP_NUM_THREADS"] = num_cores
os.environ["OPENBLAS_NUM_THREADS"] = num_cores
os.environ["MKL_NUM_THREADS"] = num_cores

def send_ipc(data):
    print(f"ECHOFLOW_IPC__{json.dumps(data)}", flush=True)

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

    device = "cpu"
    compute_type = "int8"

    try:
        model = WhisperModel(
            local_model_path, 
            device=device, 
            compute_type=compute_type, 
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
                send_ipc({"status": "warn", "message": f"⚠️ No audio stream found for timeframe {bucket_prefix}."})
                send_ipc({"status": "success", "chunk": raw_input_target})
                continue

            with open(meta_path, "r") as f:
                meta = json.load(f)

            send_ipc({"status": "progress", "chunk": chunk_name, "pct": 10})

            segments, info = model.transcribe(
                wav_path,
                beam_size=5, # 1
                best_of=5, # 1
                temperature=0.0,
                language="en",
                vad_filter=True, 
                vad_parameters=dict(min_silence_duration_ms=500), #1000
                condition_on_previous_text=False
            )

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
            send_ipc({"status": "warn", "message": f"🧹 Whisper idle. Terminating to release RAM."})
            break
        except Exception as e:
            send_ipc({"status": "error", "error": f"❌ Inference Crash: {str(e)}"})

if __name__ == "__main__":
    boot_daemon()