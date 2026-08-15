import sys
import os
import json
import soundfile as sf
import numpy as np

def safe_rename(src, dst):
    import time
    for _ in range(15):
        try:
            os.rename(src, dst)
            return
        except OSError:
            time.sleep(0.2)
    os.rename(src, dst)

def run_audio_chunker(input_path, output_workspace_dir):
    os.makedirs(output_workspace_dir, exist_ok=True)

    wav_file = input_path
    if os.path.isdir(input_path):
        for f in os.listdir(input_path):
            if f.endswith(".wav"):
                wav_file = os.path.join(input_path, f)
                break

    if not os.path.exists(wav_file):
        print(f"[AudioChunker] ❌ Error: WAV file not found in {input_path}", flush=True)
        sys.exit(1)

    audio, sample_rate = sf.read(wav_file, dtype="float32")
    if audio.ndim == 2:
        audio = audio.mean(axis=1)

    target_rate = 16000
    if sample_rate != target_rate:
        duration = len(audio) / sample_rate
        target_length = int(duration * target_rate)
        positions = np.linspace(0, len(audio) - 1, target_length)
        audio = audio[np.floor(positions).astype(int)]
        sample_rate = target_rate

    total_samples = len(audio)
    total_seconds = total_samples / sample_rate

    window_size = 30.0
    overlap = 1.0
    step = window_size - overlap

    current_bucket_idx = 0
    start_sec = 0.0

    while start_sec < total_seconds:
        end_sec = min(start_sec + window_size, total_seconds)
        bucket_name = f"bucket_{int(start_sec):03d}_{int(end_sec):03d}"
        
        tmp_dir = os.path.join(output_workspace_dir, f".tmp_{bucket_name}")
        final_dir = os.path.join(output_workspace_dir, bucket_name)
        os.makedirs(tmp_dir, exist_ok=True)

        s_idx = int(start_sec * sample_rate)
        e_idx = int(end_sec * sample_rate)
        chunk_audio = audio[s_idx:e_idx]

        sf.write(os.path.join(tmp_dir, "chunk_audio.wav"), chunk_audio, sample_rate)

        meta = {"start": start_sec, "end": end_sec, "id": current_bucket_idx + 1}
        with open(os.path.join(tmp_dir, "meta.json"), "w") as f:
            json.dump(meta, f)

        safe_rename(tmp_dir, final_dir)
        current_bucket_idx += 1
        start_sec += step
        
        if end_sec >= total_seconds:
            break

    print(f"[AudioChunker] ✅ Created {current_bucket_idx} audio buckets.", flush=True)

if __name__ == "__main__":
    if len(sys.argv) >= 3:
        run_audio_chunker(sys.argv[1], sys.argv[2])