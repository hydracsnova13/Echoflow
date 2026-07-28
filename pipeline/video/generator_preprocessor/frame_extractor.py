import sys
import cv2
import os
import math
import time
import traceback

def safe_rename(src, dst):
    # 🛡️ THE FIX: Retry loop to defeat Windows Defender file-locking (WinError 32)
    for _ in range(15):
        try:
            os.rename(src, dst)
            return
        except OSError:
            time.sleep(0.2)
    # If it fails after 3 seconds, throw the exception to log it
    os.rename(src, dst)

def extract_temporal_buckets(video_path, out_dir):
    if os.path.isdir(video_path):
        possible_video = os.path.join(video_path, "video_mute.mov")
        if os.path.exists(possible_video): video_path = possible_video
        else:
            files = [f for f in os.listdir(video_path) if f.endswith(('.mov', '.mp4', '.mkv'))]
            if files: video_path = os.path.join(video_path, files[0])

    os.makedirs(out_dir, exist_ok=True)
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened(): raise IOError(f"OpenCV failed to open video file: {video_path}")

    fps = cap.get(cv2.CAP_PROP_FPS)
    if fps == 0 or math.isnan(fps): fps = 30.0

    chunk_size_sec, overlap_sec = 30.0, 1.0
    step_sec = chunk_size_sec - overlap_sec

    frame_count = 0
    active_buckets = {}

    while True:
        ret, frame = cap.read()
        if not ret: break

        t = frame_count / fps
        min_idx = max(0, int((t - chunk_size_sec) // step_sec) + 1)
        max_idx = int(t // step_sec)

        for i in range(min_idx, max_idx + 1):
            chunk_start = i * step_sec
            chunk_end = chunk_start + chunk_size_sec

            if chunk_start <= t <= chunk_end:
                start_str, end_str = f"{int(chunk_start):03d}", f"{int(chunk_end):03d}"
                bucket_name = f"bucket_{start_str}_{end_str}"
                tmp_dir = os.path.join(out_dir, f".tmp_{bucket_name}")
                
                if bucket_name not in active_buckets:
                    os.makedirs(tmp_dir, exist_ok=True)
                    active_buckets[bucket_name] = {"tmp": tmp_dir, "final": os.path.join(out_dir, bucket_name), "end": chunk_end}

                cv2.imwrite(os.path.join(tmp_dir, f"frame_{frame_count:05d}.jpg"), frame)

        # ATOMIC RENAME: Close buckets that are finished so Go can stream them instantly
        finished = [k for k, v in active_buckets.items() if t > v["end"]]
        for k in finished:
            safe_rename(active_buckets[k]["tmp"], active_buckets[k]["final"])
            del active_buckets[k]

        frame_count += 1

    # Close remaining buckets at EOF
    for k, v in active_buckets.items():
        safe_rename(v["tmp"], v["final"])

    cap.release()
    return frame_count

def run_cli():
    if len(sys.argv) < 2: return
    try:
        frame_count = extract_temporal_buckets(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "")
        if frame_count == 0: 
            print("[FrameExtractor] ❌ Error: 0 frames extracted.", flush=True)
            sys.exit(1)
        print(f"[FrameExtractor] ✅ Extracted {frame_count} frames.", flush=True)
    except Exception as e:
        # 🛡️ THE FIX: Never die silently. Print the full traceback directly to the Wails UI.
        print(f"[FrameExtractor] ❌ CRASH: {str(e)}\n{traceback.format_exc()}", flush=True)
        sys.exit(1)

if __name__ == "__main__": run_cli()