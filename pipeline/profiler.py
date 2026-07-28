import sys
import os
import json
import cv2
import subprocess
import shutil

def profile_and_split(input_file, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    cap = cv2.VideoCapture(input_file)
    if not cap.isOpened(): raise ValueError(f"Cannot open video file: {input_file}")

    fps = cap.get(cv2.CAP_PROP_FPS)
    if fps == 0 or fps != fps: fps = 30.0

    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    timecodes = []
    frame_idx = 0

    while True:
        ret, _ = cap.read()
        if not ret: break
        msec = cap.get(cv2.CAP_PROP_POS_MSEC)
        timecodes.append({"frame": frame_idx, "timestamp_ms": msec})
        frame_idx += 1
    cap.release()

    with open(os.path.join(output_dir, "timecodes.json"), "w") as f:
        json.dump({"fps": fps, "total_frames": total_frames, "width": width, "height": height, "timecodes": timecodes}, f, indent=2)

    video_mute_path = os.path.join(output_dir, "video_mute.mov")
    audio_wav_path = os.path.join(output_dir, "audio.wav")
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ffmpeg_exe = os.path.join(project_root, ".venv", "Scripts", "ffmpeg.exe")
    
    try:
        if not os.path.exists(ffmpeg_exe): raise FileNotFoundError(f"FFmpeg not found at {ffmpeg_exe}")
        print(f"[Profiler] Splitting audio/video streams...", flush=True)
        subprocess.run([ffmpeg_exe, "-y", "-i", input_file, "-an", "-c:v", "copy", video_mute_path], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run([ffmpeg_exe, "-y", "-i", input_file, "-vn", "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1", audio_wav_path], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as e:
        print(f"[Profiler] Fallback triggered. Copying video directly.", flush=True)
        shutil.copyfile(input_file, video_mute_path)

    print(f"[Profiler] ✅ Profiled {total_frames} frames. Artifacts saved.", flush=True)

if __name__ == "__main__":
    if len(sys.argv) < 2: sys.exit(1)
    input_file = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else os.path.join(os.path.dirname(input_file), "out_MetadataProfiler")
    try: profile_and_split(input_file, output_dir)
    except Exception as e: sys.exit(1)