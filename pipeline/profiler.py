import sys
import os
import json
import cv2
import subprocess
import shutil

def profile_and_split(input_file, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    
    target_file = input_file
    if os.path.isdir(input_file):
        for f in os.listdir(input_file):
            if not f.endswith(".json") and not os.path.isdir(os.path.join(input_file, f)):
                target_file = os.path.join(input_file, f)
                break

    if not target_file or not os.path.exists(target_file):
        print(f"❌ [MetadataProfiler] Input file not found: {input_file}", flush=True)
        sys.exit(1)

    ext = os.path.splitext(target_file)[1].lower()
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ffmpeg_exe = os.path.join(project_root, "bin", "ffmpeg.exe") if os.name == 'nt' else os.path.join(project_root, "bin", "ffmpeg")
    
    if ext in ['.txt', '.json', '.srt']:
        print(f"📄 [Profiler] Text input detected ({ext}). Processing text payload...", flush=True)
        if ext == '.txt':
            with open(target_file, "r", encoding="utf-8") as tf:
                lines = [l.strip() for l in tf.readlines() if l.strip()]
            timeline = []
            full_text = []
            for idx, line in enumerate(lines):
                spk = "SPEAKER_00"
                text_content = line
                if line.startswith("[") and "]" in line:
                    parts = line.split("]", 1)
                    spk = parts[0].strip("[ ]")
                    text_content = parts[1].strip()
                
                timeline.append({
                    "start": float(idx * 5),
                    "end": float((idx + 1) * 5),
                    "speaker_id": spk,
                    "text": f"[{spk}] {text_content}"
                })
                full_text.append(f"[{spk}] {text_content}")

            payload = {
                "job_id": os.path.basename(os.path.dirname(output_dir)),
                "language": "en",
                "full_transcript": " ".join(full_text),
                "timeline": timeline
            }
            with open(os.path.join(output_dir, "master_transcript.json"), "w", encoding="utf-8") as out_f:
                json.dump(payload, out_f, indent=2, ensure_ascii=False)
        
        elif ext == '.json':
            shutil.copyfile(target_file, os.path.join(output_dir, "master_transcript.json"))
            
        print(f"[Profiler] ✅ Text payload prepared at master_transcript.json", flush=True)
        return

    audio_wav_path = os.path.join(output_dir, "audio.wav")
    if ext in ['.wav', '.mp3', '.flac', '.m4a', '.aac']:
        print(f"🎵 [Profiler] Audio input detected. Bypassing video analysis...", flush=True)
        try:
            subprocess.run([ffmpeg_exe, "-y", "-i", target_file, "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1", audio_wav_path], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as e:
            print(f"[Profiler] ⚠️ FFmpeg audio conversion failed: {e}. Copying raw file.", flush=True)
            shutil.copyfile(target_file, audio_wav_path)

        with open(os.path.join(output_dir, "timecodes.json"), "w") as f:
            json.dump({"fps": 30.0, "total_frames": 0, "width": 0, "height": 0, "timecodes": []}, f, indent=2)

        print(f"[Profiler] ✅ Audio normalization complete.", flush=True)
        return

    print(f"🎬 [Profiler] Video input detected. Extracting metadata...", flush=True)
    cap = cv2.VideoCapture(target_file)
    if not cap.isOpened(): raise ValueError(f"Cannot open video file: {target_file}")

    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
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
    
    try:
        print(f"[Profiler] Splitting audio/video streams...", flush=True)
        subprocess.run([ffmpeg_exe, "-y", "-i", target_file, "-an", "-c:v", "copy", video_mute_path], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run([ffmpeg_exe, "-y", "-i", target_file, "-vn", "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1", audio_wav_path], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as e:
        print(f"[Profiler] Fallback triggered. Copying video directly.", flush=True)
        shutil.copyfile(target_file, video_mute_path)

    print(f"[Profiler] ✅ Profiled {total_frames} frames.", flush=True)

if __name__ == "__main__":
    if len(sys.argv) < 3: sys.exit(1)
    try: 
        profile_and_split(sys.argv[1], sys.argv[2])
    except Exception as e: 
        print(f"❌ Exception: {e}", flush=True)
        sys.exit(1)