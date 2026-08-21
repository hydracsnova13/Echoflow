import sys
import os
import subprocess
import json
import shutil
import re
from pathlib import Path

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

def compose_media(input_target, output_dir):
    curr = os.path.abspath(input_target)
    job_root = None
    while curr and os.path.dirname(curr) != curr:
        if os.path.basename(curr).startswith("JOB-"):
            job_root = curr
            break
        curr = os.path.dirname(curr)
        
    if not job_root:
        print("❌ [MediaCompositor] Invalid job context.", flush=True)
        sys.exit(1)
        
    config_path = os.path.join(job_root, "job_config.json")
    media_type = "video"
    output_format = "mp4"
    subtitle_mode = "none"
    
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            cfg = json.load(f)
            media_type = cfg.get("media_type", "video")
            output_format = cfg.get("output_format", "mp4").lower()
            subtitle_mode = cfg.get("subtitle_mode", "none")
            
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"final_recomposed.{output_format}" 
    
    project_root = Path(__file__).resolve().parent.parent.parent
    bin_dir = project_root / "bin"
    ffmpeg_path = str(bin_dir / ("ffmpeg.exe" if os.name == 'nt' else "ffmpeg"))
    if not os.path.exists(ffmpeg_path):
        ffmpeg_path = "ffmpeg"
        
    dubbed_audio = os.path.join(job_root, "out_VoiceDubber", "master_dubbed.wav")
    srt_path = os.path.join(job_root, "out_NMTTranslator", "subtitles.srt")

    audio_formats = ["wav", "mp3", "aac", "flac", "wma", "ogg"]
    text_formats = ["srt", "txt", "json"]

    # ==========================================
    # ROUTE 1: TEXT OUTPUT
    # ==========================================
    if output_format in text_formats or (media_type == "text" and output_format in text_formats):
        print(f"📄 [MediaCompositor] Exporting Text Format: {output_format}", flush=True)
        if os.path.exists(srt_path):
            final_out_path = os.path.join(output_dir, output_path)
            
            # 🛡️ THE FIX: Cleanly parse the SRT into a readable Transcript
            if output_format == "txt":
                with open(srt_path, "r", encoding="utf-8") as f:
                    raw_srt = f.read()
                
                clean_lines = []
                blocks = re.split(r'\n\s*\n', raw_srt.strip())
                
                for block in blocks:
                    lines = block.splitlines()
                    # SRT blocks are: 0=Index, 1=Timestamp, 2+=Text
                    if len(lines) >= 3:
                        text_content = " ".join(lines[2:])
                        # Strip all variations of [SPEAKER_00], [SPEAKER_01], etc.
                        text_content = re.sub(r'\[SPEAKER_[^\]]+\]', '', text_content)
                        # Remove extra floating spaces
                        text_content = " ".join(text_content.split())
                        
                        if text_content:
                            clean_lines.append(text_content)
                
                # Write a clean, double-spaced transcript document
                with open(final_out_path, "w", encoding="utf-8") as f:
                    f.write("\n\n".join(clean_lines))
            else:
                # If they actually requested an .srt or .json, just copy it
                shutil.copy(srt_path, final_out_path)
                
            print(f"✅ [MediaCompositor] Final text saved to: {final_out_path}", flush=True)
            sys.exit(0)
        else:
            print("❌ [MediaCompositor] Source subtitles not found.", flush=True)
            sys.exit(1)

    # ==========================================
    # ROUTE 2: AUDIO OUTPUT
    # ==========================================
    if output_format in audio_formats or media_type == "audio":
        print(f"🎵 [MediaCompositor] Exporting Audio Format: {output_format}", flush=True)
        if not os.path.exists(dubbed_audio):
            print("❌ [MediaCompositor] Dubbed audio not found.", flush=True)
            sys.exit(1)
            
        if output_format == "wav":
            shutil.copy(dubbed_audio, os.path.join(output_dir, output_path))
        else:
            codec = "libmp3lame" if output_format == "mp3" else "aac" if output_format == "aac" else "flac" if output_format == "flac" else "copy"
            subprocess.run([ffmpeg_path, "-y", "-i", str(dubbed_audio), "-c:a", codec, os.path.join(output_dir, output_path)], capture_output=True)
            
        print(f"✅ [MediaCompositor] Final audio saved to: {output_path}", flush=True)
        sys.exit(0)
        
    # ==========================================
    # ROUTE 3: VIDEO OUTPUT
    # ==========================================
    original_video = None
    for f in os.listdir(job_root):
        if f.lower().endswith(('.mp4', '.mov', '.avi', '.mkv', '.webm', '.flv', '.wmv')):
            original_video = os.path.join(job_root, f)
            break
            
    if not original_video or not os.path.exists(original_video):
        print("❌ [MediaCompositor] Original video not found for muxing.", flush=True)
        sys.exit(1)
        
    has_dubbed_audio = os.path.exists(dubbed_audio)
    
    cmd = [
        ffmpeg_path, "-y",
        "-i", str(os.path.abspath(original_video))
    ]
    
    if has_dubbed_audio:
        cmd.extend(["-i", str(os.path.abspath(dubbed_audio))])
        encode_args = [
            "-map", "0:v:0", "-map", "1:a:0",
            "-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", "22", "-preset", "fast",
            "-c:a", "aac", "-b:a", "192k", "-ac", "2", "-ar", "44100",
            "-max_muxing_queue_size", "1024",
            "-movflags", "+faststart",
            "-shortest"
        ]
    else:
        print("⚠️ [MediaCompositor] Dubbed audio not found. Recomposing using original audio track.", flush=True)
        encode_args = [
            "-map", "0:v:0", "-map", "0:a:0?",
            "-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", "22", "-preset", "fast",
            "-c:a", "aac", "-b:a", "192k", "-ac", "2", "-ar", "44100",
            "-max_muxing_queue_size", "1024",
            "-movflags", "+faststart",
            "-shortest"
        ]

    if subtitle_mode in ["source", "target"]:
        srt_name = "source.srt" if subtitle_mode == "source" else "subtitles.srt"
        target_srt = os.path.join(job_root, "out_NMTTranslator", srt_name)
        
        if os.path.exists(target_srt):
            print(f"✍️ [MediaCompositor] Burning {subtitle_mode} subtitles into video...", flush=True)
            temp_srt = os.path.join(output_dir, "burn.srt")
            shutil.copy(target_srt, temp_srt)
            encode_args.extend(["-vf", "subtitles=burn.srt"])
            
    cmd.extend(encode_args)
    cmd.append(output_path)
    
    print(f"🎬 [MediaCompositor] Rendering final H.264 video with Audio...", flush=True)
    
    result = subprocess.run(cmd, capture_output=True, text=True, check=False, cwd=output_dir)
    
    if result.returncode != 0:
        print(f"❌ [MediaCompositor] Recomposition failed: {result.stderr.strip()}", flush=True)
        sys.exit(1)
        
    print(f"✅ [MediaCompositor] Final video composed successfully at: {output_path}", flush=True)

if __name__ == "__main__":
    if len(sys.argv) >= 3:
        compose_media(sys.argv[1], sys.argv[2])