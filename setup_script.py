import os
import urllib.request
import json
import zipfile
import io
import sys
import subprocess
import platform

def get_venv_bin(env_dir, bin_name):
    scripts_path = os.path.join(env_dir, "Scripts", f"{bin_name}.exe")
    if os.path.exists(scripts_path):
        return scripts_path
    bin_path = os.path.join(env_dir, "bin", bin_name)
    if os.path.exists(bin_path):
        return bin_path
    bin_exe_path = os.path.join(env_dir, "bin", f"{bin_name}.exe")
    if os.path.exists(bin_exe_path):
        return bin_exe_path
    return scripts_path if os.name == 'nt' else bin_path

def setup_model_garden():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    models_dir = os.path.join(base_dir, "models")
    pipeline_dir = os.path.join(base_dir, "pipeline")
    envs_dir = os.path.join(base_dir, ".envs")
    bin_dir = os.path.join(base_dir, "bin")
    
    os.makedirs(models_dir, exist_ok=True)
    os.makedirs(pipeline_dir, exist_ok=True)
    os.makedirs(envs_dir, exist_ok=True)
    os.makedirs(bin_dir, exist_ok=True)

    print("=========================================================")
    print("🚀 Initializing Echoflow Dual-Python Clean Setup")
    print("=========================================================\n")

    env_core = os.path.join(envs_dir, "env_core")
    env_tts = os.path.join(envs_dir, "env_tts")

    # =========================================================
    # 1. DUAL-VERSION VIRTUAL ENVIRONMENT PROVISIONING
    # =========================================================
    print("📦 Provisioning Isolated Environments...")
    
    if not os.path.exists(env_core):
        print("   -> Creating 'env_core' using Python 3.12...")
        try:
            subprocess.check_call(["py", "-3.12", "-m", "venv", env_core])
        except Exception:
            subprocess.check_call([sys.executable, "-m", "venv", env_core])

    if not os.path.exists(env_tts):
        print("   -> Creating 'env_tts' using Python 3.10...")
        try:
            subprocess.check_call(["py", "-3.10", "-m", "venv", env_tts])
        except Exception as e:
            print(f"❌ Error creating env_tts: {e}")
            sys.exit(1)

    python_core = get_venv_bin(env_core, "python")
    python_tts = get_venv_bin(env_tts, "python")
    pip_core = get_venv_bin(env_core, "pip")
    pip_tts = get_venv_bin(env_tts, "pip")

    print("\n⬆️ Upgrading build tools (pip, setuptools, wheel)...")
    subprocess.check_call([python_core, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"])
    
    # 🛡️ THE FIX: Restrict setuptools to <70 in env_tts to preserve pkg_resources for OpenVoice
    subprocess.check_call([python_tts, "-m", "pip", "install", "--upgrade", "pip", "setuptools<70", "wheel"])

    # =========================================================
    # 2. REQUIREMENTS INJECTION
    # =========================================================
    core_reqs = """faster-whisper>=1.0.0
silero-vad>=5.0.0
onnxruntime>=1.16.0
torch>=2.2.0
torchaudio>=2.2.0
ffmpeg-python>=0.2.0
soundfile>=0.12.1
librosa>=0.10.0
pydantic>=2.7.0
loguru>=0.7.0
typer>=0.12.0
pyttsx3>=2.90
psutil
opencv-python>=4.11.0.86
mediapipe
transformers>=4.38.2
sentencepiece>=0.1.99
huggingface-hub>=0.20.3
"""
    
    tts_reqs = """TTS==0.22.0
pyannote.audio==3.1.1
pysbd==0.3.4
pydub==0.25.1
huggingface-hub==0.20.3
wavmark
transformers==4.38.2
librosa>=0.9.1
faster-whisper>=1.0.0
cn2an==0.5.22
eng_to_ipa==0.0.2
langid==1.1.6
whisper-timestamped==1.14.2
pypinyin
jieba
pysrt
setuptools<70
"""

    req_core_file = os.path.join(envs_dir, "req_core.txt")
    req_tts_file = os.path.join(envs_dir, "req_tts.txt")
    
    with open(req_core_file, "w") as f: f.write(core_reqs)
    with open(req_tts_file, "w") as f: f.write(tts_reqs)

    print("\n⚙️ Installing packages into 'env_core' (Python 3.12)...")
    subprocess.check_call([pip_core, "install", "-r", req_core_file])

    print("\n⚙️ Installing strict PyTorch CPU wheels into 'env_tts' (Python 3.10)...")
    subprocess.check_call([pip_tts, "install", "torch==2.1.2", "torchvision==0.16.2", "torchaudio==2.1.2", "--index-url", "https://download.pytorch.org/whl/cpu"])
    
    print("\n⚙️ Installing remaining packages into 'env_tts' (Python 3.10)...")
    subprocess.check_call([pip_tts, "install", "-r", req_tts_file])

    print("\n⚙️ Force Installing OpenVoice (Bypassing internal conflicts)...")
    subprocess.check_call([pip_tts, "install", "--no-deps", "https://github.com/myshell-ai/OpenVoice/archive/refs/heads/main.zip"])

    # =========================================================
    # 3. HUGGING FACE OFFLINE VERIFICATION
    # =========================================================
    print("\n📥 Verifying / Synchronizing Model Weights via HuggingFace Hub...")
    try:
        sync_script = f"""
from huggingface_hub import snapshot_download, login
import os
login(token="YOUR_HF_TOKEN_HERE")
models_dir = r"{models_dir}"
models = {{
    "Systran/faster-whisper-small": "whisper-small",
    "facebook/nllb-200-distilled-600M": "nllb-200-distilled-600M",
    "pyannote/speaker-diarization-3.1": "offline_pyannote_model",
    "pyannote/segmentation-3.0": "pyannote_segmentation",
    "facebook/mms-tts-hin": "offline_mms_model/hin",
    "facebook/mms-tts-mar": "offline_mms_model/mar",
    "facebook/mms-tts-eng": "offline_mms_model/eng"
}}
for repo_id, folder_name in models.items():
    print(f"Syncing {{repo_id}}...")
    snapshot_download(repo_id=repo_id, local_dir=os.path.join(models_dir, folder_name), local_dir_use_symlinks=False)

print("Syncing myshell-ai/OpenVoiceV2 (Converter only)...")
snapshot_download(repo_id="myshell-ai/OpenVoiceV2", allow_patterns=["converter/*"], local_dir=os.path.join(models_dir, "offline_openvoice"), local_dir_use_symlinks=False)
"""
        sync_file = os.path.join(envs_dir, "sync_models.py")
        with open(sync_file, "w") as f: f.write(sync_script)
        subprocess.check_call([python_core, sync_file])
    except Exception as e:
        print(f"❌ Error downloading huggingface models: {e}")

    # =========================================================
    # 4. DOWNLOAD CENTRAL FFMPEG
    # =========================================================
    ffmpeg_exe = os.path.join(bin_dir, "ffmpeg.exe" if os.name == 'nt' else "ffmpeg")
    if not os.path.exists(ffmpeg_exe):
        print(f"\n📥 Downloading FFmpeg Static Build for {platform.system()}...")
        if platform.system() == "Windows":
            ffmpeg_url = "https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-win64-gpl.zip"
        elif platform.system() == "Darwin":
            ffmpeg_url = "https://evermeet.cx/ffmpeg/ffmpeg-116035-g878783457a.zip"
        else:
            ffmpeg_url = "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz"

        try:
            req = urllib.request.urlopen(ffmpeg_url)
            if ffmpeg_url.endswith(".zip"):
                with zipfile.ZipFile(io.BytesIO(req.read())) as z:
                    for file_info in z.infolist():
                        if file_info.filename.endswith(("ffmpeg.exe", "ffprobe.exe", "ffmpeg")):
                            file_info.filename = os.path.basename(file_info.filename)
                            z.extract(file_info, bin_dir)
            if os.name != 'nt' and os.path.exists(ffmpeg_exe):
                os.chmod(ffmpeg_exe, 0o755)
            print(f"✅ FFmpeg extracted to {bin_dir}")
        except Exception as e:
            print(f"❌ Failed to download FFmpeg: {str(e)}")

    # =========================================================
    # 5. GENERATE REGISTRY & MANIFEST
    # =========================================================
    print("\nGenerating hardware registry...")
    registry = {
        "WhisperSmallDaemon": {"model_path": "models/whisper-small", "framework": "faster-whisper", "estimated_ram_mb": 1200.0},
        "PyannoteDiarizerDaemon": {"model_path": "models/offline_pyannote_model", "framework": "pyannote", "estimated_ram_mb": 1000.0},
        "MMSTTSBase": {"model_path": "models/offline_mms_model", "framework": "transformers", "estimated_ram_mb": 1500.0},
        "OpenVoiceV2Daemon": {"model_path": "models/offline_openvoice", "framework": "openvoice", "estimated_ram_mb": 2500.0}
    }
    with open(os.path.join(models_dir, "registry.json"), "w", encoding="utf-8") as f:
        json.dump(registry, f, indent=4)
    
    manifest = {
        "MetadataProfiler": {"env_name": "env_core", "domain": "cpu", "execution_mode": "sequential", "depends_on": [], "script": "pipeline/profiler.py", "accepted_inputs": [".txt", ".json", ".srt", ".mp4", ".wav", ".mp3"], "produces": "directory"},
        "AudioChunker": {"env_name": "env_core", "domain": "cpu", "execution_mode": "sequential", "depends_on": ["MetadataProfiler"], "script": "pipeline/audio/audio_chunker.py", "accepted_inputs": ["directory"], "produces": "directory"},
        "WhisperTranscriber": {"env_name": "env_core", "domain": "ram", "execution_mode": "chunked", "depends_on": ["AudioChunker"], "model_ref": "WhisperSmallDaemon", "script": "pipeline/audio/whisper_transcriber.py", "accepted_inputs": ["directory"], "produces": ".json"},
        "SpeakerDiarizer": {"env_name": "env_tts", "domain": "cpu", "execution_mode": "sequential", "depends_on": ["MetadataProfiler"], "script": "pipeline/audio/speaker_diarizer.py", "accepted_inputs": ["directory", ".wav"], "produces": "directory"},
        "TranscriptAggregator": {"env_name": "env_core", "domain": "cpu", "execution_mode": "sequential", "depends_on": ["WhisperTranscriber", "SpeakerDiarizer"], "script": "pipeline/audio/transcript_aggregator.py", "accepted_inputs": ["directory"], "produces": ".json"},
        "NMTTranslator": {"env_name": "env_core", "domain": "cpu", "execution_mode": "sequential", "depends_on": ["TranscriptAggregator"], "script": "pipeline/audio/nmt_runner.py", "accepted_inputs": ["directory", ".json"], "produces": ".json, .srt"},
        "VoiceDubber": {"env_name": "env_tts", "domain": "cpu", "execution_mode": "sequential", "depends_on": ["NMTTranslator", "SpeakerDiarizer"], "script": "pipeline/audio/voice_dubber.py", "accepted_inputs": ["directory", ".srt"], "produces": ".wav"},
        "MediaCompositor": {"env_name": "env_core", "domain": "cpu", "execution_mode": "sequential", "depends_on": ["VoiceDubber"], "script": "pipeline/video/media_compositor.py", "accepted_inputs": ["directory"], "produces": ".mp4"}
    }

    with open(os.path.join(pipeline_dir, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=4)
    
    print("✅ pipeline/manifest.json updated.")
    print("🎉 Echoflow Dual-Python Clean Setup Complete!")

if __name__ == "__main__":
    setup_model_garden()