import os
import urllib.request
import json
import zipfile
import io
import sys

def setup_model_garden():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    models_dir = os.path.join(base_dir, "models")
    pipeline_dir = os.path.join(base_dir, "pipeline")
    os.makedirs(models_dir, exist_ok=True)
    os.makedirs(pipeline_dir, exist_ok=True)

    print("=========================================================")
    print("🚀 Initializing EcoFlow Multi-Branch Environment Setup")
    print("=========================================================\n")

    # ---------------------------------------------------------
    # 1. Download AI Models into Model Garden (Video + ASR)
    # ---------------------------------------------------------
    # A. OpenCV YuNet
    yunet_url = "https://github.com/opencv/opencv_zoo/raw/main/models/face_detection_yunet/face_detection_yunet_2023mar.onnx"
    yunet_path = os.path.join(models_dir, "face_detection_yunet_2023mar.onnx")

    if not os.path.exists(yunet_path):
        print("Downloading OpenCV YuNet Model...")
        urllib.request.urlretrieve(yunet_url, yunet_path)
        print("✅ YuNet downloaded successfully.")
    else:
        print("✅ YuNet already exists.")

    # B. MediaPipe Face Landmarker
    mp_task_url = "https://storage.googleapis.com/mediapipe-models/face_landmarker/face_landmarker/float16/1/face_landmarker.task"
    mp_task_path = os.path.join(models_dir, "face_landmarker.task")

    if not os.path.exists(mp_task_path):
        print("Downloading MediaPipe Face Landmarker Task Model...")
        urllib.request.urlretrieve(mp_task_url, mp_task_path)
        print("✅ MediaPipe Task Model downloaded successfully.")
    else:
        print("✅ MediaPipe Task Model already exists.")

    # C. Faster-Whisper Small Model (Offline Cache)
    whisper_dir = os.path.join(models_dir, "whisper-small")
    if not os.path.exists(whisper_dir) or not os.listdir(whisper_dir):
        print("Downloading Whisper 'small' model weights into Model Garden...")
        try:
            from faster_whisper import download_model
            # This directly downloads the HF snapshot into the models/whisper-small directory
            download_model("small", output_dir=whisper_dir)
            print("✅ Whisper 'small' model weights cached successfully.")
        except ImportError:
            print("❌ Error: 'faster-whisper' is not installed in the current environment.")
            print("   Please run 'pip install -r requirements.txt' inside your .venv.")
            sys.exit(1)
    else:
        print("✅ Whisper model already cached in Model Garden.")

    # ---------------------------------------------------------
    # 2. Download Hermetic FFmpeg
    # ---------------------------------------------------------
    venv_scripts_dir = os.path.join(base_dir, ".venv", "Scripts")
    os.makedirs(venv_scripts_dir, exist_ok=True)
    ffmpeg_exe = os.path.join(venv_scripts_dir, "ffmpeg.exe")
    
    if not os.path.exists(ffmpeg_exe):
        print("\nDownloading FFmpeg (Windows Static Build)... This might take a minute.")
        ffmpeg_url = "https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-win64-gpl.zip"
        
        req = urllib.request.urlopen(ffmpeg_url)
        with zipfile.ZipFile(io.BytesIO(req.read())) as z:
            for file_info in z.infolist():
                if file_info.filename.endswith("ffmpeg.exe") or file_info.filename.endswith("ffprobe.exe"):
                    file_info.filename = os.path.basename(file_info.filename)
                    z.extract(file_info, venv_scripts_dir)
        print(f"✅ FFmpeg extracted explicitly to {venv_scripts_dir}")
    else:
        print("✅ Hermetic FFmpeg already installed in project .venv.")

    # ---------------------------------------------------------
    # 3. Generate Hardware Model Registry (models/registry.json)
    # ---------------------------------------------------------
    print("\nGenerating hardware registry...")
    registry = {
        "YunetFaceDetector_2023": {
            "model_path": "models/face_detection_yunet_2023mar.onnx",
            "framework": "onnx",
            "estimated_ram_mb": 450.0
        },
        "GoogleFacialLandmarker": {
            "model_path": "models/face_landmarker.task",
            "framework": "mediapipe",
            "estimated_ram_mb": 600.0
        },
        "WhisperSmallDaemon": {
            "model_path": "models/whisper-small",
            "framework": "faster-whisper",
            "estimated_ram_mb": 1200.0
        }
    }
    with open(os.path.join(models_dir, "registry.json"), "w") as f:
        json.dump(registry, f, indent=4)
    print("✅ models/registry.json updated with Video + ASR RAM footprints.")

    # ---------------------------------------------------------
    # 4. Generate Combined Multi-Branch Pipeline DAG (manifest.json)
    # ---------------------------------------------------------
    print("Generating joined Multi-Branch DAG execution manifest...")
    manifest = {
        "MetadataProfiler": {
            "domain": "cpu",
            "execution_mode": "sequential",
            "depends_on": [],
            "script": "pipeline/profiler.py",
            "accepted_inputs": [
                ".3g2", ".3gp", ".avi", ".asf", ".f4v", ".flv", ".m2ts", 
                ".m2v", ".m4v", ".mkv", ".mov", ".mp4", ".mpg", ".mpeg", 
                ".mts", ".mxf", ".ogv", ".rm", ".rmvb", ".ts", ".vob", 
                ".webm", ".wmv"
            ],
            "produces": "directory"
        },
        "FrameExtractor": {
            "domain": "cpu",
            "execution_mode": "sequential",
            "depends_on": ["MetadataProfiler"],
            "script": "pipeline/video/generator_preprocessor/frame_extractor.py",
            "accepted_inputs": ["directory"],
            "produces": "directory"
        },
        "FaceDetector": {
            "domain": "ram",
            "execution_mode": "chunked",
            "depends_on": ["FrameExtractor"],
            "model_ref": "YunetFaceDetector_2023",
            "script": "pipeline/video/generator_preprocessor/face_detector.py",
            "accepted_inputs": ["directory"],
            "produces": ".json"
        },
        "FacialLandmarker": {
            "domain": "ram",
            "execution_mode": "chunked",
            "depends_on": ["FaceDetector"],
            "model_ref": "GoogleFacialLandmarker",
            "script": "pipeline/video/generator_preprocessor/landmarker.py",
            "accepted_inputs": [".json", "directory"],
            "produces": ".json"
        },
        "MouthIsolator": {
            "domain": "cpu",
            "execution_mode": "chunked",
            "depends_on": ["FacialLandmarker"],
            "script": "pipeline/video/generator_preprocessor/mouth_isolator.py",
            "accepted_inputs": [".json"],
            "produces": "directory"
        },
        "AudioChunker": {
            "domain": "cpu",
            "execution_mode": "sequential",
            "depends_on": ["MetadataProfiler"],
            "script": "pipeline/audio/audio_chunker.py",
            "accepted_inputs": ["directory"],
            "produces": "directory"
        },
        "WhisperTranscriber": {
            "domain": "ram",
            "execution_mode": "chunked",
            "depends_on": ["AudioChunker"],
            "model_ref": "WhisperSmallDaemon",
            "script": "pipeline/audio/whisper_transcriber.py",
            "accepted_inputs": ["directory"],
            "produces": ".json"
        },
        "TranscriptAggregator": {
            "domain": "cpu",
            "execution_mode": "sequential",
            "depends_on": ["WhisperTranscriber"],
            "script": "pipeline/audio/transcript_aggregator.py",
            "accepted_inputs": ["directory"],
            "produces": ".json"
        }
    }

    with open(os.path.join(pipeline_dir, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=4)
    print("✅ pipeline/manifest.json updated with Joined Multi-Branch Graph.")

    print("\n=========================================================")
    print("🎉 EcoFlow Environment Setup Complete!")
    print("All models and hermetic binaries cached in local environment.")
    print("=========================================================")

if __name__ == "__main__":
    setup_model_garden()