import sys
import os
import json
import warnings
import torch
import scipy.io.wavfile
import numpy as np
from pathlib import Path
from pydub import AudioSegment

warnings.filterwarnings("ignore")
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"

num_cores = min(4, os.cpu_count() or 4)
torch.set_num_threads(num_cores)
torch.set_num_interop_threads(num_cores)

# 🛡️ THE FIX 2: Torchaudio >= 2.1.0 removed legacy backend functions. 
import torchaudio
if not hasattr(torchaudio, 'set_audio_backend'):
    torchaudio.set_audio_backend = lambda *args, **kwargs: None
if not hasattr(torchaudio, 'get_audio_backend'):
    torchaudio.get_audio_backend = lambda: "soundfile"

# 🛡️ THE FIX 3: HuggingFace Hub > 0.22 deleted 'use_auth_token'. 
from pyannote.audio.core.model import Model
_orig_model_from_pretrained = Model.from_pretrained

@classmethod
def _patched_model_from_pretrained(cls, *args, **kwargs):
    kwargs.pop("use_auth_token", None)
    return _orig_model_from_pretrained.__func__(cls, *args, **kwargs)

Model.from_pretrained = _patched_model_from_pretrained

def run_diarization(input_target: str, output_dir: str):
    print("▶️ [SpeakerDiarizer] Starting Speaker Diarization Engine...", flush=True)

    project_root = Path(__file__).resolve().parent.parent.parent
    config_path = os.path.join(project_root, "models", "offline_pyannote_model", "config.yaml")

    bin_dir = project_root / "bin"
    if bin_dir.exists():
        os.environ["PATH"] += os.pathsep + str(bin_dir)
    AudioSegment.converter = str(bin_dir / ("ffmpeg.exe" if os.name == 'nt' else "ffmpeg"))

    source_audio = input_target
    if os.path.isdir(input_target):
        curr = os.path.abspath(input_target)
        job_root = None
        while curr and os.path.dirname(curr) != curr:
            if os.path.basename(curr).startswith("JOB-"):
                job_root = curr
                break
            curr = os.path.dirname(curr)

        if job_root:
            out_profiler = os.path.join(job_root, "out_MetadataProfiler")
            if os.path.exists(out_profiler):
                for f in os.listdir(out_profiler):
                    if f.endswith((".wav", ".mp3", ".mp4", ".flac")):
                        source_audio = os.path.join(out_profiler, f)
                        break

    if not os.path.exists(source_audio) or os.path.isdir(source_audio):
        print(f"❌ [SpeakerDiarizer] Unable to locate valid source audio file.", flush=True)
        sys.exit(1)

    # Read speaker limits from job_config.json
    min_speakers = None
    max_speakers = None
    num_spk_val = None
    job_cfg_path = os.path.join(job_root, "job_config.json") if 'job_root' in locals() and job_root else None
    
    if job_cfg_path and os.path.exists(job_cfg_path):
        with open(job_cfg_path, "r") as cfg:
            c = json.load(cfg)
            num_spk_val = str(c.get("num_speakers", "")).strip().lower()
            if num_spk_val and num_spk_val != "auto" and num_spk_val.isdigit():
                min_speakers = int(num_spk_val)
                max_speakers = int(num_spk_val)
            else:
                min_raw = str(c.get("min_speakers", "")).strip()
                max_raw = str(c.get("max_speakers", "")).strip()
                if min_raw and min_raw.isdigit(): min_speakers = int(min_raw)
                if max_raw and max_raw.isdigit(): max_speakers = int(max_raw)

    full_audio = AudioSegment.from_wav(source_audio)
    total_duration_sec = len(full_audio) / 1000.0

    # 🚀 OPTIMIZATION 1: SINGLE-SPEAKER FAST BYPASS (< 1 second execution time)
    if min_speakers == 1 and max_speakers == 1:
        print("⚡ [SpeakerDiarizer] 1 Speaker configured. Executing Fast Single-Speaker Bypass (< 1s)...", flush=True)
        os.makedirs(output_dir, exist_ok=True)
        
        diarization_map = [{
            "start": 0.0,
            "end": total_duration_sec,
            "speaker": "SPEAKER_00"
        }]

        ref_duration_ms = min(len(full_audio), 10000)
        speaker_slice = full_audio[0:ref_duration_ms]
        speaker_slice = speaker_slice.set_frame_rate(16000).set_channels(1).set_sample_width(2)
        
        output_name = os.path.join(output_dir, "SPEAKER_00_ref.wav")
        speaker_slice.export(output_name, format="wav")
        print(f"   ✅ Saved reference file: {os.path.basename(output_name)}", flush=True)

        map_file = os.path.join(output_dir, "diarization_map.json")
        with open(map_file, "w", encoding="utf-8") as f:
            json.dump(diarization_map, f, indent=2)

        print(f"✅ [SpeakerDiarizer] Fast-path Execution Complete!", flush=True)
        sys.exit(0)

    if not os.path.exists(config_path):
        print(f"❌ Error: Could not find the offline model folder at {config_path}", flush=True)
        sys.exit(1)

    print(f"🎧 [SpeakerDiarizer] Analyzing mixed audio: {os.path.basename(source_audio)} ({total_duration_sec:.1f}s)", flush=True)
    sample_rate, data = scipy.io.wavfile.read(source_audio)
    
    if len(data.shape) == 2:
        data = data.mean(axis=1) # Downmix stereo to mono
        
    waveform = torch.from_numpy(data).float()
    if data.dtype == np.int16:
        waveform = waveform / 32768.0
    waveform = waveform.unsqueeze(0) # (1, num_samples)

    # 🚀 OPTIMIZATION 2: Downsample to 16kHz mono if sample rate is higher
    if sample_rate != 16000:
        resampler = torchaudio.transforms.Resample(orig_freq=sample_rate, new_freq=16000)
        waveform = resampler(waveform)
        sample_rate = 16000
        
    audio_dict = {"waveform": waveform, "sample_rate": sample_rate}
    
    print("🧠 [SpeakerDiarizer] AI is mapping the voices (PyTorch Multithreaded CPU processing)...", flush=True)
    from pyannote.audio import Pipeline
    pipeline = Pipeline.from_pretrained(config_path)

    diarize_kwargs = {}
    if min_speakers: diarize_kwargs["min_speakers"] = min_speakers
    if max_speakers: diarize_kwargs["max_speakers"] = max_speakers

    if diarize_kwargs:
        print(f"   ⚙️ Applying custom limits: {diarize_kwargs}", flush=True)

    diarization = pipeline(audio_dict, **diarize_kwargs)
    
    if hasattr(diarization, "speaker_diarization"):
        diarization = diarization.speaker_diarization
    
    full_audio = AudioSegment.from_wav(source_audio)
    speaker_segments = {}
    diarization_map = []

    for turn, _, speaker in diarization.itertracks(yield_label=True):
        duration = (turn.end - turn.start) * 1000
        diarization_map.append({
            "start": turn.start,
            "end": turn.end,
            "speaker": speaker
        })

        if speaker not in speaker_segments or duration > speaker_segments[speaker]["duration"]:
            speaker_segments[speaker] = {"start": turn.start * 1000, "duration": duration}

    os.makedirs(output_dir, exist_ok=True)
    EXTRACTION_LENGTH_MS = 10000

    print("✂️ [SpeakerDiarizer] Slicing Reference Files...", flush=True)
    for speaker, data in speaker_segments.items():
        start_ms = data["start"]
        end_ms = start_ms + EXTRACTION_LENGTH_MS
        speaker_slice = full_audio[start_ms:end_ms]
        
        speaker_slice = speaker_slice.set_frame_rate(16000).set_channels(1).set_sample_width(2)
        
        output_name = os.path.join(output_dir, f"{speaker}_ref.wav")
        speaker_slice.export(output_name, format="wav")
        print(f"   ✅ Saved reference file: {os.path.basename(output_name)}", flush=True)

    map_file = os.path.join(output_dir, "diarization_map.json")
    with open(map_file, "w", encoding="utf-8") as f:
        json.dump(diarization_map, f, indent=2)

    print(f"✅ [SpeakerDiarizer] Execution Complete.", flush=True)

if __name__ == "__main__":
    if len(sys.argv) >= 3:
        run_diarization(sys.argv[1], sys.argv[2])
    else:
        print("❌ [SpeakerDiarizer] Missing arguments.", flush=True)
        sys.exit(1)