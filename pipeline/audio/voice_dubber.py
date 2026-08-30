import os
import sys
import shutil
import re
import warnings
import logging
import json

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
import soundfile as sf
import numpy as np
from pathlib import Path
from pydub import AudioSegment

num_cores = min(4, os.cpu_count() or 4)
os.environ["OMP_NUM_THREADS"] = str(num_cores)
os.environ["OPENBLAS_NUM_THREADS"] = str(num_cores)
os.environ["MKL_NUM_THREADS"] = str(num_cores)
os.environ["CUDA_VISIBLE_DEVICES"] = ""
import torch
torch.set_num_threads(num_cores)
torch.cuda.is_available = lambda: False

os.environ["HF_HUB_OFFLINE"] = "1"
os.environ["TRANSFORMERS_OFFLINE"] = "1"
os.environ["PYTHONWARNINGS"] = "ignore" 

warnings.filterwarnings("ignore")
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

logging.getLogger("transformers").setLevel(logging.ERROR)
logging.getLogger("jieba").setLevel(logging.ERROR)

from transformers import VitsModel, AutoTokenizer
import wavmark

class DummyWatermark:
    def to(self, *args, **kwargs):
        return self
wavmark.load_model = lambda *args, **kwargs: DummyWatermark()

from openvoice import se_extractor
from openvoice.api import ToneColorConverter

se_extractor.device = 'cpu'

def bypass_split_audio(audio_path, *args, **kwargs):
    target_dir = kwargs.get('target_dir', 'processed')
    if len(args) > 0: 
        target_dir = args[0]
    audio_name = kwargs.get('audio_name', os.path.basename(audio_path).rsplit('.', 1)[0])
    wavs_folder = os.path.join(target_dir, audio_name, 'wavs')
    os.makedirs(wavs_folder, exist_ok=True)
    shutil.copy(audio_path, os.path.join(wavs_folder, f"{audio_name}_seg0.wav"))
    return wavs_folder

se_extractor.split_audio_vad = bypass_split_audio
se_extractor.split_audio_whisper = bypass_split_audio

num_cores = os.cpu_count() or 4
torch.set_num_threads(num_cores)
torch.set_num_interop_threads(num_cores)

def load_mms_language(project_root, lang_code):
    lang_clean = str(lang_code).lower().strip()
    path_map = {
        "mr": "models/offline_mms_model/mar",
        "mar": "models/offline_mms_model/mar",
        "hi": "models/offline_mms_model/hin",
        "hin": "models/offline_mms_model/hin",
        "en": "models/offline_mms_model/eng",
        "eng": "models/offline_mms_model/eng"
    }
    rel_path = path_map.get(lang_clean, f"models/offline_mms_model/{lang_clean}")
    model_path = os.path.join(project_root, rel_path)
    
    if not os.path.exists(model_path):
        mms_root = os.path.join(project_root, "models", "offline_mms_model")
        if os.path.exists(mms_root):
            for folder in os.listdir(mms_root):
                if folder.startswith(lang_clean[:2]) or lang_clean[:2] in folder:
                    model_path = os.path.join(mms_root, folder)
                    break

    if not os.path.exists(model_path):
        print(f"❌ Error: Language '{lang_code}' MMS model not found at '{model_path}'", flush=True)
        sys.exit(1)
        
    print(f"📦 Loading MMS Base TTS from: {os.path.basename(model_path)}...", flush=True)
    tokenizer = AutoTokenizer.from_pretrained(model_path, local_files_only=True)
    model = VitsModel.from_pretrained(model_path, local_files_only=True)
    return tokenizer, model

import subprocess
import tempfile

def stretch_audio(audio_segment: AudioSegment, target_duration_ms: int, max_allowed_ms: int = None, max_speed: float = 1.45) -> AudioSegment:
    current_duration = len(audio_segment)
    if current_duration <= 0 or target_duration_ms <= 0:
        return audio_segment

    if max_allowed_ms is None:
        max_allowed_ms = target_duration_ms

    if current_duration <= target_duration_ms:
        return audio_segment

    speed_ratio = current_duration / float(target_duration_ms)
    bounded_speed = max(0.85, min(max_speed, speed_ratio))

    ffmpeg_bin = AudioSegment.converter or "ffmpeg"
    try:
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as in_f, \
             tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as out_f:
            in_path = in_f.name
            out_path = out_f.name

        audio_segment.export(in_path, format="wav")
        cmd = [
            ffmpeg_bin, "-y", "-i", in_path,
            "-filter:a", f"atempo={bounded_speed:.3f}",
            out_path
        ]
        res = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if res.returncode == 0 and os.path.exists(out_path):
            stretched = AudioSegment.from_wav(out_path)
            try:
                os.remove(in_path)
                os.remove(out_path)
            except Exception:
                pass
            if len(stretched) > max_allowed_ms:
                return stretched[:max_allowed_ms]
            return stretched
        try:
            os.remove(in_path)
            os.remove(out_path)
        except Exception:
            pass
    except Exception:
        pass

    try:
        stretched = audio_segment.speedup(playback_speed=bounded_speed, chunk_size=100, crossfade=50)
        if len(stretched) > max_allowed_ms:
            return stretched[:max_allowed_ms]
        return stretched
    except Exception:
        if len(audio_segment) > max_allowed_ms:
            return audio_segment[:max_allowed_ms]
        return audio_segment

def sanitize_text_for_tts(text: str) -> str:
    cleaned = text
    cleaned = re.sub(r"\[.*?\]", "", cleaned)
    cleaned = re.sub(r"\((.*?)\)", r"\1", cleaned)
    cleaned = cleaned.replace("&", " and ")
    # Map Devanagari Danda (।) and Double Danda (॥) to standard period for VITS pause cadence
    cleaned = cleaned.replace("।", ".").replace("॥", ".")
    cleaned = re.sub(r"[^\w\s\.,!\?'\u0900-\u097F\u0600-\u06FF\u0B80-\u0BFF\u0C00-\u0C7F\u0D00-\u0D7F]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned

def run_fast_offline_dubber(input_target, output_dir):
    project_root = Path(__file__).resolve().parent.parent.parent
    bin_dir = project_root / "bin"
    if bin_dir.exists():
        os.environ["PATH"] += os.pathsep + str(bin_dir)
    AudioSegment.converter = str(bin_dir / ("ffmpeg.exe" if os.name == 'nt' else "ffmpeg"))

    if os.path.exists("processed"):
        try:
            shutil.rmtree("processed", ignore_errors=True)
        except:
            pass

    curr = os.path.abspath(input_target)
    job_root = None
    while curr and os.path.dirname(curr) != curr:
        if os.path.basename(curr).startswith("JOB-"):
            job_root = curr
            break
        curr = os.path.dirname(curr)

    if not job_root:
        job_root = input_target

    target_language = "hi"
    output_format = "wav"
    media_type = "video"
    dubbing_voice_cloning = "yes"
    dubbing_speed_mode = "adaptive"
    config_path = os.path.join(job_root, "job_config.json")
    if os.path.exists(config_path):
        with open(config_path, "r") as cfg:
            c = json.load(cfg)
            target_language = c.get("target_language", "hi")
            output_format = c.get("output_format", "wav").lower()
            media_type = c.get("media_type", "video").lower()
            dubbing_voice_cloning = c.get("dubbing_voice_cloning", "yes").lower()
            dubbing_speed_mode = c.get("dubbing_speed_mode", "adaptive").lower()

    text_formats = ["srt", "txt", "json", "text"]
    if output_format in text_formats or (media_type == "text" and output_format in text_formats):
        print(f"⏩ [VoiceDubber] Bypassing Voice Dubbing for '{output_format}' output mode.", flush=True)
        os.makedirs(output_dir, exist_ok=True)
        sys.exit(0)

    diarizer_dir = os.path.join(job_root, "out_SpeakerDiarizer")
    srt_path = os.path.join(job_root, "out_NMTTranslator", "subtitles.srt")

    if not os.path.exists(srt_path):
        if os.path.isfile(input_target) and input_target.endswith(".srt"):
            srt_path = input_target
        else:
            print(f"❌ [VoiceDubber] Missing subtitles file at {srt_path}", flush=True)
            sys.exit(1)

    speaker_files = {}
    if os.path.exists(diarizer_dir):
        for f in os.listdir(diarizer_dir):
            if f.endswith("_ref.wav"):
                spk_name = f.replace("_ref.wav", "")
                speaker_files[spk_name] = os.path.join(diarizer_dir, f)

    if not speaker_files:
        fallback_chunk = os.path.join(job_root, "out_AudioChunker", "bucket_000_030", "chunk_audio.wav")
        if os.path.exists(fallback_chunk):
            speaker_files["SPEAKER_00"] = fallback_chunk

    if dubbing_voice_cloning == "no":
        print("⏩ [VoiceDubber] Voice Cloning disabled in config. Using pure Studio Base Voice (MMS).", flush=True)
        has_references = False
    else:
        has_references = len(speaker_files) > 0

    if not has_references and media_type != "text" and dubbing_voice_cloning != "no":
        print("❌ [VoiceDubber] No speaker reference audio files found.", flush=True)
        sys.exit(1)

    print("🚀 Initializing Stage 1: MMS-TTS (Fast Base Speech)...", flush=True)
    tokenizer, mms_model = load_mms_language(str(project_root), target_language)

    tone_color_converter = None
    if has_references:
        print("🚀 Initializing Stage 2: OpenVoice v2 (Forced Pure CPU Mode)...", flush=True)
        OPENVOICE_PATH = os.path.join(project_root, "models", "offline_openvoice", "converter")
        converter_config = os.path.join(OPENVOICE_PATH, "config.json")
        converter_ckpt = os.path.join(OPENVOICE_PATH, "checkpoint.pth")
        
        if not os.path.exists(converter_config):
            print(f"❌ Error: OpenVoice files missing in '{OPENVOICE_PATH}'", flush=True)
            sys.exit(1)
            
        tone_color_converter = ToneColorConverter(converter_config, device='cpu')
        tone_color_converter.watermark_model = None
        tone_color_converter.load_ckpt(converter_ckpt)
        print(f"✅ Both models loaded successfully on CPU using {num_cores} threads!", flush=True)
    else:
        print("⏩ Bypassing OpenVoice Stage 2 (No reference audio available for pure text input)...", flush=True)

    import pysrt
    subs = pysrt.open(srt_path)
    
    total_length_ms = (subs[-1].end.ordinal) + 2000 if len(subs) > 0 else 10000
    original_audio_path = os.path.join(job_root, "out_MetadataProfiler", "audio.wav")
    if os.path.exists(original_audio_path):
        try:
            orig_seg = AudioSegment.from_wav(original_audio_path)
            total_length_ms = max(total_length_ms, len(orig_seg))
        except Exception:
            pass

    master_timeline = AudioSegment.silent(duration=total_length_ms)
    default_speaker = list(speaker_files.keys())[0] if has_references else "SPEAKER_00"
    
    os.makedirs(output_dir, exist_ok=True)
    
    ov_temp_dir = os.path.join(output_dir, "ov_temp")
    os.makedirs(ov_temp_dir, exist_ok=True)

    print("🔗 [VoiceDubber] Consolidating timeline for optimal dubbing...", flush=True)
    consolidated = []
    for sub in subs:
        raw_text = sub.text.replace('\n', ' ')
        match = re.match(r"\[(.*?)\]\s*(.*)", raw_text)
        if match:
            speaker = match.group(1)
            text = match.group(2).strip()
        else:
            speaker = default_speaker
            text = raw_text.strip()

        cleaned_text = sanitize_text_for_tts(text)

        if not cleaned_text:
            if consolidated:
                consolidated[-1]["end_ms"] = sub.end.ordinal
            continue

        if consolidated and consolidated[-1]["speaker"] == speaker:
            prev_entry = consolidated[-1]
            gap_ms = sub.start.ordinal - prev_entry["end_ms"]
            total_dur_ms = sub.end.ordinal - prev_entry["start_ms"]
            if 0 <= gap_ms <= 350 and total_dur_ms <= 8500:
                prev_entry["end_ms"] = sub.end.ordinal
                prev_entry["text"] = f"{prev_entry['text']} {cleaned_text}".strip()
                continue

        consolidated.append({
            "speaker": speaker,
            "text": cleaned_text,
            "start_ms": sub.start.ordinal,
            "end_ms": sub.end.ordinal,
        })

    print(f"   📊 Consolidated {len(subs)} subtitle entries → {len(consolidated)} voiced segments", flush=True)

    print("🎤 [VoiceDubber] Starting voice processing...", flush=True)
    
    target_se_cache = {}
    source_se_cache = {}

    for i, entry in enumerate(consolidated):
        current_speaker = entry["speaker"]
        spoken_text = entry["text"]
        start_time_ms = entry["start_ms"]
        
        raw_target_duration_ms = entry["end_ms"] - entry["start_ms"]

        # 🛡️ THE FIX: Relaxed Dynamic Pacing Limiter
        # Humans speak ~2.5 words per second max when rushed, but ~1.5 to 2 normally. 
        # We increase the multiplier to 500ms + 2.0s padding so valid speech is never chopped off.
        word_count = len(spoken_text.split())
        estimated_max_speech_ms = (word_count * 500) + 2000 
        target_duration_ms = min(raw_target_duration_ms, estimated_max_speech_ms)

        ref_wav = speaker_files.get(current_speaker, speaker_files.get(default_speaker)) if has_references else None
        if has_references and (not ref_wav or not os.path.exists(ref_wav)):
            ref_wav = list(speaker_files.values())[0] if speaker_files else None

        print(f"   [{i+1}/{len(consolidated)}] Voice: [{current_speaker}] | Allowed Duration: {target_duration_ms}ms | Text: '{spoken_text[:45]}...'", flush=True)
        
        base_audio_path = os.path.join(output_dir, f"temp_base_{current_speaker}_{i}.wav")
        chunk_output = os.path.join(output_dir, f"final_chunk_{current_speaker}_{i}.wav")
        
        try:
            inputs = tokenizer(spoken_text, return_tensors="pt")
            with torch.no_grad():
                output = mms_model(**inputs).waveform
            sf.write(base_audio_path, output.squeeze().numpy(), mms_model.config.sampling_rate)
            
            generated_segment = None
            if has_references and ref_wav and os.path.exists(ref_wav):
                try:
                    if target_language not in source_se_cache:
                        source_se, _ = se_extractor.get_se(base_audio_path, tone_color_converter, target_dir=ov_temp_dir, vad=True)
                        source_se_cache[target_language] = source_se
                    else:
                        source_se = source_se_cache[target_language]

                    if current_speaker not in target_se_cache:
                        print(f"      🔍 Extracting Base Tone Embedding for {current_speaker}...", flush=True)
                        target_se, _ = se_extractor.get_se(ref_wav, tone_color_converter, target_dir=ov_temp_dir, vad=True)
                        target_se_cache[current_speaker] = target_se
                    
                    target_se = target_se_cache[current_speaker]
                    
                    tone_color_converter.convert(
                        audio_src_path=base_audio_path,
                        src_se=source_se,
                        tgt_se=target_se,
                        output_path=chunk_output
                    )
                    if os.path.exists(chunk_output):
                        generated_segment = AudioSegment.from_wav(chunk_output)
                except Exception as ov_err:
                    print(f"   ⚠️ OpenVoice conversion warning for {current_speaker}: {ov_err}. Using base TTS audio.", flush=True)

            if generated_segment is None and os.path.exists(base_audio_path):
                generated_segment = AudioSegment.from_wav(base_audio_path)

            if generated_segment is not None and len(generated_segment) > 0:
                if i + 1 < len(consolidated):
                    max_allowed_ms = max(target_duration_ms, consolidated[i+1]["start_ms"] - start_time_ms - 50)
                else:
                    max_allowed_ms = target_duration_ms + 3000

                max_speed = 1.15 if dubbing_speed_mode == "natural" else 1.45
                synced_segment = stretch_audio(generated_segment, target_duration_ms, max_allowed_ms, max_speed=max_speed)
                if synced_segment.max_dBFS > -100:
                    change_in_dBFS = -3.0 - synced_segment.max_dBFS
                    synced_segment = synced_segment.apply_gain(change_in_dBFS)

                master_timeline = master_timeline.overlay(synced_segment, position=start_time_ms)
            else:
                print(f"⚠️ Warning: Could not generate audio clip for segment {i+1}", flush=True)
            
        except Exception as e:
            import traceback
            print(f"❌ Error during generation for {current_speaker}: {e}", flush=True)
            traceback.print_exc()
            
        finally:
            if os.path.exists(base_audio_path):
                os.remove(base_audio_path)
            if os.path.exists(chunk_output):
                os.remove(chunk_output)

    try:
        shutil.rmtree(ov_temp_dir, ignore_errors=True)
    except:
        pass

    final_output = os.path.join(output_dir, "master_dubbed.wav")
    if os.path.exists(final_output):
        try:
            os.remove(final_output)
        except Exception:
            pass
    print(f"💾 Exporting timeline ({len(master_timeline)} ms)...", flush=True)
    out_file = master_timeline.export(final_output, format="wav")
    if hasattr(out_file, 'close'):
        out_file.close()
    print(f"🎉 Pipeline Complete! Saved to: {final_output} (Size: {os.path.getsize(final_output)} bytes)", flush=True)

if __name__ == "__main__":
    if len(sys.argv) >= 3:
        run_fast_offline_dubber(sys.argv[1], sys.argv[2])
    else:
        print("❌ [VoiceDubber] Missing arguments: input_target output_dir", flush=True)
        sys.exit(1)