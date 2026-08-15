import os
import sys
import shutil
import re
import warnings
import logging
import json
import soundfile as sf
import numpy as np
from pathlib import Path
from pydub import AudioSegment

os.environ["CUDA_VISIBLE_DEVICES"] = ""
import torch
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
    path_map = {
        "mr": "models/offline_mms_model/mar",
        "hi": "models/offline_mms_model/hin",
        "en": "models/offline_mms_model/eng"
    }
    rel_path = path_map.get(lang_code, "models/offline_mms_model/hin")
    model_path = os.path.join(project_root, rel_path)
    if not os.path.exists(model_path):
        print(f"❌ Error: Could not find MMS model at '{model_path}'.", flush=True)
        sys.exit(1)
    print(f"📦 Loading MMS Base TTS from: {os.path.basename(model_path)}...", flush=True)
    tokenizer = AutoTokenizer.from_pretrained(model_path, local_files_only=True)
    model = VitsModel.from_pretrained(model_path, local_files_only=True)
    return tokenizer, model

def stretch_audio(audio_segment: AudioSegment, target_duration_ms: int) -> AudioSegment:
    current_duration = len(audio_segment)
    if current_duration <= target_duration_ms:
        silence = AudioSegment.silent(duration=(target_duration_ms - current_duration))
        return audio_segment + silence
    else:
        speed_ratio = current_duration / target_duration_ms
        stretched = audio_segment.speedup(playback_speed=speed_ratio, chunk_size=100, crossfade=50)
        return stretched[:target_duration_ms]

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
    config_path = os.path.join(job_root, "job_config.json")
    if os.path.exists(config_path):
        with open(config_path, "r") as cfg:
            c = json.load(cfg)
            target_language = c.get("target_language", "hi")
            output_format = c.get("output_format", "wav").lower()
            media_type = c.get("media_type", "video").lower()

    # 🛡️ THE FIX 1: Safely bypass if output is pure text
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

    has_references = len(speaker_files) > 0

    # 🛡️ THE FIX 2: Do not crash if input is text and we have no references. 
    # Fallback to pure base MMS generation instead!
    if not has_references and media_type != "text":
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
    master_timeline = AudioSegment.silent(duration=total_length_ms)
    default_speaker = list(speaker_files.keys())[0] if has_references else "SPEAKER_00"
    
    os.makedirs(output_dir, exist_ok=True)
    
    ov_temp_dir = os.path.join(output_dir, "ov_temp")
    os.makedirs(ov_temp_dir, exist_ok=True)

    print("🎤 [VoiceDubber] Starting voice processing...", flush=True)
    
    target_se_cache = {}

    for i, sub in enumerate(subs):
        raw_text = sub.text.replace('\n', ' ')
        start_time_ms = sub.start.ordinal
        target_duration_ms = sub.end.ordinal - sub.start.ordinal
        
        match = re.match(r"\[(.*?)\]\s*(.*)", raw_text)
        if match:
            current_speaker = match.group(1)
            spoken_text = match.group(2).strip()
        else:
            current_speaker = default_speaker
            spoken_text = raw_text.strip()
            
        if not spoken_text:
            continue

        ref_wav = speaker_files.get(current_speaker, speaker_files.get(default_speaker)) if has_references else None
        if has_references and (not ref_wav or not os.path.exists(ref_wav)):
            print(f"⚠️ Warning: Reference missing! Skipping line: '{spoken_text}'", flush=True)
            continue
            
        print(f"   [{i+1}/{len(subs)}] Voice: [{current_speaker}] | Text: '{spoken_text[:30]}...'", flush=True)
        
        base_audio_path = os.path.join(output_dir, f"temp_base_{current_speaker}_{i}.wav")
        chunk_output = os.path.join(output_dir, f"final_chunk_{current_speaker}_{i}.wav")
        
        try:
            inputs = tokenizer(spoken_text, return_tensors="pt")
            with torch.no_grad():
                output = mms_model(**inputs).waveform
            sf.write(base_audio_path, output.squeeze().numpy(), mms_model.config.sampling_rate)
            
            if has_references:
                source_se, _ = se_extractor.get_se(base_audio_path, tone_color_converter, target_dir=ov_temp_dir, vad=True)
                
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
                generated_segment = AudioSegment.from_wav(chunk_output)
            else:
                # 🛡️ THE FIX 3: Fallback directly to generic MMS output if no references
                generated_segment = AudioSegment.from_wav(base_audio_path)
                
            synced_segment = stretch_audio(generated_segment, target_duration_ms)
            master_timeline = master_timeline.overlay(synced_segment, position=start_time_ms)
            
        except Exception as e:
            import traceback
            print(f"❌ Error during conversion for {current_speaker}: {e}", flush=True)
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
    print(f"💾 Exporting timeline...", flush=True)
    master_timeline.export(final_output, format="wav")
    print(f"🎉 Pipeline Complete! Saved to: {final_output}", flush=True)

if __name__ == "__main__":
    if len(sys.argv) >= 3:
        run_fast_offline_dubber(sys.argv[1], sys.argv[2])
    else:
        print("❌ [VoiceDubber] Missing arguments: input_target output_dir", flush=True)
        sys.exit(1)