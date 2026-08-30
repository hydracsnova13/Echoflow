import sys
import os
import json
import re
from pathlib import Path
from collections import Counter

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"
DICT_FILE = CONFIG_DIR / "domain_dictionary.json"

ASR_CORRECTIONS = {}
ASR_STEM_PATTERNS = []

if DICT_FILE.exists():
    try:
        with open(DICT_FILE, "r", encoding="utf-8") as f:
            d_data = json.load(f)
            ASR_CORRECTIONS = d_data.get("asr_corrections", {})
            stem_list = d_data.get("asr_stem_patterns", [])
            ASR_STEM_PATTERNS = [
                (re.compile(item["pattern"], re.IGNORECASE | re.UNICODE), item["replacement"])
                for item in stem_list if "pattern" in item and "replacement" in item
            ]
    except Exception as e:
        print(f"⚠️ [TranscriptAggregator] Dict load warning: {e}", flush=True)

def is_valid_speech(text: str) -> bool:
    if not text or not text.strip():
        return False
    if any(ord(c) == 0xFFFD or (ord(c) < 32 and c not in '\n\r\t') for c in text):
        return False
    if re.search(r'([^\s\d\w])\s*(?:\1\s*){3,}', text):
        return False
    if re.search(r'(.)\1{4,}', text):
        return False
    letters_only = re.findall(r'[\u0904-\u0939\u0958-\u0961a-zA-Z]', text)
    return len(letters_only) >= 2

POSTPOSITIONS = [
    "मध्ये", "साठी", "पासून", "पर्यंत", "पेक्षा", "समोर", "मागे", "पुढे", 
    "खाली", "वर", "कडे", "द्वारे", "मुळे", "प्रमाणे", "नुसार", "बद्दल",
    "च्या", "चे", "ची", "चा", "ना", "ने", "नी", "स", "ला"
]

DIALECT_PARTICLES = [
    (r'(?<![\u0900-\u097F])अनी(?![\u0900-\u097F])', 'आणि'),
    (r'(?<![\u0900-\u097F])आनिन(?![\u0900-\u097F])', 'आणि'),
    (r'(?<![\u0900-\u097F])आन्नी(?![\u0900-\u097F])', 'आणि'),
    (r'(?<![\u0900-\u097F])मदे(?![\u0900-\u097F])', 'मध्ये'),
    (r'(?<![\u0900-\u097F])मादे(?![\u0900-\u097F])', 'मध्ये'),
    (r'(?<![\u0900-\u097F])साति(?![\u0900-\u097F])', 'साठी'),
    (r'(?<![\u0900-\u097F])सती(?![\u0900-\u097F])', 'साठी'),
    (r'(?<![\u0900-\u097F])साटी(?![\u0900-\u097F])', 'साठी'),
    (r'(?<![\u0900-\u097F])मुले(?![\u0900-\u097F])', 'मुळे'),
    (r'(?<![\u0900-\u097F])मनुन(?![\u0900-\u097F])', 'म्हणून'),
    (r'(?<![\u0900-\u097F])अता(?![\u0900-\u097F])', 'आता'),
    (r'(?<![\u0900-\u097F])आहेद(?![\u0900-\u097F])', 'आहेत')
]

def clean_transcript_text(text: str) -> str:
    if not text:
        return ""
    cleaned = text
    cleaned = re.sub(r'[^\u0900-\u097Fa-zA-Z0-9\s.,!?:;\-\'\"।॥()/%]', ' ', cleaned)
    
    cleaned = re.sub(r'(?<![\u0900-\u097F])(\w{2,})(?:\s+\1(?![\\u0900-\\u097F])){2,}', r'\1', cleaned, flags=re.UNICODE)
    cleaned = re.sub(r'(.{3,}?)\1{3,}', r'\1', cleaned, flags=re.UNICODE)

    for pattern, replacement in ASR_STEM_PATTERNS:
        cleaned = pattern.sub(replacement, cleaned)

    sorted_corrections = sorted(ASR_CORRECTIONS.items(), key=lambda x: len(x[0]), reverse=True)
    for target, replacement in sorted_corrections:
        pattern = r"(?<![\u0900-\u097F])" + re.escape(target) + r"(?![\u0900-\u097F])"
        cleaned = re.sub(pattern, replacement, cleaned)
        
    for pat, repl in DIALECT_PARTICLES:
        cleaned = re.sub(pat, repl, cleaned)

    pp_pattern = r'(\b[\u0900-\u097F]{2,})\s+(' + '|'.join(POSTPOSITIONS) + r')(?=[\s.,!?।॥]|$)'
    cleaned = re.sub(pp_pattern, r'\1\2', cleaned)
    cleaned = re.sub(pp_pattern, r'\1\2', cleaned)
    
    cleaned = re.sub(r'\s+([.,!?।॥])', r'\1', cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()

def merge_short_segments(segments: list) -> list:
    if not segments:
        return []

    merged = []
    for seg in segments:
        if not merged:
            merged.append(dict(seg))
            continue

        prev = merged[-1]
        pause = seg["start"] - prev["end"]
        prev_dur = prev["end"] - prev["start"]
        curr_dur = seg["end"] - seg["start"]
        combined_dur = seg["end"] - prev["start"]
        prev_ends_sentence = bool(re.search(r'[.!?।॥]\s*$', prev["text"]))

        can_merge = (
            prev["speaker_id"] == seg["speaker_id"]
            and 0.0 <= pause <= 0.85
            and combined_dur <= 9.5
            and (not prev_ends_sentence or prev_dur < 2.5 or curr_dur < 2.5 or len(prev["text"].split()) <= 4 or len(seg["text"].split()) <= 4)
        )

        if can_merge:
            prev["end"] = seg["end"]
            prev["text"] = f"{prev['text']} {seg['text']}".strip()
        else:
            merged.append(dict(seg))

    return merged

def resolve_speaker(seg_start: float, seg_end: float, diarization_map: list) -> str:
    if not diarization_map:
        return "SPEAKER_00"

    speaker_durations = {}
    for turn in diarization_map:
        overlap_start = max(seg_start, turn["start"])
        overlap_end = min(seg_end, turn["end"])
        overlap = max(0.0, overlap_end - overlap_start)
        if overlap > 0:
            spk = turn["speaker"]
            speaker_durations[spk] = speaker_durations.get(spk, 0.0) + overlap

    return max(speaker_durations, key=speaker_durations.get) if speaker_durations else "SPEAKER_00"

def aggregate_transcripts(input_path: str, output_dir: str):
    curr = os.path.abspath(input_path)
    job_root = None
    while curr and os.path.dirname(curr) != curr:
        if os.path.basename(curr).startswith("JOB-"):
            job_root = curr
            break
        curr = os.path.dirname(curr)

    if not job_root:
        job_root = input_path

    audio_chunker_dir = os.path.join(job_root, "out_AudioChunker")
    diarizer_map_file = os.path.join(job_root, "out_SpeakerDiarizer", "diarization_map.json")

    diarization_map = []
    if os.path.exists(diarizer_map_file):
        try:
            with open(diarizer_map_file, "r", encoding="utf-8") as f:
                diarization_map = json.load(f)
            print("✅ [TranscriptAggregator] Speaker diarization map loaded.", flush=True)
        except Exception as e:
            print(f"⚠️ [TranscriptAggregator] Failed to load diarization map: {str(e)}", flush=True)

    if not os.path.exists(audio_chunker_dir):
        print(f"❌ [TranscriptAggregator] Cannot find chunk directory at {audio_chunker_dir}", flush=True)
        sys.exit(1)

    buckets = sorted([
        os.path.join(audio_chunker_dir, d) for d in os.listdir(audio_chunker_dir)
        if d.startswith("bucket_") and os.path.isdir(os.path.join(audio_chunker_dir, d))
    ])

    configured_source_lang = None
    config_path = os.path.join(job_root, "job_config.json")
    if os.path.exists(config_path):
        try:
            with open(config_path, "r", encoding="utf-8") as cfg:
                configured_source_lang = json.load(cfg).get("source_language")
        except Exception:
            pass

    master_timeline = []
    full_text_parts = []
    language_counts = Counter()

    for b_dir in buckets:
        t_file = os.path.join(b_dir, "transcript.json")
        if os.path.exists(t_file):
            try:
                with open(t_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    b_lang = data.get("language")
                    segments = data.get("segments", []) or data.get("chunks", [])
                    if segments and b_lang:
                        language_counts[b_lang] += 1

                    for seg in segments:
                        raw_text = seg.get("text", "").strip()
                        if raw_text and is_valid_speech(raw_text):
                            cleaned_t = clean_transcript_text(raw_text)
                            if not cleaned_t:
                                continue
                            start_time = float(seg.get("start", 0.0))
                            end_time = float(seg.get("end", 0.0))
                            
                            speaker_id = resolve_speaker(start_time, end_time, diarization_map)

                            master_timeline.append({
                                "start": start_time,
                                "end": end_time,
                                "speaker_id": speaker_id,
                                "text": cleaned_t
                            })
                            full_text_parts.append(cleaned_t)
            except Exception as e:
                print(f"⚠️ [TranscriptAggregator] Error parsing {t_file}: {str(e)}", flush=True)

    master_timeline.sort(key=lambda x: x["start"])

    deduped_timeline = []
    for seg in master_timeline:
        if not deduped_timeline:
            deduped_timeline.append(seg)
        else:
            prev = deduped_timeline[-1]
            if abs(seg["start"] - prev["start"]) < 0.8 or (seg["start"] < prev["end"] and seg["text"].strip().lower() == prev["text"].strip().lower()):
                if len(seg["text"]) > len(prev["text"]):
                    deduped_timeline[-1] = seg
                continue
            deduped_timeline.append(seg)

    cohesive_timeline = merge_short_segments(deduped_timeline)
    full_text_parts = [s["text"] for s in cohesive_timeline]

    if configured_source_lang:
        final_language = configured_source_lang
    elif language_counts:
        final_language = language_counts.most_common(1)[0][0]
    else:
        final_language = "mr"

    print(f"🔍 [TranscriptAggregator] Final Aggregated Source Language: {final_language}", flush=True)
    print(f"📊 [TranscriptAggregator] Canonicalized {len(master_timeline)} raw segments -> {len(cohesive_timeline)} cohesive segments", flush=True)

    payload = {
        "job_id": os.path.basename(job_root),
        "language": final_language,
        "full_transcript": " ".join(full_text_parts),
        "timeline": cohesive_timeline
    }

    os.makedirs(output_dir, exist_ok=True)
    out_json = os.path.join(output_dir, "master_transcript.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"✅ [TranscriptAggregator] Master transcript saved to {out_json}", flush=True)

if __name__ == "__main__":
    if len(sys.argv) >= 3:
        aggregate_transcripts(sys.argv[1], sys.argv[2])