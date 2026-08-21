import sys
import os
import json

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

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

    if speaker_durations:
        return max(speaker_durations, key=speaker_durations.get)
    return "SPEAKER_00"

def aggregate_transcripts(input_path: str, output_dir: str):
    curr = os.path.abspath(input_path)
    job_root = None
    while curr and os.path.dirname(curr) != curr:
        if os.path.basename(curr).startswith("JOB-"):
            job_root = curr
            break
        curr = os.path.dirname(curr)

    if not job_root:
        job_root = input_target

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

    from collections import Counter

    # Check job_config.json for explicit source_language
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
                        if raw_text:
                            start_time = float(seg.get("start", 0.0))
                            end_time = float(seg.get("end", 0.0))
                            
                            speaker_id = resolve_speaker(start_time, end_time, diarization_map)

                            # 🛡️ THE FIX: Removed the embedded tag so NMT gets pure text
                            master_timeline.append({
                                "start": start_time,
                                "end": end_time,
                                "speaker_id": speaker_id,
                                "text": raw_text
                            })
                            full_text_parts.append(raw_text)
            except Exception as e:
                print(f"⚠️ [TranscriptAggregator] Error parsing {t_file}: {str(e)}", flush=True)

    master_timeline.sort(key=lambda x: x["start"])

    # Deduplicate overlapping segments from chunk boundary overlaps
    deduped_timeline = []
    for seg in master_timeline:
        if not deduped_timeline:
            deduped_timeline.append(seg)
        else:
            prev = deduped_timeline[-1]
            # Check for timestamp collision or identical text overlap
            if abs(seg["start"] - prev["start"]) < 0.8 or (seg["start"] < prev["end"] and seg["text"].strip().lower() == prev["text"].strip().lower()):
                if len(seg["text"]) > len(prev["text"]):
                    deduped_timeline[-1] = seg # Keep longer/more complete segment
                continue
            deduped_timeline.append(seg)

    full_text_parts = [s["text"] for s in deduped_timeline]

    if configured_source_lang:
        final_language = configured_source_lang
    elif language_counts:
        final_language = language_counts.most_common(1)[0][0]
    else:
        final_language = "mr"

    print(f"🔍 [TranscriptAggregator] Final Aggregated Source Language: {final_language}", flush=True)

    payload = {
        "job_id": os.path.basename(job_root),
        "language": final_language,
        "full_transcript": " ".join(full_text_parts),
        "timeline": deduped_timeline
    }

    os.makedirs(output_dir, exist_ok=True)
    out_json = os.path.join(output_dir, "master_transcript.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"✅ [TranscriptAggregator] Master transcript saved to {out_json}", flush=True)

if __name__ == "__main__":
    if len(sys.argv) >= 3:
        aggregate_transcripts(sys.argv[1], sys.argv[2])