import sys
import os
import json
import re
import gc
import time
import glob
from pathlib import Path

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

num_cores = min(6, os.cpu_count() or 6)
os.environ["OMP_NUM_THREADS"] = str(num_cores)
os.environ["OPENBLAS_NUM_THREADS"] = str(num_cores)
os.environ["MKL_NUM_THREADS"] = str(num_cores)
os.environ["TRANSFORMERS_OFFLINE"] = "1"
os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
os.environ["TRANSFORMERS_VERBOSITY"] = "error"

import torch
torch.set_num_threads(num_cores)

from transformers import AutoModelForSeq2SeqLM, AutoTokenizer
try:
    from IndicTransToolkit import IndicProcessor
except ImportError:
    from IndicTransToolkit.IndicTransToolkit import IndicProcessor

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"
DICT_FILE = CONFIG_DIR / "domain_dictionary.json"

ASR_CORRECTIONS = {}
DOMAIN_TERMS = {}  # 🛡️ UPGRADE: Added global registry for domain terms
ASR_STEM_PATTERNS = []
ENGLISH_SPOKEN_SMOOTHING = []
HINDI_SPOKEN_SMOOTHING = []
MARATHI_SPOKEN_SMOOTHING = []

def load_domain_dictionary():
    global ASR_CORRECTIONS, DOMAIN_TERMS, ASR_STEM_PATTERNS, ENGLISH_SPOKEN_SMOOTHING, HINDI_SPOKEN_SMOOTHING, MARATHI_SPOKEN_SMOOTHING
    
    compiled_data = {
        "asr_corrections": {},
        "domain_terms": {},
        "asr_stem_patterns": [],
        "spoken_english_smoothing": [],
        "spoken_hindi_smoothing": [],
        "spoken_marathi_smoothing": []
    }

    if DICT_FILE.exists():
        try:
            with open(DICT_FILE, "r", encoding="utf-8") as f:
                base_data = json.load(f)
                for key in compiled_data.keys():
                    if key in base_data:
                        if isinstance(base_data[key], dict):
                            compiled_data[key].update(base_data[key])
                        elif isinstance(base_data[key], list):
                            compiled_data[key].extend(base_data[key])
        except Exception:
            pass

    shards_dir = CONFIG_DIR / "shards"
    parsed_shards = []
    
    if shards_dir.exists():
        for shard_path in glob.glob(str(shards_dir / "*.json")):
            try:
                with open(shard_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    timestamp = data.get("_meta", {}).get("last_updated", 0)
                    parsed_shards.append((timestamp, data))
            except Exception:
                continue
                
    parsed_shards.sort(key=lambda x: x[0])

    for _, shard_data in parsed_shards:
        if "asr_corrections" in shard_data and isinstance(shard_data["asr_corrections"], dict):
            compiled_data["asr_corrections"].update(shard_data["asr_corrections"])
        if "domain_terms" in shard_data and isinstance(shard_data["domain_terms"], dict):
            compiled_data["domain_terms"].update(shard_data["domain_terms"])
        for list_key in ["asr_stem_patterns", "spoken_english_smoothing", "spoken_hindi_smoothing", "spoken_marathi_smoothing"]:
            if list_key in shard_data and isinstance(shard_data[list_key], list):
                compiled_data[list_key].extend(shard_data[list_key])

    ASR_CORRECTIONS = compiled_data.get("asr_corrections", {})
    DOMAIN_TERMS = compiled_data.get("domain_terms", {}) # 🛡️ Load Domain Terms into active memory
    
    stem_list = compiled_data.get("asr_stem_patterns", [])
    for item in stem_list:
        if "pattern" in item and "replacement" in item:
            try:
                compiled_pat = re.compile(item["pattern"], re.IGNORECASE | re.UNICODE)
                ASR_STEM_PATTERNS.append((compiled_pat, item["replacement"]))
            except Exception: pass

    for i in compiled_data.get("spoken_english_smoothing", []):
        if "pattern" in i:
            try:
                pat = re.compile(i["pattern"], re.IGNORECASE | re.UNICODE)
                ENGLISH_SPOKEN_SMOOTHING.append((pat, i.get("replacement", "")))
            except Exception: pass
            
    for i in compiled_data.get("spoken_hindi_smoothing", []):
        if "pattern" in i:
            try:
                pat = re.compile(i["pattern"], re.IGNORECASE | re.UNICODE)
                HINDI_SPOKEN_SMOOTHING.append((pat, i.get("replacement", "")))
            except Exception: pass
            
    for i in compiled_data.get("spoken_marathi_smoothing", []):
        if "pattern" in i:
            try:
                pat = re.compile(i["pattern"], re.IGNORECASE | re.UNICODE)
                MARATHI_SPOKEN_SMOOTHING.append((pat, i.get("replacement", "")))
            except Exception: pass

load_domain_dictionary()

FOREIGN_SCRIPTS_PATTERN = re.compile(r"[\u0A00-\u0A7F\u0C00-\u0C7F\u0C80-\u0CFF\u0B80-\u0BFF\u0D00-\u0D7F\u0980-\u09FF\u0600-\u06FF\u0E00-\u0E7F]")

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
    (r'(?<![\u0900-\u097F])अता(?![\u0900-\u097F])', 'आता')
]

def sanitize_nmt_input(text: str, source_lang: str) -> str:
    if not text: return ""
    if source_lang and source_lang.lower() in ["mr", "hi"]:
        cleaned = FOREIGN_SCRIPTS_PATTERN.sub("", text)
        return re.sub(r"\s+", " ", cleaned).strip()
    return text.strip()

def clean_asr_text(text: str) -> str:
    if not text: return ""
    cleaned = text
    cleaned = re.sub(r'(\w{2,})(?:\s+\1){2,}', r'\1', cleaned, flags=re.UNICODE)
    cleaned = re.sub(r'(.{3,}?)\1{3,}', r'\1', cleaned, flags=re.UNICODE)
    for pattern_obj, replacement in ASR_STEM_PATTERNS:
        try:
            cleaned = pattern_obj.sub(replacement, cleaned)
        except Exception: pass
    sorted_corrections = sorted(ASR_CORRECTIONS.items(), key=lambda x: len(x[0]), reverse=True)
    for target, replacement in sorted_corrections:
        pattern = r"(?<![\u0900-\u097F])" + re.escape(target) + r"(?![\u0900-\u097F])"
        cleaned = re.sub(pattern, replacement, cleaned, flags=re.IGNORECASE)
    for pat, repl in DIALECT_PARTICLES:
        cleaned = re.sub(pat, repl, cleaned)
    pp_pattern = r'(\b[\u0900-\u097F]{2,})\s+(' + '|'.join(POSTPOSITIONS) + r')(?=[\s.,!?।॥]|$)'
    cleaned = re.sub(pp_pattern, r'\1\2', cleaned)
    cleaned = re.sub(pp_pattern, r'\1\2', cleaned)
    cleaned = re.sub(r'\s+([.,!?।॥])', r'\1', cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()

def smooth_natural_english(text: str) -> str:
    smoothed = text
    for pat_obj, replacement in ENGLISH_SPOKEN_SMOOTHING:
        try:
            repl_str = r'<dict_mod original="\g<0>">' + replacement.replace('\\', r'\\') + r'</dict_mod>'
            smoothed = pat_obj.sub(repl_str, smoothed)
        except Exception: pass
    return re.sub(r"\s+", " ", smoothed).strip()

def smooth_natural_hindi(text: str) -> str:
    smoothed = text
    for pat_obj, replacement in HINDI_SPOKEN_SMOOTHING:
        try:
            repl_str = r'<dict_mod original="\g<0>">' + replacement.replace('\\', r'\\') + r'</dict_mod>'
            smoothed = pat_obj.sub(repl_str, smoothed)
        except Exception: pass
    return re.sub(r"\s+", " ", smoothed).strip()

def smooth_natural_marathi(text: str) -> str:
    smoothed = text
    for pat_obj, replacement in MARATHI_SPOKEN_SMOOTHING:
        try:
            repl_str = r'<dict_mod original="\g<0>">' + replacement.replace('\\', r'\\') + r'</dict_mod>'
            smoothed = pat_obj.sub(repl_str, smoothed)
        except Exception: pass
    return re.sub(r"\s+", " ", smoothed).strip()

# 🛡️ UPGRADE: Universal 1:1 Entity Enforcer using domain_terms
def apply_domain_terms_glossary(text: str) -> str:
    if not text: return text
    smoothed = text
    # Sort terms from longest to shortest to prevent partial word corruption
    sorted_terms = sorted(DOMAIN_TERMS.items(), key=lambda x: len(x[0]), reverse=True)
    
    for target, replacement in sorted_terms:
        if not target.strip(): continue
        
        # Determine strict word boundaries depending on whether the term is English or Devanagari
        if re.search(r'[\u0900-\u097F]', target):
            pattern = r"(?<![\u0900-\u097F])" + re.escape(target) + r"(?![\u0900-\u097F])"
        else:
            pattern = r"\b" + re.escape(target) + r"\b"
            
        repl_str = r'<dict_mod original="\g<0>">' + replacement.replace('\\', r'\\') + r'</dict_mod>'
        try:
            smoothed = re.sub(pattern, repl_str, smoothed, flags=re.IGNORECASE)
        except Exception: pass
        
    return smoothed

def format_timestamp(seconds: float) -> str:
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    milliseconds = int((seconds - int(seconds)) * 1000)
    return f"{hours:02}:{minutes:02}:{secs:02},{milliseconds:03}"

class OfflineIndicTransEngine:
    LANGUAGE_CODE_MAP = {
        "en": "eng_Latn", "hi": "hin_Deva", "mr": "mar_Deva", "ta": "tam_Taml",
        "te": "tel_Telu", "kn": "kan_Knda", "gu": "guj_Gujr", "bn": "ben_Beng",
        "pa": "pan_Guru", "ur": "urd_Arab", "sa": "san_Deva", "ne": "npi_Deva",
        "or": "ory_Orya", "as": "asm_Beng", "mai": "mai_Deva"
    }

    def __init__(self, models_root: Path):
        self.models_root = models_root
        self.current_model_dir = None
        self.tokenizer = None
        self.model = None
        self.ip = None

    def get_model_flavor(self, src_code: str, tgt_code: str) -> str:
        if tgt_code == "eng_Latn": return "indictrans2-indic-en-1B"
        elif src_code == "eng_Latn": return "indictrans2-en-indic-1B"
        else: return "indictrans2-indic-indic-1B"

    def load(self, model_dir_name: str) -> None:
        if self.current_model_dir == model_dir_name and self.model is not None: return
        if self.model is not None:
            del self.model; del self.tokenizer; del self.ip; gc.collect()

        model_path = self.models_root / model_dir_name
        self.tokenizer = AutoTokenizer.from_pretrained(str(model_path), trust_remote_code=True, local_files_only=True)
        self.model = AutoModelForSeq2SeqLM.from_pretrained(str(model_path), trust_remote_code=True, local_files_only=True).to("cpu")
        self.ip = IndicProcessor(inference=True)
        self.current_model_dir = model_dir_name

    def translate_batch(self, texts: list, source_language: str, target_language: str) -> list:
        if not texts: return []
        src_code = self.LANGUAGE_CODE_MAP.get(source_language.lower(), source_language)
        tgt_code = self.LANGUAGE_CODE_MAP.get(target_language.lower(), target_language)

        model_flavor = self.get_model_flavor(src_code, tgt_code)
        self.load(model_flavor)

        mini_batch_size = 2
        translations = []

        for start in range(0, len(texts), mini_batch_size):
            chunk = texts[start:start + mini_batch_size]
            batch = self.ip.preprocess_batch(chunk, src_lang=src_code, tgt_lang=tgt_code)
            inputs = self.tokenizer(batch, truncation=True, padding="longest", return_tensors="pt", return_attention_mask=True).to("cpu")
            input_length = inputs["input_ids"].shape[1]
            dynamic_max_tokens = min(256, int(input_length * 2.5) + 20)

            with torch.no_grad():
                generated_tokens = self.model.generate(
                    **inputs,
                    use_cache=True,
                    max_new_tokens=dynamic_max_tokens,
                    num_beams=3,
                    do_sample=False,
                    repetition_penalty=1.1,         
                    no_repeat_ngram_size=3,         
                    num_return_sequences=1
                )

            with self.tokenizer.as_target_tokenizer():
                decoded_tokens = self.tokenizer.batch_decode(generated_tokens.detach().cpu().tolist(), skip_special_tokens=True, clean_up_tokenization_spaces=True)
                
            translations.extend(self.ip.postprocess_batch(decoded_tokens, lang=tgt_code))
        return translations

    def unload(self):
        if self.model is not None:
            del self.model; del self.tokenizer; del self.ip
            self.model = None; self.tokenizer = None; self.ip = None; self.current_model_dir = None
            gc.collect()

def run_nmt(input_target: str, output_dir: str):
    project_root = Path(__file__).resolve().parent.parent.parent
    models_root = project_root / "models"

    transcript_file = input_target
    if os.path.isdir(input_target):
        possible_json = os.path.join(input_target, "master_transcript.json")
        if os.path.exists(possible_json):
            transcript_file = possible_json
        else:
            for root, _, files in os.walk(input_target):
                for f in files:
                    if f == "master_transcript.json" or f.endswith(".json"):
                        transcript_file = os.path.join(root, f)
                        break

    if not os.path.exists(transcript_file):
        print(f"❌ [NMTTranslator] Input file not found", flush=True)
        sys.exit(1)

    curr = os.path.abspath(transcript_file)
    job_root = None
    while curr and os.path.dirname(curr) != curr:
        if os.path.basename(curr).startswith("JOB-"):
            job_root = curr
            break
        curr = os.path.dirname(curr)

    target_language = "en"
    configured_source_lang = None
    if job_root:
        config_path = os.path.join(job_root, "job_config.json")
        if os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as cfg:
                job_cfg = json.load(cfg)
                target_language = job_cfg.get("target_language", "en")
                configured_source_lang = job_cfg.get("source_language")

    load_domain_dictionary()

    with open(transcript_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    timeline = data.get("timeline", [])
    source_language = configured_source_lang or data.get("language", "mr")
    engine = OfflineIndicTransEngine(models_root)

    input_texts = []
    for seg in timeline:
        raw_t = seg.get("text", "")
        clean_pure = re.sub(r'<dict_mod[^>]*>', '', raw_t)
        clean_pure = re.sub(r'</dict_mod>', '', clean_pure)
        
        cleaned_t = clean_asr_text(sanitize_nmt_input(clean_pure, source_language)) if source_language.lower() in ["mr", "hi"] else clean_pure.strip()
        input_texts.append(cleaned_t if cleaned_t else clean_pure.strip())

    if input_texts:
        translated_segments = engine.translate_batch(input_texts, source_language, target_language)

        # 🛡️ UPGRADE: Pipeline now applies Regex Smoothing FIRST, followed by 1:1 Domain Terms Glossary OVERRIDE
        if target_language.lower() == "en":
            translated_segments = [apply_domain_terms_glossary(smooth_natural_english(t)) for t in translated_segments]
        elif target_language.lower() == "hi":
            translated_segments = [apply_domain_terms_glossary(smooth_natural_hindi(t)) for t in translated_segments]
        elif target_language.lower() == "mr":
            translated_segments = [apply_domain_terms_glossary(smooth_natural_marathi(t)) for t in translated_segments]

        for seg, trans_t in zip(timeline, translated_segments):
            seg["translated_text"] = trans_t

    engine.unload()

    os.makedirs(output_dir, exist_ok=True)
    json_out = os.path.join(output_dir, "master_translated.json")
    with open(json_out, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    srt_out_target = os.path.join(output_dir, "subtitles.srt")
    with open(srt_out_target, "w", encoding="utf-8") as f:
        for idx, seg in enumerate(timeline, start=1):
            start_str = format_timestamp(float(seg.get("start", 0.0)))
            end_str = format_timestamp(float(seg.get("end", 0.0)))
            text = re.sub(r'</?dict_mod[^>]*>', '', seg.get("translated_text", ""))
            speaker_id = seg.get("speaker_id", "SPEAKER_00")
            f.write(f"{idx}\n{start_str} --> {end_str}\n[{speaker_id}] {text}\n\n")

    srt_out_source = os.path.join(output_dir, "source.srt")
    with open(srt_out_source, "w", encoding="utf-8") as f:
        for idx, seg in enumerate(timeline, start=1):
            start_str = format_timestamp(float(seg.get("start", 0.0)))
            end_str = format_timestamp(float(seg.get("end", 0.0)))
            text = re.sub(r'</?dict_mod[^>]*>', '', seg.get("text", ""))
            speaker_id = seg.get("speaker_id", "SPEAKER_00")
            f.write(f"{idx}\n{start_str} --> {end_str}\n[{speaker_id}] {text}\n\n")

    candidate_terms = []
    seen_words = set()
    
    all_text = " ".join([re.sub(r'</?dict_mod[^>]*>', '', seg.get("text", "")) for seg in timeline])
    devanagari_words = re.findall(r"[\u0900-\u097F]{4,}", all_text)
    
    for word in devanagari_words:
        w_clean = word.strip()
        if w_clean and w_clean not in seen_words and w_clean not in ASR_CORRECTIONS:
            seen_words.add(w_clean)
            candidate_terms.append({
                "original": w_clean,
                "replacement": w_clean,
                "type": "asr_corrections",
                "count": devanagari_words.count(w_clean)
            })

    candidate_file = os.path.join(output_dir, "candidate_terms.json")
    with open(candidate_file, "w", encoding="utf-8") as f:
        json.dump(candidate_terms[:15], f, indent=2, ensure_ascii=False)

    print(f"✅ [NMTTranslator] IndicTrans2 execution complete. Ready for human UI audit.", flush=True)

if __name__ == "__main__":
    if len(sys.argv) >= 3:
        run_nmt(sys.argv[1], sys.argv[2])