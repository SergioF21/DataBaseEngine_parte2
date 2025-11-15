import os
import re
from typing import List

try:
    # lazy nltk imports and downloads
    import nltk
    from nltk.stem.snowball import SnowballStemmer
    from nltk.corpus import stopwords
except Exception:
    nltk = None

try:
    from unidecode import unidecode
except Exception:
    # fallback if unidecode not installed
    def unidecode(s):
        return s


_stemmer = None
_stopwords = None


def _simple_stem(word: str) -> str:
    """Very small heuristic stemmer: remove common Spanish suffixes."""
    if not word or len(word) <= 4:
        return word
    suffixes = ['mente', 'ación', 'aciones', 'amientos', 'amiento', 'imientos', 'imiento', 'idades', 'idad', 'anzas', 'ancia', 'ancia', 'ismo', 'ista', 'istas', 'able', 'ibles', 'mente', 'mente']
    for s in suffixes:
        if word.endswith(s) and len(word) - len(s) >= 3:
            return word[: -len(s)]
    # also strip common verb endings
    for s in ['ar', 'er', 'ir', 'ado', 'ido', 'ando', 'iendo', 'aré', 'eré', 'iré', 'aría', 'ería', 'iría']:
        if word.endswith(s) and len(word) - len(s) >= 3:
            return word[: -len(s)]
    return word


def _ensure_nltk_resources():
    global _stemmer, _stopwords
    # If a custom stopwords file exists, prefer it and avoid requiring NLTK
    custom_path = os.path.join(os.path.dirname(__file__), "stopwords_es.txt")
    if _stopwords is None and os.path.exists(custom_path):
        try:
            with open(custom_path, "r", encoding="utf-8") as f:
                raw = [w.strip() for w in f.readlines() if w.strip() and not w.strip().startswith("#")]
            # normalize: remove accents, lowercase, strip
            words = [unidecode(w).lower() for w in raw]
            # deduplicate
            _stopwords = set(words)
            return
        except Exception:
            # fall back to NLTK-based loading below
            pass

    if nltk is None:
        raise RuntimeError("nltk not available. Install with `pip install nltk` and run once to download resources.")
    if _stemmer is None:
        try:
            _stemmer = SnowballStemmer("spanish")
        except Exception:
            _stemmer = SnowballStemmer("spanish")
    if _stopwords is None:
        try:
            sw = stopwords.words("spanish")
            # normalize NLTK stopwords as well
            _stopwords = set(unidecode(w).lower() for w in sw)
        except LookupError:
            nltk.download("stopwords")
            sw = stopwords.words("spanish")
            _stopwords = set(unidecode(w).lower() for w in sw)
        except Exception:
            _stopwords = set()


def normalize_text(text: str) -> str:
    """Lowercase, remove accents, and delete non-alphanumeric characters except spaces."""
    if text is None:
        return ""
    text = str(text)
    text = unidecode(text)
    text = text.lower()
    # keep letters and numbers and spaces
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def tokenize(text: str) -> List[str]:
    text = normalize_text(text)
    if not text:
        return []
    return text.split()


def remove_stopwords(tokens: List[str]) -> List[str]:
    _ensure_nltk_resources()
    return [t for t in tokens if t not in _stopwords]


def stem_tokens(tokens: List[str]) -> List[str]:
    _ensure_nltk_resources()
    if _stemmer is not None:
        return [_stemmer.stem(t) for t in tokens]
    # fallback to simple stemmer
    return [_simple_stem(t) for t in tokens]


def preprocess(text: str) -> List[str]:
    """Full preprocessing pipeline: normalize -> tokenize -> remove stopwords -> stem."""
    tokens = tokenize(text)
    if not tokens:
        return []
    tokens = remove_stopwords(tokens)
    tokens = stem_tokens(tokens)
    return tokens


def concat_series_text(series) -> str:
    """Concatenate all non-null fields of a pandas Series into a single text block."""
    parts = []
    for v in series.values:
        if v is None:
            continue
        s = str(v).strip()
        if s and s.lower() not in ("nan", "none"):
            parts.append(s)
    return " ".join(parts)
