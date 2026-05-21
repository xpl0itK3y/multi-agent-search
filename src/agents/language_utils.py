# Shared language-detection utilities used by OrchestratorAgent and AnalyzerAgent.
# Keep the hint dictionary here so it is maintained in exactly one place (A-6).

# Common function-word fingerprints for three supported languages.
# Keys match the language codes returned by the language-detection helpers.
LANGUAGE_HINTS: dict[str, set[str]] = {
    "ru": {"и", "в", "не", "что", "для", "как", "это", "на", "по"},
    "es": {"el", "la", "los", "las", "para", "como", "una", "con", "del"},
    "en": {"the", "and", "for", "with", "that", "from", "this", "into", "small"},
}
