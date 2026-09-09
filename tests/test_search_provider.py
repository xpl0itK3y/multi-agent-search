from io import BytesIO

from reportlab.pdfgen import canvas

from src.config import settings
from src.providers.search import (
    ContentExtractor,
    ExtractionDomainRegistry,
    get_extraction_metrics_snapshot,
    reset_extraction_metrics,
)


def _text_pdf_bytes(text: str) -> bytes:
    output = BytesIO()
    document = canvas.Canvas(output)
    document.drawString(72, 720, text)
    document.save()
    return output.getvalue()


def test_domain_timeout_obeys_failure_threshold_and_decays_after_cooldown(mocker):
    registry = ExtractionDomainRegistry()
    now = [100.0]
    mocker.patch.object(settings, "search_domain_fail_threshold", 2)
    mocker.patch.object(settings, "search_domain_cooldown_seconds", 10)
    mocker.patch("src.providers.search.time.monotonic", side_effect=lambda: now[0])
    url = "https://slow.example/article"

    registry.record(url, outcome="failed", timed_out=True)
    assert registry.should_skip(url) is None

    registry.record(url, outcome="failed", timed_out=False)
    assert registry.should_skip(url) == "domain-cooldown"

    now[0] += 11
    assert registry.should_skip(url) is None
    registry.record(url, outcome="failed", timed_out=False)
    assert registry.should_skip(url) is None
    registry.record(url, outcome="failed", timed_out=False)
    assert registry.should_skip(url) == "domain-cooldown"


def test_domain_registry_reset_clears_active_cooldown(mocker):
    registry = ExtractionDomainRegistry()
    mocker.patch.object(settings, "search_domain_fail_threshold", 1)
    registry.record("https://slow.example/article", outcome="failed", timed_out=True)
    assert registry.should_skip("https://slow.example/article") == "domain-cooldown"

    registry.reset()

    assert registry.should_skip("https://slow.example/article") is None


def test_content_extractor_logs_stage_metrics_on_success(mocker):
    reset_extraction_metrics()
    mocker.patch(
        "src.net_safety.safe_fetch_document",
        return_value=(b"<html>body</html>", "text/html"),
    )
    mocker.patch("src.providers.search.trafilatura.extract", return_value="Useful content\nUseful content")
    info_mock = mocker.patch("src.providers.search.logger.info")

    result = ContentExtractor.extract_content("https://example.com/article")

    assert result == "Useful content"
    assert info_mock.call_count == 1
    message = info_mock.call_args.args[0]
    assert message == (
        "content_extraction_completed url=%s download_ms=%.2f extract_ms=%.2f "
        "post_process_ms=%.2f total_ms=%.2f downloaded_bytes=%s content_chars=%s success=%s"
    )
    metrics = get_extraction_metrics_snapshot()
    assert metrics["attempts"] >= 1
    assert metrics["success_count"] >= 1


def test_content_extractor_logs_failure_metrics_on_exception(mocker):
    reset_extraction_metrics()
    mocker.patch("src.net_safety.safe_fetch_document", side_effect=RuntimeError("boom"))
    error_mock = mocker.patch("src.providers.search.logger.error")

    result = ContentExtractor.extract_content("https://example.com/article")

    assert result is None
    assert error_mock.call_count == 1
    message = error_mock.call_args.args[0]
    assert message == (
        "content_extraction_failed url=%s download_ms=%.2f extract_ms=%.2f "
        "post_process_ms=%.2f total_ms=%.2f downloaded_bytes=%s content_chars=%s error=%s"
    )
    metrics = get_extraction_metrics_snapshot()
    assert metrics["attempts"] >= 1
    assert metrics["failure_count"] >= 1


def test_content_extractor_skips_blocked_video_urls(mocker):
    reset_extraction_metrics()
    fetch_mock = mocker.patch("src.net_safety.safe_fetch_document")
    info_mock = mocker.patch("src.providers.search.logger.info")

    result = ContentExtractor.extract_content("https://www.youtube.com/watch?v=abc")

    assert result is None
    fetch_mock.assert_not_called()
    assert info_mock.call_count == 1
    assert info_mock.call_args.args[0] == "content_extraction_skipped url=%s reason=%s"


def test_content_extractor_reads_pdf_without_html_parser(mocker):
    reset_extraction_metrics()
    pdf = _text_pdf_bytes("Regulatory report confirms the measured emissions result.")
    mocker.patch(
        "src.net_safety.safe_fetch_document",
        return_value=(pdf, "application/pdf"),
    )
    html_extract = mocker.patch("src.providers.search.trafilatura.extract")

    result = ContentExtractor.extract_content("https://example.gov/report")

    assert result is not None
    assert "Regulatory report confirms" in result
    html_extract.assert_not_called()
    metrics = get_extraction_metrics_snapshot()
    assert metrics["success_count"] >= 1
    assert metrics["downloaded_bytes"] >= len(pdf)


def test_content_extractor_detects_mislabeled_pdf_by_magic_bytes(mocker):
    pdf = _text_pdf_bytes("Primary study reports a statistically significant result.")
    mocker.patch(
        "src.net_safety.safe_fetch_document",
        return_value=(pdf, "application/octet-stream"),
    )
    html_extract = mocker.patch("src.providers.search.trafilatura.extract")

    result = ContentExtractor.extract_content("https://example.edu/download?id=42")

    assert result is not None
    assert "Primary study reports" in result
    html_extract.assert_not_called()
