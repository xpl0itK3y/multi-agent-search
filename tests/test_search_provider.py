from io import BytesIO

from reportlab.pdfgen import canvas

from src.providers.search import ContentExtractor, get_extraction_metrics_snapshot, reset_extraction_metrics


def _text_pdf_bytes(text: str) -> bytes:
    output = BytesIO()
    document = canvas.Canvas(output)
    document.drawString(72, 720, text)
    document.save()
    return output.getvalue()


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
