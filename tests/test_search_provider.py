from src.providers.search import ContentExtractor, get_extraction_metrics_snapshot, reset_extraction_metrics


def test_content_extractor_logs_stage_metrics_on_success(mocker):
    reset_extraction_metrics()
    mocker.patch("src.providers.search.trafilatura.fetch_url", return_value="<html>body</html>")
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
    mocker.patch("src.providers.search.trafilatura.fetch_url", side_effect=RuntimeError("boom"))
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
