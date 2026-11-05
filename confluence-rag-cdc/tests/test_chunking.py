from rag_pipeline.chunking import chunk_text, html_to_text


def test_html_to_text_keeps_headings_and_text():
    html = "<h1>Title</h1><p>Hello <b>world</b></p><ul><li>a</li><li>b</li></ul>"
    text = html_to_text(html)
    assert "# Title" in text and "Hello world" in text and "a\nb" in text


def test_chunking_respects_size():
    chunks = chunk_text("word " * 1000, chunk_size=200, chunk_overlap=20)
    assert len(chunks) > 1 and all(len(c) <= 200 for c in chunks)


def test_empty():
    assert chunk_text("") == [] and html_to_text(None) == ""
