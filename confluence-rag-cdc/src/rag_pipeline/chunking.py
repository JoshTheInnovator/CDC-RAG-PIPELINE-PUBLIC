import re

from bs4 import BeautifulSoup
from langchain_text_splitters import RecursiveCharacterTextSplitter

_HEADINGS = ["h1", "h2", "h3", "h4", "h5", "h6"]


def html_to_text(html: str | None) -> str:
    """Confluence storage-format HTML -> readable text with markdown-style headings."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for t in soup(["script", "style"]):
        t.decompose()
    for br in soup.find_all("br"):
        br.replace_with("\n")
    for h in soup.find_all(_HEADINGS):
        h.insert_before("\n\n" + "#" * int(h.name[1]) + " ")
        h.append("\n")
    for cell in soup.find_all(["td", "th"]):
        cell.append(" | ")
    for blk in soup.find_all(["p", "li", "tr", "div"]):
        blk.append("\n")
    text = soup.get_text()
    text = "\n".join(line.strip() for line in text.splitlines())
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def chunk_text(text: str, chunk_size: int = 1000, chunk_overlap: int = 150) -> list[str]:
    if not text:
        return []
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        separators=["\n## ", "\n### ", "\n\n", "\n", ". ", " ", ""],
    )
    return splitter.split_text(text)
