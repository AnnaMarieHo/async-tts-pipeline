import re


def chunk_by_sentence(text):
    pattern = r"(?<!\.)\.(?!\.)"
    chunks = re.split(pattern, text)
    return [chunk.strip() + "." for chunk in chunks if chunk.strip()]

def batch_text(chunks, batch_size) -> list[str]:
    return [chunks[i:i+batch_size] for i in range(0, len(chunks), batch_size)]

def received_text(text):
    print(text)

