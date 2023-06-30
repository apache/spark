import os

def write_to_location(location: str, content: str) -> None:
    os.makedirs(os.path.dirname(location), exist_ok=True)
    with open(location, "a") as f:
        f.write(content)

