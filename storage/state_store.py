import os
from config import STATE_FILE

def load_state(default_start: int) -> int:
    if not os.path.exists(STATE_FILE):
        return default_start
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return max(int(f.read().strip()), default_start)
    except Exception:
        return default_start

def save_state(next_card_id: int):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        f.write(str(next_card_id))
