import json
import gzip
import os
import random

MODEL_PATH = "src/main/resources/models/akamai_model.json"
LOG_DIR = "logs"

def load_model():
    with open(MODEL_PATH, "r") as f:
        return json.load(f)

def extract_random_line(file_path):
    with gzip.open(file_path, 'rt') as f:
        lines = f.readlines()
        return random.choice(lines).strip() if lines else None

def print_debug_for_line(line, model):
    fields = line.split()
    print(f"\nLINEA LOG:\n{line}\n")
    print("INDICI:")
    for idx, val in enumerate(fields):
        print(f"{idx}: {val}")
    
    print("\nMAPPATURA MODEL:")
    for key, idx in model.items():
        val = fields[idx] if idx < len(fields) else "[OUT OF BOUNDS]"
        print(f"{key} (#{idx}): {val}")

def main():
    model = load_model()
    for fname in os.listdir(LOG_DIR):
        if fname.endswith("MF.gz"):
            full_path = os.path.join(LOG_DIR, fname)
            print(f"\nFILE: {fname}")
            line = extract_random_line(full_path)
            if line:
                print_debug_for_line(line, model)
            else:
                print("nessuna riga trovata.")

if __name__ == "__main__":
    main()
