import os
import shutil

TARGET_DIRS = {
    "__pycache__",
    "cache",
    "data",
    "static",
}

def remove_dirs(root="."):
    for current_root, dirs, _ in os.walk(root):
        for d in list(dirs):
            if d in TARGET_DIRS:
                path = os.path.join(current_root, d)
                print(f"Removing: {path}")
                shutil.rmtree(path, ignore_errors=True)
                dirs.remove(d)  # prevent descending into deleted dir

if __name__ == "__main__":
    remove_dirs()
