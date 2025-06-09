import sys
import os
import traceback

def inspect_path():
    print("=== sys.path ===")
    for p in sys.path:
        print(" ", p)
    print()

def inspect_cwd():
    cwd = os.getcwd()
    print("=== Current Working Directory ===")
    print(" ", cwd)
    print("=== Contents of CWD ===")
    for name in os.listdir(cwd):
        print("  ", name)
    print()

def inspect_utilities_folder():
    util_dir = os.path.join(os.getcwd(), "utilities")
    print("=== utilities folder check ===")
    print(" Path exists?", os.path.isdir(util_dir))
    if os.path.isdir(util_dir):
        print(" Contents:")
        for name in os.listdir(util_dir):
            print("  ", name)
    print()

def try_imports():
    print("=== Trying imports ===")
    try:
        import utilities
        print("Imported utilities from:", utilities.__file__)
    except Exception:
        print("Failed to import utilities:")
        traceback.print_exc()
    print()
    try:
        import utilities.constants as constants
        print("Imported constants from:", constants.__file__)
        print("  Sample value, e.g. AZURE_COSMOSDB_ENDPOINT =", constants.AZURE_COSMOSDB_ENDPOINT)
    except Exception:
        print("Failed to import utilities.constants:")
        traceback.print_exc()
    print()

if __name__ == "__main__":
    inspect_path()
    inspect_cwd()
    inspect_utilities_folder()
    try_imports()
