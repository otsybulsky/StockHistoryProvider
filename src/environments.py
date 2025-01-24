from decouple import config

SOME_SECRET = config("SOME_SECRET", default=None)
assert SOME_SECRET is not None
SOME_SECRET = str(SOME_SECRET).strip()

if __name__ == "__main__":
    print("Environment variables:")
    print(f"SOME_SECRET={SOME_SECRET}")
