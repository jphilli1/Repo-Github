import subprocess
import sys

def install_requirements():
    print(f"Installing requirements from requirements.txt...")
    try:
        # This runs 'pip install -r requirements.txt' using the current python interpreter
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("\n[SUCCESS] Dependencies installed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] Installation failed: {e}")

if __name__ == "__main__":
    install_requirements()