import subprocess

def run_command(command):
    """Executes a command and checks its success."""
    try:
        print(f"============== RUNNING COMMAND: {' '.join(command)} ==============")
        # Run the command and capture output
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, check=True)
        print(f"============== SUCCESSFULLY =============")
        print("Output:", result.stdout)
        return True  # Indicate success
    except subprocess.CalledProcessError as e:
        print(f"============== FAIL! =============")
        print("Return code:", e.returncode)
        print("Error message:", e.stderr)
        return False  # Indicate failure

commands = [
    ["bash", "submit.sh"],  # Ensure correct path for python
    ['/home/duy.caov/notebooks/apps/vn.bdp.crm/duy_test/duycaovenv/bin/python', 'render.py'], 
    ['git', 'add', '.'],
    ["git", "commit", "-m", "testing v1"],
    ["git", "push"]
]

if __name__ == "__main__":
    # Execute each command and check the result
    for cmd in commands:
        success = run_command(cmd)
        if not success:
            print("Stopping execution due to failure.")
            break  
