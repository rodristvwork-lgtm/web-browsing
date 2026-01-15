
import subprocess
import sys

resp = subprocess.run(["ps", "aux"], stdout = subprocess.PIPE)
resp_ok = resp.stdout.decode("utf-8")
if "browsertime.py" in resp_ok:
    sys.exit(1)
elif "loading.py" in resp_ok:
    sys.exit(2)
else:
    sys.exit(0)
