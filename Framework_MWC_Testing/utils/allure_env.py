import os
import platform
import sys

def write_allure_environment(allure_results_dir, **kwargs):
    os.makedirs(allure_results_dir, exist_ok=True)
    env_path = os.path.join(allure_results_dir, "environment.properties")

    default_env = {
        "OS": platform.system(),
        "OS.Version": platform.release(),
        "Python": sys.version.split()[0],
    }

    default_env.update(kwargs)

    with open(env_path, "w", encoding="utf-8") as f:
        for k, v in default_env.items():
            f.write(f"{k}={v}\n")
