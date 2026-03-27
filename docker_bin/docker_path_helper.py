import os
import sys

def get_docker_exe():
    """
    Devuelve la ruta absoluta a docker.exe incluido en la app, o 'docker' si no está.
    Soporta ejecución con PyInstaller (sys._MEIPASS).
    """
    base = getattr(sys, '_MEIPASS', os.path.abspath("."))
    local_path = os.path.join(base, "docker_bin", "docker.exe")
    if os.path.exists(local_path):
        return local_path
    return "docker"
