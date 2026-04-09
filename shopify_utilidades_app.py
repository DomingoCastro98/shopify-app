import os
import io
import json
import math
from pathlib import Path
import queue
import re
import shlex
import socket
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import traceback
import webbrowser
import zipfile
import tkinter as tk
import unicodedata
import urllib.error
import urllib.request
import urllib.parse
from datetime import datetime
from tkinter import filedialog, messagebox, simpledialog, ttk
from typing import Callable

# Añadir helper para docker.exe local
from docker_bin.docker_path_helper import get_docker_exe

# ──────────────────────────────────────────────────────────────────────────────
#  VERSIÓN Y ACTUALIZACIÓN AUTOMÁTICA
# ──────────────────────────────────────────────────────────────────────────────
APP_VERSION = "1.2.5"  # <-- actualiza este valor en cada release

# URL pública donde publicas tu version.json (GitHub raw, servidor propio, etc.)
# Ejemplo GitHub: "https://raw.githubusercontent.com/TU_USUARIO/TU_REPO/main/version.json"
# El archivo version.json debe tener este formato:
# {
#   "version": "1.1.0",
#   "download_url": "https://github.com/TU_USUARIO/TU_REPO/releases/download/v1.1.0/wordpress_utilidades_app.py",
#   "notes": "Descripción de los cambios"
# }
_UPDATE_CHECK_URLS = [
    "https://raw.githubusercontent.com/DomingoCastro98/shopify-app/main/version.json"
]


def _is_frozen_app() -> bool:
    return bool(getattr(sys, "frozen", False))


def _current_install_target() -> str:
    """Ruta del archivo que debe reemplazarse al actualizar (.exe o .py)."""
    if _is_frozen_app():
        return os.path.abspath(sys.executable)
    return os.path.abspath(__file__)


def _restart_command_for_target(target_path: str) -> list[str]:
    """Comando para relanzar la app tras aplicar la actualización."""
    if _is_frozen_app():
        return [target_path]
    return [sys.executable, target_path]


def _select_download_url(update_info: dict) -> str:
    """Elige la URL de descarga adecuada según el modo de ejecución."""
    if _is_frozen_app():
        return (
            update_info.get("download_url_exe")
            or update_info.get("download_url")
            or ""
        )
    return (
        update_info.get("download_url_py")
        or update_info.get("download_url")
        or ""
    )


def _parse_version(v: str) -> tuple[int, ...]:
    """Convierte '1.2.3' en (1, 2, 3) para comparar numéricamente."""
    try:
        return tuple(int(x) for x in v.strip().lstrip("v").split("."))
    except Exception:
        return (0,)


def _check_for_updates_worker(current_version: str, callback: "Callable[[dict], None]") -> None:
    """Hilo secundario: consulta la URL de versión y dispara callback si hay novedad."""
    try:
        data: dict | None = None
        for check_url in _UPDATE_CHECK_URLS:
            try:
                req = urllib.request.Request(
                    check_url,
                    headers={
                        "User-Agent": f"ShopifyUtilidades-UpdateChecker/{current_version}"},
                )
                with urllib.request.urlopen(req, timeout=10) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                if isinstance(data, dict):
                    break
            except Exception:
                continue

        if not data:
            return

        remote_ver = data.get("version", "")
        if _parse_version(remote_ver) > _parse_version(current_version):
            callback(data)
    except Exception:
        pass  # Sin conexión o URL no configurada → silencioso


def _ps_quote(value: str) -> str:
    """Escapa comillas simples para strings de PowerShell entre comillas simples."""
    return value.replace("'", "''")


def _launch_updater_and_exit(new_file_path: str, current_file_path: str, restart_cmd: list[str]) -> None:
    """
    Lanza un actualizador gráfico (PowerShell + WinForms) que:
      1. Espera cierre de la app
      2. Reemplaza archivo actual (.py/.exe) con reintentos
      3. Relanza la app
    Luego cierra el proceso actual para liberar el ejecutable.
    """
    if not restart_cmd:
        return

    restart_exe = restart_cmd[0]
    restart_args = restart_cmd[1:]
    restart_args_ps = ", ".join(f"'{_ps_quote(a)}'" for a in restart_args)
    if not restart_args_ps:
        restart_args_ps = ""

    ps_script = f"""
$ErrorActionPreference = 'SilentlyContinue'
$pidToWait = {os.getpid()}
$newFile = '{_ps_quote(new_file_path)}'
$currentFile = '{_ps_quote(current_file_path)}'
$restartExe = '{_ps_quote(restart_exe)}'
$restartArgs = @({restart_args_ps})
$selfScript = $MyInvocation.MyCommand.Path

Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing
[System.Windows.Forms.Application]::EnableVisualStyles()

$form = New-Object System.Windows.Forms.Form
$form.Text = 'Actualizando Shopify Utilidades'
$form.Size = New-Object System.Drawing.Size(500, 220)
$form.StartPosition = 'CenterScreen'
$form.TopMost = $true
$form.FormBorderStyle = 'FixedDialog'
$form.MaximizeBox = $false
$form.MinimizeBox = $false

$label = New-Object System.Windows.Forms.Label
$label.AutoSize = $false
$label.Size = New-Object System.Drawing.Size(460, 34)
$label.Location = New-Object System.Drawing.Point(18, 14)
$label.Text = 'Preparando actualización...'
$label.Font = New-Object System.Drawing.Font('Segoe UI', 10)
$form.Controls.Add($label)

$bar = New-Object System.Windows.Forms.ProgressBar
$bar.Location = New-Object System.Drawing.Point(18, 64)
$bar.Size = New-Object System.Drawing.Size(460, 22)
$bar.Minimum = 0
$bar.Maximum = 100
$bar.Style = 'Continuous'
$form.Controls.Add($bar)

$pct = New-Object System.Windows.Forms.Label
$pct.AutoSize = $false
$pct.Size = New-Object System.Drawing.Size(460, 24)
$pct.Location = New-Object System.Drawing.Point(18, 96)
$pct.Text = '0%'
$pct.Font = New-Object System.Drawing.Font('Segoe UI', 9)
$form.Controls.Add($pct)

$closeBtn = New-Object System.Windows.Forms.Button
$closeBtn.Size = New-Object System.Drawing.Size(120, 30)
$closeBtn.Location = New-Object System.Drawing.Point(18, 136)
$closeBtn.Text = 'Cerrar'
$closeBtn.Font = New-Object System.Drawing.Font('Segoe UI', 9)
$closeBtn.Add_Click({{ $form.Close() }})
$form.Controls.Add($closeBtn)

function Set-Ui([int]$value, [string]$text) {{
    if ($value -lt 0) {{ $value = 0 }}
    if ($value -gt 100) {{ $value = 100 }}
    $bar.Value = $value
    $label.Text = $text
    $pct.Text = "$value%"
    [System.Windows.Forms.Application]::DoEvents()
}}

$form.Show()
Set-Ui 5 'Esperando cierre de la app...'

$waitTicks = 0
while (Get-Process -Id $pidToWait -ErrorAction SilentlyContinue) {{
    Start-Sleep -Milliseconds 250
    $waitTicks++
    $step = 5 + [int]([Math]::Min(15, $waitTicks / 2))
    Set-Ui $step 'Esperando cierre de la app...'
    if ($waitTicks -ge 120) {{ break }}
}}

$copied = $false
for ($i = 1; $i -le 80; $i++) {{
    try {{
        Unblock-File -LiteralPath $newFile -ErrorAction SilentlyContinue
        Copy-Item -LiteralPath $newFile -Destination $currentFile -Force -ErrorAction Stop
        $copied = $true
        break
    }} catch {{}}

    $prog = 20 + [int](($i / 80) * 70)
    Set-Ui $prog "Aplicando actualización... intento $i/80"
    Start-Sleep -Milliseconds 250
}}

if ($copied) {{
    # IMPORTANTE: NO borrar carpetas _MEI antes de relanzar el .exe.
    # PyInstaller extrae la DLL de Python en una carpeta _MEI nueva al arrancar.
    # Si se borran _MEI* justo antes del relanzamiento, el nuevo proceso puede
    # encontrar su carpeta a medias o vacía → "Failed to load Python DLL".
    # Las carpetas _MEI del proceso anterior ya quedaron libres al cerrarlo;
    # Windows las limpiará en el siguiente arranque o limpieza de temp.
    Remove-Item -LiteralPath $newFile -Force -ErrorAction SilentlyContinue

    Set-Ui 100 'Instalación finalizada.'
    Start-Sleep -Milliseconds 1000
    [System.Windows.Forms.MessageBox]::Show(
        'La actualización se ha instalado correctamente. Por favor, abre la aplicación manualmente.',
        'Actualización exitosa',
        [System.Windows.Forms.MessageBoxButtons]::OK,
        [System.Windows.Forms.MessageBoxIcon]::Information
    ) | Out-Null
    $form.Close()
}} else {{
    [System.Windows.Forms.MessageBox]::Show(
        'No se pudo reemplazar el archivo porque sigue en uso. Cierra la app y vuelve a intentar.',
        'Error de actualización',
        [System.Windows.Forms.MessageBoxButtons]::OK,
        [System.Windows.Forms.MessageBoxIcon]::Error
    ) | Out-Null
}}

while ($form.Visible) {{
    [System.Windows.Forms.Application]::DoEvents()
    Start-Sleep -Milliseconds 100
}}

# Limpieza del propio script temporal del updater
Start-Process -FilePath 'cmd.exe' -ArgumentList '/c', "ping 127.0.0.1 -n 2 >nul & del /f /q \"$selfScript\"" -WindowStyle Hidden | Out-Null
""".strip()

    fd, ps1_path = tempfile.mkstemp(prefix="wpu_update_", suffix=".ps1")
    try:
        # UTF-8 con BOM para que PowerShell en Windows respete acentos.
        with os.fdopen(fd, "w", encoding="utf-8-sig") as f:
            f.write(ps_script)
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        return

    try:
        subprocess.Popen(
            [
                "powershell.exe",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                ps1_path,
            ],
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            close_fds=True,
        )
    except Exception:
        return

    os._exit(0)


# ──────────────────────────────────────────────────────────────────────────────

try:
    import docker  # type: ignore[import-not-found]
    # type: ignore[import-not-found]
    from docker.errors import APIError, DockerException, NotFound
except Exception:  # pragma: no cover
    docker = None  # type: ignore[assignment]
    APIError = Exception  # type: ignore[assignment]
    DockerException = Exception  # type: ignore[assignment]
    NotFound = Exception  # type: ignore[assignment]


def _looks_like_container_spec(path_value: str) -> bool:
    if ":" not in path_value:
        return False
    drive, _tail = os.path.splitdrive(path_value)
    return not bool(drive)


def _sdk_create_client(base_url: str | None, timeout_seconds: int | None) -> object:
    if docker is None:
        raise RuntimeError(
            "Docker SDK no disponible. Instala paquete Python 'docker'.")
    if base_url:
        client = docker.DockerClient(
            base_url=base_url, timeout=timeout_seconds)
    else:
        client = docker.from_env(timeout=timeout_seconds)
    client.ping()
    try:
        client.api.timeout = None
    except Exception:
        pass
    return client


def _sdk_cp_from_container_impl(client: object, container_name: str, src_path: str, local_path: str) -> None:
    container = client.containers.get(container_name)
    stream, _stat = container.get_archive(src_path)
    payload = b"".join(stream)
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:") as tar:
        members = tar.getmembers()
        if not members:
            raise RuntimeError("No se recibieron datos desde contenedor")
        first = members[0]
        extracted = tar.extractfile(first)
        if extracted is None:
            raise RuntimeError("No se pudo extraer archivo")
        os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
        with open(local_path, "wb") as fh:
            fh.write(extracted.read())


def _sdk_cp_to_container_impl(client: object, local_path: str, container_name: str, target_path: str) -> None:
    container = client.containers.get(container_name)
    target_norm = target_path.replace("\\", "/")
    parent_dir = os.path.dirname(target_norm.rstrip("/")) or "/"
    target_name = os.path.basename(target_norm.rstrip("/"))
    if not target_name:
        target_name = os.path.basename(local_path.rstrip("\\/"))

    fd, temp_tar_path = tempfile.mkstemp(prefix="wpu_sdk_cp_", suffix=".tar")
    os.close(fd)
    try:
        with tarfile.open(temp_tar_path, mode="w") as tar:
            tar.add(local_path, arcname=target_name)
        with open(temp_tar_path, "rb") as tar_stream:
            ok = container.put_archive(parent_dir, tar_stream.read())
        if not ok:
            raise RuntimeError("No se pudo copiar al contenedor")
    finally:
        try:
            os.remove(temp_tar_path)
        except OSError:
            pass


def _run_sdk_cp_helper(direction: str, base_url: str | None, src: str, dst: str) -> int:
    client = _sdk_create_client(base_url=base_url, timeout_seconds=None)
    if direction == "from":
        container_name, container_path = src.split(":", 1)
        _sdk_cp_from_container_impl(
            client, container_name, container_path, dst)
        return 0
    if direction == "to":
        container_name, container_path = dst.split(":", 1)
        _sdk_cp_to_container_impl(client, src, container_name, container_path)
        return 0
    raise RuntimeError(f"Direccion de copia no soportada: {direction}")


def _run_helper_cli_from_argv(argv: list[str]) -> int | None:
    if len(argv) >= 2 and argv[1] == "--wpu-sdk-cp":
        if len(argv) != 6:
            print("Uso helper invalido", file=sys.stderr)
            return 2
        _mode = argv[1]
        direction = argv[2]
        base_url = argv[3] or None
        src = argv[4]
        dst = argv[5]
        try:
            return _run_sdk_cp_helper(direction, base_url, src, dst)
        except Exception as exc:
            print(str(exc), file=sys.stderr)
            return 1
    return None


class _Tooltip:
    def __init__(self, widget: tk.Widget, text: str) -> None:
        self.widget = widget
        self.text = text
        self.window: tk.Toplevel | None = None
        self._after_id: str | None = None
        self._x = 0
        self._y = 0
        widget.bind("<Enter>", self._schedule_show, add="+")
        widget.bind("<Leave>", self._hide, add="+")
        widget.bind("<ButtonPress>", self._hide, add="+")
        widget.bind("<Destroy>", self._cleanup, add="+")

    def _schedule_show(self, event: tk.Event | None = None) -> None:
        if event is not None:
            self._x = event.x_root + 14
            self._y = event.y_root + 18
        self._cancel_scheduled()
        self._after_id = self.widget.after(500, self._show)

    def _cancel_scheduled(self) -> None:
        if self._after_id is not None:
            try:
                self.widget.after_cancel(self._after_id)
            except Exception:
                pass
            self._after_id = None

    def _show(self) -> None:
        self._after_id = None
        if self.window is not None or not self.widget.winfo_exists():
            return
        try:
            self.window = tk.Toplevel(self.widget)
            self.window.overrideredirect(True)
            self.window.attributes("-topmost", True)
            self.window.configure(bg="#111827")
            self.window.geometry(f"+{self._x}+{self._y}")
            label = tk.Label(
                self.window,
                text=self.text,
                bg="#111827",
                fg="#f9fafb",
                padx=10,
                pady=5,
                justify="left",
                wraplength=280,
                font=("Segoe UI", 8),
            )
            label.pack()
        except Exception:
            self.window = None

    def _hide(self, _event: tk.Event | None = None) -> None:
        self._cancel_scheduled()
        if self.window is not None:
            try:
                self.window.destroy()
            except Exception:
                pass
            self.window = None

    def _cleanup(self, _event: tk.Event | None = None) -> None:
        self._hide()


class ShopifyUtilitiesApp:
    def _cancel_profiles_load_guard(self) -> None:
        """Cancela el job de timeout del guard de carga de perfiles si está activo."""
        if self._profiles_load_guard_job_id is not None:
            try:
                self.root.after_cancel(self._profiles_load_guard_job_id)
            except Exception:
                pass
            self._profiles_load_guard_job_id = None

    def _fail_profiles_loading(self, msg: str | None = None) -> None:
        """Marca la carga de perfiles como fallida y actualiza la UI en el hilo principal."""
        self._profiles_loading = False
        self._profiles_loading_scope = None
        self._profiles_load_job_id = None
        self._cancel_profiles_load_guard()
        self._set_profiles_loading_ui(False)
        if hasattr(self, 'profiles_listbox') and self.profiles_listbox:
            self.profiles_listbox.configure(state="normal")
            self.profiles_listbox.delete(0, tk.END)
            error_text = msg or "Error al cargar perfiles remotos"
            self.profiles_listbox.insert(tk.END, error_text)
        self._profiles_remote_backoff_until = time.time(
        ) + self._profiles_remote_retry_cooldown_sec

    def _register_tooltip(self, widget: tk.Widget | None, text: str) -> None:
        if widget is None:
            return
        try:
            if not widget.winfo_exists():
                return
        except Exception:
            return
        self._tooltips.append(_Tooltip(widget, text))

    def _set_last_action(self, text: str) -> None:
        self._last_action_text = text.strip() or "-"
        self.last_action_var.set(f"Ultima accion: {self._last_action_text}")

    def _register_recent_error(self, text: str) -> None:
        detail = text.strip() or "Error sin detalle"
        self._recent_errors.insert(0, detail)
        self._recent_errors = self._recent_errors[:3]
        self.recent_error_var.set(f"Error reciente: {self._recent_errors[0]}")

    def _format_active_container_text(self) -> tuple[str, str]:
        if not self._container_rows_snapshot:
            return "-", "-"

        chosen_name = "-"
        chosen_port = "-"
        for name, state, _health, port, _image in self._container_rows_snapshot:
            if name == "(sin contenedores)":
                continue
            chosen_name = name
            chosen_port = port if state == "ARRANCADO" and port != "-" else chosen_port
            if state == "ARRANCADO":
                break

        if chosen_name == "-":
            first = self._container_rows_snapshot[0]
            chosen_name = first[0]
            chosen_port = first[3] if first[3] else "-"
        return chosen_name, chosen_port

    def _refresh_observability_panel(self) -> None:
        docker_text = "Docker: disponible" if self._docker_last_ready else "Docker: no disponible"
        if self.docker_mode == "remote" and self.docker_host:
            docker_text = f"{docker_text} ({self.docker_host})"
        self.docker_ready_var.set(docker_text)
        active_name, active_port = self._format_active_container_text()
        self.active_container_var.set(f"Contenedor activo: {active_name}")
        self.active_port_var.set(f"Puerto activo: {active_port}")
        self.mode_state_var.set(
            f"Modo: remoto" if self.docker_mode == "remote" and self.docker_host else "Modo: local"
        )
        if self._recent_errors:
            self.recent_error_var.set(
                f"Error reciente: {self._recent_errors[0]}")
        elif not self.recent_error_var.get().strip():
            self.recent_error_var.set("Error reciente: -")

    def _mark_docker_state(self, available: bool) -> None:
        self._docker_last_ready = available
        self._refresh_observability_panel()

    def _apply_app_icon(self) -> None:
        """Configura un icono estilo cesta de Shopify sin depender de archivos externos."""
        try:
            icon_file = self._find_first_existing(["shopify_basket.ico"])
            if icon_file and os.path.isfile(icon_file) and sys.platform == "win32":
                self.root.iconbitmap(icon_file)
                return
        except Exception:
            pass

        try:
            icon = tk.PhotoImage(width=64, height=64)

            # Fondo transparente no fiable en todos los WM: usamos fondo claro neutro.
            icon.put("#f6f6f7", to=(0, 0, 64, 64))

            # Cuerpo de la cesta
            icon.put("#95bf47", to=(8, 20, 56, 58))
            icon.put("#5e8e3e", to=(8, 20, 56, 24))

            # Asa
            icon.put("#5e8e3e", to=(18, 12, 46, 16))
            icon.put("#5e8e3e", to=(16, 14, 20, 20))
            icon.put("#5e8e3e", to=(44, 14, 48, 20))

            # Marca simplificada estilo Shopify (S blanca)
            icon.put("#ffffff", to=(22, 28, 42, 32))
            icon.put("#ffffff", to=(22, 32, 26, 40))
            icon.put("#ffffff", to=(22, 40, 42, 44))
            icon.put("#ffffff", to=(38, 44, 42, 50))
            icon.put("#ffffff", to=(22, 50, 42, 54))

            self.root.iconphoto(True, icon)
            self._app_icon_image = icon
        except Exception:
            pass

    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Utilidades Shopify + Docker")
        self.root.geometry("1280x720")
        self.root.minsize(820, 500)
        self.root.configure(background="#f6f6f7")

        self.app_dir = os.path.dirname(os.path.abspath(__file__))
        self.tools_dir = os.path.dirname(self.app_dir)
        self._apply_app_icon()
        try:
            self.root.state("zoomed")
        except tk.TclError:
            self.root.attributes("-zoomed", True)

        self.profiles_file = os.path.join(self.app_dir, "perfiles_shopify.ini")
        self.private_profiles_dir = os.path.join(os.environ.get(
            "LOCALAPPDATA", self.tools_dir), "ShopifyUtilidades")
        self.private_profiles_file = os.path.join(
            self.private_profiles_dir, "private_profiles.json")
        self.remote_profiles_volume = "shu_profiles_remote"
        self.remote_profiles_path = "/data/profiles.json"
        self.remote_history_volume = "shu_history_remote"
        self.remote_history_path = "/data/historial_gestor.log"
        self.history_file = os.path.join(self.app_dir, "historial_shopify.log")
        self.audit_actor = self._build_audit_actor()
        self._migrate_legacy_state_files()

        self.status_var = tk.StringVar(value="Docker: comprobando...")
        self.last_refresh_var = tk.StringVar(value="Ultima actualizacion: -")
        self.connection_mode_var = tk.StringVar(value="Modo: local")
        self.docker_ready_var = tk.StringVar(value="Docker: comprobando...")
        self.mode_state_var = tk.StringVar(value="Modo: local")
        self.active_container_var = tk.StringVar(value="Contenedor activo: -")
        self.active_port_var = tk.StringVar(value="Puerto activo: -")
        self.last_action_var = tk.StringVar(value="Ultima accion: -")
        self.recent_error_var = tk.StringVar(value="Error reciente: -")
        self.profile_name_var = tk.StringVar(value="")
        self.profile_scope_var = tk.StringVar(value="privado")
        self.network_container_var = tk.StringVar(value="")
        self.network_driver_var = tk.StringVar(value="bridge")
        self.volume_driver_var = tk.StringVar(value="local")
        self.history_level_var = tk.StringVar(value="TODOS")
        self.history_search_var = tk.StringVar(value="")
        self.log_container_var = tk.StringVar(value="")
        self.log_lines_var = tk.StringVar(value="100")
        self.log_auto_refresh_var = tk.BooleanVar(value=False)
        self.log_follow_var = tk.BooleanVar(value=False)
        self.docker_mode = "remote"
        self.docker_host = "tcp://192.168.200.51:2375"
        self.discovered_lan_hosts: list[str] = []
        self.docker_cli_available: bool | None = None
        self.docker_sdk_client: object | None = None
        self._sdk_last_fail_at: float = 0.0
        self._sdk_retry_cooldown_sec: float = 5.0
        self._docker_last_ready = False
        self._docker_last_checked_at = 0.0
        self._docker_check_in_progress = False
        self._docker_check_queue: queue.Queue[tuple[bool, str, str]] = queue.Queue(
        )
        self._docker_check_job_id: str | None = None
        self._history_refresh_in_progress = False
        self._history_refresh_requested = False
        self._history_refresh_queue: queue.Queue[tuple[bool, object]] = queue.Queue(
        )
        self._history_refresh_job_id: str | None = None
        self._history_pending_lines: list[str] = []
        self._history_pending_lock = threading.Lock()
        self._profiles_loading = False
        self._profiles_load_requested = False
        self._profiles_load_queue: queue.Queue[tuple[str, bool, object]] = queue.Queue(
        )
        self._profiles_load_job_id: str | None = None
        self._profiles_load_guard_job_id: str | None = None
        self._profiles_loading_scope: str | None = None
        self._profiles_pending_name: str | None = None
        self._profiles_load_started_at: float = 0.0
        self._profiles_load_timeout_sec: float = 15.0
        self._profiles_remote_retry_cooldown_sec: float = 20.0
        self._profiles_remote_backoff_until: float = 0.0
        self._helper_label_key = "shu.helper"
        self._helper_label_value = "1"
        self._helper_cleanup_in_progress = False
        self._helper_cleanup_last_at: float = 0.0

        self.refresh_job_id: str | None = None
        self.logs_refresh_job_id: str | None = None
        self.logs_follow_poll_job_id: str | None = None
        self.container_cache: list[str] = []
        self.container_image_cache: dict[str, str] = {}
        self.profiles_data: dict[str, list[str]] = {}
        self.private_profiles_data: dict[str, list[str]] = {}
        self.remote_profiles_data: dict[str, list[str]] = {}
        self.network_data: dict[str, dict[str, object]] = {}
        self.volume_data: dict[str, dict[str, object]] = {}
        self.history_lines: list[str] = []
        self.logs_follow_process: subprocess.Popen | None = None
        self.logs_follow_queue: queue.Queue[str] = queue.Queue()
        self._sdk_follow_stop_event: threading.Event | None = None
        self._sdk_follow_active = False
        self.docker_autostart_attempted = False
        self.tabs: ttk.Notebook | None = None
        self.dynamic_tabs: dict[str, ttk.Frame] = {}
        self._tooltips: list[_Tooltip] = []
        self._recent_errors: list[str] = []
        self._last_action_text = "-"
        self._container_rows_snapshot: list[tuple[str, str, str, str, str]] = [
            ]
        self.sidebar_frame: tk.Frame | None = None
        self.sidebar_logo_title_label: tk.Label | None = None
        self.sidebar_logo_subtitle_label: tk.Label | None = None
        self.sidebar_status_label: tk.Label | None = None
        self.sidebar_observability_frame: tk.Frame | None = None
        self.sidebar_observability_labels: list[tk.Label] = []
        self.sidebar_shortcuts_frame: tk.Frame | None = None
        self.sidebar_quit_button: tk.Button | None = None
        self.sidebar_nav_buttons: list[tuple[tk.Button, str, str]] = []
        self.is_compact_layout = False
        self._layout_reflow_job: str | None = None
        self.spinner_job_id: str | None = None
        self.spinner_index = 0
        self.spinner_base_text = ""
        # History tab spinner (initialized in _build_history_tab, pre-declared here)
        self._history_spinner_frame: object = None  # type: ignore[assignment]
        self._history_spinner_job: str | None = None
        self._history_spinner_index: int = 0
        # type: ignore[assignment]
        self._history_spinner_dot_label: object = None
        self.docker_status_dot: tk.Label | None = None
        self.connection_mode_badge: tk.Label | None = None
        self.container_action_btns: list[ttk.Button] = []
        self.profile_action_btns: list[ttk.Button] = []
        self._container_spinner_job: str | None = None
        self._container_spinner_items: list[str] = []
        self._container_spinner_frame: int = 0
        self._container_loading_job: str | None = None
        self._container_loading_frame: int = 0
        self._profile_spinner_job: str | None = None
        self._profile_spinner_name: str = ""
        self._profile_spinner_frame2: int = 0
        self.container_admin_tree: ttk.Treeview | None = None
        self.history_tab_frame: ttk.Frame | None = None
        self.container_tab_frame: ttk.Frame | None = None
        self.volumes_tab_frame: ttk.Frame | None = None
        self.profiles_tab_frame: ttk.Frame | None = None
        self.networks_tab_frame: ttk.Frame | None = None
        self.logs_tab_frame: ttk.Frame | None = None
        self._last_auto_heavy_refresh_at: float = 0.0
        self._auto_heavy_refresh_interval_sec: float = 30.0
        self._shopify_auth_monitor_job: str | None = None
        self._shopify_auth_monitor_running = False
        self._shopify_auth_prompt_seen: dict[str, str] = {}
        self._shopify_auth_prompt_last_shown_at: dict[str, float] = {}
        self._shopify_auth_prompt_cooldown_sec = 300.0
        self._shopify_auth_dialog_active = False
        self._shopify_auth_monitor_interval_ms = 8000

        self._configure_styles()
        self._build_ui()
        self._bind_global_shortcuts()
        if not self._prompt_startup_connection_mode():
            self.root.after(10, self.root.destroy)
            return
        self.status_var.trace_add("write", self._update_status_dot)
        self._update_connection_mode_badge()
        self.refresh_everything()
        self._schedule_shopify_auth_monitor(4000)

        # Comprobar actualizaciones en segundo plano (2s de retraso para que la UI esté lista)
        self.root.after(2000, self._start_update_check)

    # ── Actualización automática ──────────────────────────────────────────────

    def _start_update_check(self) -> None:
        """Lanza la comprobación de versión en hilo secundario."""
        t = threading.Thread(
            target=_check_for_updates_worker,
            args=(APP_VERSION, self._on_update_available),
            daemon=True,
        )
        t.start()

    def _on_update_available(self, update_info: dict) -> None:
        """Callback ejecutado desde el hilo de red; despacha al hilo principal de Tkinter."""
        self.root.after(0, lambda: self._show_update_dialog(update_info))

    def _show_update_dialog(self, update_info: dict) -> None:
        """Muestra una ventana de actualización con progreso de descarga."""
        remote_ver = update_info.get("version", "?")
        download_url = _select_download_url(update_info)
        notes = update_info.get("notes", "")

        dlg = tk.Toplevel(self.root)
        dlg.title("Actualización disponible")
        dlg.geometry("500x420")
        dlg.minsize(500, 420)
        dlg.resizable(True, True)
        dlg.grab_set()
        dlg.configure(bg="#f6f6f7")

        # Centrar sobre la ventana principal
        self.root.update_idletasks()
        x = self.root.winfo_x() + (self.root.winfo_width() - 500) // 2
        y = self.root.winfo_y() + (self.root.winfo_height() - 420) // 2
        dlg.geometry(f"+{x}+{y}")

        tk.Label(
            dlg,
            text=f"🆕  Nueva versión disponible: v{remote_ver}",
            font=("Segoe UI Semibold", 13),
            bg="#f6f6f7",
            fg="#008060",
        ).pack(pady=(22, 4))

        tk.Label(
            dlg,
            text=f"Versión instalada: v{APP_VERSION}",
            font=("Segoe UI", 10),
            bg="#f6f6f7",
            fg="#6d7175",
        ).pack()

        progress_var = tk.DoubleVar(value=0)
        status_var = tk.StringVar(value="")
        progress_panel, _ = self._build_progress_panel(
            dlg,
            "Actualizacion",
            "Descarga la nueva version y aplica el reemplazo de forma segura.",
            status_var,
            progress_var,
            style_name="AppWarm.Horizontal.TProgressbar",
        )
        progress_panel.pack(side="bottom", fill="x", padx=22, pady=(8, 14))

        btn_frame = tk.Frame(dlg, bg="#f6f6f7")
        btn_frame.pack(side="bottom", pady=(0, 6))

        update_btn = ttk.Button(
            btn_frame, text="⬇  Descargar e instalar", style="Accent.TButton")
        skip_btn = ttk.Button(btn_frame, text="Ahora no",
                              style="Ghost.TButton")
        update_btn.pack(side="left", padx=6)
        skip_btn.pack(side="left", padx=6)

        notes_frame = tk.Frame(dlg, bg="#f6f6f7")
        notes_frame.pack(fill="both", expand=True, padx=22, pady=(10, 6))
        if notes:
            tk.Label(
                notes_frame,
                text="Notas de la actualización",
                font=("Segoe UI Semibold", 10),
                bg="#f6f6f7",
                fg="#202223",
                anchor="w",
            ).pack(fill="x", pady=(0, 4))

            notes_box_wrap = tk.Frame(notes_frame, bg="#f6f6f7")
            notes_box_wrap.pack(fill="both", expand=True)

            notes_scroll = ttk.Scrollbar(notes_box_wrap, orient="vertical")
            notes_text = tk.Text(
                notes_box_wrap,
                height=7,
                wrap="word",
                font=("Segoe UI", 10),
                bg="#ffffff",
                fg="#4b5563",
                relief="solid",
                borderwidth=1,
                yscrollcommand=notes_scroll.set,
            )
            notes_scroll.config(command=notes_text.yview)
            notes_scroll.pack(side="right", fill="y")
            notes_text.pack(side="left", fill="both", expand=True)
            notes_text.insert("1.0", notes)
            notes_text.configure(state="disabled")
        else:
            tk.Label(
                notes_frame,
                text="Esta versión incluye mejoras y correcciones de estabilidad.",
                font=("Segoe UI", 10),
                bg="#f6f6f7",
                fg="#6d7175",
                wraplength=440,
                justify="left",
            ).pack(fill="x")

        def do_skip() -> None:
            dlg.destroy()

        def do_update() -> None:
            if not download_url:
                mode = ".exe" if _is_frozen_app() else ".py"
                messagebox.showerror(
                    "Error",
                    f"No hay URL de descarga configurada para modo {mode}.",
                    parent=dlg,
                )
                return
            update_btn.configure(state="disabled")
            skip_btn.configure(state="disabled")
            status_var.set("Iniciando descarga...")
            t = threading.Thread(
                target=self._download_and_apply_update,
                args=(download_url, dlg, status_var,
                      progress_var, update_btn, skip_btn),
                daemon=True,
            )
            t.start()

        update_btn.configure(command=do_update)
        skip_btn.configure(command=do_skip)

    def _download_and_apply_update(
        self,
        url: str,
        dlg: tk.Toplevel,
        status_var: tk.StringVar,
        progress_var: tk.DoubleVar,
        update_btn: ttk.Button,
        skip_btn: ttk.Button,
    ) -> None:
        """Descarga la actualización con barra de progreso y luego aplica el reemplazo."""

        def ui(fn: "Callable[[], None]") -> None:
            """Despacha al hilo principal de forma segura."""
            try:
                self.root.after(0, fn)
            except Exception:
                pass

        current_target = _current_install_target()
        restart_cmd = _restart_command_for_target(current_target)

        parsed = urllib.parse.urlparse(url)
        _, ext = os.path.splitext(parsed.path)
        if not ext:
            ext = ".tmp"

        fd, tmp_path = tempfile.mkstemp(prefix="wpu_new_", suffix=ext)
        os.close(fd)

        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": f"ShopifyUtilidades-Updater/{APP_VERSION}"})
            with urllib.request.urlopen(req, timeout=60) as resp:
                total = int(resp.headers.get("Content-Length") or 0)
                downloaded = 0
                chunk_size = 16 * 1024

                with open(tmp_path, "wb") as fh:
                    while True:
                        chunk = resp.read(chunk_size)
                        if not chunk:
                            break
                        fh.write(chunk)
                        downloaded += len(chunk)
                        if total > 0:
                            pct = downloaded / total * 100
                            _pct = pct  # closure capture
                            ui(lambda p=_pct: progress_var.set(p))
                        kb = downloaded // 1024
                        _kb = kb
                        ui(lambda k=_kb: status_var.set(
                            f"Descargando... {k} KB"))

                if total > 0 and downloaded != total:
                    raise RuntimeError(
                        f"Descarga incompleta ({downloaded} de {total} bytes)."
                    )

            # Verificación mínima: que el archivo no esté vacío
            size = os.path.getsize(tmp_path)
            if size < 100:
                raise RuntimeError(
                    "El archivo descargado parece incompleto o inválido.")

            # Validaciones de integridad según tipo de actualización.
            if _is_frozen_app():
                # Un ejecutable PE en Windows debe iniciar con bytes MZ.
                with open(tmp_path, "rb") as fh:
                    magic = fh.read(2)
                if magic != b"MZ":
                    raise RuntimeError(
                        "El archivo descargado no parece un .exe válido (firma PE inválida)."
                    )
            else:
                # Para modo script, validar que el contenido sea texto Python razonable.
                with open(tmp_path, "rb") as fh:
                    head = fh.read(4096)
                try:
                    head_text = head.decode("utf-8", errors="ignore")
                except Exception:
                    head_text = ""
                if not any(tok in head_text for tok in ("import ", "def ", "class ", "#")):
                    raise RuntimeError(
                        "El archivo descargado no parece un script Python válido."
                    )

            ui(lambda: progress_var.set(100))
            ui(lambda: status_var.set(
                "Descarga completada. Aplicando actualización..."))

            # Pequeña pausa para que el usuario vea el mensaje
            time.sleep(1.2)

            # Cerrar diálogo y lanzar el actualizador externo
            def _apply() -> None:
                try:
                    dlg.destroy()
                except Exception:
                    pass
                _launch_updater_and_exit(tmp_path, current_target, restart_cmd)

            ui(_apply)

        except Exception as exc:
            try:
                os.remove(tmp_path)
            except OSError:
                pass

            def _err(msg: str = str(exc), failed_url: str = url) -> None:
                status_var.set(f"Error: {msg}")
                try:
                    update_btn.configure(state="normal")
                    skip_btn.configure(state="normal")
                except Exception:
                    pass
                messagebox.showerror(
                    "Error de actualización",
                    (
                        "No se pudo descargar la actualización:\n\n"
                        f"{msg}\n\n"
                        "URL consultada:\n"
                        f"{failed_url}"
                    ),
                    parent=dlg,
                )

            ui(_err)

    # ── Fin actualización automática ──────────────────────────────────────────

    def _configure_styles(self) -> None:
        style = ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass

        # ── Visual system ───────────────────────────────────────────────────
        bg = "#edf2f7"
        surface = "#ffffff"
        surface2 = "#f3f6fb"
        text = "#0f172a"
        text2 = "#5b6778"
        muted = "#748097"
        accent = "#0f766e"
        accent_hv = "#115e59"
        accent_lt = "#d8f3ef"
        border = "#cbd5e1"
        selected = "#e6fbf7"
        danger = "#dc2626"

        self.root.configure(bg=bg)

        style.configure(".", font=("Segoe UI", 10),
                        background=bg, foreground=text)
        style.configure("TFrame", background=bg)
        style.configure("Card.TFrame", background=surface)
        style.configure("TLabel", background=bg, foreground=text)
        style.configure("Surface.TLabel", background=surface, foreground=text)
        style.configure("Title.TLabel", font=(
            "Segoe UI Semibold", 15), background=bg, foreground=text)
        style.configure("Section.TLabel", font=(
            "Segoe UI Semibold", 11), background=bg, foreground=accent)
        style.configure("Muted.TLabel", background=bg,
                        foreground=muted, font=("Segoe UI", 9))
        style.configure("Chip.TLabel", background=accent_lt,
                        foreground=accent, font=("Segoe UI Semibold", 9))

        style.configure("TNotebook", background=bg,
                        borderwidth=0, tabmargins=(0, 0, 0, 0))
        style.configure("TNotebook.Tab", padding=(
            16, 10), background="#dfe7ef", foreground=text2, font=("Segoe UI Semibold", 10))
        style.map(
            "TNotebook.Tab",
            background=[("selected", surface), ("active", accent_lt)],
            foreground=[("selected", accent), ("active", text)],
        )

        style.configure(
            "TButton",
            padding=(12, 9),
            background="#f3f6fb",
            foreground=text,
            borderwidth=1,
            relief="solid",
            bordercolor=border,
        )
        style.map(
            "TButton",
            background=[("active", "#e8eef5"), ("pressed", "#c7d2e0")],
            foreground=[("active", text)],
            relief=[("active", "solid")],
        )
        style.configure("Accent.TButton", padding=(
            13, 9), background=accent, foreground="#ffffff", borderwidth=0, relief="flat")
        style.map("Accent.TButton", background=[
                  ("active", accent_hv), ("pressed", "#0b534f")], relief=[("active", "flat")])
        style.configure("Ghost.TButton", padding=(
            9, 6), background=bg, foreground=text2, borderwidth=0, relief="flat")
        style.map("Ghost.TButton", background=[
                  ("active", "#e8eef5")], relief=[("active", "flat")])
        style.configure("Admin.TButton", padding=(
            10, 8), background="#f3f6fb", foreground=text, borderwidth=1, relief="solid")
        style.map("Admin.TButton", background=[
                  ("active", "#e8eef5"), ("pressed", "#c7d2e0")], foreground=[("active", text)])
        style.configure("Danger.TButton", padding=(
            10, 8), background="#fff5f5", foreground=danger, borderwidth=1, relief="solid")
        style.map("Danger.TButton", background=[
                  ("active", "#fee2e2"), ("pressed", "#fecaca")], foreground=[("active", "#b91c1c")])

        style.configure("TLabelframe", background=bg,
                        borderwidth=1, relief="solid", bordercolor=border)
        style.configure("TLabelframe.Label", background=bg,
                        foreground=accent, font=("Segoe UI Semibold", 10))

        style.configure("TEntry", fieldbackground=surface,
                        bordercolor=border, insertcolor="#202223")
        style.configure("TCombobox", fieldbackground=surface,
                        bordercolor=border)
        style.map(
            "TCombobox",
            fieldbackground=[("readonly", surface),
                              ("readonly focus", surface)],
            foreground=[("readonly", text), ("readonly focus", text)],
            selectbackground=[("readonly", surface),
                               ("readonly focus", surface)],
            selectforeground=[("readonly", text), ("readonly focus", text)],
        )

        style.configure("Horizontal.TProgressbar", troughcolor="#d8e1ea",
                        background=accent, borderwidth=0, thickness=10)
        style.configure("App.Horizontal.TProgressbar", troughcolor="#d8e1ea",
                        background=accent, borderwidth=0, thickness=14)
        style.configure("AppWarm.Horizontal.TProgressbar", troughcolor="#fde68a",
                        background="#f59e0b", borderwidth=0, thickness=14)
        style.configure("AppInfo.Horizontal.TProgressbar", troughcolor="#bfdbfe",
                        background="#0284c7", borderwidth=0, thickness=14)

        style.configure("TScrollbar", troughcolor=surface2,
                        background="#c9cccf", relief="flat", arrowsize=13)
        style.map("TScrollbar", background=[("active", muted)])

        style.configure(
            "Treeview",
            background=surface,
            fieldbackground=surface,
            foreground=text,
            rowheight=32,
            relief="flat",
            borderwidth=0,
        )
        style.configure(
            "Treeview.Heading",
            background=surface2,
            foreground=text2,
            font=("Segoe UI Semibold", 10),
            relief="flat",
        )
        style.map("Treeview", background=[
                  ("selected", selected)], foreground=[("selected", accent)])
        style.map("Treeview.Heading", background=[("active", border)])

        style.configure("TSeparator", background=border)
        style.configure("TCheckbutton", background=bg, foreground=text)

    def _build_ui(self) -> None:
        _SB = "#0f172a"
        _SBH = "#1e293b"
        _SHD = "#020617"
        _FG = "#e2e8f0"
        _FGM = "#94a3b8"
        _ACC = "#0f766e"
        _ACC2 = "#99f6e4"

        self.root.columnconfigure(0, weight=0)
        self.root.columnconfigure(1, weight=1)
        self.root.rowconfigure(0, weight=1)

        # ── Sidebar ─────────────────────────────────────────────────────────
        sidebar = tk.Frame(self.root, bg=_SB, width=234)
        sidebar.grid(row=0, column=0, sticky="nsw")
        sidebar.grid_propagate(False)
        self.sidebar_frame = sidebar

        # Logo area — Shopify brand
        logo_f = tk.Frame(sidebar, bg=_SHD, padx=18, pady=18)
        logo_f.pack(fill="x")
        tk.Frame(logo_f, bg=_ACC, height=3).pack(fill="x", pady=(0, 14))
        # Shopify bag icon (Unicode shopping bag) + brand name
        icon_row = tk.Frame(logo_f, bg=_SHD)
        icon_row.pack(anchor="w", fill="x")
        tk.Label(icon_row, text="\U0001f6cd", fg="#38bdf8", bg=_SHD,
                 font=("Segoe UI", 18)).pack(side="left")
        title_col = tk.Frame(icon_row, bg=_SHD)
        title_col.pack(side="left", padx=(8, 0))
        self.sidebar_logo_title_label = tk.Label(title_col, text="Shopify", fg="#ffffff", bg=_SHD,
                     font=("Segoe UI Semibold", 14))
        self.sidebar_logo_title_label.pack(anchor="w")
        self.sidebar_logo_subtitle_label = tk.Label(title_col, text="Docker Utilities", fg=_ACC2, bg=_SHD,
                                font=("Segoe UI", 8))
        self.sidebar_logo_subtitle_label.pack(anchor="w")
        tk.Frame(logo_f, bg="#1f2937", height=1).pack(fill="x", pady=(14, 0))

        # Navigation buttons
        nav_items = [
            ("\u2699  Crear / Recrear entorno",   self.open_setup_wizard),
            ("\u2b07  Importar theme/datos",       self.open_import_wizard),
            ("\u2b06  Exportar theme/datos",       self.open_export_wizard),
            ("\u25b6  Gestionar contenedores",     self.open_containers_manager),
            ("\U0001f4d8  Abrir documentacion",    self.open_docs),
        ]
        nav_f = tk.Frame(sidebar, bg=_SB)
        nav_f.pack(fill="x", pady=(6, 0))
        self.sidebar_nav_buttons = []
        for label, cmd in nav_items:
            compact_label = label.split("  ", 1)[0].strip()
            btn = tk.Button(
                nav_f, text=label, command=cmd, anchor="w",
                padx=18, pady=11, relief="flat", bd=0,
                bg=_SB, fg=_FG,
                activebackground=_SBH, activeforeground="#ffffff",
                font=("Segoe UI", 10), cursor="hand2", highlightthickness=0,
            )
            btn.pack(fill="x")
            btn.bind("<Enter>", lambda e,
                     b=btn: b.configure(bg=_SBH, fg="#ffffff"))
            btn.bind("<Leave>", lambda e, b=btn: b.configure(bg=_SB, fg=_FG))
            self.sidebar_nav_buttons.append((btn, label, compact_label))
            self._register_tooltip(btn, label.replace("  ", "\n", 1))

        obs_f = tk.Frame(sidebar, bg="#111827", padx=12, pady=10)
        obs_f.pack(fill="x", padx=12, pady=(10, 0))
        tk.Label(obs_f, text="Estado", fg="#f8fafc", bg="#111827",
                 font=("Segoe UI Semibold", 9)).pack(anchor="w")
        obs_lines = [
            self.docker_ready_var,
            self.mode_state_var,
            self.active_container_var,
            self.active_port_var,
            self.last_action_var,
            self.recent_error_var,
        ]
        self.sidebar_observability_labels = []
        for var in obs_lines:
            lbl = tk.Label(
                obs_f,
                textvariable=var,
                fg="#dbeafe",
                bg="#111827",
                font=("Segoe UI", 8),
                anchor="w",
                justify="left",
                wraplength=192,
            )
            lbl.pack(anchor="w", fill="x", pady=(4, 0))
            self.sidebar_observability_labels.append(lbl)
        self.sidebar_observability_frame = obs_f

        shortcuts_f = tk.Frame(sidebar, bg=_SB, padx=16, pady=8)
        shortcuts_f.pack(fill="x")
        tk.Label(shortcuts_f, text="Atajos", fg="#f8fafc", bg=_SB, font=("Segoe UI Semibold", 9)).grid(
            row=0, column=0, columnspan=2, sticky="w", pady=(0, 6)
        )

        shortcuts_left = [
            "Ctrl+R  Refrescar",
            "Ctrl+I  Importar",
            "Ctrl+E  Exportar",
        ]
        shortcuts_right = [
            "Ctrl+L  Crear entorno",
            "Ctrl+B  Compacto",
            "F1  Documentacion",
            "Ctrl+Q  Salir",
        ]

        left_col = tk.Frame(shortcuts_f, bg=_SB)
        left_col.grid(row=1, column=0, sticky="nw", padx=(0, 10))
        right_col = tk.Frame(shortcuts_f, bg=_SB)
        right_col.grid(row=1, column=1, sticky="nw")

        for idx, shortcut in enumerate(shortcuts_left):
            tk.Label(left_col, text=shortcut, fg="#94a3b8", bg=_SB, font=(
                "Segoe UI", 8), anchor="w").pack(anchor="w", pady=1)
        for idx, shortcut in enumerate(shortcuts_right):
            tk.Label(right_col, text=shortcut, fg="#94a3b8", bg=_SB, font=(
                "Segoe UI", 8), anchor="w").pack(anchor="w", pady=1)

        self.sidebar_shortcuts_frame = shortcuts_f

        # Separator
        tk.Frame(sidebar, bg="#333333", height=1).pack(
            fill="x", padx=16, pady=(12, 6))

        # Close button
        quit_b = tk.Button(
            sidebar, text="\u00d7  Cerrar aplicacion", command=self.on_close,
            anchor="w", padx=18, pady=10, relief="flat", bd=0,
            bg=_SB, fg=_FGM,
            activebackground="#7f1d1d", activeforeground="#fca5a5",
            font=("Segoe UI", 10), cursor="hand2", highlightthickness=0,
        )
        quit_b.pack(fill="x")
        quit_b.bind("<Enter>", lambda e: quit_b.configure(
            bg="#7f1d1d", fg="#fca5a5"))
        quit_b.bind("<Leave>", lambda e: quit_b.configure(bg=_SB, fg=_FGM))
        self.sidebar_quit_button = quit_b
        self._register_tooltip(quit_b, "Cerrar la aplicacion")

        # Docker status at the bottom of the sidebar
        status_f = tk.Frame(sidebar, bg=_SHD, padx=16, pady=12)
        status_f.pack(side="bottom", fill="x")
        dot_row = tk.Frame(status_f, bg=_SHD)
        dot_row.pack(fill="x")
        self.docker_status_dot = tk.Label(dot_row, text="\u25cf", fg="#888888", bg=_SHD,
                                          font=("Segoe UI", 12))
        self.docker_status_dot.pack(side="left")
        self.sidebar_status_label = tk.Label(
            dot_row,
            textvariable=self.status_var,
            fg="#cbd5e1",
            bg=_SHD,
            font=("Segoe UI", 9),
            wraplength=160,
            justify="left",
        )
        self.sidebar_status_label.pack(side="left", padx=(6, 0))
        self._register_tooltip(self.sidebar_status_label,
                               "Estado rapido de Docker y la interfaz")

        # ── Main content area ────────────────────────────────────────────────
        main = tk.Frame(self.root, bg="#edf2f7")
        main.grid(row=0, column=1, sticky="nsew")
        main.columnconfigure(0, weight=1)
        main.rowconfigure(2, weight=1)

        # Header bar (title + status badges)
        hdr = tk.Frame(main, bg="#f8fafc", padx=22, pady=16)
        hdr.grid(row=0, column=0, sticky="ew")
        hdr.columnconfigure(0, weight=1)
        left_hdr = tk.Frame(hdr, bg="#f8fafc")
        left_hdr.grid(row=0, column=0, sticky="w")
        tk.Label(left_hdr, text="Shopify Utilities", fg="#0f172a", bg="#f8fafc",
                 font=("Segoe UI Semibold", 16)).pack(anchor="w")
        tk.Label(left_hdr, text="Administra entornos, themes y Docker desde una sola interfaz.",
                 fg="#64748b", bg="#f8fafc", font=("Segoe UI", 9)).pack(anchor="w", pady=(2, 0))

        badge_row = tk.Frame(hdr, bg="#f8fafc")
        badge_row.grid(row=0, column=1, sticky="e")
        tk.Label(
            badge_row,
            text=f"v{APP_VERSION}",
            fg="#0f766e",
            bg="#ccfbf1",
            font=("Segoe UI Semibold", 8),
            padx=8,
            pady=3,
        ).pack(side="left")
        self.connection_mode_badge = tk.Label(
            badge_row,
            textvariable=self.connection_mode_var,
            fg="#0f172a",
            bg="#e2e8f0",
            font=("Segoe UI Semibold", 9),
            padx=8,
            pady=3,
        )
        self.connection_mode_badge.pack(side="left", padx=(10, 0))
        mode_btn = ttk.Button(
            badge_row,
            text="Cambiar modo Docker",
            style="Ghost.TButton",
            command=self.change_connection_mode,
        )
        mode_btn.pack(side="left", padx=(10, 0))
        self._register_tooltip(mode_btn, "Alterna entre Docker local y remoto")
        self._register_tooltip(
            self.connection_mode_badge, "Modo Docker actual")
        tk.Label(badge_row, textvariable=self.last_refresh_var, fg="#64748b", bg="#f8fafc",
                 font=("Segoe UI", 9)).pack(side="left", padx=(12, 0))

        # Thin accent separator under header
        tk.Frame(main, bg="#0f766e", height=3).grid(
            row=1, column=0, sticky="ew")

        # Notebook wrapper
        nb_shell = tk.Frame(main, bg="#cbd5e1", padx=1, pady=1)
        nb_shell.grid(row=2, column=0, sticky="nsew", padx=16, pady=14)
        nb_shell.columnconfigure(0, weight=1)
        nb_shell.rowconfigure(0, weight=1)

        nb_wrap = ttk.Frame(nb_shell, padding=(16, 12))
        nb_wrap.grid(row=0, column=0, sticky="nsew")
        nb_wrap.columnconfigure(0, weight=1)
        nb_wrap.rowconfigure(0, weight=1)

        self.tabs = ttk.Notebook(nb_wrap)
        self.tabs.grid(row=0, column=0, sticky="nsew")

        container_tab = ttk.Frame(self.tabs, padding=10)
        volumes_tab = ttk.Frame(self.tabs, padding=10)
        profiles_tab = ttk.Frame(self.tabs, padding=10)
        networks_tab = ttk.Frame(self.tabs, padding=10)
        history_tab = ttk.Frame(self.tabs, padding=10)
        logs_tab = ttk.Frame(self.tabs, padding=10)

        self.tabs.add(container_tab, text="  Contenedores  ")
        self.tabs.add(volumes_tab,   text="  Volumenes  ")
        self.tabs.add(profiles_tab,  text="  Perfiles  ")
        self.tabs.add(networks_tab,  text="  Redes  ")
        self.tabs.add(history_tab,   text="  Historial  ")
        self.tabs.add(logs_tab,      text="  Logs  ")

        self.container_tab_frame = container_tab
        self.volumes_tab_frame = volumes_tab
        self.profiles_tab_frame = profiles_tab
        self.networks_tab_frame = networks_tab
        self.logs_tab_frame = logs_tab

        self._build_containers_tab(container_tab)
        self._build_volumes_tab(volumes_tab)
        self._build_profiles_tab(profiles_tab)
        self._build_networks_tab(networks_tab)
        self._build_history_tab(history_tab)
        self._build_logs_tab(logs_tab)
        self.history_tab_frame = history_tab
        self.tabs.bind("<<NotebookTabChanged>>", self._on_history_tab_selected)

        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.root.bind("<Configure>", self._schedule_layout_reflow, add="+")
        self.root.after(120, self._apply_responsive_layout)

    def _create_scrollable_surface(self, parent: ttk.Frame, padding: tuple[int, int] = (10, 10)) -> ttk.Frame:
        host = ttk.Frame(parent, style="Card.TFrame")
        host.pack(fill="both", expand=True)
        host.columnconfigure(0, weight=1)
        host.rowconfigure(0, weight=1)

        canvas = tk.Canvas(
            host,
            background="#f6f6f7",
            borderwidth=0,
            highlightthickness=0,
            relief="flat",
        )
        canvas.grid(row=0, column=0, sticky="nsew")

        y_scroll = ttk.Scrollbar(host, orient="vertical", command=canvas.yview)
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll = ttk.Scrollbar(
            host, orient="horizontal", command=canvas.xview)
        x_scroll.grid(row=1, column=0, sticky="ew")
        canvas.configure(yscrollcommand=y_scroll.set,
                         xscrollcommand=x_scroll.set)

        content = ttk.Frame(canvas, padding=padding)
        window_id = canvas.create_window((0, 0), window=content, anchor="nw")

        def sync_scroll_region(_event: object = None) -> None:
            canvas.configure(scrollregion=canvas.bbox("all"))

        def fit_content_width(event: tk.Event) -> None:
            required_width = content.winfo_reqwidth()
            canvas.itemconfigure(window_id, width=max(
                event.width, required_width))

        def _on_mouse_wheel(event: tk.Event) -> str:
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
            return "break"

        content.bind("<Configure>", sync_scroll_region)
        canvas.bind("<Configure>", fit_content_width)
        content.bind("<Enter>", lambda _e: canvas.bind_all(
            "<MouseWheel>", _on_mouse_wheel))
        content.bind("<Leave>", lambda _e: canvas.unbind_all("<MouseWheel>"))
        self.root.after(50, sync_scroll_region)
        return content

    def _bind_progress_percent(self, progress_var: tk.DoubleVar) -> tk.StringVar:
        percent_var = tk.StringVar(value="0%")

        def sync(*_args: object) -> None:
            try:
                value = float(progress_var.get())
            except Exception:
                value = 0.0
            value = max(0.0, min(100.0, value))
            percent_var.set(f"{value:.0f}%")

        try:
            progress_var.trace_add("write", sync)
        except Exception:
            pass
        sync()
        return percent_var

    def _build_progress_panel(
        self,
        parent: tk.Misc,
        title: str,
        subtitle: str,
        status_var: tk.StringVar | None,
        progress_var: tk.DoubleVar,
        style_name: str = "App.Horizontal.TProgressbar",
    ) -> tuple[tk.Frame, tk.StringVar]:
        panel = tk.Frame(parent, bg="#f8fafc", padx=14, pady=12,
                         highlightthickness=1, highlightbackground="#d8e1ea")
        panel.columnconfigure(0, weight=1)

        header = tk.Frame(panel, bg="#f8fafc")
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)

        tk.Label(
            header,
            text=title,
            bg="#f8fafc",
            fg="#0f172a",
            font=("Segoe UI Semibold", 10),
        ).grid(row=0, column=0, sticky="w")

        percent_var = self._bind_progress_percent(progress_var)
        tk.Label(
            header,
            textvariable=percent_var,
            bg="#d8f3ef",
            fg="#0f766e",
            font=("Segoe UI Semibold", 9),
            padx=8,
            pady=2,
        ).grid(row=0, column=1, sticky="e")

        tk.Label(
            panel,
            text=subtitle,
            bg="#f8fafc",
            fg="#64748b",
            font=("Segoe UI", 9),
            justify="left",
            wraplength=560,
        ).grid(row=1, column=0, sticky="ew", pady=(4, 8))

        bar = ttk.Progressbar(
            panel,
            orient="horizontal",
            mode="determinate",
            maximum=100,
            variable=progress_var,
            style=style_name,
        )
        bar.grid(row=2, column=0, sticky="ew")

        if status_var is not None:
            tk.Label(
                panel,
                textvariable=status_var,
                bg="#f8fafc",
                fg="#0f172a",
                font=("Segoe UI", 9),
                justify="left",
                wraplength=560,
            ).grid(row=3, column=0, sticky="ew", pady=(8, 0))

        return panel, percent_var

    def open_containers_manager(self) -> None:
        if not self.docker_ready():
            messagebox.showerror("Docker", self._docker_unavailable_message())
            return

        window = self._open_or_focus_work_tab(
            "container_admin", "Gestion contenedores")
        if window is None:
            messagebox.showerror(
                "Interfaz", "No se pudo abrir la pestaña de gestión de contenedores.")
            return

        for child in window.winfo_children():
            child.destroy()

        outer = ttk.Frame(window, padding=4)
        outer.pack(fill="both", expand=True)
        outer.columnconfigure(0, weight=1)
        outer.rowconfigure(1, weight=1)
        self._add_work_tab_header(
            outer, "Gestión avanzada de contenedores", "container_admin")

        table_frame = ttk.Frame(outer)
        table_frame.grid(row=1, column=0, sticky="nsew")
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(0, weight=1)
        table_frame.rowconfigure(1, weight=0)

        cols = ("name", "state", "image", "ports", "protection")
        self.container_admin_tree = ttk.Treeview(
            table_frame, columns=cols, show="headings", selectmode="browse")
        self.container_admin_tree.heading("name", text="Contenedor")
        self.container_admin_tree.heading("state", text="Estado")
        self.container_admin_tree.heading("image", text="Imagen")
        self.container_admin_tree.heading("ports", text="Puertos")
        self.container_admin_tree.heading("protection", text="Proteccion")
        self.container_admin_tree.column("name", width=200, anchor="w")
        self.container_admin_tree.column("state", width=120, anchor="center")
        self.container_admin_tree.column("image", width=280, anchor="w")
        self.container_admin_tree.column("ports", width=260, anchor="w")
        self.container_admin_tree.column("protection", width=320, anchor="w")
        self.container_admin_tree.grid(row=0, column=0, sticky="nsew")

        yscroll = ttk.Scrollbar(
            table_frame, orient="vertical", command=self.container_admin_tree.yview)
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll = ttk.Scrollbar(
            table_frame, orient="horizontal", command=self.container_admin_tree.xview)
        xscroll.grid(row=1, column=0, sticky="ew")
        self.container_admin_tree.configure(
            yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)

        actions = ttk.Frame(outer)
        actions.grid(row=2, column=0, sticky="ew", pady=(10, 0))
        ttk.Button(actions, text="Refrescar", command=self._refresh_container_admin_table,
                   style="Admin.TButton").pack(side="left")
        ttk.Button(actions, text="Renombrar", command=self._rename_container_admin,
                   style="Admin.TButton").pack(side="left", padx=6)
        ttk.Button(actions, text="Borrar", command=self._delete_container_admin,
                   style="Danger.TButton").pack(side="left", padx=6)
        ttk.Button(actions, text="Arrancar", command=lambda: self._toggle_container_admin(
            "start"), style="Admin.TButton").pack(side="left", padx=6)
        ttk.Button(actions, text="Apagar", command=lambda: self._toggle_container_admin(
            "stop"), style="Admin.TButton").pack(side="left", padx=6)
        ttk.Button(actions, text="Seleccionar tema activo", command=self._select_active_theme_admin,
                   style="Admin.TButton").pack(side="left", padx=6)
        ttk.Button(actions, text="Borrar tema", command=self._delete_theme_admin,
                   style="Danger.TButton").pack(side="left", padx=6)
        ttk.Button(actions, text="Acceso Remoto (SSH)", command=self._remote_access_container_admin,
                   style="Admin.TButton").pack(side="left", padx=6)

        self._refresh_container_admin_table()

    def _refresh_container_admin_table(self) -> None:
        if self.container_admin_tree is None or not self.container_admin_tree.winfo_exists():
            return

        for item in self.container_admin_tree.get_children():
            self.container_admin_tree.delete(item)

        code, out, err = self._run(["docker", "ps", "-a", "--format",
                                   "{{.Names}}|{{.Status}}|{{.Image}}|{{.Ports}}|{{.Command}}"])
        if code != 0:
            messagebox.showwarning(
                "Contenedores", err or "No se pudieron listar contenedores.")
            return

        if not out.strip():
            self.container_admin_tree.insert("", "end", values=(
                "(sin contenedores Shopify)", "-", "-", "-", "-"))
            return

        shopify_found = False
        for line in out.splitlines():
            parts = line.split("|", 4)
            if len(parts) < 5:
                continue
            name = parts[0].strip()
            status_raw = parts[1].strip()
            image = parts[2].strip()
            ports = parts[3].strip() or "-"
            command = parts[4].strip()
            service_label = self._container_service_label(name, image)
            is_running = status_raw.lower().startswith("up")
            if service_label not in ("Contenedor de Shopify", "Contenedor Shopify Node"):
                continue
            # Filtrar solo contenedores Shopify
            state = "ARRANCADO" if is_running else "APAGADO"
            shopify_found = True
            protection = self._container_protection_text(name, image)
            tags: list[str] = []
            service_tag = self._container_service_tag(name, image)
            if service_tag:
                tags.append(service_tag)
            self.container_admin_tree.insert("", "end", values=(
                name, state, image, ports, protection), tags=tuple(tags))

        if not shopify_found:
            self.container_admin_tree.insert("", "end", values=(
                "(sin contenedores Shopify)", "-", "-", "-", "-"))

    def _selected_container_admin(self) -> str | None:
        if self.container_admin_tree is None or not self.container_admin_tree.winfo_exists():
            return None
        selected = self.container_admin_tree.selection()
        if not selected:
            return None
        values = self.container_admin_tree.item(selected[0], "values")
        if not values:
            return None
        name = str(values[0]).strip()
        if not name or name in {"(sin contenedores)", "(sin contenedores Shopify)"}:
            return None
        return name

    def _rename_container_admin(self) -> None:
        container = self._selected_container_admin()
        if not container:
            messagebox.showwarning(
                "Contenedores", "Selecciona un contenedor para renombrar.")
            return

        new_name = simpledialog.askstring(
            "Renombrar contenedor", f"Nuevo nombre para '{container}':", initialvalue=container)
        if not new_name:
            return
        new_name = new_name.strip()
        if not new_name or new_name == container:
            messagebox.showwarning("Contenedores", "Nombre nuevo no válido.")
            return

        def _rename_container_operation():
            code, _, err = self._run(["docker", "rename", container, new_name])
            if code != 0:
                raise RuntimeError(
                    err or "No se pudo renombrar el contenedor.")

            self.log_event("CONTAINER", container, "OK",
                           f"Renombrado a {new_name}")
            self.refresh_everything()
            self._refresh_container_admin_table()
            return True

        self._run_with_loading_modal(
            f"Renombrando contenedor {container} a {new_name}", _rename_container_operation)

    def _delete_container_admin(self) -> None:
        container = self._selected_container_admin()
        if not container:
            messagebox.showwarning(
                "Contenedores", "Selecciona un contenedor para borrar.")
            return

        matches = self._profiles_containing_container(container)
        if matches:
            scope_lines: list[str] = []
            if matches.get("privado"):
                scope_lines.append("Perfiles privados: " +
                                   ", ".join(matches["privado"]))
            if matches.get("remoto"):
                scope_lines.append("Perfiles remotos: " +
                                   ", ".join(matches["remoto"]))

            confirm_remove = messagebox.askyesno(
                "Contenedores",
                (
                    f"El contenedor '{container}' esta incluido en perfiles.\n\n"
                    + "\n".join(scope_lines)
                    + "\n\nQuieres quitarlo de esos perfiles antes de borrar el contenedor?"
                ),
            )
            if not confirm_remove:
                messagebox.showwarning(
                    "Contenedores",
                    f"No se puede borrar '{container}' sin quitarlo de los perfiles donde esta incluido.",
                )
                return

            removed_ok, remove_error = self._remove_container_from_profile_scopes(
                container, matches)
            if not removed_ok:
                messagebox.showerror(
                    "Contenedores", remove_error or "No se pudo quitar el contenedor de los perfiles.")
                return

            removed_scopes = ", ".join(
                [k for k in ("privado", "remoto") if matches.get(k)])
            self.log_event("CONTAINER", container, "OK",
                           f"Quitado de perfiles antes de borrar ({removed_scopes})")

        if not messagebox.askyesno("Contenedores", f"Eliminar contenedor '{container}'?\n\nSe forzará parada si está arrancado."):
            return

        def _delete_container_operation():
            code, out, err = self._run(["docker", "rm", "-f", container])
            if code != 0:
                details = err or out or f"Docker devolvio codigo {code} sin detalle."
                raise RuntimeError(
                    f"No se pudo borrar el contenedor.\n\n{details}")

            self.log_event("CONTAINER", container, "OK",
                           "Eliminado desde gestor avanzado")
            self.refresh_everything()
            self.refresh_profiles_ui(force=True)
            self._refresh_container_admin_table()
            return True

        self._run_with_loading_modal(
            f"Eliminando contenedor {container}", _delete_container_operation)

    def _toggle_container_admin(self, mode: str) -> None:
        container = self._selected_container_admin()
        if not container:
            messagebox.showwarning("Contenedores", "Selecciona un contenedor.")
            return

        action = "start" if mode == "start" else "stop"
        estado = "arrancado" if action == "start" else "apagado"

        def _toggle_operation() -> bool:
            code, _, err = self._run(["docker", action, container])
            if code != 0:
                raise RuntimeError(err or f"No se pudo {action} {container}.")
            self.log_event("CONTAINER", container, "OK",
                           f"Contenedor {estado} desde gestor avanzado")
            self.refresh_everything()
            self._refresh_container_admin_table()
            messagebox.showinfo(
                "Contenedores", f"Contenedor {estado}: {container}")
            return True

        self._run_with_loading_modal(
            f"{estado.capitalize()} contenedor {container}",
            _toggle_operation,
            auto_close_success_ms=500,
        )

    def _select_active_theme_admin(self) -> None:
        container = self._selected_container_admin()
        if not container:
            messagebox.showwarning(
                "Tema activo", "Selecciona un contenedor Shopify.")
            return
        self._select_active_theme_for_container(container)

    def _delete_theme_admin(self) -> None:
        container = self._selected_container_admin()
        if not container:
            messagebox.showwarning(
                "Borrar tema", "Selecciona un contenedor Shopify.")
            return
        self._delete_theme_for_container(container)

    def _remote_access_container_admin(self) -> None:
        container = self._selected_container_admin()
        if not container:
            messagebox.showwarning(
                "Contenedores", "Selecciona un contenedor para el acceso remoto.")
            return
        self._remote_access_impl(container)

    def _remote_access_impl(self, container: str) -> None:
        if not self.docker_ready():
            messagebox.showerror("Docker", self._docker_unavailable_message())
            return

        # Verificar si el contenedor está encendido
        c_code, c_out, _ = self._run(
            ["docker", "inspect", "--format", "{{.State.Running}}", container])
        if c_code != 0 or c_out.strip().lower() != "true":
            messagebox.showwarning(
                "Acceso Remoto", f"El contenedor '{container}' debe estar encendido para habilitar el acceso remoto.")
            return

        # Detectar el puerto 8080 mapeado en el host para este contenedor
        _, ports_out, _ = self._run([
            "docker", "inspect", "--format",
            "{{range $p, $b := .NetworkSettings.Ports}}{{$p}}->{{range $b}}{{.HostPort}}{{end}} {{end}}",
            container
        ])
        host_port = "8080"
        if ports_out:
            for part in ports_out.split():
                if part.startswith("8080/tcp->"):
                    mapped = part.split("->", 1)[1].strip()
                    if mapped:
                        host_port = mapped
                        break

        # Usar el host de acceso que ya emplea el resto de la app.
        access_host = self._access_host_for_urls()
        codeserver_url = f"http://{access_host}:{host_port}"

        # Verificar si code-server ya está corriendo dentro del contenedor
        _, ps_out, _ = self._run(
            ["docker", "exec", container, "sh", "-c", "pgrep -f code-server || echo ''"])
        already_running = bool((ps_out or "").strip())

        if already_running:
            self._show_codeserver_instructions(
                container, codeserver_url, host_port)
            return

        confirm = messagebox.askyesno(
            "Acceso Remoto — code-server",
            f"Vas a iniciar code-server en el contenedor '{container}'.\n\n"
            "✅ Sin contraseña — cualquiera en la red puede conectarse.\n"
            "✅ Abre VS Code directamente en la app nativa o en el navegador.\n"
            f"✅ URL de acceso: {codeserver_url}\n\n"
            "¿Continuar?"
        )
        if not confirm:
            return

        # Comando: instalar code-server si falta, luego arrancarlo sin auth
        install_and_run = (
            "sh -c \""
            "if ! command -v code-server >/dev/null 2>&1; then "
            "  echo '[cs] Instalando code-server...' >> /tmp/code-server.log 2>&1; "
            "  for i in 1 2 3; do npm install -g --unsafe-perm code-server >> /tmp/code-server.log 2>&1 && break; sleep 2; done; "
            "fi; "
            "echo '[cs] Arrancando code-server sin auth...' >> /tmp/code-server.log 2>&1; "
            "nohup code-server --bind-addr 0.0.0.0:8080 --auth none /app/horizon "
            "  >> /tmp/code-server.log 2>&1 &"
            "\""
        )

        def run_codeserver():
            self.log_event("REMOTE-ACCESS", container, "INFO",
                           "Iniciando code-server (sin contraseña)...")
            code, out, err = self._run(
                ["docker", "exec", "-d", container, "sh", "-c", install_and_run])
            if code == 0:
                time.sleep(3)
                self.root.after(0, lambda: __import__(
                    "webbrowser").open(codeserver_url))
                self.root.after(0, lambda: self._show_codeserver_instructions(
                    container, codeserver_url, host_port))
            else:
                self.root.after(0, lambda: messagebox.showerror(
                    "Acceso Remoto", f"Error al lanzar code-server: {err or 'desconocido'}"))

        threading.Thread(target=run_codeserver, daemon=True).start()

    def _show_codeserver_instructions(self, container: str, codeserver_url: str, host_port: str) -> None:
        vscode_url = f"vscode://vscode-remote/localhost:{host_port}"
        msg = (
            f"✅ code-server activo en '{container}'.\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "OPCIÓN A — APP nativa de VS Code:\n"
            "  1. Instala la extensión 'ms-vscode.remote-server'\n"
            "     (Ctrl+Shift+X → busca 'Remote - Server')\n"
            "  2. Ctrl+Shift+P → 'Remote: Connect to Remote Server'\n"
            f"  3. Introduce la URL: {codeserver_url}\n"
            "  4. ¡Listo! Editas el código directamente desde la app.\n\n"
            "OPCIÓN B — Navegador (sin instalar nada):\n"
            f"  Abre: {codeserver_url}\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "⚠️  Sin contraseña — cualquiera en la red puede acceder.\n"
            "    Para parar: apaga el contenedor o reinícialo.\n\n"
            "Los archivos del tema están en /app/horizon dentro del contenedor.\n"
            "Cualquier guardado se refleja al instante en el dev server de Shopify."
        )
        messagebox.showinfo("Acceso Remoto — code-server", msg)

    def _build_containers_tab(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(0, weight=1)

        table_frame = ttk.Frame(parent)
        table_frame.grid(row=0, column=0, sticky="nsew")
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(0, weight=1)
        table_frame.rowconfigure(1, weight=0)

        columns = ("name", "state", "health", "port", "protection")
        self.tree = ttk.Treeview(
            table_frame, columns=columns, show="headings", height=14, selectmode="extended")
        self.tree.heading("name", text="Contenedor")
        self.tree.heading("state", text="Estado")
        self.tree.heading("health", text="Salud")
        self.tree.heading("port", text="Puerto")
        self.tree.heading("protection", text="Proteccion")
        self.tree.column("name", width=340, anchor="w")
        self.tree.column("state", width=120, anchor="center")
        self.tree.column("health", width=120, anchor="center")
        self.tree.column("port", width=120, anchor="center")
        self.tree.column("protection", width=320, anchor="w")
        self.tree.grid(row=0, column=0, sticky="nsew")
        self.tree.tag_configure("running",   foreground="#059669")
        self.tree.tag_configure("stopped",   foreground="#ef4444")
        self.tree.tag_configure("unhealthy", foreground="#d97706")

        yscroll = ttk.Scrollbar(
            table_frame, orient="vertical", command=self.tree.yview)
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll = ttk.Scrollbar(
            table_frame, orient="horizontal", command=self.tree.xview)
        xscroll.grid(row=1, column=0, sticky="ew")
        self.tree.configure(yscrollcommand=yscroll.set,
                            xscrollcommand=xscroll.set)

        action_row = ttk.Frame(parent)
        action_row.grid(row=1, column=0, sticky="ew", pady=(10, 4))

        ttk.Button(action_row, text="Refrescar", command=self.refresh_everything).pack(
            side="left", padx=(0, 6))
        self.container_action_btns = []
        for _lbl, _cmd in [
            ("Arrancar seleccionados", self.start_selected),
            ("Apagar seleccionados",   self.stop_selected),
            ("Seleccionar tema activo", self._select_active_theme_selected),
            ("Borrar tema", self._delete_theme_selected),
            ("Acceso Remoto (SSH)",   self.remote_access_selected),
            ("Arrancar todos",          self.start_all),
            ("Apagar todos",            self.stop_all),
        ]:
            _btn = ttk.Button(action_row, text=_lbl, command=_cmd)
            _btn.pack(side="left", padx=6)
            self.container_action_btns.append(_btn)

    def _build_profiles_tab(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)

        top = ttk.Frame(parent)
        top.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        top.columnconfigure(3, weight=1)
        top.columnconfigure(4, weight=1)

        ttk.Label(top, text="Almacen:").grid(
            row=0, column=0, sticky="w", padx=(0, 6))
        scope_combo = ttk.Combobox(
            top,
            textvariable=self.profile_scope_var,
            values=["privado", "remoto"],
            state="readonly",
            width=12,
        )
        scope_combo.current(0)
        scope_combo.grid(row=0, column=1, sticky="w")
        scope_combo.bind("<<ComboboxSelected>>", self.on_profile_scope_changed)

        ttk.Label(top, text="Nombre perfil:").grid(
            row=0, column=2, sticky="w", padx=(12, 6))
        ttk.Entry(top, textvariable=self.profile_name_var).grid(
            row=0, column=3, columnspan=2, sticky="ew")

        ttk.Button(top, text="Guardar/Actualizar", command=self.save_profile).grid(
            row=1, column=0, sticky="ew", padx=(0, 6), pady=(8, 0))
        ttk.Button(top, text="Quitar del perfil", command=self.remove_selected_from_profile).grid(
            row=1, column=1, sticky="ew", padx=6, pady=(8, 0))
        ttk.Button(top, text="Eliminar", command=self.delete_profile).grid(
            row=1, column=2, sticky="ew", padx=6, pady=(8, 0))
        self.copy_profile_btn = ttk.Button(
            top, text="Copiar a remoto", command=self.copy_selected_profile)
        self.copy_profile_btn.grid(
            row=2, column=0, sticky="ew", padx=(0, 6), pady=(6, 0))
        self.profile_action_btns = []
        _btn_start = ttk.Button(top, text="Arrancar perfil",
                                command=lambda: self.run_selected_profile("start"))
        _btn_start.grid(row=2, column=1, sticky="ew", padx=6, pady=(6, 0))
        self.profile_action_btns.append(_btn_start)
        _btn_stop = ttk.Button(top, text="Apagar perfil",
                               command=lambda: self.run_selected_profile("stop"))
        _btn_stop.grid(row=2, column=2, sticky="ew", padx=6, pady=(6, 0))
        self.profile_action_btns.append(_btn_stop)

        body = ttk.Frame(parent)
        body.grid(row=1, column=0, sticky="nsew")
        body.columnconfigure(0, weight=1)
        body.columnconfigure(1, weight=1)
        body.rowconfigure(1, weight=1)

        self.profiles_header_label = ttk.Label(body, text="Perfiles privados")
        self.profiles_header_label.grid(row=0, column=0, sticky="w")
        ttk.Label(body, text="Contenedores del perfil").grid(
            row=0, column=1, sticky="w")

        self.profiles_listbox = tk.Listbox(
            body, exportselection=False,
            bg="#ffffff", fg="#0f172a", selectbackground="#dbeafe", selectforeground="#1e40af",
            relief="solid", borderwidth=1, highlightthickness=0, font=("Segoe UI", 10),
            activestyle="none",
        )
        self.profiles_listbox.grid(row=1, column=0, sticky="nsew", padx=(0, 6))
        self.profiles_listbox.bind(
            "<<ListboxSelect>>", self.on_profile_selected)

        self.profile_containers_listbox = tk.Listbox(
            body, selectmode="extended", exportselection=False,
            bg="#ffffff", fg="#0f172a", selectbackground="#dbeafe", selectforeground="#1e40af",
            relief="solid", borderwidth=1, highlightthickness=0, font=("Segoe UI", 10),
            activestyle="none",
        )
        self.profile_containers_listbox.grid(
            row=1, column=1, sticky="nsew", padx=(6, 0))

        bottom = ttk.Frame(parent)
        bottom.grid(row=2, column=0, sticky="w", pady=(8, 0))
        ttk.Button(bottom, text="Refrescar perfiles",
                   command=lambda: self.refresh_profiles_ui(force=True)).pack(side="left")
        ttk.Button(bottom, text="Limpiar seleccion",
                   command=self.clear_profile_editor).pack(side="left", padx=6)
        self.profiles_loading_label = ttk.Label(
            bottom, text="", style="Muted.TLabel")
        self.profiles_loading_label.pack(side="left", padx=(10, 0))

    def _build_networks_tab(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)

        top = ttk.Frame(parent)
        top.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        top.columnconfigure(0, weight=1)

        ttk.Button(top, text="Refrescar networks",
                   command=self.refresh_networks_with_modal).pack(side="left")
        ttk.Label(top, text="Driver:").pack(side="left", padx=(14, 6))
        ttk.Combobox(
            top,
            textvariable=self.network_driver_var,
            values=["bridge", "overlay", "macvlan", "ipvlan"],
            state="readonly",
            width=10,
        ).pack(side="left")
        ttk.Button(top, text="Crear network",
                   command=self.create_network).pack(side="left", padx=6)
        ttk.Button(top, text="Renombrar network",
                   command=self.rename_network).pack(side="left", padx=6)
        ttk.Button(top, text="Eliminar network",
                   command=self.delete_network).pack(side="left", padx=6)

        body = ttk.Frame(parent)
        body.grid(row=1, column=0, sticky="nsew")
        body.columnconfigure(0, weight=1)
        body.columnconfigure(1, weight=1)
        body.rowconfigure(1, weight=1)

        ttk.Label(body, text="Networks").grid(row=0, column=0, sticky="w")
        ttk.Label(body, text="Contenedores conectados").grid(
            row=0, column=1, sticky="w")

        self.networks_tree = ttk.Treeview(body, columns=(
            "name", "driver", "count"), show="headings", height=12)
        self.networks_tree.heading("name", text="Network")
        self.networks_tree.heading("driver", text="Driver")
        self.networks_tree.heading("count", text="Contenedores")
        self.networks_tree.column("name", width=240, anchor="w")
        self.networks_tree.column("driver", width=110, anchor="center")
        self.networks_tree.column("count", width=110, anchor="center")
        self.networks_tree.grid(row=1, column=0, sticky="nsew", padx=(0, 6))
        self.networks_tree.bind("<<TreeviewSelect>>", self.on_network_selected)

        networks_y_scroll = ttk.Scrollbar(
            body, orient="vertical", command=self.networks_tree.yview)
        networks_y_scroll.grid(row=1, column=0, sticky="nse", padx=(0, 6))
        networks_x_scroll = ttk.Scrollbar(
            body, orient="horizontal", command=self.networks_tree.xview)
        networks_x_scroll.grid(
            row=2, column=0, sticky="ew", padx=(0, 6), pady=(4, 0))
        self.networks_tree.configure(
            yscrollcommand=networks_y_scroll.set, xscrollcommand=networks_x_scroll.set)

        self.network_containers_listbox = tk.Listbox(
            body, exportselection=False,
            bg="#ffffff", fg="#0f172a", selectbackground="#dbeafe", selectforeground="#1e40af",
            relief="solid", borderwidth=1, highlightthickness=0, font=("Segoe UI", 10),
            activestyle="none",
        )
        self.network_containers_listbox.grid(
            row=1, column=1, sticky="nsew", padx=(6, 0))
        self.network_containers_listbox.bind("<Button-1>", lambda _e: "break")
        self.network_containers_listbox.bind("<B1-Motion>", lambda _e: "break")
        self.network_containers_listbox.bind(
            "<ButtonRelease-1>", lambda _e: "break")
        self.network_containers_listbox.bind("<Key>", lambda _e: "break")

        action = ttk.Frame(parent)
        action.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        action.columnconfigure(1, weight=1)

        ttk.Label(action, text="Contenedor objetivo:").grid(
            row=0, column=0, sticky="w", padx=(0, 6))
        self.network_container_combo = ttk.Combobox(
            action,
            textvariable=self.network_container_var,
            state="readonly",
            values=[],
        )
        self.network_container_combo.grid(row=0, column=1, sticky="ew")
        ttk.Button(action, text="Conectar", command=self.connect_container_to_network).grid(
            row=0, column=2, padx=6)
        ttk.Button(action, text="Desconectar",
                   command=self.disconnect_container_from_network).grid(row=0, column=3)

        ttk.Label(action, text="Seleccion multiple:").grid(
            row=1, column=0, sticky="nw", padx=(0, 6), pady=(8, 0))
        self.network_targets_listbox = tk.Listbox(
            action,
            selectmode="extended",
            exportselection=False,
            height=5,
            bg="#ffffff",
            fg="#0f172a",
            selectbackground="#dbeafe",
            selectforeground="#1e40af",
            relief="solid",
            borderwidth=1,
            highlightthickness=0,
            font=("Segoe UI", 10),
            activestyle="none",
        )
        self.network_targets_listbox.grid(
            row=1, column=1, sticky="ew", pady=(8, 0))

    def _build_volumes_tab(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)

        top = ttk.Frame(parent)
        top.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        top.columnconfigure(0, weight=1)

        ttk.Button(top, text="Refrescar volumes",
                   command=self.refresh_volumes_with_modal).pack(side="left")
        ttk.Label(top, text="Driver:").pack(side="left", padx=(14, 6))
        ttk.Combobox(
            top,
            textvariable=self.volume_driver_var,
            values=["local", "nfs", "tmpfs"],
            state="readonly",
            width=10,
        ).pack(side="left")
        ttk.Button(top, text="Crear volume", command=self.create_volume).pack(
            side="left", padx=6)
        ttk.Button(top, text="Inspeccionar", command=self.inspect_selected_volumes).pack(
            side="left", padx=6)
        ttk.Button(top, text="Clonar volume",
                   command=self.clone_volume).pack(side="left", padx=6)
        ttk.Button(top, text="Vaciar volume", command=self.clear_volume_contents).pack(
            side="left", padx=6)
        ttk.Button(top, text="Eliminar volume",
                   command=self.delete_selected_volumes).pack(side="left", padx=6)
        ttk.Button(top, text="Prune volumes",
                   command=self.prune_volumes).pack(side="left", padx=6)

        body = ttk.Frame(parent)
        body.grid(row=1, column=0, sticky="nsew")
        body.columnconfigure(0, weight=1)
        body.columnconfigure(1, weight=1)
        body.rowconfigure(1, weight=1)

        ttk.Label(body, text="Volumes").grid(row=0, column=0, sticky="w")
        ttk.Label(body, text="Contenedores que usan el volume").grid(
            row=0, column=1, sticky="w")

        self.volumes_tree = ttk.Treeview(
            body,
            columns=("name", "driver", "scope", "inuse", "mountpoint"),
            show="headings",
            height=12,
            selectmode="extended",
        )
        self.volumes_tree.heading("name", text="Volume")
        self.volumes_tree.heading("driver", text="Driver")
        self.volumes_tree.heading("scope", text="Scope")
        self.volumes_tree.heading("inuse", text="Uso")
        self.volumes_tree.heading("mountpoint", text="Mountpoint")
        self.volumes_tree.column("name", width=220, anchor="w")
        self.volumes_tree.column("driver", width=110, anchor="center")
        self.volumes_tree.column("scope", width=100, anchor="center")
        self.volumes_tree.column("inuse", width=90, anchor="center")
        self.volumes_tree.column("mountpoint", width=330, anchor="w")
        self.volumes_tree.grid(row=1, column=0, sticky="nsew", padx=(0, 6))
        self.volumes_tree.bind("<<TreeviewSelect>>", self.on_volume_selected)

        volumes_y_scroll = ttk.Scrollbar(
            body, orient="vertical", command=self.volumes_tree.yview)
        volumes_y_scroll.grid(row=1, column=0, sticky="nse", padx=(0, 6))
        volumes_x_scroll = ttk.Scrollbar(
            body, orient="horizontal", command=self.volumes_tree.xview)
        volumes_x_scroll.grid(row=2, column=0, sticky="ew",
                              padx=(0, 6), pady=(4, 0))
        self.volumes_tree.configure(
            yscrollcommand=volumes_y_scroll.set, xscrollcommand=volumes_x_scroll.set)

        self.volume_containers_listbox = tk.Listbox(
            body,
            exportselection=False,
            bg="#ffffff",
            fg="#0f172a",
            selectbackground="#dbeafe",
            selectforeground="#1e40af",
            relief="solid",
            borderwidth=1,
            highlightthickness=0,
            font=("Segoe UI", 10),
            activestyle="none",
        )
        self.volume_containers_listbox.grid(
            row=1, column=1, sticky="nsew", padx=(6, 0))
        self.volume_containers_listbox.bind("<Button-1>", lambda _e: "break")
        self.volume_containers_listbox.bind("<B1-Motion>", lambda _e: "break")
        self.volume_containers_listbox.bind(
            "<ButtonRelease-1>", lambda _e: "break")
        self.volume_containers_listbox.bind("<Key>", lambda _e: "break")

    def _build_history_tab(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)

        top = ttk.Frame(parent)
        top.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        top.columnconfigure(3, weight=1)

        ttk.Label(top, text="Nivel:").grid(
            row=0, column=0, sticky="w", padx=(0, 6))
        level_combo = ttk.Combobox(
            top,
            textvariable=self.history_level_var,
            values=["TODOS", "OK", "ERROR", "WARN", "INFO"],
            state="readonly",
            width=12,
        )
        level_combo.current(0)
        level_combo.grid(row=0, column=1, sticky="w")
        level_combo.bind("<<ComboboxSelected>>", self.apply_history_filter)

        ttk.Label(top, text="Buscar:").grid(
            row=0, column=2, sticky="w", padx=(12, 6))
        search = ttk.Entry(top, textvariable=self.history_search_var)
        search.grid(row=0, column=3, sticky="ew")
        search.bind("<KeyRelease>", self.apply_history_filter)

        ttk.Button(top, text="Refrescar", command=self.refresh_history).grid(
            row=0, column=4, padx=6)
        ttk.Button(top, text="Limpiar filtro", command=self.clear_history_filters).grid(
            row=0, column=5, padx=(0, 6))
        ttk.Button(top, text="Copiar visible",
                   command=self.copy_visible_history).grid(row=0, column=6)

        # Body: text widget + spinner overlay stacked in a container
        body_container = ttk.Frame(parent)
        body_container.grid(row=1, column=0, sticky="nsew")
        body_container.columnconfigure(0, weight=1)
        body_container.rowconfigure(0, weight=1)

        body = ttk.Frame(body_container)
        body.grid(row=0, column=0, sticky="nsew")
        body.columnconfigure(0, weight=1)
        body.rowconfigure(0, weight=1)

        self.history_text = tk.Text(
            body,
            wrap="none",
            height=16,
            bg="#ffffff",
            fg="#202223",
            insertbackground="#202223",
            relief="flat",
            borderwidth=1,
            selectbackground="#e3f1ec",
            highlightthickness=0,
            font=("Segoe UI", 9),
        )
        self.history_text.grid(row=0, column=0, sticky="nsew")

        y_scroll = ttk.Scrollbar(
            body, orient="vertical", command=self.history_text.yview)
        y_scroll.grid(row=0, column=1, sticky="ns")
        self.history_text.configure(yscrollcommand=y_scroll.set)

        x_scroll = ttk.Scrollbar(
            body, orient="horizontal", command=self.history_text.xview)
        x_scroll.grid(row=1, column=0, sticky="ew")
        self.history_text.configure(xscrollcommand=x_scroll.set)

        self.history_text.configure(state="disabled")

        # ── Spinner overlay (visible solo mientras carga) ──────────────────
        self._history_spinner_frame: tk.Frame = tk.Frame(
            body_container, bg="#ffffff")
        self._history_spinner_job: str | None = None
        self._history_spinner_index: int = 0

        spinner_inner = tk.Frame(self._history_spinner_frame, bg="#ffffff")
        spinner_inner.place(relx=0.5, rely=0.45, anchor="center")

        self._history_spinner_dot_label = tk.Label(
            spinner_inner,
            text="⬤",
            fg="#008060",
            bg="#ffffff",
            font=("Segoe UI", 22),
        )
        self._history_spinner_dot_label.pack()

        tk.Label(
            spinner_inner,
            text="Cargando historial...",
            fg="#6d7175",
            bg="#ffffff",
            font=("Segoe UI", 10),
        ).pack(pady=(6, 0))

    def _build_logs_tab(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)

        top = ttk.Frame(parent)
        top.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        top.columnconfigure(1, weight=1)

        ttk.Label(top, text="Contenedor:").grid(
            row=0, column=0, sticky="w", padx=(0, 6))
        self.log_container_combo = ttk.Combobox(
            top,
            textvariable=self.log_container_var,
            state="readonly",
            values=[],
        )
        self.log_container_combo.grid(row=0, column=1, sticky="ew")

        ttk.Label(top, text="Lineas:").grid(
            row=0, column=2, sticky="w", padx=(12, 6))
        self.log_lines_spinbox = tk.Spinbox(
            top, from_=10, to=5000, increment=10, textvariable=self.log_lines_var, width=8)
        self.log_lines_spinbox.grid(row=0, column=3, sticky="w")
        self.log_lines_spinbox.configure(
            background="#ffffff",
            foreground="#1f2937",
            insertbackground="#1f2937",
            relief="solid",
            borderwidth=1,
        )

        ttk.Button(top, text="Ver logs", command=self.fetch_logs).grid(
            row=0, column=4, padx=6)
        ttk.Checkbutton(top, text="Seguir (-f)", variable=self.log_follow_var,
                        command=self.on_follow_mode_toggled).grid(row=0, column=5, padx=6)
        ttk.Checkbutton(top, text="Auto-refresco", variable=self.log_auto_refresh_var,
                        command=self.toggle_logs_auto_refresh).grid(row=0, column=6, padx=6)
        ttk.Button(top, text="Exportar txt", command=self.export_visible_logs).grid(
            row=0, column=7, padx=(6, 0))
        ttk.Button(top, text="Copiar visible", command=self.copy_visible_logs).grid(
            row=0, column=8, padx=(6, 0))

        body = ttk.Frame(parent)
        body.grid(row=1, column=0, sticky="nsew")
        body.columnconfigure(0, weight=1)
        body.rowconfigure(0, weight=1)

        self.logs_text = tk.Text(
            body,
            wrap="none",
            height=16,
            bg="#ffffff",
            fg="#1f2937",
            insertbackground="#1f2937",
            relief="flat",
            borderwidth=1,
            selectbackground="#bfdbfe",
            highlightthickness=0,
        )
        self.logs_text.grid(row=0, column=0, sticky="nsew")

        y_scroll = ttk.Scrollbar(
            body, orient="vertical", command=self.logs_text.yview)
        y_scroll.grid(row=0, column=1, sticky="ns")
        self.logs_text.configure(yscrollcommand=y_scroll.set)

        x_scroll = ttk.Scrollbar(
            body, orient="horizontal", command=self.logs_text.xview)
        x_scroll.grid(row=1, column=0, sticky="ew")
        self.logs_text.configure(xscrollcommand=x_scroll.set)

        self.logs_text.insert(
            "1.0", "Selecciona un contenedor y pulsa 'Ver logs'.")
        self.logs_text.configure(state="disabled")

    def _run(self, args: list[str]) -> tuple[int, str, str]:
        if args and args[0].lower() == "docker" and self._should_use_docker_sdk():
            return self._run_docker_via_sdk(args)

        final_args = self._build_docker_command(args)
        process = subprocess.run(
            final_args,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=self.tools_dir,
            shell=False,
            env=self._docker_process_env(),
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        return process.returncode, process.stdout.strip(), process.stderr.strip()

    def _docker_process_env(self, force_host: str | None = None) -> dict[str, str]:
        env = os.environ.copy()
        host = force_host
        if not host and self.docker_mode == "remote" and self.docker_host:
            host = self.docker_host
        if host:
            env["DOCKER_HOST"] = host
            env.pop("DOCKER_CONTEXT", None)
            # Evita errores de tipo CreateFile cuando quedan rutas TLS invalidas en variables globales.
            env.pop("DOCKER_TLS_VERIFY", None)
            env.pop("DOCKER_CERT_PATH", None)
            env.pop("DOCKER_TLS", None)
        return env

    def _detect_docker_cli(self) -> bool:
        try:
            process = subprocess.run(
                ["where", "docker"],
                capture_output=True,
                text=True,
                cwd=self.tools_dir,
                shell=False,
                env=self._docker_process_env(),
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
            return process.returncode == 0
        except Exception:
            return False

    def _should_use_docker_sdk(self) -> bool:
        if self.docker_cli_available is None:
            self.docker_cli_available = self._detect_docker_cli()
        return not bool(self.docker_cli_available)

    def _get_docker_sdk_client(self, host_override: str | None = None, timeout_seconds: int | None = 5) -> object | None:
        if docker is None:
            return None

        is_short_timeout = timeout_seconds is not None and timeout_seconds <= 5
        now = time.time()
        if (
            host_override is None
            and is_short_timeout
            and self.docker_sdk_client is None
            and self._sdk_last_fail_at > 0
            and (now - self._sdk_last_fail_at) < self._sdk_retry_cooldown_sec
        ):
            return None

        if host_override is None and is_short_timeout and self.docker_sdk_client is not None:
            return self.docker_sdk_client

        base_url = host_override
        if base_url is None and self.docker_mode == "remote" and self.docker_host:
            base_url = self.docker_host

        try:
            if base_url:
                client = docker.DockerClient(
                    base_url=base_url, timeout=timeout_seconds)
            else:
                client = docker.from_env(timeout=timeout_seconds)
            client.ping()
            if host_override is None and is_short_timeout:
                self.docker_sdk_client = client
                self._sdk_last_fail_at = 0.0
            return client
        except Exception:
            if host_override is None and is_short_timeout:
                self._sdk_last_fail_at = time.time()
            return None

    def _ports_mapping_text(self, container: object) -> str:
        try:
            ports = (container.attrs.get("NetworkSettings", {})
                     or {}).get("Ports", {}) or {}
            parts: list[str] = []
            for cport, bindings in ports.items():
                if not bindings:
                    continue
                for item in bindings:
                    host_ip = item.get("HostIp", "0.0.0.0")
                    host_port = item.get("HostPort", "")
                    parts.append(f"{host_ip}:{host_port}->{cport}")
            return ", ".join(parts)
        except Exception:
            return ""

    def _status_text(self, container: object) -> str:
        status = (getattr(container, "status", "") or "").lower()
        if status == "running":
            return "Up"
        if status == "created":
            return "Created"
        if status == "paused":
            return "Paused"
        if status == "restarting":
            return "Restarting"
        return "Exited"

    def _render_ps_format_line(self, container: object, template: str) -> str:
        image = ""
        command = ""
        try:
            tags = getattr(container.image, "tags", []) or []
            image = tags[0] if tags else (container.attrs.get(
                "Config", {}) or {}).get("Image", "")
        except Exception:
            image = ""

        try:
            cfg = container.attrs.get("Config", {}) or {}
            cmd = cfg.get("Cmd")
            if isinstance(cmd, list):
                command = " ".join(str(x) for x in cmd)
            else:
                command = str(cmd or "")
        except Exception:
            command = ""

        name = getattr(container, "name", "")
        status = self._status_text(container)
        ports = self._ports_mapping_text(container)
        out = template
        out = out.replace("{{.Names}}", name)
        out = out.replace("{{.Status}}", status)
        out = out.replace("{{.Image}}", image)
        out = out.replace("{{.Ports}}", ports)
        out = out.replace("{{.Command}}", command)
        return out

    def _run_sdk_cp_helper_subprocess(self, host_override: str | None, src: str, dst: str) -> tuple[int, str, str]:
        direction = "from" if _looks_like_container_spec(src) else "to"
        # En modo compilado (PyInstaller), argv[1] debe ser --wpu-sdk-cp
        # para evitar que se abra una segunda instancia de la UI.
        helper_args = [sys.executable]
        if not getattr(sys, "frozen", False):
            helper_args.append(os.path.abspath(__file__))
        helper_args.extend([
            "--wpu-sdk-cp",
            direction,
            host_override or "",
            src,
            dst,
        ])
        process = subprocess.run(
            helper_args,
            capture_output=True,
            text=True,
            cwd=self.tools_dir,
            shell=False,
            env=self._docker_process_env(force_host=host_override),
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        return process.returncode, process.stdout.strip(), process.stderr.strip()

    def _run_docker_via_sdk(self, args: list[str]) -> tuple[int, str, str]:
        docker_args = args[1:]
        host_override: str | None = None
        if len(docker_args) >= 2 and docker_args[0] == "-H":
            host_override = docker_args[1]
            docker_args = docker_args[2:]
        if not docker_args:
            return 1, "", "Comando docker vacio"

        cmd = docker_args[0]
        rest = docker_args[1:]

        # Para operaciones largas (import/export/copia/exec/logs) evitar
        # timeout fijo de lectura, ya que depende del tamano de datos y rendimiento del host remoto.
        if cmd in {"cp", "exec", "run", "logs", "pull"}:
            try:
                client.api.timeout = None
            except Exception:
                pass

        op_timeout: int | None = 5
        if cmd in {"cp", "exec", "run", "logs", "pull"}:
            op_timeout = None
        elif cmd in {"start", "stop", "restart", "rm", "network", "volume"}:
            op_timeout = 30

        client = self._get_docker_sdk_client(
            host_override=host_override, timeout_seconds=op_timeout)
        if client is None:
            return 1, "", "Docker SDK no disponible. Instala paquete Python 'docker'."

        try:
            if cmd == "info":
                info = client.api.info()
                return 0, str(info.get("ServerVersion", "OK")), ""

            if cmd == "ps":
                all_flag = "-a" in rest or "-aq" in rest
                quiet = "-q" in rest or "-aq" in rest
                fmt = ""
                if "--format" in rest:
                    idx = rest.index("--format")
                    if idx + 1 < len(rest):
                        fmt = rest[idx + 1]
                containers = client.containers.list(all=all_flag)
                if quiet:
                    return 0, "\n".join(c.id for c in containers), ""
                if fmt:
                    lines = [self._render_ps_format_line(
                        c, fmt) for c in containers]
                    return 0, "\n".join(lines), ""
                return 0, "\n".join(c.name for c in containers), ""

            if cmd in {"start", "stop", "restart"}:
                if not rest:
                    return 1, "", f"docker {cmd}: faltan contenedores"
                for cname in rest:
                    cont = client.containers.get(cname)
                    if cmd == "start":
                        cont.start()
                    elif cmd == "stop":
                        cont.stop(timeout=10)
                    else:
                        cont.restart(timeout=10)
                return 0, "", ""

            if cmd == "rename":
                if len(rest) != 2:
                    return 1, "", "docker rename: argumentos invalidos"
                cont = client.containers.get(rest[0])
                cont.rename(rest[1])
                return 0, "", ""

            if cmd == "rm":
                force = "-f" in rest
                names = [x for x in rest if x != "-f"]
                for cname in names:
                    cont = client.containers.get(cname)
                    cont.remove(force=force)
                return 0, "", ""

            if cmd == "network":
                if not rest:
                    return 1, "", "docker network: falta subcomando"
                sub = rest[0]
                sub_args = rest[1:]
                if sub == "ls":
                    fmt = ""
                    if "--format" in sub_args:
                        idx = sub_args.index("--format")
                        if idx + 1 < len(sub_args):
                            fmt = sub_args[idx + 1]
                    lines: list[str] = []
                    for net in client.networks.list():
                        name = net.name
                        driver = (net.attrs.get("Driver", "")
                                  if getattr(net, "attrs", None) else "")
                        if fmt:
                            line = fmt.replace("{{.Name}}", name).replace(
                                "{{.Driver}}", driver)
                        else:
                            line = f"{name}|{driver}"
                        lines.append(line)
                    return 0, "\n".join(lines), ""
                if sub == "create":
                    driver = "bridge"
                    name = ""
                    if "--driver" in sub_args:
                        idx = sub_args.index("--driver")
                        if idx + 1 < len(sub_args):
                            driver = sub_args[idx + 1]
                            rem = [x for i, x in enumerate(sub_args) if i not in {
                                                           idx, idx + 1}]
                            name = rem[-1] if rem else ""
                    else:
                        name = sub_args[-1] if sub_args else ""
                    if not name:
                        return 1, "", "docker network create: falta nombre"
                    net = client.networks.create(name=name, driver=driver)
                    return 0, net.id, ""
                if sub == "rm":
                    for net_name in sub_args:
                        client.networks.get(net_name).remove()
                    return 0, "", ""
                if sub == "connect":
                    if len(sub_args) < 2:
                        return 1, "", "docker network connect: argumentos invalidos"
                    client.networks.get(sub_args[0]).connect(sub_args[1])
                    return 0, "", ""
                if sub == "disconnect":
                    if len(sub_args) < 2:
                        return 1, "", "docker network disconnect: argumentos invalidos"
                    client.networks.get(sub_args[0]).disconnect(sub_args[1])
                    return 0, "", ""
                return 1, "", f"Subcomando network no soportado: {sub}"

            if cmd == "volume":
                if not rest:
                    return 1, "", "docker volume: falta subcomando"
                sub = rest[0]
                sub_args = rest[1:]
                names = [x for x in sub_args if x != "-f"]
                if sub == "ls":
                    fmt = ""
                    if "--format" in sub_args:
                        idx = sub_args.index("--format")
                        if idx + 1 < len(sub_args):
                            fmt = sub_args[idx + 1]
                    lines: list[str] = []
                    for vol in client.volumes.list():
                        attrs = getattr(vol, "attrs", {}) or {}
                        name = getattr(vol, "name", "")
                        driver = str(attrs.get("Driver", ""))
                        scope = str(attrs.get("Scope", ""))
                        mountpoint = str(attrs.get("Mountpoint", ""))
                        if fmt:
                            line = fmt
                            line = line.replace("{{.Name}}", name)
                            line = line.replace("{{.Driver}}", driver)
                            line = line.replace("{{.Scope}}", scope)
                            line = line.replace("{{.Mountpoint}}", mountpoint)
                        else:
                            line = f"{name}|{driver}|{scope}|{mountpoint}"
                        lines.append(line)
                    return 0, "\n".join(lines), ""
                if sub == "create":
                    driver = "local"
                    if "--driver" in sub_args:
                        idx = sub_args.index("--driver")
                        if idx + 1 < len(sub_args):
                            driver = sub_args[idx + 1]
                            names = [x for i, x in enumerate(
                                sub_args) if i not in {idx, idx + 1}]
                    out: list[str] = []
                    for name in names:
                        v = client.volumes.create(name=name, driver=driver)
                        out.append(v.name)
                    return 0, "\n".join(out), ""
                if sub == "rm":
                    for name in names:
                        client.volumes.get(name).remove(force=True)
                    return 0, "", ""
                if sub == "inspect":
                    payload: list[dict[str, object]] = []
                    for name in names:
                        payload.append(client.volumes.get(name).attrs)
                    return 0, json.dumps(payload, ensure_ascii=False, indent=2), ""
                if sub == "prune":
                    remove_all = "--all" in sub_args or "-a" in sub_args
                    if remove_all:
                        pruned = client.api.prune_volumes(
                            filters={"all": True})
                    else:
                        pruned = client.volumes.prune()
                    return 0, json.dumps(pruned, ensure_ascii=False, indent=2), ""
                return 1, "", f"Subcomando volume no soportado: {sub}"

            if cmd == "inspect":
                if len(rest) >= 3 and rest[0] == "--format" and rest[1] == "{{.State.Running}}":
                    cont = client.containers.get(rest[2])
                    cont.reload()
                    running = (cont.attrs.get("State", {})
                               or {}).get("Running", False)
                    return 0, ("true" if running else "false"), ""
                if len(rest) >= 3 and rest[0] == "--format" and rest[1] == "{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}":
                    cont = client.containers.get(rest[2])
                    cont.reload()
                    nets = ((cont.attrs.get("NetworkSettings", {})
                            or {}).get("Networks", {}) or {}).keys()
                    return 0, " ".join(str(n) for n in nets), ""
                if len(rest) >= 3 and rest[0] == "--format" and rest[1] == "{{range .Mounts}}{{if eq .Type \"volume\"}}{{.Name}} {{end}}{{end}}":
                    cont = client.containers.get(rest[2])
                    cont.reload()
                    mounts = cont.attrs.get("Mounts", []) or []
                    names: list[str] = []
                    for mount in mounts:
                        if str(mount.get("Type", "")) == "volume":
                            name = str(mount.get("Name", "")).strip()
                            if name:
                                names.append(name)
                    return 0, " ".join(names), ""
                return 1, "", "docker inspect: formato no soportado"

            if cmd == "image":
                if not rest:
                    return 1, "", "docker image: falta subcomando"
                sub = rest[0]
                sub_args = rest[1:]
                if sub == "inspect":
                    if not sub_args:
                        return 1, "", "docker image inspect: falta imagen"
                    payload: list[dict[str, object]] = []
                    for image_ref in sub_args:
                        image = client.images.get(image_ref)
                        payload.append(image.attrs)
                    return 0, json.dumps(payload, ensure_ascii=False, indent=2), ""
                if sub == "ls":
                    fmt = ""
                    if "--format" in sub_args:
                        idx = sub_args.index("--format")
                        if idx + 1 < len(sub_args):
                            fmt = sub_args[idx + 1]
                    lines: list[str] = []
                    for image in client.images.list():
                        attrs = getattr(image, "attrs", {}) or {}
                        repo_tags = attrs.get("RepoTags", []) or []
                        image_id = str(attrs.get("Id", ""))
                        tag_text = "<none>"
                        if repo_tags:
                            tag_text = ",".join(str(tag) for tag in repo_tags)
                        if fmt:
                            line = fmt.replace("{{.Repository}}", tag_text).replace(
                                "{{.ID}}", image_id)
                        else:
                            line = f"{tag_text}|{image_id}"
                        lines.append(line)
                    return 0, "\n".join(lines), ""
                return 1, "", f"Subcomando image no soportado: {sub}"

            if cmd == "pull":
                if not rest:
                    return 1, "", "docker pull: falta imagen"
                image_ref = rest[-1]
                if ":" in image_ref and not image_ref.startswith("sha256:"):
                    repo, tag = image_ref.rsplit(":", 1)
                    image = client.images.pull(repo, tag=tag)
                else:
                    image = client.images.pull(image_ref)
                image_id = getattr(image, "id", "") or ""
                return 0, image_id, ""

            if cmd == "port":
                if not rest:
                    return 1, "", "docker port: falta contenedor"
                cont = client.containers.get(rest[0])
                ports = (cont.attrs.get("NetworkSettings", {})
                         or {}).get("Ports", {}) or {}
                if len(rest) >= 2:
                    requested = rest[1]
                    key = requested if "/" in requested else f"{requested}/tcp"
                    binds = ports.get(key) or []
                    lines = [
                        f"{b.get('HostIp', '0.0.0.0')}:{b.get('HostPort', '')}" for b in binds]
                    return 0, "\n".join(lines), ""
                lines: list[str] = []
                for cport, binds in ports.items():
                    if not binds:
                        continue
                    for b in binds:
                        lines.append(
                            f"{cport} -> {b.get('HostIp', '0.0.0.0')}:{b.get('HostPort', '')}")
                return 0, "\n".join(lines), ""

            if cmd == "exec":
                idx = 0
                user = None
                if len(rest) >= 3 and rest[0] == "-u":
                    user = rest[1]
                    idx = 2
                if idx >= len(rest):
                    return 1, "", "docker exec: falta contenedor"
                cont_name = rest[idx]
                exec_cmd = rest[idx + 1:]
                if not exec_cmd:
                    return 1, "", "docker exec: falta comando"

                cont = client.containers.get(cont_name)
                if exec_cmd == ["env"]:
                    env_list = (cont.attrs.get("Config", {})
                                or {}).get("Env", []) or []
                    return 0, "\n".join(env_list), ""

                cmd_value: object
                if len(exec_cmd) == 1:
                    cmd_value = exec_cmd[0]
                else:
                    cmd_value = exec_cmd
                result = cont.exec_run(
                    cmd=cmd_value, user=user, stdout=True, stderr=True)
                exit_code = int(result.exit_code)
                output = result.output.decode("utf-8", errors="replace") if isinstance(
                    result.output, (bytes, bytearray)) else str(result.output)
                if exit_code == 0:
                    return 0, output.strip(), ""
                return exit_code, "", output.strip() or "Fallo en docker exec"

            if cmd == "cp":
                if len(rest) != 2:
                    return 1, "", "docker cp: argumentos invalidos"
                src, dst = rest
                if _looks_like_container_spec(src) or _looks_like_container_spec(dst):
                    return self._run_sdk_cp_helper_subprocess(host_override=host_override, src=src, dst=dst)
                return 1, "", "docker cp: direccion no soportada"

            if cmd == "logs":
                tail = 100
                follow = False
                i = 0
                while i < len(rest):
                    token = rest[i]
                    if token == "--tail" and i + 1 < len(rest):
                        tail = int(rest[i + 1])
                        i += 2
                        continue
                    if token == "-f":
                        follow = True
                        i += 1
                        continue
                    break
                if i >= len(rest):
                    return 1, "", "docker logs: falta contenedor"
                cont = client.containers.get(rest[i])
                if follow:
                    return 1, "", "docker logs -f no soportado en este modo"
                data = cont.logs(stdout=True, stderr=True, tail=tail)
                return 0, data.decode("utf-8", errors="replace").strip(), ""

            if cmd == "run":
                detach = False
                auto_remove = False
                name = None
                network = None
                user = None
                entrypoint = None
                working_dir = None
                restart_policy: dict[str, str] | None = None
                environment: dict[str, str] = {}
                volumes: dict[str, dict[str, str]] = {}
                ports: dict[str, object] = {}

                i = 0
                while i < len(rest):
                    token = rest[i]
                    if token == "-d":
                        detach = True
                        i += 1
                        continue
                    if token == "--rm":
                        auto_remove = True
                        i += 1
                        continue
                    if token == "--name" and i + 1 < len(rest):
                        name = rest[i + 1]
                        i += 2
                        continue
                    if token == "--network" and i + 1 < len(rest):
                        network = rest[i + 1]
                        i += 2
                        continue
                    if token in {"-u", "--user"} and i + 1 < len(rest):
                        user = rest[i + 1]
                        i += 2
                        continue
                    if token == "--entrypoint" and i + 1 < len(rest):
                        entrypoint = rest[i + 1]
                        i += 2
                        continue
                    if token == "--restart" and i + 1 < len(rest):
                        restart_policy = {"Name": rest[i + 1]}
                        i += 2
                        continue
                    if token in {"-w", "--workdir"} and i + 1 < len(rest):
                        working_dir = rest[i + 1]
                        i += 2
                        continue
                    if token == "-e" and i + 1 < len(rest):
                        kv = rest[i + 1]
                        if "=" in kv:
                            k, v = kv.split("=", 1)
                            environment[k] = v
                        i += 2
                        continue
                    if token == "-v" and i + 1 < len(rest):
                        vm = rest[i + 1]
                        parts = vm.split(":", 2)
                        if len(parts) >= 2:
                            src, bind = parts[0], parts[1]
                            mode = parts[2] if len(parts) == 3 else "rw"
                            volumes[src] = {"bind": bind, "mode": mode}
                        i += 2
                        continue
                    if token == "-p" and i + 1 < len(rest):
                        pm = rest[i + 1]
                        if ":" in pm:
                            host, cont_port = pm.split(":", 1)
                            key = cont_port if "/" in cont_port else f"{cont_port}/tcp"
                            ports[key] = int(host)
                        i += 2
                        continue
                    break

                if i >= len(rest):
                    return 1, "", "docker run: falta imagen"
                image = rest[i]
                command = rest[i + 1:] if (i + 1) < len(rest) else None
                if isinstance(command, list) and len(command) == 1:
                    command = command[0]

                result = client.containers.run(
                    image,
                    command=command,
                    detach=detach,
                    remove=auto_remove,
                    name=name,
                    network=network,
                    user=user,
                    entrypoint=entrypoint,
                    working_dir=working_dir,
                    restart_policy=restart_policy,
                    environment=environment or None,
                    volumes=volumes or None,
                    ports=ports or None,
                )
                if detach:
                    return 0, getattr(result, "id", ""), ""
                if isinstance(result, (bytes, bytearray)):
                    return 0, result.decode("utf-8", errors="replace").strip(), ""
                return 0, str(result), ""

            return 1, "", f"Comando docker no soportado por SDK: {cmd}"
        except NotFound as exc:
            return 1, "", str(exc)
        except APIError as exc:
            return 1, "", str(exc)
        except DockerException as exc:
            return 1, "", str(exc)
        except Exception as exc:
            return 1, "", str(exc)
        finally:
            self._schedule_helper_container_cleanup()

    def _build_docker_command(self, args: list[str]) -> list[str]:
        if not args:
            return args
        if args[0].lower() != "docker":
            return args
        if self.docker_mode != "remote" or not self.docker_host:
            return args
        return ["docker", "-H", self.docker_host, *args[1:]]

    def _prompt_startup_connection_mode(self) -> bool:
        self.discovered_lan_hosts = self._discover_lan_hosts()

        dialog = tk.Toplevel(self.root)
        dialog.title("Modo Docker")
        dialog.resizable(False, False)
        dialog.transient(self.root)
        dialog.grab_set()

        initial_mode = self.docker_mode if self.docker_mode in {
            "local", "remote"} else "local"
        current_host = self.docker_host
        if current_host.startswith("tcp://"):
            current_host = current_host[6:]
        elif current_host.startswith("http://"):
            current_host = current_host[7:]
        elif current_host.startswith("https://"):
            current_host = current_host[8:]

        mode_var = tk.StringVar(value=initial_mode)
        lan_default = ""
        if current_host and current_host in self.discovered_lan_hosts:
            lan_default = current_host
        elif self.discovered_lan_hosts:
            lan_default = self.discovered_lan_hosts[0]
        lan_var = tk.StringVar(value=lan_default)
        manual_var = tk.StringVar(value="")
        result = {"accepted": False}

        body = ttk.Frame(dialog, padding=14)
        body.grid(row=0, column=0, sticky="nsew")
        body.columnconfigure(0, weight=1)

        ttk.Label(
            body,
            text="Selecciona como quieres conectar con Docker",
            style="Title.TLabel",
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(
            body,
            text="Local usa Docker Desktop en este equipo. Remoto permite host LAN o dominio/IP publico.",
            style="Muted.TLabel",
        ).grid(row=1, column=0, sticky="w", pady=(2, 10))

        mode_box = ttk.Frame(body)
        mode_box.grid(row=2, column=0, sticky="w")
        ttk.Radiobutton(mode_box, text="Modo local", value="local",
                        variable=mode_var).grid(row=0, column=0, sticky="w")
        ttk.Radiobutton(mode_box, text="Modo remoto", value="remote", variable=mode_var).grid(
            row=0, column=1, sticky="w", padx=(16, 0))

        remote_frame = ttk.LabelFrame(body, text="Destino remoto", padding=10)
        remote_frame.grid(row=3, column=0, sticky="ew", pady=(10, 0))
        remote_frame.columnconfigure(1, weight=1)

        ttk.Label(remote_frame, text="Host LAN detectado:").grid(
            row=0, column=0, sticky="w", padx=(0, 8), pady=(0, 6))
        lan_combo = ttk.Combobox(remote_frame, textvariable=lan_var,
                                 state="readonly", values=self.discovered_lan_hosts)
        lan_combo.grid(row=0, column=1, sticky="ew", pady=(0, 6))

        ttk.Label(remote_frame, text="Dominio/IP manual:").grid(row=1,
                  column=0, sticky="w", padx=(0, 8))
        manual_entry = ttk.Entry(remote_frame, textvariable=manual_var)
        manual_entry.grid(row=1, column=1, sticky="ew")

        ttk.Label(
            remote_frame,
            text="Formato: 192.168.1.50, 192.168.1.50:2375, mi-dominio.com o tcp://host:puerto",
            style="Muted.TLabel",
        ).grid(row=2, column=0, columnspan=2, sticky="w", pady=(6, 0))

        buttons = ttk.Frame(body)
        buttons.grid(row=4, column=0, sticky="e", pady=(12, 0))

        def refresh_remote_controls(*_args: object) -> None:
            is_remote = mode_var.get() == "remote"
            lan_state = "readonly" if is_remote and self.discovered_lan_hosts else "disabled"
            lan_combo.configure(state=lan_state)
            manual_entry.configure(state="normal" if is_remote else "disabled")

        def accept_mode() -> None:
            selected_mode = mode_var.get()
            if selected_mode == "local":
                self.docker_mode = "local"
                self.docker_host = ""
                self.docker_sdk_client = None
                self._sdk_last_fail_at = 0.0
            else:
                raw_host = (manual_var.get() or lan_var.get()).strip()
                normalized = self._normalize_docker_host(raw_host)
                if not normalized:
                    messagebox.showwarning(
                        "Modo remoto",
                        "Indica un host remoto valido (LAN, dominio o IP publica).",
                        parent=dialog,
                    )
                    return
                self.docker_mode = "remote"
                self.docker_host = normalized
                self.docker_sdk_client = None
                self._sdk_last_fail_at = 0.0

            result["accepted"] = True
            dialog.destroy()

        def cancel_mode() -> None:
            dialog.destroy()

        ttk.Button(buttons, text="Cancelar", style="Ghost.TButton",
                   command=cancel_mode).grid(row=0, column=0, padx=(0, 8))
        ttk.Button(buttons, text="Continuar", style="Accent.TButton",
                   command=accept_mode).grid(row=0, column=1)

        mode_var.trace_add("write", refresh_remote_controls)
        refresh_remote_controls()

        dialog.protocol("WM_DELETE_WINDOW", cancel_mode)
        dialog.update_idletasks()
        self._center_window(dialog)
        self.root.wait_window(dialog)

        if not result["accepted"]:
            return False

        if self.docker_mode == "remote":
            self.status_var.set(f"Docker remoto: {self.docker_host}")
        else:
            self.status_var.set("Docker local: comprobando...")
        self._update_connection_mode_badge()
        return True

    def change_connection_mode(self) -> None:
        old_mode = self.docker_mode
        old_host = self.docker_host

        if not self._prompt_startup_connection_mode():
            return

        if old_mode == self.docker_mode and old_host == self.docker_host:
            return

        self.docker_sdk_client = None
        self._sdk_last_fail_at = 0.0

        if self.docker_mode == "remote":
            detail = f"Modo remoto activo: {self.docker_host}"
            self.log_event("DOCKER", "modo", "INFO",
                           f"Cambio de modo a remoto ({self.docker_host})")
        else:
            detail = "Modo local activo"
            self.log_event("DOCKER", "modo", "INFO", "Cambio de modo a local")

        self.refresh_history()
        self.refresh_everything()
        messagebox.showinfo("Modo Docker", detail)

    def _normalize_docker_host(self, raw_host: str) -> str:
        host = raw_host.strip()
        if not host:
            return ""

        host = host.replace(" ", "")
        if host.startswith("http://"):
            host = f"tcp://{host[7:]}"
        elif host.startswith("https://"):
            host = f"tcp://{host[8:]}"

        if host.startswith("tcp://") or host.startswith("ssh://") or host.startswith("npipe://"):
            return host

        if ":" not in host.rsplit("]", 1)[-1]:
            detected_port = self._pick_remote_docker_port(host)
            host = f"{host}:{detected_port}"

        return f"tcp://{host}"

    @staticmethod
    def _normalize_docker_resource_name(raw_name: str, fallback: str) -> str:
        name = (raw_name or "").strip().strip('"').strip("'")
        if not name:
            return fallback

        if "?" in name or "=" in name:
            tail = name.split("?", 1)[-1]
            if "=" in tail:
                candidate = tail.rsplit("=", 1)[-1].strip()
                if candidate:
                    name = candidate

        name = re.sub(r"[^a-zA-Z0-9_.-]+", "-", name).strip("-._")
        return name or fallback

    def _is_tcp_open(self, host: str, port: int, timeout: float = 0.8) -> bool:
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return True
        except Exception:
            return False

    def _pick_remote_docker_port(self, host: str) -> int:
        # Prioriza 2375 por compatibilidad histórica; si no responde y 2376 sí, usa 2376.
        if self._is_tcp_open(host, 2375):
            return 2375
        if self._is_tcp_open(host, 2376):
            return 2376
        return 2375

    def _discover_lan_hosts(self) -> list[str]:
        candidates: set[str] = set()

        try:
            code, output, _ = self._run(["arp", "-a"])
            if code == 0 and output:
                for ip in re.findall(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", output):
                    octets = [int(part) for part in ip.split(".")]
                    if len(octets) != 4:
                        continue
                    if any(part > 255 for part in octets):
                        continue
                    if ip.startswith("127.") or ip == "0.0.0.0" or ip == "255.255.255.255":
                        continue
                    candidates.add(ip)
        except Exception:
            pass

        return sorted(candidates)

    def _center_window(self, window: tk.Toplevel) -> None:
        window.update_idletasks()
        width = window.winfo_width()
        height = window.winfo_height()
        x = (window.winfo_screenwidth() // 2) - (width // 2)
        y = (window.winfo_screenheight() // 2) - (height // 2)
        window.geometry(f"+{x}+{y}")

    def _resource_candidates(self, relative_path: str) -> list[str]:
        candidates: list[str] = []

        # Desarrollo: recursos junto al codigo de la app.
        candidates.append(os.path.join(self.app_dir, relative_path))

        # Compatibilidad hacia atras: recursos en carpeta padre (utilidades).
        candidates.append(os.path.join(self.tools_dir, relative_path))

        # Ejecutable PyInstaller onefile: recursos extraidos en _MEIPASS
        meipass = getattr(sys, "_MEIPASS", "")
        if meipass:
            candidates.append(os.path.join(meipass, relative_path))

        # Ejecutable instalado junto a archivos auxiliares
        exe_dir = os.path.dirname(sys.executable) if getattr(
            sys, "frozen", False) else ""
        if exe_dir:
            candidates.append(os.path.join(exe_dir, relative_path))

        # Fallback adicional: cwd
        candidates.append(os.path.join(os.getcwd(), relative_path))
        return candidates

    def _migrate_legacy_state_files(self) -> None:
        legacy_to_current = [
            (os.path.join(self.tools_dir, "perfiles_shopify.ini"), self.profiles_file),
            (os.path.join(self.tools_dir, "historial_shopify.log"), self.history_file),
        ]
        for legacy_path, current_path in legacy_to_current:
            try:
                if os.path.isfile(current_path) or not os.path.isfile(legacy_path):
                    continue
                os.replace(legacy_path, current_path)
            except Exception:
                # Si no se puede mover, mantenemos compatibilidad sin bloquear arranque.
                pass

    def _find_first_existing(self, relative_paths: list[str]) -> str:
        for rel in relative_paths:
            for candidate in self._resource_candidates(rel):
                if os.path.isfile(candidate):
                    return candidate
        return ""

    def _extract_host_port_from_docker_host(self, value: str) -> tuple[str, int] | None:
        host = (value or "").strip()
        if not host:
            return None

        if host.startswith("tcp://"):
            host = host[6:]
        elif host.startswith("http://"):
            host = host[7:]
        elif host.startswith("https://"):
            host = host[8:]
        elif host.startswith("ssh://"):
            return None

        if host.startswith("[") and "]" in host:
            # IPv6 con corchetes: [::1]:2375
            end = host.find("]")
            ip6 = host[1:end]
            rest = host[end + 1:]
            if rest.startswith(":") and rest[1:].isdigit():
                return ip6, int(rest[1:])
            return ip6, 2375

        if ":" in host:
            h, p = host.rsplit(":", 1)
            if p.isdigit():
                return h, int(p)
        return host, 2375

    def _extract_ssh_host_from_docker_host(self, value: str) -> str | None:
        host = (value or "").strip()
        if not host.startswith("ssh://"):
            return None
        target = host[6:].strip().split("/", 1)[0]
        if not target:
            return None
        if "@" in target:
            target = target.split("@", 1)[1]
        if target.startswith("[") and "]" in target:
            end = target.find("]")
            ip6 = target[1:end].strip()
            return ip6 or None
        if ":" in target:
            return target.rsplit(":", 1)[0].strip() or None
        return target or None

    def _diagnose_remote_docker_host(self) -> str:
        if self.docker_mode != "remote" or not self.docker_host:
            return ""

        now = time.time()
        if (now - self._last_remote_diag_at) < 8.0 and self._last_remote_diag_text:
            return self._last_remote_diag_text

        parsed = self._extract_host_port_from_docker_host(self.docker_host)
        if parsed is None:
            diag = "Host remoto en modo SSH. Verifica que Docker acepte conexiones por SSH y que las credenciales sean validas."
            self._last_remote_diag_at = now
            self._last_remote_diag_text = diag
            return diag

        host, port = parsed
        try:
            resolved = socket.gethostbyname(host)
        except Exception:
            diag = f"No se pudo resolver DNS del host remoto '{host}'."
            self._last_remote_diag_at = now
            self._last_remote_diag_text = diag
            return diag

        try:
            with socket.create_connection((host, port), timeout=2):
                diag = (
                    f"Host remoto responde por TCP ({host}:{port}, {resolved}), "
                    "pero Docker rechazo la conexion. Revisa TLS/credenciales o que el daemon acepte API remota."
                )
        except Exception:
            if port == 2375 and self._is_tcp_open(host, 2376, timeout=1.2):
                diag = (
                    f"El puerto 2375 no responde en {host}, pero 2376 sí esta abierto. "
                    "Prueba conectando como tcp://HOST:2376 (normalmente requiere TLS)."
                )
                self._last_remote_diag_at = now
                self._last_remote_diag_text = diag
                return diag
            diag = (
                f"Host remoto resuelve ({host} -> {resolved}) pero el puerto {port} no responde. "
                "Abre firewall y expone Docker API en ese puerto."
            )

        self._last_remote_diag_at = now
        self._last_remote_diag_text = diag
        return diag

    def _docker_unavailable_message(self) -> str:
        if self._docker_check_in_progress:
            return "Comprobando conexion Docker... espera unos segundos y vuelve a intentarlo."

        details: list[str] = ["Docker no esta disponible."]
        if self.docker_mode == "remote" and self.docker_host:
            details.append(f"Modo remoto: {self.docker_host}")
            diag = self._diagnose_remote_docker_host()
            if diag:
                details.append(diag)
        else:
            if not self._detect_docker_cli() and docker is None:
                details.append(
                    "No se encontro docker.exe ni el paquete Python 'docker'.")
                details.append(
                    "Instala Docker Desktop o recompila la app incluyendo dependencia 'docker'.")

        if self.last_docker_error_detail:
            details.append(f"Error tecnico: {self.last_docker_error_detail}")
        return "\n\n".join(details)

    def _ensure_profiles_file(self) -> None:
        if os.path.isfile(self.profiles_file):
            return
        with open(self.profiles_file, "w", encoding="utf-8") as fh:
            fh.write("; Formato: nombre_perfil=contenedor1,contenedor2\n")
            fh.write("; Ejemplo: tienda=shopify-dev1\n")

    @staticmethod
    def _build_audit_actor() -> str:
        user = (os.environ.get("USERNAME") or os.environ.get(
            "USER") or "desconocido").strip() or "desconocido"
        host = (os.environ.get("COMPUTERNAME") or socket.gethostname()
                or "equipo-desconocido").strip() or "equipo-desconocido"
        return f"{user}@{host}"

    def _ensure_remote_history_volume(self, client: object) -> None:
        try:
            # type: ignore[union-attr]
            client.volumes.get(self.remote_history_volume)
        except Exception:
            # type: ignore[union-attr]
            client.volumes.create(name=self.remote_history_volume)

    def _append_remote_history_line(self, line: str) -> None:
        if self.docker_mode != "remote":
            raise RuntimeError(
                "El historial requiere modo remoto activo para registrar auditoria compartida.")

        client = self._get_docker_sdk_client(timeout_seconds=20)
        if client is None:
            raise RuntimeError(
                "No se pudo conectar con Docker remoto para escribir historial.")

        self._ensure_remote_history_volume(client)
        cmd = [
            "sh",
            "-c",
            f"mkdir -p /data && touch {self.remote_history_path} && printf '%s\\n' \"$WPU_HISTORY_LINE\" >> {self.remote_history_path}",
        ]
        client.containers.run(
            "alpine",
            command=cmd,
            remove=True,
            labels={self._helper_label_key: self._helper_label_value,
                "wpu.role": "history-write"},
            environment={"WPU_HISTORY_LINE": line},
            volumes={self.remote_history_volume: {
                "bind": "/data", "mode": "rw"}},
        )

    def _read_remote_history_lines(self) -> list[str]:
        if self.docker_mode != "remote":
            raise RuntimeError(
                "Activa modo remoto para consultar historial compartido.")

        client = self._get_docker_sdk_client(timeout_seconds=20)
        if client is None:
            raise RuntimeError(
                "No se pudo conectar con Docker remoto para leer historial.")

        self._ensure_remote_history_volume(client)
        cmd = [
            "sh",
            "-c",
            f"mkdir -p /data && touch {self.remote_history_path} && cat {self.remote_history_path}",
        ]
        data = client.containers.run(
            "alpine",
            command=cmd,
            remove=True,
            labels={self._helper_label_key: self._helper_label_value,
                "wpu.role": "history-read"},
            volumes={self.remote_history_volume: {
                "bind": "/data", "mode": "rw"}},
        )
        raw = data.decode(
            "utf-8", errors="replace") if isinstance(data, (bytes, bytearray)) else str(data)
        return [line.rstrip("\n") for line in raw.splitlines()]

    def _on_history_tab_selected(self, _event: object = None) -> None:
        if self.tabs is None or self.history_tab_frame is None:
            return
        selected_id = self.tabs.select()
        if not selected_id:
            return
        selected_widget = self.tabs.nametowidget(selected_id)
        if selected_widget is self.history_tab_frame:
            self.refresh_history()

    def _is_history_tab_visible(self) -> bool:
        if self.tabs is None or self.history_tab_frame is None:
            return False
        selected_id = self.tabs.select()
        if not selected_id:
            return False
        return self.tabs.nametowidget(selected_id) is self.history_tab_frame

    def _refresh_history_if_visible(self) -> None:
        if self._is_history_tab_visible():
            self.refresh_history()

    def _schedule_helper_container_cleanup(self, force: bool = False) -> None:
        now = time.time()
        if self._helper_cleanup_in_progress:
            return
        if not force and (now - self._helper_cleanup_last_at) < 20.0:
            return

        self._helper_cleanup_in_progress = True
        self._helper_cleanup_last_at = now

        def worker() -> None:
            try:
                client = self._get_docker_sdk_client(timeout_seconds=10)
                if client is None:
                    return

                labeled = client.containers.list(  # type: ignore[union-attr]
                    all=True,
                    filters={
                        "status": "exited", "label": f"{self._helper_label_key}={self._helper_label_value}"},
                )
                for cont in labeled:
                    try:
                        cont.remove(force=True)
                    except Exception:
                        pass

                # Cleanup de helpers antiguos sin labels (retrocompatibilidad).
                legacy = client.containers.list(  # type: ignore[union-attr]
                    all=True,
                    filters={"status": "exited", "ancestor": "alpine"},
                )
                for cont in legacy:
                    try:
                        cfg = getattr(cont, "attrs", {}).get("Config", {})
                        cmd = cfg.get("Cmd")
                        cmd_text = " ".join(str(x) for x in cmd) if isinstance(
                            cmd, list) else str(cmd or "")
                        cmd_low = cmd_text.lower()
                        if "/data" in cmd_low and (
                            "profiles" in cmd_low
                            or "historial" in cmd_low
                            or "wpu_batch" in cmd_low
                            or "wpu_history_line" in cmd_low
                        ):
                            cont.remove(force=True)
                    except Exception:
                        pass
            except Exception:
                pass
            finally:
                self._helper_cleanup_in_progress = False

        threading.Thread(target=worker, daemon=True).start()

    def _render_history_message(self, message: str) -> None:
        self.history_text.configure(state="normal")
        self.history_text.delete("1.0", tk.END)
        self.history_text.insert("1.0", message)
        self.history_text.configure(state="disabled")

    def _show_history_loading_spinner(self) -> None:
        """Muestra el overlay spinner sobre el historial y lanza la animación."""
        if not hasattr(self, "_history_spinner_frame"):
            return
        self._history_spinner_frame.place(
            relx=0, rely=0, relwidth=1, relheight=1)
        self._history_spinner_frame.lift()
        self._history_spinner_index = 0
        self._animate_history_spinner()

    def _animate_history_spinner(self) -> None:
        if not hasattr(self, "_history_spinner_frame"):
            return
        if not self._history_spinner_frame.winfo_ismapped():
            return
        pulses = ["⬤", "◉", "○", "◉"]
        idx = self._history_spinner_index % len(pulses)
        self._history_spinner_dot_label.configure(text=pulses[idx])
        self._history_spinner_index += 1
        self._history_spinner_job = self.root.after(
            220, self._animate_history_spinner)

    def _hide_history_loading_spinner(self) -> None:
        """Oculta el overlay spinner del historial."""
        if hasattr(self, "_history_spinner_job") and self._history_spinner_job is not None:
            try:
                self.root.after_cancel(self._history_spinner_job)
            except Exception:
                pass
            self._history_spinner_job = None
        if hasattr(self, "_history_spinner_frame"):
            self._history_spinner_frame.place_forget()

    def _history_refresh_worker(self) -> None:
        try:
            if self.docker_mode != "remote":
                # LOCAL MODE: write pending lines to local file and read it back
                # Flush pending lines first
                with self._history_pending_lock:
                    pending = list(self._history_pending_lines)
                    self._history_pending_lines.clear()

                if pending:
                    try:
                        os.makedirs(os.path.dirname(
                            self.history_file) or ".", exist_ok=True)
                        with open(self.history_file, "a", encoding="utf-8") as fh:
                            for line in pending:
                                fh.write(line + "\n")
                    except Exception:
                        pass

                lines: list[str] = []
                try:
                    if os.path.isfile(self.history_file):
                        with open(self.history_file, "r", encoding="utf-8", errors="replace") as fh:
                            lines = [ln.rstrip("\n") for ln in fh.readlines()]
                except Exception:
                    pass
                self._history_refresh_queue.put((True, lines))
                return

            # REMOTE MODE: use Docker volume with direct docker commands

            # Flush pending lines JUST BEFORE writing to capture lines added during execution
            with self._history_pending_lock:
                pending = list(self._history_pending_lines)
                self._history_pending_lines.clear()

            # Write pending lines first (with retry on failure)
            if pending:
                batch = "".join(ln + "\n" for ln in pending)
                try:
                    import subprocess


                    result = subprocess.run([
                    "docker", "-H", self.docker_host,
                    "run", "--rm",
                    "-v", f"{self.remote_history_volume}:/data",
                    "-e", f"WPU_BATCH={batch}",
                     "alpine", "sh", "-c", f"printf '%s' \"$WPU_BATCH\" >> {self.remote_history_path}",
                    ], capture_output=True, text=True, timeout=30,
                     creationflags=subprocess.CREATE_NO_WINDOW)  # ← añadir esto
                    if result.returncode == 0:
                        pass
                    else:
                        # Re-queue pending lines on failure
                        with self._history_pending_lock:
                            self._history_pending_lines.extend(pending)
                        raise Exception(f"Write failed: {result.stderr}")
                except subprocess.TimeoutExpired:
                    # Re-queue pending lines on timeout
                    with self._history_pending_lock:
                        self._history_pending_lines.extend(pending)
                    raise
                except Exception as e:
                    raise

            # Read the full log
            try:
                import subprocess
                result = subprocess.run([
                    "docker", "-H", self.docker_host,
                    "run", "--rm",
                    "-v", f"{self.remote_history_volume}:/data",
                    "alpine", "cat", f"{self.remote_history_path}"
                ], capture_output=True, text=True, timeout=30,
                   creationflags=subprocess.CREATE_NO_WINDOW)
                if result.returncode == 0:
                    data = result.stdout
                else:
                    data = ""
            except Exception as read_error:
                data = ""
            raw = data.decode("utf-8", errors="replace") if isinstance(data, (bytes, bytearray)) else str(data)
            lines = [ln.rstrip("\n") for ln in raw.splitlines()]
            # Limpiar líneas vacías y espacios en blanco
            lines = [ln for ln in lines if ln.strip()]
            self._history_refresh_queue.put((True, lines))
        except Exception as exc:
            self._history_refresh_queue.put((False, str(exc)))

    def _poll_history_refresh_queue(self) -> None:
        try:
            ok, payload = self._history_refresh_queue.get_nowait()
        except queue.Empty:
            self._history_refresh_job_id = self.root.after(100, self._poll_history_refresh_queue)
            return

        self._history_refresh_in_progress = False
        self._history_refresh_job_id = None  # FIX: permitir que refresh_history() reinicie el polling en la próxima llamada
        self._stop_status_spinner()  # Detener el spinner del historial
        self._hide_history_loading_spinner()  # Ocultar overlay spinner del historial

        # Guardar el estado de solicitud pendiente ANTES de procesar
        was_requested = self._history_refresh_requested
        self._history_refresh_requested = False
        
        if ok:
            self.history_lines = list(payload) if isinstance(payload, list) else []
            self.apply_history_filter()
        else:
            self.history_lines = []
            detail = str(payload)
            if self.docker_mode == "remote":
                self._render_history_message(
                    "Historial remoto no disponible.\n\n"
                    "Activa modo remoto y valida acceso al daemon Docker para auditoria compartida.\n\n"
                    f"Detalle: {detail}"
                )
            else:
                self._render_history_message(
                    f"Historial no disponible.\n\n"
                    f"Detalle: {detail}"
                )

        # Reiniciar si había una solicitud PENDIENTE para capturar cualquier cambio reciente
        # Usar after() para dar tiempo a que las líneas pendientes se acumulen
        if was_requested:
            self.root.after(500, self.refresh_history)

    def log_event(self, accion: str, objetivo: str, estado: str, detalle: str) -> None:
        stamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        actor = self.audit_actor
        line = f"[{stamp}] [{estado}] {accion} | {objetivo} | usuario={actor} | {detalle}"
        print(f"[DEBUG] log_event: {line}")
        self._set_last_action(f"{accion} / {objetivo} / {estado}")
        if estado.upper() in {"ERROR", "WARN"}:
            self._register_recent_error(f"{accion} | {objetivo} | {detalle}")
        
        # Write directly to storage (Docker volume in remote mode, local file otherwise)
        if self.docker_mode == "remote":
            try:
                import subprocess
                batch = line + "\n"
                result = subprocess.run([
                "docker", "-H", self.docker_host, "run", "--rm",
                "-v", f"{self.remote_history_volume}:/data",
                "-e", f"WPU_BATCH={batch}",
                "alpine", "sh", "-c", f"mkdir -p /data && printf '%s' \"$WPU_BATCH\" >> {self.remote_history_path}",
                ], capture_output=True, text=True, timeout=30,
                   creationflags=subprocess.CREATE_NO_WINDOW)  # ← añadir
                if result.returncode == 0:
                    print(f"[DEBUG] log_event: Directly wrote to Docker volume")
                else:
                    print(f"[DEBUG] log_event: Failed to write to Docker: {result.stderr}")
                    # Fallback to pending lines
                    with self._history_pending_lock:
                        self._history_pending_lines.append(line)
            except Exception as e:
                print(f"[DEBUG] log_event: Error writing to Docker: {e}")
                # Fallback to pending lines
                with self._history_pending_lock:
                    self._history_pending_lines.append(line)
        else:
            # Local mode: write to local file
            try:
                os.makedirs(os.path.dirname(self.history_file) or ".", exist_ok=True)
                with open(self.history_file, "a", encoding="utf-8") as fh:
                    fh.write(line + "\n")
                print(f"[DEBUG] log_event: Wrote to local file")
            except Exception as e:
                print(f"[DEBUG] log_event: Error writing to local file: {e}")
                # Fallback to pending lines
                with self._history_pending_lock:
                    self._history_pending_lines.append(line)
        
        # Trigger refresh to display the new event
        self._refresh_observability_panel()
        self.root.after(100, self.refresh_history)

    def docker_ready(self) -> bool:
        now = time.time()
        stale = (now - self._docker_last_checked_at) > 6.0

        if stale and not self._docker_check_in_progress:
            self._start_async_docker_check()

        return self._docker_last_ready

    def _start_async_docker_check(self) -> None:
        if self._docker_check_in_progress:
            return

        self._docker_check_in_progress = True
        # Evita parpadeo en UI: si ya sabemos que Docker esta disponible,
        # el re-check periodico ocurre en segundo plano sin cambiar el texto.
        if not self._docker_last_ready:
            self.status_var.set("Docker: comprobando...")

        worker = threading.Thread(target=self._docker_ready_probe_worker, daemon=True)
        worker.start()

        if self._docker_check_job_id is None:
            self._docker_check_job_id = self.root.after(100, self._poll_docker_check_queue)

    def _docker_ready_probe_worker(self) -> None:
        result = self._probe_docker_ready_blocking()
        self._docker_check_queue.put(result)

    def _poll_docker_check_queue(self) -> None:
        was_ready = self._docker_last_ready
        try:
            ready, status_text, detail = self._docker_check_queue.get_nowait()
        except queue.Empty:
            self._docker_check_job_id = self.root.after(100, self._poll_docker_check_queue)
            return

        self._docker_last_ready = ready
        self._docker_last_checked_at = time.time()
        self._docker_check_in_progress = False
        self._docker_check_job_id = None
        self.last_docker_error_detail = detail

        if self.status_var.get() != status_text:
            self.status_var.set(status_text)
        if (not ready) and detail:
            self.log_event("DOCKER", self.docker_host or "local", "ERROR", detail)
        self._refresh_observability_panel()

        if ready:
            self.refresh_containers(show_errors=False, full_repaint=False)
            if not was_ready:
                # Al recuperar conexion, refrescamos tambien vistas dependientes de Docker.
                self.refresh_volumes()
                self.refresh_networks()
                self.refresh_profiles_ui()
        else:
            self._stop_container_loading_spinner()

    def _probe_docker_ready_blocking(self) -> tuple[bool, str, str]:
        self.docker_cli_available = self._detect_docker_cli()

        if not self.docker_cli_available:
            code, _, err = self._run(["docker", "info"])
            if code == 0:
                if self.docker_mode == "remote":
                    return True, f"Docker remoto: disponible ({self.docker_host})", ""
                return True, "Docker local: disponible (SDK)", ""
            detail = (err or "Sin respuesta de Docker SDK").strip()
            if self.docker_mode == "remote":
                diag = self._diagnose_remote_docker_host()
                status = f"Docker remoto: no disponible ({diag})" if diag else "Docker remoto: no disponible"
            else:
                status = "Docker: no disponible"
            return False, status, detail

        if self.docker_mode == "remote":
            code, _, err = self._run(["docker", "info"])
            if code == 0:
                return True, f"Docker remoto: disponible ({self.docker_host})", ""
            detail = (err or "Fallo de conexion con host remoto").strip()
            diag = self._diagnose_remote_docker_host()
            status = f"Docker remoto: no disponible ({diag})" if diag else "Docker remoto: no disponible"
            return False, status, detail

        code, _, _ = self._run(["docker", "info"])
        if code == 0:
            self.docker_autostart_attempted = False
            return True, "Docker: disponible", ""

        if not self.docker_autostart_attempted:
            self.docker_autostart_attempted = True
            started = self._start_docker_desktop()
            if started and self._wait_for_docker_ready(timeout_seconds=90):
                return True, "Docker: disponible", ""

        return False, "Docker: no disponible", "Docker local no responde a 'docker info'."

    def _start_docker_desktop(self) -> bool:
        candidates = [
            os.path.join(os.environ.get("ProgramFiles", ""), "Docker", "Docker", "Docker Desktop.exe"),
            os.path.join(os.environ.get("ProgramFiles(x86)", ""), "Docker", "Docker", "Docker Desktop.exe"),
            os.path.join(os.environ.get("LocalAppData", ""), "Docker", "Docker", "Docker Desktop.exe"),
        ]

        for exe_path in candidates:
            if not exe_path or not os.path.isfile(exe_path):
                continue
            try:
                subprocess.Popen([exe_path], cwd=self.tools_dir, shell=False, creationflags=subprocess.CREATE_NO_WINDOW)
                return True
            except Exception:
                continue
        return False

    def _wait_for_docker_ready(self, timeout_seconds: int = 90) -> bool:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            code, _, _ = self._run(["docker", "info"])
            if code == 0:
                return True
            time.sleep(2)
        return False

    def _start_status_spinner(self, base_text: str) -> None:
        self._stop_status_spinner()
        self.spinner_base_text = base_text
        self.spinner_index = 0
        self._animate_status_spinner()

    def _animate_status_spinner(self) -> None:
        frames = ["|", "/", "-", "\\"]
        frame = frames[self.spinner_index % len(frames)]
        self.status_var.set(f"{self.spinner_base_text}... {frame}")
        self.spinner_index += 1
        self.spinner_job_id = self.root.after(120, self._animate_status_spinner)

    def _stop_status_spinner(self) -> None:
        if self.spinner_job_id is not None:
            self.root.after_cancel(self.spinner_job_id)
            self.spinner_job_id = None
        self.status_var.set("Listo")

    def _show_loading_modal(self, message: str) -> tk.Toplevel:
        """
        Muestra un modal con spinner animado mientras se ejecuta una operación.
        El modal tiene tres estados:
          • En proceso : spinner girando + mensaje de acción
          • Completado : icono ✔ verde + mensaje de éxito + botón Cerrar
          • Error      : icono ✘ rojo  + mensaje de error  + botón Cerrar
        """
        MODAL_W, MODAL_H = 420, 200

        modal = tk.Toplevel(self.root)
        modal.title("Procesando...")
        modal.geometry(f"{MODAL_W}x{MODAL_H}")
        modal.resizable(False, False)
        modal.transient(self.root)
        modal.grab_set()
        # Impedir cierre manual con la X mientras procesa
        modal.protocol("WM_DELETE_WINDOW", lambda: None)
        modal.configure(bg="#f8fafc")

        # ── Centrar sobre la ventana principal ────────────────────────────────
        self.root.update_idletasks()
        rx = self.root.winfo_x() + (self.root.winfo_width()  - MODAL_W) // 2
        ry = self.root.winfo_y() + (self.root.winfo_height() - MODAL_H) // 2
        modal.geometry(f"{MODAL_W}x{MODAL_H}+{rx}+{ry}")

        # ── Borde superior de color (acento teal) ─────────────────────────────
        accent_bar = tk.Frame(modal, bg="#0f766e", height=4)
        accent_bar.pack(fill="x", side="top")

        # ── Contenido principal ───────────────────────────────────────────────
        content = tk.Frame(modal, bg="#f8fafc")
        content.pack(fill="both", expand=True, padx=28, pady=18)

        # Icono / spinner  (fila superior)
        icon_lbl = tk.Label(
            content,
            text="",
            font=("Segoe UI", 28),
            fg="#14b8a6",
            bg="#f8fafc",
        )
        icon_lbl.grid(row=0, column=0, rowspan=2, padx=(0, 16), sticky="ns")

        # Título de la acción
        title_lbl = tk.Label(
            content,
            text="Ejecutando acción…",
            font=("Segoe UI Semibold", 11),
            fg="#0f172a",
            bg="#f8fafc",
            anchor="w",
            justify="left",
            wraplength=300,
        )
        title_lbl.grid(row=0, column=1, sticky="sw", pady=(4, 0))

        # Detalle / mensaje secundario
        detail_lbl = tk.Label(
            content,
            text=message,
            font=("Segoe UI", 9),
            fg="#64748b",
            bg="#f8fafc",
            anchor="w",
            justify="left",
            wraplength=300,
        )
        detail_lbl.grid(row=1, column=1, sticky="nw", pady=(2, 0))

        content.columnconfigure(1, weight=1)

        # ── Separador y pie ───────────────────────────────────────────────────
        sep = ttk.Separator(modal, orient="horizontal")
        sep.pack(fill="x", padx=0, pady=(0, 0))

        footer = tk.Frame(modal, bg="#f6f6f7", height=46)
        footer.pack(fill="x", side="bottom")
        footer.pack_propagate(False)

        close_btn = ttk.Button(
            footer,
            text="Cerrar",
            style="Accent.TButton",
            command=lambda: _do_close(),
        )
        # El botón empieza oculto; aparece al completar
        close_btn.place(relx=0.5, rely=0.5, anchor="center")
        close_btn.place_forget()

        # ── Estado interno del modal ──────────────────────────────────────────
        modal.spinner_label  = icon_lbl
        modal.title_label    = title_lbl
        modal.detail_label   = detail_lbl
        modal.close_btn      = close_btn
        modal.footer_frame   = footer
        modal.accent_bar     = accent_bar
        modal.spinner_index  = 0
        modal.spinner_job    = None
        modal._done          = False

        def _do_close():
            if modal.winfo_exists():
                if modal.spinner_job:
                    try:
                        modal.after_cancel(modal.spinner_job)
                    except Exception:
                        pass
                modal.grab_release()
                modal.destroy()

        modal._do_close = _do_close

        def animate_spinner():
            if not modal.winfo_exists() or modal._done:
                return
            frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
            modal.spinner_label.config(text=frames[modal.spinner_index % len(frames)])
            modal.spinner_index += 1
            modal.spinner_job = modal.after(90, animate_spinner)

        animate_spinner()
        return modal

    def _close_loading_modal(self, modal: tk.Toplevel) -> None:
        """
        Marca el modal como 'completado con éxito':
          • Detiene el spinner
          • Muestra icono ✔ verde y texto "Acción completada"
          • Habilita el botón Cerrar
          • Permite cerrar con la X
        """
        if not (modal and modal.winfo_exists()):
            return
        modal._done = True
        if modal.spinner_job:
            try:
                modal.after_cancel(modal.spinner_job)
            except Exception:
                pass
            modal.spinner_job = None

        # Actualizar barra de acento a verde
        try:
            modal.accent_bar.configure(bg="#16a34a")
        except Exception:
            pass

        # Icono de éxito
        modal.spinner_label.config(text="✔", fg="#16a34a", font=("Segoe UI", 26))

        # Texto principal
        modal.title_label.config(text="Acción completada", fg="#15803d")

        # Mostrar botón cerrar
        modal.close_btn.place(relx=0.5, rely=0.5, anchor="center")

        # Permitir cierre manual con la X
        modal.protocol("WM_DELETE_WINDOW", modal._do_close)

    def _close_loading_modal_error(self, modal: tk.Toplevel, error_msg: str = "") -> None:
        """
        Marca el modal como 'fallido':
          • Detiene el spinner
          • Muestra icono ✘ rojo y el mensaje de error
          • Habilita el botón Cerrar
        """
        if not (modal and modal.winfo_exists()):
            return
        modal._done = True
        if modal.spinner_job:
            try:
                modal.after_cancel(modal.spinner_job)
            except Exception:
                pass
            modal.spinner_job = None

        # Actualizar barra de acento a rojo
        try:
            modal.accent_bar.configure(bg="#dc2626")
        except Exception:
            pass

        # Icono de error
        modal.spinner_label.config(text="✘", fg="#dc2626", font=("Segoe UI", 26))

        # Texto principal
        modal.title_label.config(text="Se produjo un error", fg="#dc2626")

        # Detalle del error (truncado si es muy largo)
        err_text = (error_msg[:120] + "…") if len(error_msg) > 120 else error_msg
        modal.detail_label.config(text=err_text or "Operación fallida.", fg="#b91c1c")

        # Mostrar botón cerrar
        modal.close_btn.place(relx=0.5, rely=0.5, anchor="center")

        # Permitir cierre manual con la X
        modal.protocol("WM_DELETE_WINDOW", modal._do_close)

    def _finish_loading_modal(
        self,
        modal: tk.Toplevel,
        success: bool,
        error_msg: str = "",
        auto_close_success_ms: int | None = None,
    ) -> None:
        if not (modal and modal.winfo_exists()):
            return
        if success:
            self._close_loading_modal(modal)
            if auto_close_success_ms is not None and auto_close_success_ms >= 0:
                try:
                    modal.after(auto_close_success_ms, modal._do_close)
                except Exception:
                    pass
        else:
            self._close_loading_modal_error(modal, error_msg)

    def _run_with_loading_modal(
        self,
        message: str,
        operation_func,
        *args,
        auto_close_success_ms: int | None = None,
        on_success: Callable[[object], None] | None = None,
        on_error: Callable[[str], None] | None = None,
        **kwargs,
    ):
        """
        Ejecuta *operation_func* en un hilo secundario mostrando un modal con:
          • Spinner animado mientras procesa
          • Estado "Completado" (verde) al terminar con éxito
          • Estado "Error" (rojo) si lanza excepción o devuelve False
        El usuario debe pulsar "Cerrar" para descartar el modal.
        Los messagebox de éxito/error dentro de operation_func siguen funcionando
        con normalidad; se muestran *antes* de que el modal cambie de estado.
        """
        modal = self._show_loading_modal(message)

        def execute_operation():
            success = True
            err_msg = ""
            op_result: object = None
            try:
                result = operation_func(*args, **kwargs)
                op_result = result
                # Considerar False explícito como fallo (convenio existente)
                if result is False:
                    success = False
                    err_msg = "La operación no se completó correctamente."
            except Exception as exc:
                success = False
                err_msg = str(exc)

            # Actualizar el modal en el hilo principal
            if success:
                def _on_success_ui(res: object = op_result) -> None:
                    self._finish_loading_modal(
                        modal,
                        True,
                        auto_close_success_ms=auto_close_success_ms,
                    )
                    if on_success is not None:
                        on_success(res)

                self.root.after(0, _on_success_ui)
            else:
                def _on_error_ui(msg: str = err_msg) -> None:
                    self._finish_loading_modal(modal, False, error_msg=msg)
                    if on_error is not None:
                        on_error(msg)

                self.root.after(0, _on_error_ui)

        threading.Thread(target=execute_operation, daemon=True).start()

    def _selected_tab_widget(self) -> object | None:
        if self.tabs is None:
            return None
        selected_id = self.tabs.select()
        if not selected_id:
            return None
        try:
            return self.tabs.nametowidget(selected_id)
        except Exception:
            return None

    def refresh_everything(self, auto: bool = False) -> None:
        if auto:
            self.refresh_containers(show_errors=False, full_repaint=False)
            self.refresh_logs_targets()
            self._refresh_history_if_visible()

            now = time.time()
            selected = self._selected_tab_widget()
            if (now - self._last_auto_heavy_refresh_at) >= self._auto_heavy_refresh_interval_sec:
                if selected is self.profiles_tab_frame:
                    self.refresh_profiles_ui()
                elif selected is self.networks_tab_frame:
                    self.refresh_networks()
                elif selected is self.volumes_tab_frame:
                    self.refresh_volumes()
                self._last_auto_heavy_refresh_at = now

            self._schedule_auto_refresh()
            return

        self.refresh_containers(show_errors=False, full_repaint=True)
        self.refresh_volumes()
        self.refresh_profiles_ui()
        self.refresh_networks()
        self._refresh_history_if_visible()
        self.refresh_logs_targets()
        self._last_auto_heavy_refresh_at = time.time()
        self._schedule_auto_refresh()

    def _schedule_auto_refresh(self) -> None:
        if self.refresh_job_id is not None:
            self.root.after_cancel(self.refresh_job_id)
        self.refresh_job_id = self.root.after(7000, lambda: self.refresh_everything(auto=True))

    def refresh_logs_targets(self) -> None:
        if not self.container_cache:
            self.container_cache = self.get_all_container_names()
        prev = self.log_container_var.get().strip()
        self.log_container_combo.configure(values=self.container_cache)
        if prev and prev in self.container_cache:
            self.log_container_var.set(prev)
        elif self.container_cache:
            self.log_container_var.set(self.container_cache[0])
        else:
            self.log_container_var.set("")

    def get_all_container_names(self) -> list[str]:
        code, out, _ = self._run(["docker", "ps", "-a", "--format", "{{.Names}}|{{.Image}}|{{.Command}}"])
        if code != 0 or not out:
            self.container_image_cache = {}
            return []
        names: list[str] = []
        image_cache: dict[str, str] = {}
        for line in out.splitlines():
            parts = line.split("|", 2)
            if len(parts) < 3:
                continue
            name = parts[0].strip()
            image = parts[1].strip()
            command = parts[2].strip()
            if not name:
                continue
            if self._is_hidden_helper_container(name, image, command):
                continue
            names.append(name)
            image_cache[name] = image
        self.container_image_cache = image_cache
        return names

    @staticmethod
    def _is_hidden_helper_container(name: str, image: str, command: str) -> bool:
        _ = name
        image_l = image.strip().lower()
        if not image_l.startswith("alpine"):
            return False

        cmd_l = command.lower()
        data_markers = (
            "/data",
            "profiles.json",
            "historial_shopify.log",
            "wpu_batch",
            "wpu_history_line",
        )
        if any(marker in cmd_l for marker in data_markers):
            return True

        # Helper temporal para escritura de perfiles remotos
        if "sleep 20" in cmd_l:
            return True

        return False

    def _collect_profile_container_names(self) -> set[str]:
        names: set[str] = set()
        for profiles_map in (self.profiles_data, self.private_profiles_data, self.remote_profiles_data):
            if not isinstance(profiles_map, dict):
                continue
            for containers in profiles_map.values():
                if not isinstance(containers, list):
                    continue
                for item in containers:
                    name = str(item).strip()
                    if name:
                        names.add(name)
        return names

    def _profiles_containing_container(self, container_name: str) -> dict[str, list[str]]:
        container = container_name.strip()
        if not container:
            return {}

        scopes: dict[str, dict[str, list[str]]] = {}

        private_profiles = self.private_profiles_data
        try:
            private_profiles = self.read_private_profiles()
            self.private_profiles_data = private_profiles
        except Exception:
            pass
        scopes["privado"] = private_profiles if isinstance(private_profiles, dict) else {}

        if self.docker_mode == "remote":
            remote_profiles = self.remote_profiles_data
            try:
                remote_profiles = self.read_remote_profiles()
                self.remote_profiles_data = remote_profiles
            except Exception:
                pass
            scopes["remoto"] = remote_profiles if isinstance(remote_profiles, dict) else {}
        elif isinstance(self.remote_profiles_data, dict) and self.remote_profiles_data:
            scopes["remoto"] = self.remote_profiles_data

        matches: dict[str, list[str]] = {}
        for scope_name, profile_map in scopes.items():
            found_profiles: list[str] = []
            for profile_name, containers in profile_map.items():
                if not isinstance(containers, list):
                    continue
                normalized = [str(item).strip() for item in containers if str(item).strip()]
                if container in normalized:
                    found_profiles.append(str(profile_name))
            if found_profiles:
                matches[scope_name] = sorted(found_profiles, key=str.lower)
        return matches

    def _remove_container_from_profile_scopes(self, container_name: str, matches: dict[str, list[str]]) -> tuple[bool, str]:
        container = container_name.strip()
        if not container:
            return False, "Nombre de contenedor vacio."

        for scope_name in ("privado", "remoto"):
            profile_names = matches.get(scope_name, [])
            if not profile_names:
                continue
            try:
                profiles = self._read_profiles_for_scope(scope_name)
            except Exception as exc:
                return False, f"No se pudieron cargar perfiles {scope_name}: {exc}"

            changed = False
            for profile_name in profile_names:
                current = profiles.get(profile_name, [])
                if not isinstance(current, list):
                    continue
                updated = [item for item in current if str(item).strip() != container]
                if len(updated) != len(current):
                    profiles[profile_name] = updated
                    changed = True

            if changed:
                try:
                    self._write_profiles_for_scope(scope_name, profiles)
                except Exception as exc:
                    return False, f"No se pudieron guardar perfiles {scope_name}: {exc}"

        current_scope = self._current_profiles_scope()
        try:
            self.profiles_data = self._read_profiles_for_scope(current_scope)
        except Exception:
            pass
        return True, ""

    @staticmethod
    def _container_service_label(name: str, image: str = "") -> str | None:
        token = f"{name} {image}".lower()
        name_l = name.lower()

        if "shopify" in token or "shopify-cli" in token:
            return "Contenedor de Shopify"

        if "node" in token and ("theme" in name_l or "shop" in name_l or "dev" in name_l):
            return "Contenedor Shopify Node"

        return None

    def _container_protection_text(self, name: str, image: str = "") -> str:
        reasons: list[str] = []
        service_label = self._container_service_label(name, image)
        if service_label:
            reasons.append(service_label)

        log_target = self.log_container_var.get().strip()
        if (not service_label) and log_target and name == log_target:
            reasons.append("Contenedor de logs")

        profiled = self._collect_profile_container_names()
        if name in profiled:
            reasons.append("Incluido en perfiles")

        if not reasons:
            return "-"

        unique_reasons = list(dict.fromkeys(reasons))
        return f"{'; '.join(unique_reasons)}"

    def _container_service_tag(self, name: str, image: str = "") -> str | None:
        service_label = self._container_service_label(name, image)
        if service_label in ("Contenedor de Shopify", "Contenedor Shopify Node"):
            return "svc_shopify"
        if service_label == "Contenedor de DB":
            return "svc_db"
        # No phpMyAdmin in Shopify setup
        return None

        log_target = self.log_container_var.get().strip()
        if log_target and name == log_target:
            return "svc_logs"
        return None

    def parse_container_rows(self, text: str) -> list[tuple[str, str, str, str]]:
        rows: list[tuple[str, str, str, str]] = []
        for line in text.splitlines():
            parts = line.split("|", 2)
            if len(parts) < 2:
                continue
            name = parts[0].strip()
            status_raw = parts[1].strip()
            ports = parts[2].strip() if len(parts) == 3 else ""

            state = "ARRANCADO" if status_raw.lower().startswith("up") else "APAGADO"
            health = "Sin healthcheck"
            s = status_raw.lower()
            if "unhealthy" in s:
                health = "\u26a0 Unhealthy"
            elif "healthy" in s:
                health = "\u2714 Healthy"
            elif "starting" in s:
                health = "\u21bb Starting"

            port = self.extract_port(ports) if state == "ARRANCADO" else "-"
            rows.append((name, state, health, port))
        return rows

    @staticmethod
    def extract_port(ports: str) -> str:
        if not ports:
            return "-"

        first = ports.split(",")[0].strip()
        if "->" in first:
            left = first.split("->", 1)[0]
            match = re.search(r":(\d+)$", left)
            if match:
                return match.group(1)
            only = re.search(r"(\d+)", left)
            if only:
                return only.group(1)

        plain = re.search(r"(\d+)", first)
        return plain.group(1) if plain else "-"

    def refresh_containers(self, show_errors: bool = True, full_repaint: bool = True) -> None:
        # Guardar los nombres seleccionados para restaurarlos tras el refresco.
        previously_selected: set[str] = set()
        for item_id in self.tree.selection():
            vals = self.tree.item(item_id, "values")
            if vals:
                previously_selected.add(str(vals[0]))

        if full_repaint:
            # En refresco manual mostramos estado de carga y repintamos toda la tabla.
            self._start_container_loading_spinner()
            self.root.update_idletasks()
        else:
            # En refresco automatico no vaciamos la tabla para evitar parpadeo.
            self._stop_container_loading_spinner()

        if not self.docker_ready():
            self.last_refresh_var.set("Ultima actualizacion: Docker no disponible")
            self._container_rows_snapshot = []
            if full_repaint:
                self.container_cache = []
                self.container_image_cache = {}
            if self._docker_check_in_progress and full_repaint:
                self._start_container_loading_spinner()
            elif full_repaint:
                self._stop_container_loading_spinner()
            self._refresh_observability_panel()
            return

        code, out, err = self._run(["docker", "ps", "-a", "--format", "{{.Names}}|{{.Status}}|{{.Ports}}|{{.Image}}|{{.Command}}"])
        if code != 0:
            self.last_refresh_var.set("Ultima actualizacion: error al listar contenedores")
            if err and show_errors:
                messagebox.showwarning("Docker", f"No se pudo leer contenedores.\n\n{err}")
            if full_repaint:
                self.container_cache = []
                self.container_image_cache = {}
                self._stop_container_loading_spinner()
                self._container_rows_snapshot = []
            self._refresh_observability_panel()
            return

        rows: list[tuple[str, str, str, str, str]] = []
        for line in out.splitlines():
            parts = line.split("|", 4)
            if len(parts) < 5:
                continue
            name = parts[0].strip()
            status_raw = parts[1].strip()
            ports = parts[2].strip()
            image = parts[3].strip()
            command = parts[4].strip()
            if self._is_hidden_helper_container(name, image, command):
                continue
            service_label = self._container_service_label(name, image)
            is_running = status_raw.lower().startswith("up")
            if service_label not in ("Contenedor de Shopify", "Contenedor Shopify Node") and not is_running:
                continue

            state = "ARRANCADO" if is_running else "APAGADO"
            health = "Sin healthcheck"
            s = status_raw.lower()
            if "unhealthy" in s:
                health = "\u26a0 Unhealthy"
            elif "healthy" in s:
                health = "\u2714 Healthy"
            elif "starting" in s:
                health = "\u21bb Starting"

            port = self.extract_port(ports) if state == "ARRANCADO" else "-"
            rows.append((name, state, health, port, image))

        self._container_rows_snapshot = rows
        self.container_cache = [row[0] for row in rows]
        self.container_image_cache = {row[0]: row[4] for row in rows}
        self._stop_container_loading_spinner()

        display_rows: list[tuple[str, tuple[str, str, str, str, str], tuple[str, ...]]] = []
        for row in rows:
            state_val = row[1]
            health_val = row[2]
            if state_val == "ARRANCADO":
                tag = "unhealthy" if "Unhealthy" in health_val else "running"
            else:
                tag = "stopped"
            protection = self._container_protection_text(row[0], row[4])
            tags: list[str] = [tag]
            service_tag = self._container_service_tag(row[0], row[4])
            if service_tag:
                tags.append(service_tag)
            display_rows.append((row[0], (row[0], row[1], row[2], row[3], protection), tuple(tags)))

        if full_repaint:
            for item_id in self.tree.get_children():
                self.tree.delete(item_id)

            if not display_rows:
                self.tree.insert("", "end", values=("(sin contenedores)", "-", "-", "-", "-"))
            else:
                for name, values, tags in display_rows:
                    iid = self.tree.insert("", "end", values=values, tags=tags)
                    if name in previously_selected:
                        self.tree.selection_add(iid)
            self.last_refresh_var.set("Ultima actualizacion: correcta")
            self._refresh_observability_panel()
            return

        # Refresco automatico: actualizar filas en sitio para evitar desaparecer/reaparecer.
        placeholder_ids: list[str] = []
        existing_by_name: dict[str, str] = {}
        for item_id in self.tree.get_children():
            values = self.tree.item(item_id, "values")
            if not values:
                continue
            current_name = str(values[0]).strip()
            if current_name == "(sin contenedores)":
                placeholder_ids.append(item_id)
                continue
            if current_name:
                existing_by_name[current_name] = item_id

        desired_names = {name for name, _values, _tags in display_rows}

        for old_name, old_item_id in list(existing_by_name.items()):
            if old_name not in desired_names:
                self.tree.delete(old_item_id)
                existing_by_name.pop(old_name, None)

        if not display_rows:
            for item_id in self.tree.get_children():
                self.tree.delete(item_id)
            self.tree.insert("", "end", values=("(sin contenedores)", "-", "-", "-", "-"))
            self.last_refresh_var.set("Ultima actualizacion: correcta")
            return

        for item_id in placeholder_ids:
            if self.tree.exists(item_id):
                self.tree.delete(item_id)

        for index, (name, values, tags) in enumerate(display_rows):
            item_id = existing_by_name.get(name)
            if item_id and self.tree.exists(item_id):
                self.tree.item(item_id, values=values, tags=tags)
            else:
                item_id = self.tree.insert("", "end", values=values, tags=tags)
                existing_by_name[name] = item_id
            self.tree.move(item_id, "", index)
            if name in previously_selected:
                self.tree.selection_add(item_id)

        self.last_refresh_var.set("Ultima actualizacion: correcta")
        self._refresh_observability_panel()

    def selected_containers(self) -> list[str]:
        selection = self.tree.selection()
        if not selection:
            return []

        names: list[str] = []
        for item_id in selection:
            values = self.tree.item(item_id, "values")
            if not values:
                continue
            name = str(values[0])
            if name and name not in {"(sin contenedores)", "(sin contenedores Shopify)"} and name not in names:
                names.append(name)
        return names

    def _select_active_theme_for_container(self, container: str) -> None:
        service_label = self._container_service_label(container, self.container_image_cache.get(container, ""))
        if service_label not in {"Contenedor de Shopify", "Contenedor Shopify Node"}:
            messagebox.showwarning("Tema activo", "Solo puedes seleccionar tema activo en contenedores Shopify.")
            return

        code, running, _ = self._run(["docker", "inspect", "--format", "{{.State.Running}}", container])
        if code != 0 or running.strip().lower() != "true":
            messagebox.showwarning("Tema activo", f"El contenedor '{container}' debe estar encendido.")
            return

        themes, err = self._list_container_themes(container)
        if err:
            messagebox.showerror("Tema activo", err)
            return
        if not themes:
            messagebox.showwarning("Tema activo", "No se encontraron temas disponibles dentro del contenedor.")
            return

        current_theme_code, current_theme_out, _ = self._run([
            "docker", "exec", container, "sh", "-c",
            "if [ -f /app/.active_theme_name ]; then cat /app/.active_theme_name; elif [ -f /app/.active_theme_dir ]; then basename \"$(cat /app/.active_theme_dir)\"; fi"
        ])
        current_theme = current_theme_out.strip() if current_theme_code == 0 else ""

        chooser = tk.Toplevel(self.root)
        chooser.title(f"Seleccionar tema activo - {container}")
        chooser.transient(self.root)
        chooser.grab_set()
        chooser.resizable(False, False)

        ttk.Label(
            chooser,
            text="Selecciona el tema que quieres dejar activo en este contenedor:",
            wraplength=420,
            justify="left",
        ).pack(anchor="w", padx=14, pady=(14, 8))

        list_frame = ttk.Frame(chooser)
        list_frame.pack(fill="both", expand=True, padx=14)

        scrollbar = ttk.Scrollbar(list_frame, orient="vertical")
        theme_listbox = tk.Listbox(
            list_frame,
            height=min(12, max(5, len(themes))),
            exportselection=False,
            yscrollcommand=scrollbar.set,
        )
        scrollbar.config(command=theme_listbox.yview)
        scrollbar.pack(side="right", fill="y")
        theme_listbox.pack(side="left", fill="both", expand=True)

        selected_index = 0
        for idx, theme_name in enumerate(themes):
            theme_listbox.insert(tk.END, theme_name)
            if current_theme and theme_name == current_theme:
                selected_index = idx
        theme_listbox.selection_set(selected_index)
        theme_listbox.see(selected_index)

        button_row = ttk.Frame(chooser)
        button_row.pack(fill="x", padx=14, pady=14)

        def close_dialog() -> None:
            if chooser.winfo_exists():
                chooser.grab_release()
                chooser.destroy()

        def apply_theme() -> None:
            selection = theme_listbox.curselection()
            if not selection:
                messagebox.showwarning("Tema activo", "Selecciona un tema de la lista.")
                return

            theme_name = themes[selection[0]]
            theme_path = f"/app/{theme_name}"
            safe_theme_name = (
                theme_name.replace("\\", "\\\\")
                .replace("&", "\\&")
                .replace("|", "\\|")
                .replace('"', '\\"')
            )

            def _apply_operation() -> bool:
                set_cmd = (
                    f"printf '%s' '{theme_name}' > /app/.active_theme_name && "
                    f"printf '%s' '{theme_path}' > /app/.active_theme_dir && "
                    "if [ -f /app/entrypoint.sh ]; then "
                    f"sed -i 's|^THEME_NAME=.*$|THEME_NAME={safe_theme_name}|' /app/entrypoint.sh && "
                    f"sed -i 's|^THEME_DIR=.*$|THEME_DIR={theme_path}|' /app/entrypoint.sh && "
                    "chmod +x /app/entrypoint.sh; "
                    "fi"
                )
                code_set, _, err_set = self._run(["docker", "exec", "-u", "root", container, "sh", "-c", set_cmd])
                if code_set != 0:
                    raise RuntimeError(err_set or "No se pudo guardar el tema activo.")

                code_restart, _, err_restart = self._run(["docker", "restart", container])
                if code_restart != 0:
                    raise RuntimeError(err_restart or "No se pudo reiniciar el contenedor para aplicar el tema activo.")

                self.log_event("THEME", container, "OK", f"Tema activo seleccionado: {theme_name}")
                self.refresh_everything()
                self._refresh_container_admin_table()
                messagebox.showinfo("Tema activo", f"Tema activo actualizado a '{theme_name}' en {container}.")
                return True

            close_dialog()
            self._run_with_loading_modal(
                f"Estableciendo tema activo {theme_name}",
                _apply_operation,
                auto_close_success_ms=500,
            )

        ttk.Button(button_row, text="Cancelar", command=close_dialog).pack(side="right")
        ttk.Button(button_row, text="Seleccionar", command=apply_theme, style="Admin.TButton").pack(side="right", padx=(0, 8))

        chooser.bind("<Escape>", lambda _event: close_dialog())
        chooser.bind("<Return>", lambda _event: apply_theme())

    def _list_container_themes(self, container: str) -> tuple[list[str], str]:
        list_cmd = (
            "for base in /app /workspace /theme /themes; do "
            "if [ -d \"$base\" ]; then "
            "find \"$base\" -mindepth 1 -maxdepth 3 -type f -path \"*/config/settings_schema.json\" 2>/dev/null | "
            "while read -r schema; do root=$(dirname \"$(dirname \"$schema\")\"); "
            "name=$(basename \"$root\"); [ -n \"$name\" ] && printf \"%s\\n\" \"$name\"; done; "
            "fi; done | grep -v '^node_modules$' | grep -v '^tmp$' | grep -v '^dist$' | sort -u"
        )
        code, out, err = self._run(["docker", "exec", container, "sh", "-c", list_cmd])
        if code != 0:
            return [], err or "No se pudieron listar temas del contenedor."
        themes = [line.strip() for line in out.splitlines() if line.strip()]
        return themes, ""

    def _delete_theme_for_container(self, container: str) -> None:
        service_label = self._container_service_label(container, self.container_image_cache.get(container, ""))
        if service_label not in {"Contenedor de Shopify", "Contenedor Shopify Node"}:
            messagebox.showwarning("Borrar tema", "Solo puedes borrar temas en contenedores Shopify.")
            return

        code, running, _ = self._run(["docker", "inspect", "--format", "{{.State.Running}}", container])
        if code != 0 or running.strip().lower() != "true":
            messagebox.showwarning("Borrar tema", f"El contenedor '{container}' debe estar encendido.")
            return

        themes, err = self._list_container_themes(container)
        if err:
            messagebox.showerror("Borrar tema", err)
            return
        if not themes:
            messagebox.showwarning("Borrar tema", "No se encontraron temas disponibles dentro del contenedor.")
            return

        current_theme_code, current_theme_out, _ = self._run([
            "docker", "exec", container, "sh", "-c",
            "if [ -f /app/.active_theme_name ]; then cat /app/.active_theme_name; elif [ -f /app/.active_theme_dir ]; then basename \"$(cat /app/.active_theme_dir)\"; fi"
        ])
        current_theme = current_theme_out.strip() if current_theme_code == 0 else ""

        chooser = tk.Toplevel(self.root)
        chooser.title(f"Borrar tema - {container}")
        chooser.transient(self.root)
        chooser.grab_set()
        chooser.resizable(False, False)

        ttk.Label(
            chooser,
            text="Selecciona el tema que quieres eliminar de este contenedor:",
            wraplength=420,
            justify="left",
        ).pack(anchor="w", padx=14, pady=(14, 8))

        list_frame = ttk.Frame(chooser)
        list_frame.pack(fill="both", expand=True, padx=14)

        scrollbar = ttk.Scrollbar(list_frame, orient="vertical")
        theme_listbox = tk.Listbox(
            list_frame,
            height=min(12, max(5, len(themes))),
            exportselection=False,
            yscrollcommand=scrollbar.set,
        )
        scrollbar.config(command=theme_listbox.yview)
        scrollbar.pack(side="right", fill="y")
        theme_listbox.pack(side="left", fill="both", expand=True)

        selected_index = 0
        for idx, theme_name in enumerate(themes):
            theme_listbox.insert(tk.END, theme_name)
            if current_theme and theme_name == current_theme:
                selected_index = idx
        theme_listbox.selection_set(selected_index)
        theme_listbox.see(selected_index)

        button_row = ttk.Frame(chooser)
        button_row.pack(fill="x", padx=14, pady=14)

        def close_dialog() -> None:
            if chooser.winfo_exists():
                chooser.grab_release()
                chooser.destroy()

        def delete_theme() -> None:
            selection = theme_listbox.curselection()
            if not selection:
                messagebox.showwarning("Borrar tema", "Selecciona un tema de la lista.")
                return

            theme_name = themes[selection[0]]
            theme_path = f"/app/{theme_name}"
            safe_theme_name = shlex.quote(theme_name)
            safe_theme_path = shlex.quote(theme_path)
            replacement_theme = ""

            def choose_replacement_theme(options: list[str]) -> str | None:
                chooser2 = tk.Toplevel(self.root)
                chooser2.title(f"Elegir reemplazo - {container}")
                chooser2.transient(self.root)
                chooser2.grab_set()
                chooser2.resizable(False, False)

                ttk.Label(
                    chooser2,
                    text=(
                        f"El tema '{theme_name}' es el activo actual.\n"
                        "Selecciona el tema que quieres activar antes de borrarlo:"
                    ),
                    wraplength=420,
                    justify="left",
                ).pack(anchor="w", padx=14, pady=(14, 8))

                frame2 = ttk.Frame(chooser2)
                frame2.pack(fill="both", expand=True, padx=14)

                scroll2 = ttk.Scrollbar(frame2, orient="vertical")
                listbox2 = tk.Listbox(
                    frame2,
                    height=min(12, max(5, len(options))),
                    exportselection=False,
                    yscrollcommand=scroll2.set,
                )
                scroll2.config(command=listbox2.yview)
                scroll2.pack(side="right", fill="y")
                listbox2.pack(side="left", fill="both", expand=True)

                for option in options:
                    listbox2.insert(tk.END, option)
                listbox2.selection_set(0)
                listbox2.see(0)

                selected = {"value": None}

                def close2() -> None:
                    if chooser2.winfo_exists():
                        chooser2.grab_release()
                        chooser2.destroy()

                def accept2() -> None:
                    cur = listbox2.curselection()
                    if not cur:
                        messagebox.showwarning("Tema activo", "Selecciona un tema de reemplazo.")
                        return
                    selected["value"] = options[cur[0]]
                    close2()

                btn_row2 = ttk.Frame(chooser2)
                btn_row2.pack(fill="x", padx=14, pady=14)
                ttk.Button(btn_row2, text="Cancelar", command=close2).pack(side="right")
                ttk.Button(btn_row2, text="Usar como activo", command=accept2, style="Admin.TButton").pack(side="right", padx=(0, 8))
                chooser2.bind("<Escape>", lambda _event: close2())
                chooser2.bind("<Return>", lambda _event: accept2())

                self.root.wait_window(chooser2)
                return selected["value"]

            remaining_themes = [item for item in themes if item != theme_name]
            current_theme_name = current_theme
            if current_theme_name == theme_name and remaining_themes:
                replacement_theme = choose_replacement_theme(remaining_themes) or ""
                if not replacement_theme:
                    return

            if not messagebox.askyesno(
                "Confirmar borrado",
                f"Vas a eliminar el tema '{theme_name}' del contenedor '{container}'.\n\nEsta acción no se puede deshacer.",
            ):
                return

            def _delete_operation() -> bool:
                delete_cmd = f"rm -rf {safe_theme_path}"
                code_rm, _, err_rm = self._run(["docker", "exec", "-u", "root", container, "sh", "-c", delete_cmd])
                if code_rm != 0:
                    raise RuntimeError(err_rm or "No se pudo borrar el tema.")

                if current_theme_name == theme_name:
                    if replacement_theme:
                        next_theme = replacement_theme
                        next_path = f"/app/{next_theme}"
                        next_theme_q = shlex.quote(next_theme)
                        next_path_q = shlex.quote(next_path)
                        safe_next_theme_name = next_theme.replace("\\", "\\\\").replace("&", "\\&").replace("|", "\\|").replace('"', '\\"')
                        update_cmd = (
                            f"printf '%s' {next_theme_q} > /app/.active_theme_name && "
                            f"printf '%s' {next_path_q} > /app/.active_theme_dir && "
                            "if [ -f /app/entrypoint.sh ]; then "
                            f"sed -i 's|^THEME_NAME=.*$|THEME_NAME={safe_next_theme_name}|' /app/entrypoint.sh && "
                            f"sed -i 's|^THEME_DIR=.*$|THEME_DIR={next_path}|' /app/entrypoint.sh && "
                            "chmod +x /app/entrypoint.sh; "
                            "fi"
                        )
                        code_upd, _, err_upd = self._run(["docker", "exec", "-u", "root", container, "sh", "-c", update_cmd])
                        if code_upd != 0:
                            raise RuntimeError(err_upd or "Se borro el tema, pero no se pudo activar otro tema.")
                    else:
                        clear_cmd = "rm -f /app/.active_theme_name /app/.active_theme_dir"
                        self._run(["docker", "exec", "-u", "root", container, "sh", "-c", clear_cmd])

                code_restart, _, err_restart = self._run(["docker", "restart", container])
                if code_restart != 0:
                    raise RuntimeError(err_restart or "No se pudo reiniciar el contenedor tras borrar el tema.")

                self.log_event("THEME", container, "OK", f"Tema borrado: {theme_name}")
                self.refresh_everything()
                self._refresh_container_admin_table()
                messagebox.showinfo("Borrar tema", f"Tema '{theme_name}' eliminado de {container}.")
                return True

            close_dialog()
            self._run_with_loading_modal(
                f"Borrando tema {theme_name}",
                _delete_operation,
                auto_close_success_ms=500,
            )

        ttk.Button(button_row, text="Cancelar", command=close_dialog).pack(side="right")
        ttk.Button(button_row, text="Borrar", command=delete_theme, style="Danger.TButton").pack(side="right", padx=(0, 8))

        chooser.bind("<Escape>", lambda _event: close_dialog())
        chooser.bind("<Return>", lambda _event: delete_theme())

    def _select_active_theme_selected(self) -> None:
        names = self.selected_containers()
        if not names:
            messagebox.showwarning("Tema activo", "Selecciona un contenedor Shopify.")
            return
        if len(names) > 1:
            messagebox.showwarning("Tema activo", "Selecciona solo un contenedor para elegir el tema activo.")
            return
        self._select_active_theme_for_container(names[0])

    def _delete_theme_selected(self) -> None:
        names = self.selected_containers()
        if not names:
            messagebox.showwarning("Borrar tema", "Selecciona un contenedor Shopify.")
            return
        if len(names) > 1:
            messagebox.showwarning("Borrar tema", "Selecciona solo un contenedor para borrar un tema.")
            return
        self._delete_theme_for_container(names[0])

    def run_docker_action(
        self,
        args: list[str],
        success_msg: str,
        target_names: list[str] | None = None,
    ) -> None:
        if not self.docker_ready():
            messagebox.showerror("Docker", self._docker_unavailable_message())
            return

        action_message = "Ejecutando acción Docker"
        if len(args) >= 2:
            action_message = f"Ejecutando '{args[1]}' en contenedores"
        modal = self._show_loading_modal(action_message)

        # Localizar filas del Treeview que se van a ver afectadas
        spinner_items: list[str] = []
        if target_names:
            name_set = set(target_names)
            for iid in self.tree.get_children():
                vals = self.tree.item(iid, "values")
                if vals and str(vals[0]) in name_set:
                    spinner_items.append(iid)

        self._set_container_action_btns_state("disabled")
        self._start_container_spinner(spinner_items)

        result_q: queue.Queue[tuple[str, str]] = queue.Queue()

        def worker() -> None:
            objetivo = " ".join(args[2:]) if len(args) > 2 else "global"
            code, _, err = self._run(args)
            if code == 0:
                result_q.put(("ok", objetivo))
            else:
                result_q.put(("error", err or "Operacion fallida"))

        threading.Thread(target=worker, daemon=True).start()

        def poll() -> None:
            try:
                kind, payload = result_q.get_nowait()
            except queue.Empty:
                self.root.after(200, poll)
                return

            self._stop_container_spinner()
            self._set_container_action_btns_state("normal")

            if kind == "ok":
                self._finish_loading_modal(modal, True, auto_close_success_ms=500)
                self.log_event("DOCKER", payload, "OK", " ".join(args))
                self.refresh_everything()
                messagebox.showinfo("Docker", success_msg)
            else:
                self._finish_loading_modal(modal, False, error_msg=payload)
                self.log_event("DOCKER", "global", "ERROR", payload)
                self.refresh_everything()
                messagebox.showerror("Docker", payload)

        self.root.after(200, poll)

    def _set_container_action_btns_state(self, state: str) -> None:
        for btn in self.container_action_btns:
            try:
                btn.configure(state=state)
            except tk.TclError:
                pass

    def _start_container_spinner(self, item_ids: list[str]) -> None:
        self._stop_container_spinner()
        self._container_spinner_items = item_ids
        self._container_spinner_frame = 0
        if item_ids:
            self._animate_container_spinner()

    def _animate_container_spinner(self) -> None:
        frames = ["\u29d7", "\u29d6", "\u29d5", "\u29d4"]
        text = frames[self._container_spinner_frame % len(frames)] + " Procesando"
        for iid in self._container_spinner_items:
            try:
                self.tree.set(iid, "state", text)
            except tk.TclError:
                pass
        self._container_spinner_frame += 1
        self._container_spinner_job = self.root.after(250, self._animate_container_spinner)

    def _stop_container_spinner(self) -> None:
        if self._container_spinner_job is not None:
            self.root.after_cancel(self._container_spinner_job)
            self._container_spinner_job = None
        self._container_spinner_items = []

    def _start_container_loading_spinner(self) -> None:
        if not hasattr(self, "tree"):
            return
        if self._container_loading_job is not None:
            return

        self._container_loading_frame = 0
        for item in self.tree.get_children():
            self.tree.delete(item)
        self.tree.insert("", "end", iid="__loading__", values=("Cargando contenedores...", "-", "-", "-", "-"))
        self._animate_container_loading_spinner()

    def _animate_container_loading_spinner(self) -> None:
        if not hasattr(self, "tree"):
            return

        frames = ["|", "/", "-", "\\"]
        frame = frames[self._container_loading_frame % len(frames)]
        if self.tree.exists("__loading__"):
            self.tree.item("__loading__", values=(f"Cargando contenedores... {frame}", "-", "-", "-", "-"))
        self._container_loading_frame += 1
        self._container_loading_job = self.root.after(140, self._animate_container_loading_spinner)

    def _stop_container_loading_spinner(self) -> None:
        if self._container_loading_job is not None:
            self.root.after_cancel(self._container_loading_job)
            self._container_loading_job = None
        if hasattr(self, "tree") and self.tree.exists("__loading__"):
            self.tree.delete("__loading__")

    # ── Profile spinner helpers ────────────────────────────────────────────

    def _start_profile_spinner(self, profile_name: str) -> None:
        self._stop_profile_spinner()
        self._profile_spinner_name = profile_name
        self._profile_spinner_frame2 = 0
        self._animate_profile_spinner()

    def _animate_profile_spinner(self) -> None:
        frames = ["⧗", "⧖", "⧕", "⧔"]
        text = frames[self._profile_spinner_frame2 % len(frames)] + " Procesando..."
        for i in range(self.profiles_listbox.size()):
            entry = self.profiles_listbox.get(i)
            if entry == self._profile_spinner_name or entry.endswith(" Procesando..."):
                self.profiles_listbox.delete(i)
                self.profiles_listbox.insert(i, text)
                self.profiles_listbox.itemconfig(i, fg="#f59e0b")
                self.profiles_listbox.selection_set(i)
                break
        self._profile_spinner_frame2 += 1
        self._profile_spinner_job = self.root.after(250, self._animate_profile_spinner)

    def _stop_profile_spinner(self) -> None:
        if self._profile_spinner_job is not None:
            self.root.after_cancel(self._profile_spinner_job)
            self._profile_spinner_job = None

    def start_selected(self) -> None:
        names = self.selected_containers()
        if not names:
            messagebox.showwarning("Seleccion", "Selecciona al menos un contenedor.")
            return
        self.run_docker_action(["docker", "start"] + names, f"Contenedores arrancados: {', '.join(names)}", target_names=names)

    def stop_selected(self) -> None:
        names = self.selected_containers()
        if not names:
            messagebox.showwarning("Seleccion", "Selecciona al menos un contenedor.")
            return
        self.run_docker_action(["docker", "stop"] + names, f"Contenedores apagados: {', '.join(names)}", target_names=names)

    def remote_access_selected(self) -> None:
        names = self.selected_containers()
        if not names:
            messagebox.showwarning("Seleccion", "Selecciona un contenedor.")
            return
        if len(names) > 1:
            messagebox.showwarning("Seleccion", "Selecciona solo un contenedor para el acceso remoto.")
            return
        self._remote_access_impl(names[0])

    def start_all(self) -> None:
        code, out, _ = self._run(["docker", "ps", "-aq"])
        if code != 0 or not out:
            messagebox.showwarning("Docker", "No hay contenedores para arrancar.")
            return
        self.run_docker_action(["docker", "start"] + out.splitlines(), "Contenedores arrancados.", target_names=self.container_cache[:])

    def stop_all(self) -> None:
        code, out, _ = self._run(["docker", "ps", "-q"])
        if code != 0 or not out:
            messagebox.showwarning("Docker", "No hay contenedores en ejecucion para apagar.")
            return
        self.run_docker_action(["docker", "stop"] + out.splitlines(), "Contenedores apagados.", target_names=self.container_cache[:])

    def _read_legacy_ini_profiles(self) -> dict[str, list[str]]:
        self._ensure_profiles_file()
        profiles: dict[str, list[str]] = {}
        with open(self.profiles_file, "r", encoding="utf-8") as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith(";") or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                name = key.strip()
                containers = [item.strip() for item in value.split(",") if item.strip()]
                if name:
                    profiles[name] = containers
        return profiles

    def _default_profiles_payload(self) -> dict[str, object]:
        return {
            "version": 1,
            "updated_at": "",
            "updated_by": "",
            "profiles": {},
        }

    def _sanitize_profiles_mapping(self, data: object) -> dict[str, list[str]]:
        if not isinstance(data, dict):
            return {}
        result: dict[str, list[str]] = {}
        for key, value in data.items():
            name = str(key).strip()
            if not name:
                continue
            if isinstance(value, list):
                containers = [str(item).strip() for item in value if str(item).strip()]
            else:
                containers = []
            result[name] = containers
        return result

    def _ensure_private_profiles_file(self) -> None:
        os.makedirs(self.private_profiles_dir, exist_ok=True)
        if os.path.isfile(self.private_profiles_file):
            return

        payload = self._default_profiles_payload()
        legacy = self._read_legacy_ini_profiles()
        if legacy:
            payload["profiles"] = legacy
        with open(self.private_profiles_file, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=True, indent=2)

    def read_private_profiles(self) -> dict[str, list[str]]:
        self._ensure_private_profiles_file()
        try:
            with open(self.private_profiles_file, "r", encoding="utf-8") as fh:
                payload = json.load(fh)
        except Exception:
            payload = self._default_profiles_payload()
        return self._sanitize_profiles_mapping(payload.get("profiles", {}))

    def write_private_profiles(self, profiles: dict[str, list[str]]) -> None:
        self._ensure_private_profiles_file()
        payload = self._default_profiles_payload()
        payload["updated_at"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        payload["updated_by"] = os.environ.get("COMPUTERNAME", "desconocido")
        payload["profiles"] = dict(sorted(profiles.items(), key=lambda item: item[0].lower()))
        with open(self.private_profiles_file, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=True, indent=2)

    def _ensure_remote_profiles_volume(self, client: object | None = None) -> None:
        if client is None:
            client = self._get_docker_sdk_client(timeout_seconds=20)
        if client is None:
            raise RuntimeError("No se pudo conectar con Docker para perfiles remotos.")
        try:
            client.volumes.get(self.remote_profiles_volume)
        except Exception:
            client.volumes.create(name=self.remote_profiles_volume)

    def read_remote_profiles(self) -> dict[str, list[str]]:
        if self.docker_mode != "remote":
            return {}

        # Ruta de lectura alineada con el comando CLI validado en terminal.
        # Esto evita discrepancias entre SDK y docker CLI en algunos daemons remotos.
        code_v, _out_v, err_v = self._run(["docker", "volume", "create", self.remote_profiles_volume])
        if code_v != 0:
            raise RuntimeError(err_v or "No se pudo asegurar el volumen remoto de perfiles.")

        default_payload = json.dumps(self._default_profiles_payload(), ensure_ascii=True)
        script = (
            "mkdir -p /data; "
            f"if [ ! -f {self.remote_profiles_path} ]; then "
            "printf '%s' \"$WPU_DEFAULT\" > "
            f"{self.remote_profiles_path}; "
            "fi; "
            f"cat {self.remote_profiles_path}"
        )
        code, out, err = self._run(
            [
                "docker",
                "run",
                "--rm",
                "-e",
                f"WPU_DEFAULT={default_payload}",
                "-v",
                f"{self.remote_profiles_volume}:/data",
                "alpine",
                "sh",
                "-c",
                script,
            ]
        )
        if code != 0:
            raise RuntimeError(err or "No se pudo leer profiles.json remoto.")

        raw = out.strip()
        if not raw:
            raw = default_payload
        raw = raw.lstrip("\ufeff")
        try:
            payload = json.loads(raw)
        except Exception:
            # Si el JSON remoto esta dañado, se reconstruye la estructura minima
            # para que la UI vuelva a cargar y se pueda guardar de nuevo.
            payload = self._default_profiles_payload()
            try:
                self.write_remote_profiles(self._sanitize_profiles_mapping(payload.get("profiles", {})))
            except Exception:
                pass
        return self._sanitize_profiles_mapping(payload.get("profiles", {}))

    def write_remote_profiles(self, profiles: dict[str, list[str]]) -> None:
        if self.docker_mode != "remote":
            raise RuntimeError("Los perfiles remotos solo estan disponibles en modo remoto.")

        client = self._get_docker_sdk_client(timeout_seconds=20)
        if client is None:
            raise RuntimeError("No se pudo conectar con Docker remoto.")

        self._ensure_remote_profiles_volume(client)

        payload = self._default_profiles_payload()
        payload["updated_at"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        payload["updated_by"] = os.environ.get("COMPUTERNAME", "desconocido")
        payload["profiles"] = dict(sorted(profiles.items(), key=lambda item: item[0].lower()))
        raw = json.dumps(payload, ensure_ascii=True, indent=2).encode("utf-8")

        helper = client.containers.create(
            "alpine",
            command=["sh", "-c", "sleep 20"],
            labels={self._helper_label_key: self._helper_label_value, "wpu.role": "profiles-write"},
            volumes={self.remote_profiles_volume: {"bind": "/data", "mode": "rw"}},
        )
        try:
            helper.start()
            buf = io.BytesIO()
            with tarfile.open(fileobj=buf, mode="w") as tar:
                info = tarfile.TarInfo(name="profiles.json")
                info.size = len(raw)
                tar.addfile(info, io.BytesIO(raw))
            buf.seek(0)
            ok = helper.put_archive("/data", buf.read())
            if not ok:
                raise RuntimeError("No se pudo guardar profiles.json en el volumen remoto.")
        finally:
            try:
                helper.remove(force=True)
            except Exception:
                pass

    def _current_profiles_scope(self) -> str:
        return self.profile_scope_var.get().strip().lower() or "privado"

    def _current_profiles_label(self) -> str:
        return "Perfiles remotos" if self._current_profiles_scope() == "remoto" else "Perfiles privados"

    def _target_profiles_scope(self) -> str:
        return "privado" if self._current_profiles_scope() == "remoto" else "remoto"

    def _target_profiles_label(self) -> str:
        return "privado" if self._target_profiles_scope() == "privado" else "remoto"

    def _profile_container_display_name(self, container_name: str, in_selected_profile: bool) -> str:
        if in_selected_profile:
            return f"{container_name} (En el perfil seleccionado)"
        return container_name

    def _profile_container_actual_name(self, displayed_name: str) -> str:
        suffix = " (En el perfil seleccionado)"
        if displayed_name.endswith(suffix):
            return displayed_name[:-len(suffix)]
        return displayed_name

    def _render_profile_containers(self, selected_profile_name: str | None = None) -> None:
        selected_containers = set(self.profiles_data.get(selected_profile_name, [])) if selected_profile_name else set()
        self.profile_containers_listbox.delete(0, tk.END)
        for cname in self.container_cache:
            image_ref = self.container_image_cache.get(cname, "").strip().lower()
            if image_ref.startswith("alpine"):
                continue
            self.profile_containers_listbox.insert(
                tk.END,
                self._profile_container_display_name(cname, cname in selected_containers),
            )

        self.profile_containers_listbox.selection_clear(0, tk.END)
        if not selected_profile_name:
            return

        for idx in range(self.profile_containers_listbox.size()):
            item = self._profile_container_actual_name(self.profile_containers_listbox.get(idx))
            if item in selected_containers:
                self.profile_containers_listbox.selection_set(idx)

    def _read_profiles_for_scope(self, scope: str) -> dict[str, list[str]]:
        if scope == "remoto":
            self.remote_profiles_data = self.read_remote_profiles()
            return self.remote_profiles_data
        self.private_profiles_data = self.read_private_profiles()
        return self.private_profiles_data

    def _write_profiles_for_scope(self, scope: str, profiles: dict[str, list[str]]) -> None:
        if scope == "remoto":
            self.remote_profiles_data = profiles
            self.write_remote_profiles(profiles)
            return
        self.private_profiles_data = profiles
        self.write_private_profiles(profiles)

    def _select_profile_in_ui(self, profile_name: str) -> None:
        for idx in range(self.profiles_listbox.size()):
            if self.profiles_listbox.get(idx) == profile_name:
                self.profiles_listbox.selection_clear(0, tk.END)
                self.profiles_listbox.selection_set(idx)
                self.profiles_listbox.see(idx)
                self.profile_name_var.set(profile_name)
                self._render_profile_containers(profile_name)
                return

    def _load_profiles_for_current_scope(self) -> dict[str, list[str]]:
        return self._read_profiles_for_scope(self._current_profiles_scope())

    def _write_profiles_for_current_scope(self, profiles: dict[str, list[str]]) -> None:
        self._write_profiles_for_scope(self._current_profiles_scope(), profiles)

    def _set_profiles_loading_ui(self, loading: bool) -> None:
        # Si existen los widgets, cambia el estado visual de carga
        if hasattr(self, 'profiles_loading_label') and self.profiles_loading_label:
            self.profiles_loading_label.configure(text="Cargando perfiles..." if loading else "")
        if hasattr(self, 'profiles_spinner') and self.profiles_spinner:
            self.profiles_spinner.configure(state="normal" if loading else "disabled")
        if hasattr(self, 'profiles_listbox') and self.profiles_listbox:
            self.profiles_listbox.configure(state="disabled" if loading else "normal")

    # ── Async remote-profiles loading ────────────────────────────────────────

    def _clear_profiles_load_queue(self) -> None:
        """Vacía cualquier resultado pendiente en la cola de carga de perfiles."""
        try:
            while not self._profiles_load_queue.empty():
                self._profiles_load_queue.get_nowait()
        except Exception:
            pass

    def _profiles_load_worker(self, scope: str) -> None:
        """
        Ejecutado en un hilo secundario. Lee los perfiles del scope indicado
        y deposita el resultado en _profiles_load_queue como:
          ("ok",   True,  profiles_dict)   → éxito
          ("error",False, error_msg_str)   → fallo
        """
        try:
            profiles = self._read_profiles_for_scope(scope)
            self._profiles_load_queue.put(("ok", True, profiles))
        except Exception as exc:
            self._profiles_load_queue.put(("error", False, str(exc)))

    def _poll_profiles_load_queue(self) -> None:
        """
        Ejecutado periódicamente en el hilo principal (via root.after).
        Comprueba si el worker de carga de perfiles ha terminado y actualiza la UI.
        """
        # Si la carga ya no está activa (cancelada externamente), salimos.
        if not self._profiles_loading:
            self._profiles_load_job_id = None
            return

        try:
            status, ok, payload = self._profiles_load_queue.get_nowait()
        except queue.Empty:
            # Todavía está cargando — volver a programar el poll.
            self._profiles_load_job_id = self.root.after(150, self._poll_profiles_load_queue)
            return

        # Tenemos resultado.
        self._profiles_load_job_id = None
        self._cancel_profiles_load_guard()
        self._profiles_loading = False
        scope = self._profiles_loading_scope
        self._profiles_loading_scope = None

        if status == "ok" and ok:
            self.profiles_data = payload
            self._set_profiles_loading_ui(False)
            if hasattr(self, 'profiles_listbox') and self.profiles_listbox:
                self.profiles_listbox.configure(state="normal")
                self.profiles_listbox.delete(0, tk.END)
                for name in sorted(self.profiles_data.keys(), key=str.lower):
                    self.profiles_listbox.insert(tk.END, name)
            self._render_profile_containers()
            # Si mientras cargaba se pidió un re-refresh, lo lanzamos ahora.
            if self._profiles_load_requested:
                self._profiles_load_requested = False
                self.root.after(100, lambda: self.refresh_profiles_ui(force=True))
        else:
            # Error en la carga.
            error_msg = payload if isinstance(payload, str) else "Error desconocido"
            self._fail_profiles_loading(f"Error cargando {scope or 'perfiles remotos'}: {error_msg}")

    def _profiles_load_guard_timeout(self) -> None:
        """
        Llamado si la carga de perfiles remotos tarda demasiado (timeout).
        Cancela la operación y muestra un mensaje al usuario.
        """
        self._profiles_load_guard_job_id = None
        if self._profiles_loading:
            self._fail_profiles_loading(
                f"Tiempo de espera agotado al cargar perfiles remotos "
                f"(>{self._profiles_load_timeout_sec}s). "
                "Comprueba la conexión con Docker remoto y pulsa 'Refrescar'."
            )

    # ── Fin async remote-profiles loading ────────────────────────────────────

    def on_profile_scope_changed(self, _event: object | None = None) -> None:
        self.clear_profile_editor()
        self.refresh_profiles_ui(force=True)

    def _remote_access_impl(self, container: str) -> None:
        if not self.docker_ready():
            messagebox.showerror("Docker", self._docker_unavailable_message())
            return

        c_code, c_out, c_err = self._run(["docker", "inspect", "--format", "{{.State.Running}}", container])
        if c_code != 0:
            messagebox.showerror("Acceso Remoto", c_err or f"No se pudo inspeccionar el contenedor '{container}'.")
            return
        if c_out.strip().lower() != "true":
            messagebox.showwarning("Acceso Remoto", f"El contenedor '{container}' debe estar encendido para habilitar el acceso remoto.")
            return

        ssh_port = 22
        code_p, out_p, _ = self._run(["docker", "port", container, "22"])
        if code_p == 0 and out_p.strip():
            try:
                ssh_port = int(out_p.strip().splitlines()[0].split(":")[-1])
            except Exception:
                ssh_port = 22

        self._show_vscode_ssh_setup_dialog(
            shopify_container=container,
            ssh_port=ssh_port,
            ws_path="",
        )

    @staticmethod
    def _resolver_comando(nombre: str) -> str:
        # Si el comando es 'docker', primero intenta usar el docker.exe local incluido
        if nombre == "docker":
            try:
                from docker_bin.docker_path_helper import get_docker_exe
                ruta = get_docker_exe()
                if ruta and os.path.exists(ruta):
                    return ruta
            except Exception:
                pass
        """Devuelve la ruta absoluta del comando buscando en el PATH y en rutas típicas de Windows."""
        import shutil
        ruta = shutil.which(nombre)
        if ruta:
            return ruta

        rutas_extra = {
            "docker": [
                r"C:\Program Files\Docker\Docker\resources\bin\docker.exe",
                r"C:\ProgramData\DockerDesktop\version-bin\docker.exe",
            ],
            "ssh": [
                r"C:\Windows\System32\OpenSSH\ssh.exe",
                r"C:\Program Files\Git\usr\bin\ssh.exe",
            ],
            "ssh-keygen": [
                r"C:\Windows\System32\OpenSSH\ssh-keygen.exe",
                r"C:\Program Files\Git\usr\bin\ssh-keygen.exe",
            ],
            "code": [
                r"C:\Program Files\Microsoft VS Code\bin\code.cmd",
                r"C:\Users\{}\AppData\Local\Programs\Microsoft VS Code\bin\code.cmd".format(
                    __import__("os").environ.get("USERNAME", "")
                ),
            ],
        }

        for ruta_posible in rutas_extra.get(nombre, []):
            if __import__("os").path.exists(ruta_posible):
                return ruta_posible

        raise FileNotFoundError(
            f"No se encontró el comando '{nombre}' en el sistema.\n\n"
            f"Asegúrate de que está instalado y su carpeta está en el PATH de Windows.\n"
            f"Si acabas de instalarlo, reinicia la aplicación."
        )

    def abrir_vscode_en_contenedor(self, container_name: str) -> None:
        """
        Configura completamente el entorno SSH en el contenedor y abre VS Code via Remote-SSH:
          1. Genera/reutiliza la clave SSH local del cliente Windows
          2. Instala openssh-server en el contenedor si no está presente
          3. Arranca sshd dentro del contenedor
          4. Inyecta la clave pública en authorized_keys del contenedor
          5. Detecta el puerto SSH accesible (port mapping o IP del contenedor)
          6. Escribe la entrada en ~/.ssh/config para que VSCode sepa conectarse
          7. Abre VS Code con la extensión Remote-SSH apuntando al tema en /app
        Compatible con Docker Local, Docker Remoto SSH y Docker Remoto TCP.
        """
        import os as _os
        import textwrap as _textwrap

        # ── 0. Modo de conexión ───────────────────────────────────────────────
        is_remote = self.docker_mode == "remote" and bool(self.docker_host)
        ssh_target = None   # "user@host" para modo SSH remoto
        use_tcp = False

        if is_remote:
            host = self.docker_host.strip()
            if host.startswith("ssh://"):
                ssh_target = host[6:]
            elif host.startswith("tcp://") or host.startswith("http"):
                use_tcp = True
            else:
                ssh_target = host

        # Entorno con rutas típicas de Windows
        env = _os.environ.copy()
        extra_paths = [
            r"C:\Program Files\Docker\Docker\resources\bin",
            r"C:\ProgramData\DockerDesktop\version-bin",
            r"C:\Windows\System32\OpenSSH",
            r"C:\Program Files\Git\usr\bin",
            r"C:\Program Files\Microsoft VS Code\bin",
        ]
        env["PATH"] = _os.pathsep.join(extra_paths) + _os.pathsep + env.get("PATH", "")
        if use_tcp:
            env["DOCKER_HOST"] = self.docker_host.strip()

        # Helper para ejecutar comandos dentro del contenedor vía docker exec o ssh+docker
        def _exec_in_container(sh_cmd: str, check: bool = False) -> subprocess.CompletedProcess:
            if ssh_target:
                ssh_exe = self._resolver_comando("ssh")
                cmd = [
                    ssh_exe, "-o", "StrictHostKeyChecking=no",
                    "-o", "BatchMode=yes",
                    ssh_target,
                    f"docker exec -u root {container_name} sh -c {sh_cmd!r}",
                ]
            else:
                docker_exe = self._resolver_comando("docker")
                cmd = [docker_exe, "exec", "-u", "root", container_name, "sh", "-c", sh_cmd]
            return subprocess.run(cmd, capture_output=True, text=True,
                                  creationflags=0x08000000, env=env)

        try:
            # ── 1. Clave SSH local (cliente Windows) ────────────────────────
            ssh_dir = _os.path.expanduser("~/.ssh")
            _os.makedirs(ssh_dir, exist_ok=True)

            id_ed25519_pub = _os.path.join(ssh_dir, "id_ed25519.pub")
            id_rsa_pub     = _os.path.join(ssh_dir, "id_rsa.pub")

            pub_key_path = None
            if _os.path.exists(id_ed25519_pub):
                pub_key_path = id_ed25519_pub
            elif _os.path.exists(id_rsa_pub):
                pub_key_path = id_rsa_pub
            else:
                self.log_event("SSH", container_name, "INFO", "Generando par de claves SSH locales (ed25519)...")
                priv_key_path = _os.path.join(ssh_dir, "id_ed25519")
                if _os.path.exists(priv_key_path):
                    try:
                        _os.remove(priv_key_path)
                    except Exception as ex:
                        self.log_event("SSH", container_name, "WARNING", f"No se pudo borrar clave huérfana: {ex}")
                try:
                    ssh_keygen_exe = self._resolver_comando("ssh-keygen")
                    r = subprocess.run(
                        [ssh_keygen_exe, "-t", "ed25519", "-f", priv_key_path, "-N", ""],
                        capture_output=True, text=True, creationflags=0x08000000, env=env,
                    )
                    if r.returncode != 0:
                        raise Exception(f"ssh-keygen falló:\n{r.stderr}")
                except FileNotFoundError:
                    messagebox.showwarning(
                        "Requisito faltante: OpenSSH",
                        "Tu equipo no tiene instalado el 'Cliente OpenSSH' de Windows.\n\n"
                        "Para instalarlo:\n"
                        "1. Abre 'Configuración' de Windows.\n"
                        "2. Ve a 'Aplicaciones' → 'Características opcionales'.\n"
                        "3. Agrega 'Cliente OpenSSH'.\n\n"
                        "Una vez instalado, reinicia la aplicación.",
                    )
                    self.log_event("SSH", container_name, "WARNING", "Falta Cliente OpenSSH en Windows.")
                    return
                pub_key_path = f"{priv_key_path}.pub"

            with open(pub_key_path, "r", encoding="utf-8") as fh:
                pub_key = fh.read().strip()
            if not pub_key:
                raise Exception(f"El archivo de clave pública '{pub_key_path}' está vacío.")

            # ── 2. Instalar openssh-server en el contenedor si no existe ───
            self.log_event("SSH", container_name, "INFO", "Verificando openssh-server en el contenedor...")
            check_sshd = _exec_in_container("which sshd 2>/dev/null || command -v sshd 2>/dev/null")
            if not check_sshd.stdout.strip():
                self.log_event("SSH", container_name, "INFO", "Instalando openssh-server (puede tardar unos segundos)...")
                install_script = (
                    "if command -v apt-get >/dev/null 2>&1; then "
                    "  apt-get update -qq && apt-get install -y -qq openssh-server 2>&1; "
                    "elif command -v apk >/dev/null 2>&1; then "
                    "  apk add --no-cache openssh 2>&1; "
                    "elif command -v yum >/dev/null 2>&1; then "
                    "  yum install -y -q openssh-server 2>&1; "
                    "else "
                    "  echo 'ERROR: gestor de paquetes no reconocido' >&2; exit 1; "
                    "fi"
                )
                r_install = _exec_in_container(install_script)
                if r_install.returncode != 0:
                    raise Exception(
                        f"No se pudo instalar openssh-server en el contenedor.\n\n"
                        f"Salida:\n{r_install.stdout}\n{r_install.stderr}"
                    )
                self.log_event("SSH", container_name, "INFO", "openssh-server instalado correctamente.")

            # ── 3. Preparar y arrancar sshd ──────────────────────────────
            self.log_event("SSH", container_name, "INFO", "Configurando y arrancando sshd...")
            sshd_setup = (
                # Generar claves del host si no existen
                "mkdir -p /run/sshd && "
                "[ -f /etc/ssh/ssh_host_rsa_key ] || ssh-keygen -A -q 2>/dev/null; "
                # Activar login como root con clave pública
                "sed -i 's/^#*PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config 2>/dev/null; "
                "sed -i 's/^#*PubkeyAuthentication.*/PubkeyAuthentication yes/' /etc/ssh/sshd_config 2>/dev/null; "
                "sed -i 's/^#*PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config 2>/dev/null; "
                # Arrancar sshd si no está corriendo
                "if ! pgrep -x sshd >/dev/null 2>&1; then "
                "  /usr/sbin/sshd 2>/dev/null || /usr/bin/sshd 2>/dev/null || sshd; "
                "fi"
            )
            r_sshd = _exec_in_container(sshd_setup)
            if r_sshd.returncode != 0:
                raise Exception(
                    f"No se pudo arrancar sshd en el contenedor.\n\n"
                    f"Salida:\n{r_sshd.stdout}\n{r_sshd.stderr}"
                )
            self.log_event("SSH", container_name, "INFO", "sshd corriendo en el contenedor.")

            # ── 4. Inyectar la clave pública en authorized_keys ──────────
            self.log_event("SSH", container_name, "INFO", "Inyectando clave pública en el contenedor...")
            partes_clave = pub_key.split()
            key_body = partes_clave[1] if len(partes_clave) >= 2 else pub_key
            inject_sh = (
                f"mkdir -p /root/.ssh && "
                f"chmod 700 /root/.ssh && "
                f"grep -qF '{key_body}' /root/.ssh/authorized_keys 2>/dev/null "
                f"  || printf '%s\\n' '{pub_key}' >> /root/.ssh/authorized_keys && "
                f"chmod 600 /root/.ssh/authorized_keys"
            )
            r_inject = _exec_in_container(inject_sh)
            if r_inject.returncode != 0:
                raise Exception(
                    f"Error inyectando la clave SSH:\n{r_inject.stderr}"
                )

            # ── 5. Descubrir host y puerto SSH accesibles desde Windows ──
            #   Prioridad: port mapping en localhost  → IP directa del contenedor:22
            self.log_event("SSH", container_name, "INFO", "Detectando puerto SSH del contenedor...")

            ssh_host = "localhost"
            ssh_port = 22

            if ssh_target:
                # En modo SSH remoto, host es el servidor remoto.
                # El puerto SSH al contenedor debe estar mapeado en el servidor.
                remote_host = ssh_target.split("@")[-1]
                ssh_host = remote_host
                # Intentar obtener el port mapping en el servidor remoto
                ssh_exe = self._resolver_comando("ssh")
                r_port = subprocess.run(
                    [ssh_exe, "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes",
                     ssh_target, f"docker port {container_name} 22 2>/dev/null"],
                    capture_output=True, text=True, creationflags=0x08000000, env=env,
                )
                port_out = r_port.stdout.strip()
                if port_out:
                    # Formato "0.0.0.0:PUERTO" o ":::PUERTO"
                    ssh_port = int(port_out.split(":")[-1])
                else:
                    # Sin mapping → usar IP del contenedor en el servidor remoto
                    r_ip = subprocess.run(
                        [ssh_exe, "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes",
                         ssh_target,
                         f"docker inspect -f '{{{{range .NetworkSettings.Networks}}}}{{{{.IPAddress}}}}{{{{end}}}}' {container_name} 2>/dev/null"],
                        capture_output=True, text=True, creationflags=0x08000000, env=env,
                    )
                    container_ip = r_ip.stdout.strip()
                    if container_ip:
                        ssh_host = container_ip
                        ssh_port = 22
                    else:
                        raise Exception(
                            f"No se pudo determinar cómo acceder al contenedor '{container_name}' por SSH.\n\n"
                            f"Asegúrate de que el contenedor expone el puerto 22 con -p <puerto>:22 "
                            f"o que la red Docker es accesible desde el servidor remoto."
                        )
            else:
                # Modo local o TCP
                docker_exe = self._resolver_comando("docker")
                r_port = subprocess.run(
                    [docker_exe, "port", container_name, "22"],
                    capture_output=True, text=True, creationflags=0x08000000, env=env,
                )
                port_out = r_port.stdout.strip()
                if port_out:
                    # Hay mapping
                    if use_tcp:
                        parsed = urllib.parse.urlparse(self.docker_host)
                        ssh_host = parsed.hostname if parsed.hostname else self.docker_host.split("://")[-1].split(":")[0]
                    else:
                        ssh_host = "localhost"
                    ssh_port = int(port_out.split(":")[-1])
                else:
                    # Sin mapping → usar IP directa del contenedor
                    r_ip = subprocess.run(
                        [docker_exe, "inspect", "-f",
                         "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                         container_name],
                        capture_output=True, text=True, creationflags=0x08000000, env=env,
                    )
                    container_ip = r_ip.stdout.strip()
                    if container_ip:
                        ssh_host = container_ip
                        ssh_port = 22
                    else:
                        raise Exception(
                            f"No se pudo obtener la IP del contenedor '{container_name}'.\n\n"
                            f"Verifica que el contenedor está en ejecución."
                        )

            self.log_event("SSH", container_name, "INFO",
                           f"SSH accesible en {ssh_host}:{ssh_port}")

            # ── 6. Escribir entrada en ~/.ssh/config ────────────────────
            self.log_event("SSH", container_name, "INFO", "Verificando ~/.ssh/config...")
            ssh_config_path = _os.path.join(ssh_dir, "config")

            existing = ""
            if _os.path.exists(ssh_config_path):
                with open(ssh_config_path, "r", encoding="utf-8") as fh:
                    existing = fh.read()

            import re as _re
            pattern = _re.compile(
                rf"^Host\s+{_re.escape(container_name)}\s*\n"
                rf"(?:[ \t]+.*\n)*",
                _re.MULTILINE,
            )
            has_existing = bool(pattern.search(existing))

            def _get_config_block(h, p):
                return _textwrap.dedent(f"""\
                    Host {container_name}
                        HostName {h}
                        Port {p}
                        User root
                        StrictHostKeyChecking no
                        UserKnownHostsFile /dev/null
                        IdentityFile {_os.path.join(ssh_dir, "id_ed25519").replace(chr(92), "/")}
                """)

            if has_existing:
                overwrite_result = [None]

                dlg = tk.Toplevel(self.root)
                dlg.title("Sobreescribir configuración SSH")
                dlg.geometry("500x260")
                dlg.resizable(False, False)
                dlg.grab_set()

                dlg.update_idletasks()
                x = self.root.winfo_x() + (self.root.winfo_width() - 500) // 2
                y = self.root.winfo_y() + (self.root.winfo_height() - 260) // 2
                dlg.geometry(f"+{x}+{y}")

                tk.Label(dlg, text="El dominio SSH para este contenedor ya existe en tu configuración.\n¿Deseas sobreescribirlo? Puedes modificar los valores a continuación:", justify="center", wraplength=460, font=("Segoe UI", 10)).pack(pady=15)

                f_inputs = tk.Frame(dlg)
                f_inputs.pack(pady=5)

                tk.Label(f_inputs, text="HostName:", font=("Segoe UI Semibold", 10)).grid(row=0, column=0, sticky="e", padx=5, pady=5)
                host_var = tk.StringVar(value=ssh_host)
                tk.Entry(f_inputs, textvariable=host_var, width=30).grid(row=0, column=1, padx=5, pady=5)

                tk.Label(f_inputs, text="Port:", font=("Segoe UI Semibold", 10)).grid(row=1, column=0, sticky="e", padx=5, pady=5)
                port_var = tk.StringVar(value=str(ssh_port))
                tk.Entry(f_inputs, textvariable=port_var, width=15).grid(row=1, column=1, sticky="w", padx=5, pady=5)

                def on_accept():
                    overwrite_result[0] = (host_var.get(), port_var.get())
                    dlg.destroy()

                def on_skip():
                    overwrite_result[0] = "SKIP"
                    dlg.destroy()

                def on_cancel():
                    dlg.destroy()

                from tkinter import ttk as _ttk
                f_btn = tk.Frame(dlg)
                f_btn.pack(pady=20)
                _ttk.Button(f_btn, text="Sobreescribir y Conectar", command=on_accept).pack(side="left", padx=5)
                _ttk.Button(f_btn, text="Usar actual y Conectar", command=on_skip).pack(side="left", padx=5)
                _ttk.Button(f_btn, text="Cancelar", command=on_cancel).pack(side="left", padx=5)

                self.root.wait_window(dlg)

                if overwrite_result[0] is None:
                    self.log_event("SSH", container_name, "WARNING", "Acceso remoto cancelado por el usuario.")
                    return
                elif overwrite_result[0] == "SKIP":
                    self.log_event("SSH", container_name, "INFO", "Usando configuración SSH existente.")
                else:
                    ssh_host = overwrite_result[0][0]
                    ssh_port = int(overwrite_result[0][1])
                    config_block = _get_config_block(ssh_host, ssh_port)
                    existing_clean = pattern.sub("", existing).rstrip("\n")
                    new_config = (existing_clean + "\n\n" + config_block).lstrip("\n")
                    with open(ssh_config_path, "w", encoding="utf-8", newline="\n") as fh:
                        fh.write(new_config)
                    self.log_event("SSH", container_name, "INFO", "Configuración SSH actualizada tras confirmación del usuario.")
            else:
                config_block = _get_config_block(ssh_host, ssh_port)
                new_config = (existing.rstrip("\n") + "\n\n" + config_block).lstrip("\n")
                with open(ssh_config_path, "w", encoding="utf-8", newline="\n") as fh:
                    fh.write(new_config)
                self.log_event("SSH", container_name, "INFO", "Nueva entrada SSH creada.")

            self.log_event("SSH", container_name, "INFO",
                           f"~/.ssh/config actualizado: Host {container_name} → {ssh_host}:{ssh_port}")

            # ── 7. Descubrir la ruta del tema en /app ───────────────────
            r_theme = _exec_in_container("find /app -mindepth 1 -maxdepth 1 -type d 2>/dev/null | head -n 1")
            theme_path = r_theme.stdout.strip() or "/app"

            # ── 8. Abrir VS Code via Remote-SSH ─────────────────────────
            self.log_event("VSCODE", container_name, "INFO",
                           f"Abriendo VS Code (Remote-SSH) en {container_name}:{theme_path}...")

            folder_uri = f"vscode-remote://ssh-remote+{container_name}{theme_path}"

            try:
                code_exe = self._resolver_comando("code")
            except FileNotFoundError as exc:
                raise Exception(str(exc))

            subprocess.Popen(
                [code_exe, "--folder-uri", folder_uri],
                creationflags=0x08000000, env=env,
            )

            messagebox.showinfo(
                "Acceso remoto iniciado",
                f"VS Code se está abriendo con la extensión Remote-SSH.\n\n"
                f"Contenedor : {container_name}\n"
                f"SSH host   : {ssh_host}:{ssh_port}\n"
                f"Carpeta    : {theme_path}\n\n"
                f"Si VS Code te pide la plataforma del host, selecciona 'Linux'.",
            )

        except Exception as exc:
            error_details = traceback.format_exc()
            self.log_event("VSCODE", container_name, "ERROR", str(exc))
            messagebox.showerror("Error SSH/VSCode", f"Fallo en la ejecución:\n\n{error_details}")

        # Ignorar respuestas obsoletas de cargas anteriores.
        if self._profiles_loading_scope and scope != self._profiles_loading_scope:
            self._profiles_load_job_id = self.root.after(20, self._poll_profiles_load_queue)
            return

        try:
            self._profiles_loading = False
            self._profiles_loading_scope = None
            self._profiles_load_job_id = None
            self._profiles_load_started_at = 0.0
            self._cancel_profiles_load_guard()

            if scope != self._current_profiles_scope():
                self._set_profiles_loading_ui(False)
                if self._profiles_load_requested:
                    self._profiles_load_requested = False
                    self.refresh_profiles_ui(force=True)
                return

            if ok:
                self.profiles_data = payload if isinstance(payload, dict) else {}
                self._profiles_remote_backoff_until = 0.0
                if scope == "remoto":
                    self.profiles_loading_label.configure(text=f"Perfiles remotos cargados: {len(self.profiles_data)}")
            else:
                self.profiles_data = {}
                self._profiles_remote_backoff_until = time.time() + self._profiles_remote_retry_cooldown_sec
                if scope == "remoto":
                    self.last_docker_error_detail = str(payload)
                    self.profiles_loading_label.configure(text="Error remoto. Pulsa 'Refrescar perfiles' para reintentar")

            pending = self._profiles_pending_name
            self._profiles_pending_name = None

            prev_selected: str | None = pending
            if prev_selected is None:
                cur = self.profiles_listbox.curselection()
                if cur:
                    idx = int(cur[0])
                    if 0 <= idx < self.profiles_listbox.size():
                        prev_selected = self._profile_spinner_name if self._profile_spinner_job else self.profiles_listbox.get(idx)

            self.profiles_listbox.configure(state="normal")
            self.profiles_listbox.delete(0, tk.END)
            restore_idx: int | None = None
            for i, name in enumerate(sorted(self.profiles_data.keys(), key=str.lower)):
                self.profiles_listbox.insert(tk.END, name)
                if name == prev_selected:
                    restore_idx = i

            if restore_idx is not None:
                self.profiles_listbox.selection_set(restore_idx)
                self.profiles_listbox.see(restore_idx)
                restored_name = self.profiles_listbox.get(restore_idx)
                self._render_profile_containers(restored_name)
                self.profile_name_var.set(restored_name)
            else:
                self._render_profile_containers()

            # Quitar estado de carga solo cuando la UI final ya esta renderizada.
            self._set_profiles_loading_ui(False)

            if self._profiles_load_requested:
                self._profiles_load_requested = False
                self.refresh_profiles_ui(force=True)
        except Exception as exc:
            self.last_docker_error_detail = f"Error UI perfiles: {exc}"
            self._fail_profiles_loading("No se pudieron pintar perfiles remotos.")

    def refresh_profiles_ui(self, force: bool = False) -> None:
        self.profiles_header_label.configure(text=self._current_profiles_label())
        self.copy_profile_btn.configure(text=f"Copiar a {self._target_profiles_label()}")

        # Mantener visible lista de contenedores aunque perfiles siga cargando.
        self._render_profile_containers()

        scope = self._current_profiles_scope()

        if scope == "privado":
            # Carga local inmediata para evitar spinner eterno en almacenamiento privado.
            self._profiles_loading = False
            self._profiles_loading_scope = None
            self._profiles_load_requested = False
            self._profiles_load_started_at = 0.0
            self._profiles_remote_backoff_until = 0.0
            self._set_profiles_loading_ui(False)
            try:
                self.profiles_data = self._read_profiles_for_scope(scope)
            except Exception as exc:
                self.profiles_data = {}
                self.profiles_listbox.delete(0, tk.END)
                self.profiles_listbox.insert(tk.END, f"No se pudieron cargar perfiles privados: {exc}")
                self._render_profile_containers()
                return

            self.profiles_listbox.delete(0, tk.END)
            for name in sorted(self.profiles_data.keys(), key=str.lower):
                self.profiles_listbox.insert(tk.END, name)

            self._render_profile_containers()
            return

        if (not force) and time.time() < self._profiles_remote_backoff_until:
            self._set_profiles_loading_ui(False)
            self.profiles_listbox.delete(0, tk.END)
            self.profiles_listbox.insert(tk.END, "Reintento remoto en pausa")
            self.profiles_loading_label.configure(text="Pulsa 'Refrescar perfiles' para reintentar")
            return

        # Para almacenamiento remoto intentamos leer siempre; docker_ready puede
        # estar temporalmente desfasado y bloquear una lectura valida.

        if self._profiles_loading:
            if force:
                self._profiles_load_requested = True
            # Si por cualquier motivo se perdio el polling, lo reenganchamos.
            if self._profiles_load_job_id is None:
                self._profiles_load_job_id = self.root.after(100, self._poll_profiles_load_queue)
            if self._profiles_load_guard_job_id is None:
                self._profiles_load_guard_job_id = self.root.after(
                    int((self._profiles_load_timeout_sec + 2) * 1000),
                    self._profiles_load_guard_timeout,
                )
            return

        # Always async so users see loading state in both stores (initial load and scope changes).
        self._cancel_profiles_load_guard()
        self._clear_profiles_load_queue()
        self._set_profiles_loading_ui(True)
        self._profiles_loading = True
        self._profiles_loading_scope = scope
        self._profiles_load_started_at = time.time()
        threading.Thread(target=self._profiles_load_worker, args=(scope,), daemon=True).start()
        if self._profiles_load_job_id is None:
            self._profiles_load_job_id = self.root.after(100, self._poll_profiles_load_queue)
        self._profiles_load_guard_job_id = self.root.after(
            int((self._profiles_load_timeout_sec + 2) * 1000),
            self._profiles_load_guard_timeout,
        )

    def on_profile_selected(self, _event: object) -> None:
        selected = self.profiles_listbox.curselection()
        if not selected:
            return
        name = self.profiles_listbox.get(selected[0])
        self.profile_name_var.set(name)
        self._render_profile_containers(name)

    def clear_profile_editor(self) -> None:
        self.profile_name_var.set("")
        self.profiles_listbox.selection_clear(0, tk.END)
        self._render_profile_containers()

    def save_profile(self) -> None:
        name = self.profile_name_var.get().strip()
        if not name or " " in name:
            messagebox.showwarning("Perfiles", "El nombre del perfil no puede estar vacio ni tener espacios.")
            return

        if self._current_profiles_scope() == "remoto" and self.docker_mode != "remote":
            messagebox.showwarning("Perfiles", "Cambia a modo remoto para guardar perfiles remotos.")
            return

        selected_indexes = self.profile_containers_listbox.curselection()
        if not selected_indexes:
            messagebox.showwarning("Perfiles", "Selecciona al menos un contenedor para el perfil.")
            return

        containers = [self._profile_container_actual_name(self.profile_containers_listbox.get(i)) for i in selected_indexes]
        
        def _save_profile_operation():
            self.profiles_data[name] = containers
            self._write_profiles_for_current_scope(self.profiles_data)
            scope_name = self._current_profiles_scope().upper()
            self.log_event(f"PERFIL-{scope_name}", name, "OK", f"Guardado/actualizado: {','.join(containers)}")
            self.refresh_profiles_ui(force=True)
            self._select_profile_in_ui(name)
            self.refresh_history()
            return True
        
        self._run_with_loading_modal(f"Guardando perfil {name}", _save_profile_operation)

    def remove_selected_from_profile(self) -> None:
        if self._current_profiles_scope() == "remoto" and self.docker_mode != "remote":
            messagebox.showwarning("Perfiles", "Cambia a modo remoto para editar perfiles remotos.")
            return

        selected_profile = self.profiles_listbox.curselection()
        if not selected_profile:
            messagebox.showwarning("Perfiles", "Selecciona un perfil.")
            return

        profile_name = self.profiles_listbox.get(selected_profile[0])
        selected_indexes = self.profile_containers_listbox.curselection()
        if not selected_indexes:
            messagebox.showwarning("Perfiles", "Selecciona uno o varios contenedores para quitar del perfil.")
            return

        to_remove = {self._profile_container_actual_name(self.profile_containers_listbox.get(i)) for i in selected_indexes}
        current = list(self.profiles_data.get(profile_name, []))
        updated = [c for c in current if c not in to_remove]

        if len(updated) == len(current):
            messagebox.showwarning("Perfiles", "Los contenedores seleccionados no pertenecen al perfil.")
            return

        self.profiles_data[profile_name] = updated
        self._write_profiles_for_current_scope(self.profiles_data)
        self.profile_name_var.set(profile_name)
        scope_name = self._current_profiles_scope().upper()
        self.log_event(f"PERFIL-{scope_name}", profile_name, "OK", f"Contenedores quitados: {','.join(sorted(to_remove))}")
        self.refresh_profiles_ui(force=True)
        self._select_profile_in_ui(profile_name)
        self.refresh_history()
        messagebox.showinfo("Perfiles", f"Perfil actualizado: {profile_name}")

    def copy_selected_profile(self) -> None:
        selected = self.profiles_listbox.curselection()
        if not selected:
            messagebox.showwarning("Perfiles", "Selecciona un perfil para copiar.")
            return

        source_scope = self._current_profiles_scope()
        target_scope = self._target_profiles_scope()
        profile_name = self.profiles_listbox.get(selected[0])

        if target_scope == "remoto" and self.docker_mode != "remote":
            messagebox.showwarning("Perfiles", "Cambia a modo remoto para copiar perfiles al almacen remoto.")
            return

        try:
            target_profiles = self._read_profiles_for_scope(target_scope)
        except Exception as exc:
            messagebox.showerror("Perfiles", f"No se pudo cargar el almacen {target_scope}: {exc}")
            return

        if profile_name in target_profiles and not messagebox.askyesno(
            "Perfiles",
            f"El perfil '{profile_name}' ya existe en {target_scope}. Quieres sobrescribirlo?",
        ):
            return

        target_profiles[profile_name] = list(self.profiles_data.get(profile_name, []))
        try:
            self._write_profiles_for_scope(target_scope, target_profiles)
        except Exception as exc:
            messagebox.showerror("Perfiles", f"No se pudo copiar el perfil a {target_scope}: {exc}")
            return

        self.log_event(
            f"PERFIL-{source_scope.upper()}",
            profile_name,
            "OK",
            f"Copiado a {target_scope}",
        )
        self.refresh_history()
        messagebox.showinfo("Perfiles", f"Perfil '{profile_name}' copiado a {target_scope}.")

    def delete_profile(self) -> None:
        if self._current_profiles_scope() == "remoto" and self.docker_mode != "remote":
            messagebox.showwarning("Perfiles", "Cambia a modo remoto para borrar perfiles remotos.")
            return

        selected = self.profiles_listbox.curselection()
        if not selected:
            messagebox.showwarning("Perfiles", "Selecciona un perfil para eliminar.")
            return
        name = self.profiles_listbox.get(selected[0])
        if not messagebox.askyesno("Perfiles", f"Eliminar perfil '{name}'?"):
            return

        def _delete_profile_operation():
            if name in self.profiles_data:
                del self.profiles_data[name]
                self._write_profiles_for_current_scope(self.profiles_data)
                scope_name = self._current_profiles_scope().upper()
                self.log_event(f"PERFIL-{scope_name}", name, "OK", "Perfil eliminado")
                self.refresh_profiles_ui(force=True)
                self.clear_profile_editor()
                self.refresh_history()
                return True
            return False
        
        self._run_with_loading_modal(f"Eliminando perfil {name}", _delete_profile_operation)

    def run_selected_profile(self, mode: str) -> None:
        selected = self.profiles_listbox.curselection()
        if not selected:
            messagebox.showwarning("Perfiles", "Selecciona un perfil.")
            return

        if not self.docker_ready():
            messagebox.showerror("Docker", self._docker_unavailable_message())
            return

        profile_name = self.profiles_listbox.get(selected[0])
        containers = self.profiles_data.get(profile_name, [])
        if not containers:
            messagebox.showwarning("Perfiles", "El perfil esta vacio.")
            return

        action = "start" if mode == "start" else "stop"

        for btn in self.profile_action_btns:
            try:
                btn.configure(state="disabled")
            except tk.TclError:
                pass
        self._start_profile_spinner(profile_name)

        result_q: queue.Queue[tuple[str, list[str]]] = queue.Queue()

        def worker() -> None:
            errors: list[str] = []
            for cname in containers:
                code, _, err = self._run(["docker", action, cname])
                if code != 0:
                    errors.append(f"{cname}: {err or 'error'}")
            result_q.put(("done", errors))

        threading.Thread(target=worker, daemon=True).start()

        def poll() -> None:
            try:
                _, errors = result_q.get_nowait()
            except queue.Empty:
                self.root.after(200, poll)
                return

            self._stop_profile_spinner()
            for btn in self.profile_action_btns:
                try:
                    btn.configure(state="normal")
                except tk.TclError:
                    pass

            self.refresh_everything()
            if errors:
                self._finish_loading_modal(modal, False, error_msg="; ".join(errors))
                self.log_event("PERFIL", profile_name, "ERROR", "; ".join(errors))
                messagebox.showwarning("Perfiles", "Algunas acciones fallaron:\n\n" + "\n".join(errors))
                return

            verb = "arrancado" if action == "start" else "apagado"
            self._finish_loading_modal(modal, True, auto_close_success_ms=500)
            self.log_event("PERFIL", profile_name, "OK", f"Perfil {verb}")
            self.refresh_history()
            messagebox.showinfo("Perfiles", f"Perfil {verb}: {profile_name}")

        self.root.after(200, poll)

    def selected_network_name(self) -> str | None:
        selected = self.networks_tree.selection()
        if not selected:
            return None
        values = self.networks_tree.item(selected[0], "values")
        if not values:
            return None
        return str(values[0])

    def refresh_networks(self) -> None:
        prev_net = self.selected_network_name()
        prev_targets_selected: set[str] = set()
        if hasattr(self, "network_targets_listbox"):
            for idx in self.network_targets_listbox.curselection():
                prev_targets_selected.add(self.network_targets_listbox.get(idx))

        for item in self.networks_tree.get_children():
            self.networks_tree.delete(item)
        self.network_containers_listbox.delete(0, tk.END)

        if not self.docker_ready():
            self.network_data = {}
            self.networks_tree.insert("", "end", values=("(Docker no disponible)", "-", "-"))
            self.network_containers_listbox.insert(tk.END, "Docker no disponible")
            self.network_container_combo.configure(values=[])
            self.network_container_var.set("")
            return

        if not self.container_cache:
            self.container_cache = self.get_all_container_names()

        code, out, err = self._run(["docker", "network", "ls", "--format", "{{.Name}}|{{.Driver}}"])
        if code != 0:
            self.networks_tree.insert("", "end", values=("(Error al listar)", "-", "-"))
            self.network_containers_listbox.insert(tk.END, "No se pudieron cargar networks")
            messagebox.showwarning("Networks", err or "No se pudieron listar networks")
            return

        result: dict[str, dict[str, object]] = {}
        for line in out.splitlines():
            parts = line.split("|", 1)
            if len(parts) != 2:
                continue
            name = parts[0].strip()
            driver = parts[1].strip()
            if name in {"bridge", "host", "none"}:
                continue
            result[name] = {"driver": driver, "containers": []}

        for cname in self.container_cache:
            code_i, out_i, _ = self._run(
                [
                    "docker",
                    "inspect",
                    "--format",
                    "{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}",
                    cname,
                ]
            )
            if code_i != 0:
                continue
            connected = [x.strip() for x in out_i.split() if x.strip()]
            for net in connected:
                if net in result:
                    containers = result[net]["containers"]
                    if isinstance(containers, list):
                        containers.append(cname)

        self.network_data = result
        if not result:
            self.networks_tree.insert("", "end", values=("(sin networks)", "-", "0"))
        for name in sorted(result.keys(), key=str.lower):
            info = result[name]
            containers = info["containers"]
            count = len(containers) if isinstance(containers, list) else 0
            self.networks_tree.insert("", "end", values=(name, str(info["driver"]), count))

        if prev_net:
            restored_iid: str | None = None
            for iid in self.networks_tree.get_children():
                values = self.networks_tree.item(iid, "values")
                if values and str(values[0]) == prev_net:
                    restored_iid = iid
                    break

            if restored_iid is not None:
                self.networks_tree.selection_set(restored_iid)
                self.networks_tree.focus(restored_iid)
                self.networks_tree.see(restored_iid)

                info = self.network_data.get(prev_net, {})
                containers = info.get("containers", [])
                if isinstance(containers, list):
                    for cname in containers:
                        self.network_containers_listbox.insert(tk.END, cname)

        self.network_container_combo.configure(values=self.container_cache)
        if hasattr(self, "network_targets_listbox"):
            self.network_targets_listbox.delete(0, tk.END)
            for cname in self.container_cache:
                self.network_targets_listbox.insert(tk.END, cname)
            if prev_targets_selected:
                for idx in range(self.network_targets_listbox.size()):
                    cname = self.network_targets_listbox.get(idx)
                    if cname in prev_targets_selected:
                        self.network_targets_listbox.selection_set(idx)
        if self.container_cache and not self.network_container_var.get():
            self.network_container_var.set(self.container_cache[0])

    def refresh_networks_with_modal(self) -> None:
        modal = self._show_loading_modal("Actualizando networks")

        def _refresh() -> None:
            try:
                self.refresh_networks()
                self._finish_loading_modal(modal, True, auto_close_success_ms=250)
            except Exception as exc:
                self._finish_loading_modal(modal, False, error_msg=str(exc))

        self.root.after(50, _refresh)

    def on_network_selected(self, _event: object) -> None:
        self.network_containers_listbox.delete(0, tk.END)
        net_name = self.selected_network_name()
        if not net_name:
            return
        info = self.network_data.get(net_name, {})
        containers = info.get("containers", [])
        if isinstance(containers, list):
            for cname in containers:
                self.network_containers_listbox.insert(tk.END, cname)

    def create_network(self) -> None:
        if not self.docker_ready():
            messagebox.showerror("Docker", self._docker_unavailable_message())
            return
        name = simpledialog.askstring("Crear network", "Nombre de la nueva network:")
        if not name:
            return
        driver = self.network_driver_var.get().strip() or "bridge"
        
        def _create_network_operation():
            code, _, err = self._run(["docker", "network", "create", "--driver", driver, name.strip()])
            if code != 0:
                self.log_event("NETWORK", name.strip(), "ERROR", err or "No se pudo crear")
                raise RuntimeError(err or "No se pudo crear la network")
            self.log_event("NETWORK", name.strip(), "OK", f"Network creada con driver {driver}")
            self.refresh_networks()
            self.refresh_history()
            return True
        
        self._run_with_loading_modal(f"Creando network {name.strip()}", _create_network_operation)

    def delete_network(self) -> None:
        net_name = self.selected_network_name()
        if not net_name:
            messagebox.showwarning("Networks", "Selecciona una network para eliminar.")
            return
        if not messagebox.askyesno("Networks", f"Eliminar network '{net_name}'?"):
            return
        
        def _delete_network_operation():
            code, _, err = self._run(["docker", "network", "rm", net_name])
            if code != 0:
                self.log_event("NETWORK", net_name, "ERROR", err or "No se pudo eliminar")
                raise RuntimeError(err or "No se pudo eliminar la network")
            self.log_event("NETWORK", net_name, "OK", "Network eliminada")
            self.refresh_networks()
            self.refresh_history()
            return True
        
        self._run_with_loading_modal(f"Eliminando network {net_name}", _delete_network_operation)

    def rename_network(self) -> None:
        old_name = self.selected_network_name()
        if not old_name:
            messagebox.showwarning("Networks", "Selecciona una network para renombrar.")
            return

        new_name = simpledialog.askstring("Renombrar network", f"Nuevo nombre para '{old_name}':")
        if not new_name:
            return
        new_name = new_name.strip()
        if not new_name or new_name == old_name:
            messagebox.showwarning("Networks", "Nombre nuevo no valido.")
            return

        def _rename_network_operation():
            code, _, err = self._run(["docker", "network", "create", new_name])
            if code != 0:
                self.log_event("NETWORK", new_name, "ERROR", err or "No se pudo crear nueva network")
                raise RuntimeError(err or "No se pudo crear la nueva network")

            old_containers = self.network_data.get(old_name, {}).get("containers", [])
            if isinstance(old_containers, list):
                for cname in old_containers:
                    self._run(["docker", "network", "connect", new_name, cname])
                    self._run(["docker", "network", "disconnect", old_name, cname])

            code_rm, _, err_rm = self._run(["docker", "network", "rm", old_name])
            if code_rm != 0:
                self.log_event("NETWORK", old_name, "WARN", f"Creada {new_name}, no se pudo eliminar original")
            else:
                self.log_event("NETWORK", old_name, "OK", f"Renombrada a {new_name}")
        
        self._run_with_loading_modal(f"Renombrando network {old_name} a {new_name}", _rename_network_operation)

    def connect_container_to_network(self) -> None:
        net_name = self.selected_network_name()
        if not net_name:
            messagebox.showwarning("Networks", "Selecciona una network.")
            return

        targets: list[str] = []
        if hasattr(self, "network_targets_listbox"):
            for idx in self.network_targets_listbox.curselection():
                cname = self.network_targets_listbox.get(idx)
                if cname and cname not in targets:
                    targets.append(cname)
        if not targets:
            container = self.network_container_var.get().strip()
            if container:
                targets = [container]

        if not targets:
            messagebox.showwarning("Networks", "Selecciona un contenedor objetivo.")
            return

        def _connect_operation() -> tuple[list[str], list[str], str]:
            errors: list[str] = []
            ok_targets: list[str] = []
            for container in targets:
                code, _, err = self._run(["docker", "network", "connect", net_name, container])
                if code != 0:
                    errors.append(f"{container}: {err or 'error'}")
                else:
                    ok_targets.append(container)
            return ok_targets, errors, net_name

        def _connect_success(payload: object) -> None:
            ok_targets, errors, net_for_log = payload if isinstance(payload, tuple) else ([], ["Resultado inesperado"], net_name)
            self.refresh_networks()
            self.refresh_history()
            if ok_targets:
                self.log_event("NETWORK", net_for_log, "OK", f"Conectados: {', '.join(ok_targets)}")
            if errors:
                self.log_event("NETWORK", net_for_log, "ERROR", "; ".join(errors))
                messagebox.showwarning(
                    "Networks",
                    "Algunas conexiones fallaron.\n\n"
                    + (f"Conectados: {', '.join(ok_targets)}\n\n" if ok_targets else "")
                    + "Errores:\n"
                    + "\n".join(errors),
                )
                return
            messagebox.showinfo("Networks", f"Contenedores conectados: {', '.join(ok_targets)} -> {net_for_log}")

        self._run_with_loading_modal(
            f"Conectando contenedores a {net_name}",
            _connect_operation,
            auto_close_success_ms=500,
            on_success=_connect_success,
        )

    def disconnect_container_from_network(self) -> None:
        net_name = self.selected_network_name()
        if not net_name:
            messagebox.showwarning("Networks", "Selecciona una network.")
            return

        targets: list[str] = []
        if hasattr(self, "network_targets_listbox"):
            for idx in self.network_targets_listbox.curselection():
                cname = self.network_targets_listbox.get(idx)
                if cname and cname not in targets:
                    targets.append(cname)
        if not targets:
            container = self.network_container_var.get().strip()
            if container:
                targets = [container]

        if not targets:
            messagebox.showwarning("Networks", "Selecciona uno o varios contenedores objetivo.")
            return

        def _disconnect_operation() -> tuple[list[str], list[str], str]:
            errors: list[str] = []
            ok_targets: list[str] = []
            for container in targets:
                code, _, err = self._run(["docker", "network", "disconnect", net_name, container])
                if code != 0:
                    errors.append(f"{container}: {err or 'error'}")
                else:
                    ok_targets.append(container)
            return ok_targets, errors, net_name

        def _disconnect_success(payload: object) -> None:
            ok_targets, errors, net_for_log = payload if isinstance(payload, tuple) else ([], ["Resultado inesperado"], net_name)
            self.refresh_networks()
            self.refresh_history()
            if ok_targets:
                self.log_event("NETWORK", net_for_log, "OK", f"Desconectados: {', '.join(ok_targets)}")
            if errors:
                self.log_event("NETWORK", net_for_log, "ERROR", "; ".join(errors))
                messagebox.showwarning(
                    "Networks",
                    "Algunas desconexiones fallaron.\n\n"
                    + (f"Desconectados: {', '.join(ok_targets)}\n\n" if ok_targets else "")
                    + "Errores:\n"
                    + "\n".join(errors),
                )
                return
            messagebox.showinfo("Networks", f"Contenedores desconectados: {', '.join(ok_targets)} de {net_for_log}")

        self._run_with_loading_modal(
            f"Desconectando contenedores de {net_name}",
            _disconnect_operation,
            auto_close_success_ms=500,
            on_success=_disconnect_success,
        )

    def selected_volume_names(self) -> list[str]:
        names: list[str] = []
        if not hasattr(self, "volumes_tree"):
            return names
        for item_id in self.volumes_tree.selection():
            values = self.volumes_tree.item(item_id, "values")
            if not values:
                continue
            name = str(values[0]).strip()
            if name and name not in names:
                names.append(name)
        return names

    def refresh_volumes(self) -> None:
        if not hasattr(self, "volumes_tree"):
            return

        prev_selected = set(self.selected_volume_names())
        for item in self.volumes_tree.get_children():
            self.volumes_tree.delete(item)
        if hasattr(self, "volume_containers_listbox"):
            self.volume_containers_listbox.delete(0, tk.END)

        if not self.docker_ready():
            self.volume_data = {}
            self.volumes_tree.insert("", "end", values=("(Docker no disponible)", "-", "-", "-", "-"))
            if hasattr(self, "volume_containers_listbox"):
                self.volume_containers_listbox.insert(tk.END, "Docker no disponible")
            return

        if not self.container_cache:
            self.container_cache = self.get_all_container_names()

        code, out, err = self._run(
            ["docker", "volume", "ls", "--format", "{{.Name}}|{{.Driver}}|{{.Scope}}|{{.Mountpoint}}"]
        )
        if code != 0:
            self.volumes_tree.insert("", "end", values=("(Error al listar)", "-", "-", "-", "-"))
            if hasattr(self, "volume_containers_listbox"):
                self.volume_containers_listbox.insert(tk.END, "No se pudieron cargar volumes")
            messagebox.showwarning("Volumes", err or "No se pudieron listar volumes")
            return

        result: dict[str, dict[str, object]] = {}
        for line in out.splitlines():
            parts = line.split("|", 3)
            if len(parts) < 2:
                continue
            name = parts[0].strip()
            if not name:
                continue
            driver = parts[1].strip() if len(parts) >= 2 else ""
            scope = parts[2].strip() if len(parts) >= 3 else ""
            mountpoint = parts[3].strip() if len(parts) >= 4 else ""
            result[name] = {
                "driver": driver,
                "scope": scope,
                "mountpoint": mountpoint,
                "containers": [],
            }

        for cname in self.container_cache:
            code_i, out_i, _ = self._run(
                [
                    "docker",
                    "inspect",
                    "--format",
                    "{{range .Mounts}}{{if eq .Type \"volume\"}}{{.Name}} {{end}}{{end}}",
                    cname,
                ]
            )
            if code_i != 0:
                continue
            for vname in [x.strip() for x in out_i.split() if x.strip()]:
                if vname in result:
                    containers = result[vname]["containers"]
                    if isinstance(containers, list):
                        containers.append(cname)

        self.volume_data = result
        if not result:
            self.volumes_tree.insert("", "end", values=("(sin volumes)", "-", "-", "0", "-"))
        for name in sorted(result.keys(), key=str.lower):
            info = result[name]
            containers = info.get("containers", [])
            in_use = len(containers) if isinstance(containers, list) else 0
            iid = self.volumes_tree.insert(
                "",
                "end",
                values=(
                    name,
                    str(info.get("driver", "")),
                    str(info.get("scope", "")),
                    in_use,
                    str(info.get("mountpoint", "")),
                ),
            )
            if name in prev_selected:
                self.volumes_tree.selection_add(iid)

        self.on_volume_selected(None)

    def refresh_volumes_with_modal(self) -> None:
        modal = self._show_loading_modal("Actualizando volumes")

        def _refresh() -> None:
            try:
                self.refresh_volumes()
                self._finish_loading_modal(modal, True, auto_close_success_ms=250)
            except Exception as exc:
                self._finish_loading_modal(modal, False, error_msg=str(exc))

        self.root.after(50, _refresh)

    def on_volume_selected(self, _event: object | None) -> None:
        if not hasattr(self, "volume_containers_listbox"):
            return
        self.volume_containers_listbox.delete(0, tk.END)
        selected = self.selected_volume_names()
        if not selected:
            return
        if len(selected) > 1:
            self.volume_containers_listbox.insert(tk.END, "(Seleccion multiple)")
            return

        info = self.volume_data.get(selected[0], {})
        containers = info.get("containers", [])
        if isinstance(containers, list) and containers:
            for cname in containers:
                self.volume_containers_listbox.insert(tk.END, cname)
        else:
            self.volume_containers_listbox.insert(tk.END, "(No esta siendo usado)")

    def create_volume(self) -> None:
        if not self.docker_ready():
            messagebox.showerror("Docker", self._docker_unavailable_message())
            return
        name = simpledialog.askstring("Crear volume", "Nombre del nuevo volume:")
        if not name:
            return
        name = name.strip()
        if not name:
            messagebox.showwarning("Volumes", "Nombre no valido.")
            return
        driver = self.volume_driver_var.get().strip() or "local"
        
        def _create_volume_operation():
            code, _, err = self._run(["docker", "volume", "create", "--driver", driver, name])
            if code != 0:
                self.log_event("VOLUME", name, "ERROR", err or "No se pudo crear")
                raise RuntimeError(err or "No se pudo crear el volume")
            self.log_event("VOLUME", name, "OK", f"Volume creado con driver {driver}")
            self.refresh_volumes()
            self.refresh_history()
            return True
        
        self._run_with_loading_modal(f"Creando volume {name}", _create_volume_operation)

    def inspect_selected_volumes(self) -> None:
        names = self.selected_volume_names()
        if not names:
            messagebox.showwarning("Volumes", "Selecciona uno o varios volumes para inspeccionar.")
            return

        def _inspect_operation() -> str:
            code, out, err = self._run(["docker", "volume", "inspect", *names])
            if code != 0:
                raise RuntimeError(err or "No se pudo inspeccionar el volume")
            return out.strip() or "(Sin datos)"

        def _inspect_success(payload: object) -> None:
            content = str(payload or "(Sin datos)")
            self.log_event("VOLUME", ", ".join(names), "OK", "Inspeccion completada")
            self._open_text_viewer("Inspeccion de volumes", content)

        def _inspect_error(msg: str) -> None:
            self.log_event("VOLUME", ", ".join(names), "ERROR", msg or "No se pudo inspeccionar")
            messagebox.showerror("Volumes", msg or "No se pudo inspeccionar el volume")

        self._run_with_loading_modal(
            f"Inspeccionando {len(names)} volume(s)",
            _inspect_operation,
            auto_close_success_ms=500,
            on_success=_inspect_success,
            on_error=_inspect_error,
        )

    def delete_selected_volumes(self) -> None:
        names = self.selected_volume_names()
        if not names:
            messagebox.showwarning("Volumes", "Selecciona uno o varios volumes para eliminar.")
            return
        if not messagebox.askyesno("Volumes", f"Eliminar {len(names)} volume(s)?\n\n" + "\n".join(names)):
            return

        def _delete_volumes_operation():
            errors: list[str] = []
            ok_names: list[str] = []
            for name in names:
                code, _, err = self._run(["docker", "volume", "rm", "-f", name])
                if code != 0:
                    errors.append(f"{name}: {err or 'error'}")
                else:
                    ok_names.append(name)

            self.refresh_volumes()
            self.refresh_history()
            if ok_names:
                self.log_event("VOLUME", ", ".join(ok_names), "OK", "Volume(s) eliminado(s)")
            if errors:
                self.log_event("VOLUME", ", ".join(names), "ERROR", "; ".join(errors))
                raise RuntimeError(
                    "Algunas eliminaciones fallaron.\n\n"
                    + (f"Eliminados: {', '.join(ok_names)}\n\n" if ok_names else "")
                    + "Errores:\n"
                    + "\n".join(errors),
                )
            return True
        
        self._run_with_loading_modal(f"Eliminando {len(names)} volume(s)", _delete_volumes_operation)

    def prune_volumes(self) -> None:
        if not self.docker_ready():
            messagebox.showerror("Docker", self._docker_unavailable_message())
            return
        protected = {self.remote_history_volume, self.remote_profiles_volume}

        code, out, err = self._run(["docker", "volume", "ls", "--format", "{{.Name}}"])
        if code != 0:
            self.log_event("VOLUME", "prune", "ERROR", err or "No se pudo listar volumes")
            messagebox.showerror("Volumes", err or "No se pudieron listar volumes")
            return

        all_volumes = [line.strip() for line in out.splitlines() if line.strip()]
        if not all_volumes:
            messagebox.showinfo("Volumes", "No hay volumes para evaluar.")
            return

        if not self.container_cache:
            self.container_cache = self.get_all_container_names()

        # Proteger volumes que contengan _history_remote o _profiles_remote en el nombre
        protected_volume_patterns = ["_history_remote", "_profiles_remote"]
        for vname in all_volumes:
            if any(pattern in vname for pattern in protected_volume_patterns):
                protected.add(vname)

        used_volumes: set[str] = set()
        for cname in self.container_cache:
            code_i, out_i, _ = self._run(
                [
                    "docker",
                    "inspect",
                    "--format",
                    "{{range .Mounts}}{{if eq .Type \"volume\"}}{{.Name}} {{end}}{{end}}",
                    cname,
                ]
            )
            if code_i != 0:
                continue
            for vname in [x.strip() for x in out_i.split() if x.strip()]:
                used_volumes.add(vname)

        removable = [v for v in all_volumes if v not in protected and v not in used_volumes]

        if not removable:
            protected_by_pattern = [v for v in all_volumes if any(p in v for p in protected_volume_patterns)]
            protected_by_pattern_msg = (
                f"Volumes protegidos por patrón: {', '.join(protected_by_pattern)}\n\n"
                if protected_by_pattern
                else ""
            )
            messagebox.showinfo(
                "Volumes",
                f"No hay volumes sin uso para eliminar.\n\n"
                f"Volumes siempre protegidos: {self.remote_history_volume}, {self.remote_profiles_volume}\n"
                f"{protected_by_pattern_msg}"
                f"Total de volumes protegidos: {len(protected)}",
            )
            return

        protected_by_pattern = [v for v in all_volumes if any(p in v for p in protected_volume_patterns)]
        protected_by_pattern_msg = (
            f"\nVolumes protegidos por patrón (*_history_remote, *_profiles_remote): {', '.join(protected_by_pattern)}"
            if protected_by_pattern
            else ""
        )

        if not messagebox.askyesno(
            "Volumes",
            "Eliminar volumes sin uso?\n\n"
            f"Volumes siempre protegidos: {self.remote_history_volume}, {self.remote_profiles_volume}{protected_by_pattern_msg}\n\n"
            f"Se intentaran eliminar {len(removable)} volume(s).",
        ):
            return

        protected_count = len([v for v in all_volumes if v in protected])
        used_count = len(used_volumes)

        def _prune_operation() -> tuple[list[str], list[str], int, int]:
            removed: list[str] = []
            errors: list[str] = []
            for vname in removable:
                code_rm, _, err_rm = self._run(["docker", "volume", "rm", "-f", vname])
                if code_rm == 0:
                    removed.append(vname)
                else:
                    errors.append(f"{vname}: {err_rm or 'error'}")
            return removed, errors, protected_count, used_count

        def _prune_success(payload: object) -> None:
            removed, errors, p_count, u_count = payload if isinstance(payload, tuple) else ([], ["Resultado inesperado"], protected_count, used_count)
            detail = f"Eliminados={len(removed)}; protegidos={p_count}; en uso={u_count}"
            if errors:
                self.log_event("VOLUME", "prune", "WARN", detail + "; errores=" + " | ".join(errors))
            else:
                self.log_event("VOLUME", "prune", "OK", detail)

            self.refresh_volumes()
            self.refresh_history()

            if errors:
                messagebox.showwarning(
                    "Volumes",
                    "Prune parcial completado.\n\n"
                    f"Eliminados: {len(removed)}\n"
                    f"Protegidos: {self.remote_history_volume}, {self.remote_profiles_volume}\n\n"
                    "Errores:\n" + "\n".join(errors),
                )
                return

            messagebox.showinfo(
                "Volumes",
                "Prune completado.\n\n"
                f"Eliminados: {len(removed)}\n"
                f"Protegidos: {self.remote_history_volume}, {self.remote_profiles_volume}",
            )

        self._run_with_loading_modal(
            f"Aplicando prune a {len(removable)} volume(s)",
            _prune_operation,
            auto_close_success_ms=500,
            on_success=_prune_success,
        )

    def clone_volume(self) -> None:
        names = self.selected_volume_names()
        if len(names) != 1:
            messagebox.showwarning("Volumes", "Selecciona un unico volume de origen para clonar.")
            return

        source_name = names[0]
        target_name = simpledialog.askstring("Clonar volume", f"Nuevo nombre para el clon de '{source_name}':")
        if not target_name:
            return
        target_name = target_name.strip()
        if not target_name:
            messagebox.showwarning("Volumes", "Nombre destino no valido.")
            return
        if target_name == source_name:
            messagebox.showwarning("Volumes", "El nombre destino debe ser diferente al origen.")
            return

        def _clone_operation() -> tuple[str, str]:
            code_create, _, err_create = self._run(["docker", "volume", "create", target_name])
            if code_create != 0:
                raise RuntimeError(err_create or "No se pudo crear el volume destino")

            code_copy, _, err_copy = self._run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "-v",
                    f"{source_name}:/from:ro",
                    "-v",
                    f"{target_name}:/to",
                    "busybox",
                    "sh",
                    "-c",
                    "cd /from && tar cf - . | tar xf - -C /to",
                ]
            )
            if code_copy != 0:
                raise RuntimeError(err_copy or "No se pudo clonar el volume")
            return source_name, target_name

        def _clone_success(payload: object) -> None:
            src, dst = payload if isinstance(payload, tuple) else (source_name, target_name)
            self.log_event("VOLUME", src, "OK", f"Clonado en {dst}")
            self.refresh_volumes()
            self.refresh_history()
            messagebox.showinfo("Volumes", f"Volume clonado: {src} -> {dst}")

        def _clone_error(msg: str) -> None:
            self.log_event("VOLUME", source_name, "ERROR", msg or "Fallo al clonar datos")
            messagebox.showerror("Volumes", msg or "No se pudo clonar el volume")

        self._run_with_loading_modal(
            f"Clonando volume {source_name} a {target_name}",
            _clone_operation,
            auto_close_success_ms=500,
            on_success=_clone_success,
            on_error=_clone_error,
        )

    def clear_volume_contents(self) -> None:
        names = self.selected_volume_names()
        if len(names) != 1:
            messagebox.showwarning("Volumes", "Selecciona un unico volume para vaciar su contenido.")
            return
        vname = names[0]
        if not messagebox.askyesno(
            "Volumes",
            f"Vaciar TODO el contenido de '{vname}'?\n\nEsta accion no se puede deshacer.",
        ):
            return

        def _clear_operation() -> str:
            code, _, err = self._run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "-v",
                    f"{vname}:/data",
                    "busybox",
                    "sh",
                    "-c",
                    "rm -rf /data/* /data/.[!.]* /data/..?* 2>/dev/null || true",
                ]
            )
            if code != 0:
                raise RuntimeError(err or "No se pudo vaciar el volume")
            return vname

        def _clear_success(payload: object) -> None:
            target = str(payload or vname)
            self.log_event("VOLUME", target, "OK", "Contenido eliminado")
            self.refresh_history()
            messagebox.showinfo("Volumes", f"Volume vaciado: {target}")

        def _clear_error(msg: str) -> None:
            self.log_event("VOLUME", vname, "ERROR", msg or "No se pudo vaciar")
            messagebox.showerror("Volumes", msg or "No se pudo vaciar el volume")

        self._run_with_loading_modal(
            f"Vaciando volume {vname}",
            _clear_operation,
            auto_close_success_ms=500,
            on_success=_clear_success,
            on_error=_clear_error,
        )

    def _open_text_viewer(self, title: str, content: str) -> None:
        dialog = tk.Toplevel(self.root)
        dialog.title(title)
        dialog.geometry("900x520")
        dialog.minsize(640, 380)
        dialog.transient(self.root)

        frame = ttk.Frame(dialog, padding=10)
        frame.pack(fill="both", expand=True)
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(0, weight=1)

        text = tk.Text(
            frame,
            wrap="none",
            bg="#ffffff",
            fg="#1f2937",
            insertbackground="#1f2937",
            relief="solid",
            borderwidth=1,
            highlightthickness=0,
            font=("Consolas", 10),
        )
        text.grid(row=0, column=0, sticky="nsew")

        y_scroll = ttk.Scrollbar(frame, orient="vertical", command=text.yview)
        y_scroll.grid(row=0, column=1, sticky="ns")
        text.configure(yscrollcommand=y_scroll.set)

        x_scroll = ttk.Scrollbar(frame, orient="horizontal", command=text.xview)
        x_scroll.grid(row=1, column=0, sticky="ew")
        text.configure(xscrollcommand=x_scroll.set)

        text.insert("1.0", content)
        text.configure(state="disabled")

        actions = ttk.Frame(dialog, padding=(10, 0, 10, 10))
        actions.pack(fill="x")
        ttk.Button(actions, text="Cerrar", command=dialog.destroy).pack(side="right")

    def refresh_history(self) -> None:
        if self._history_refresh_in_progress:
            self._history_refresh_requested = True
            return

        self._history_refresh_in_progress = True
        
        # Mostrar spinner de carga si no hay líneas cargadas aún
        if not hasattr(self, 'history_lines') or not self.history_lines:
            self._show_history_loading_spinner()
        
        threading.Thread(target=self._history_refresh_worker, daemon=True).start()

        if self._history_refresh_job_id is None:
            self._history_refresh_job_id = self.root.after(100, self._poll_history_refresh_queue)

    def _parse_log_lines(self) -> int:
        raw = self.log_lines_var.get().strip()
        try:
            value = int(raw)
        except ValueError:
            value = 100
        value = max(10, min(5000, value))
        self.log_lines_var.set(str(value))
        return value

    def fetch_logs(self, preserve_scroll: bool = False) -> None:
        if not self.docker_ready():
            messagebox.showerror("Docker", self._docker_unavailable_message())
            return

        container = self.log_container_var.get().strip()
        if not container:
            messagebox.showwarning("Logs", "Selecciona un contenedor.")
            return

        tail = self._parse_log_lines()

        if self.log_follow_var.get():
            self._start_follow_logs(container, tail)
            return

        saved_x = self.logs_text.xview()[0] if preserve_scroll else 0.0
        saved_y = self.logs_text.yview()[0] if preserve_scroll else None

        self._stop_follow_logs()
        code, out, err = self._run(["docker", "logs", "--tail", str(tail), container])

        content_parts = []
        if out:
            content_parts.append(out)
        if err:
            content_parts.append(err)
        content = "\n".join(content_parts).strip()
        if not content:
            content = "(Sin salida de logs en este momento)"

        self.logs_text.configure(state="normal")
        self.logs_text.delete("1.0", tk.END)
        self.logs_text.insert(tk.END, content)

        if preserve_scroll:
            self.logs_text.xview_moveto(saved_x)
            self.logs_text.yview_moveto(saved_y)
        else:
            self.logs_text.see(tk.END)
            self.logs_text.xview_moveto(0.0)

        self.logs_text.configure(state="disabled")

        if code == 0:
            self.log_event("LOGS", container, "OK", f"Ultimas {tail} lineas")
        else:
            self.log_event("LOGS", container, "ERROR", f"Fallo al leer logs: {err or 'error'}")
        self.refresh_history()

    def _auto_fetch_logs(self) -> None:
        if not self.log_auto_refresh_var.get():
            return
        if self.log_follow_var.get():
            return
        self.fetch_logs(preserve_scroll=True)
        self.logs_refresh_job_id = self.root.after(4000, self._auto_fetch_logs)

    def toggle_logs_auto_refresh(self) -> None:
        if self.logs_refresh_job_id is not None:
            self.root.after_cancel(self.logs_refresh_job_id)
            self.logs_refresh_job_id = None

        if self.log_auto_refresh_var.get() and self.log_follow_var.get():
            self.log_auto_refresh_var.set(False)
            messagebox.showinfo("Logs", "Desactiva 'Seguir (-f)' para usar Auto-refresco.")
            return

        if self.log_auto_refresh_var.get():
            self._auto_fetch_logs()

    def on_follow_mode_toggled(self) -> None:
        if self.log_follow_var.get() and self.log_auto_refresh_var.get():
            self.log_auto_refresh_var.set(False)
            if self.logs_refresh_job_id is not None:
                self.root.after_cancel(self.logs_refresh_job_id)
                self.logs_refresh_job_id = None

        if not self.log_follow_var.get():
            self._stop_follow_logs()

    def _start_follow_logs(self, container: str, tail: int) -> None:
        self._stop_follow_logs()
        if self._should_use_docker_sdk():
            self._start_follow_logs_sdk(container, tail)
            return
        try:
            cmd = self._build_docker_command(["docker", "logs", "-f", "--tail", str(tail), container])
            self.logs_follow_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=self.tools_dir,
                shell=False,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
        except Exception as exc:
            self.log_event("LOGS", container, "ERROR", f"Fallo al iniciar seguimiento: {exc}")
            self.refresh_history()
            messagebox.showerror("Logs", f"No se pudo iniciar seguimiento de logs.\n\n{exc}")
            return

        self.logs_text.configure(state="normal")
        self.logs_text.delete("1.0", tk.END)
        self.logs_text.insert(tk.END, f"Siguiendo logs de {container}... (Ctrl + boton para detener cambiando modo)\n\n")
        self.logs_text.configure(state="disabled")

        reader_thread = threading.Thread(target=self._read_follow_output, daemon=True)
        reader_thread.start()
        self.logs_follow_poll_job_id = self.root.after(150, self._poll_follow_output)
        self.log_event("LOGS", container, "INFO", f"Seguimiento en vivo iniciado (tail={tail})")
        self.refresh_history()

    def _start_follow_logs_sdk(self, container: str, tail: int) -> None:
        client = self._get_docker_sdk_client(timeout_seconds=300)
        if client is None:
            messagebox.showerror("Logs", "Docker SDK no disponible para seguimiento en vivo.")
            return

        try:
            client.api.timeout = None
        except Exception:
            pass

        try:
            cont = client.containers.get(container)
            stream = cont.logs(stream=True, follow=True, tail=tail, stdout=True, stderr=True)
        except Exception as exc:
            self.log_event("LOGS", container, "ERROR", f"Fallo al iniciar seguimiento SDK: {exc}")
            self.refresh_history()
            messagebox.showerror("Logs", f"No se pudo iniciar seguimiento de logs.\n\n{exc}")
            return

        self._sdk_follow_stop_event = threading.Event()
        self._sdk_follow_active = True

        self.logs_text.configure(state="normal")
        self.logs_text.delete("1.0", tk.END)
        self.logs_text.insert(tk.END, f"Siguiendo logs de {container}... (modo SDK)\n\n")
        self.logs_text.configure(state="disabled")

        reader_thread = threading.Thread(target=self._read_follow_output_sdk, args=(stream,), daemon=True)
        reader_thread.start()
        self.logs_follow_poll_job_id = self.root.after(150, self._poll_follow_output)
        self.log_event("LOGS", container, "INFO", f"Seguimiento en vivo iniciado (SDK, tail={tail})")
        self.refresh_history()

    def _read_follow_output_sdk(self, stream: object) -> None:
        try:
            for chunk in stream:
                if self._sdk_follow_stop_event is not None and self._sdk_follow_stop_event.is_set():
                    break
                if isinstance(chunk, (bytes, bytearray)):
                    text = chunk.decode("utf-8", errors="replace")
                else:
                    text = str(chunk)
                if text:
                    self.logs_follow_queue.put(text)
        except Exception as exc:
            self.logs_follow_queue.put(f"\n[seguimiento finalizado con error: {exc}]\n")
        finally:
            self._sdk_follow_active = False

    def _read_follow_output(self) -> None:
        process = self.logs_follow_process
        if process is None or process.stdout is None:
            return

        for line in process.stdout:
            self.logs_follow_queue.put(line)

    def _poll_follow_output(self) -> None:
        chunks: list[str] = []
        while True:
            try:
                chunks.append(self.logs_follow_queue.get_nowait())
            except queue.Empty:
                break

        if chunks:
            self.logs_text.configure(state="normal")
            self.logs_text.insert(tk.END, "".join(chunks))
            self.logs_text.see(tk.END)
            self.logs_text.configure(state="disabled")

        process = self.logs_follow_process
        if process is None:
            if self._sdk_follow_active:
                self.logs_follow_poll_job_id = self.root.after(150, self._poll_follow_output)
                return
            self.logs_follow_poll_job_id = None
            return

        if process.poll() is None:
            self.logs_follow_poll_job_id = self.root.after(150, self._poll_follow_output)
            return

        self.logs_follow_poll_job_id = None
        exit_code = process.returncode
        self.logs_text.configure(state="normal")
        self.logs_text.insert(tk.END, f"\n[seguimiento finalizado, codigo {exit_code}]\n")
        self.logs_text.see(tk.END)
        self.logs_text.configure(state="disabled")
        self.logs_follow_process = None

    def _stop_follow_logs(self) -> None:
        if self.logs_follow_poll_job_id is not None:
            self.root.after_cancel(self.logs_follow_poll_job_id)
            self.logs_follow_poll_job_id = None

        if self._sdk_follow_stop_event is not None:
            self._sdk_follow_stop_event.set()
            self._sdk_follow_stop_event = None
        self._sdk_follow_active = False

        process = self.logs_follow_process
        if process is not None:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill()
            self.logs_follow_process = None

        while True:
            try:
                self.logs_follow_queue.get_nowait()
            except queue.Empty:
                break

    def on_close(self) -> None:
        if self.refresh_job_id is not None:
            self.root.after_cancel(self.refresh_job_id)
            self.refresh_job_id = None
        if self.logs_refresh_job_id is not None:
            self.root.after_cancel(self.logs_refresh_job_id)
            self.logs_refresh_job_id = None
        if self._docker_check_job_id is not None:
            self.root.after_cancel(self._docker_check_job_id)
            self._docker_check_job_id = None
        if self._history_refresh_job_id is not None:
            self.root.after_cancel(self._history_refresh_job_id)
            self._history_refresh_job_id = None
        if self._profiles_load_job_id is not None:
            self.root.after_cancel(self._profiles_load_job_id)
            self._profiles_load_job_id = None
        self._stop_status_spinner()
        self._stop_container_spinner()
        self._stop_profile_spinner()
        self._stop_follow_logs()
        self.root.destroy()

    def export_visible_logs(self) -> None:
        content = self.logs_text.get("1.0", tk.END).strip()
        if not content or content == "Selecciona un contenedor y pulsa 'Ver logs'.":
            messagebox.showwarning("Logs", "No hay contenido de logs para exportar.")
            return

        container = self.log_container_var.get().strip() or "contenedor"
        # Sanitize container name for Windows filenames
        invalid_chars = '<>:"/\\|?*'
        safe_container = ''.join('_' if c in invalid_chars else c for c in container)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        default_name = f"logs_{safe_container}_{stamp}.txt"

        output_path = filedialog.asksaveasfilename(
            title="Guardar logs como",
            initialdir=self.tools_dir,
            initialfile=default_name,
            defaultextension=".txt",
            filetypes=[("Archivo de texto", "*.txt"), ("Todos los archivos", "*.*")],
        )

        if not output_path:
            return

        try:
            with open(output_path, "w", encoding="utf-8", errors="replace") as fh:
                fh.write(content)
            self.log_event("LOGS", container, "OK", f"Exportado a {output_path}")
            self.refresh_history()
            messagebox.showinfo("Logs", f"Logs exportados correctamente.\n\n{output_path}")
        except Exception as exc:
            self.log_event("LOGS", container, "ERROR", f"Fallo al exportar: {exc}")
            self.refresh_history()
            messagebox.showerror("Logs", f"No se pudieron exportar los logs.\n\n{exc}")

    def copy_visible_logs(self) -> None:
        content = self.logs_text.get("1.0", tk.END).strip()
        if not content or content == "Selecciona un contenedor y pulsa 'Ver logs'.":
            messagebox.showwarning("Logs", "No hay contenido visible para copiar.")
            return

        container = self.log_container_var.get().strip() or "contenedor"
        self.root.clipboard_clear()
        self.root.clipboard_append(content)
        self.root.update()
        self.log_event("LOGS", container, "INFO", "Contenido visible copiado al portapapeles")
        self.refresh_history()
        messagebox.showinfo("Logs", "Contenido copiado al portapapeles.")

    def apply_history_filter(self, _event: object = None) -> None:
        level = self.history_level_var.get().strip().upper()
        query = self.history_search_var.get().strip()
        query_tokens = [
            token
            for token in self._normalize_text(query).split()
            if token
        ]

        filtered: list[str] = []
        for line in self.history_lines:
            detected_level = self._detect_history_level(line)
            if level != "TODOS" and detected_level != level:
                continue
            if query_tokens:
                normalized_line = self._normalize_text(line)
                if not all(token in normalized_line for token in query_tokens):
                    continue
            filtered.append(line)

        x_first, _x_last = self.history_text.xview()
        y_first, _y_last = self.history_text.yview()

        self.history_text.configure(state="normal")
        self.history_text.delete("1.0", tk.END)
        if filtered:
            self.history_text.insert(tk.END, "\n".join(filtered))
        else:
            self.history_text.insert(tk.END, "Sin registros para el filtro actual.")
        self.history_text.xview_moveto(x_first)
        self.history_text.yview_moveto(y_first)
        self.history_text.configure(state="disabled")

    @staticmethod
    def _detect_history_level(line: str) -> str:
        upper = line.upper()

        # Formato heredado: ... RESULTADO=OK ...
        match_resultado = re.search(r"\bRESULTADO\s*=\s*(OK|ERROR|WARN|INFO)\b", upper)
        if match_resultado:
            return match_resultado.group(1)

        # Formato GUI actual: [OK] / [ERROR] / [WARN] / [INFO]
        match_brackets = re.search(r"\[(OK|ERROR|WARN|INFO)\]", upper)
        if match_brackets:
            return match_brackets.group(1)

        return "INFO"

    @staticmethod
    def _normalize_text(value: str) -> str:
        normalized = unicodedata.normalize("NFKD", value)
        no_accents = "".join(ch for ch in normalized if not unicodedata.combining(ch))
        return no_accents.lower()

    def clear_history_filters(self) -> None:
        self.history_level_var.set("TODOS")
        self.history_search_var.set("")
        self.apply_history_filter()

    def copy_visible_history(self) -> None:
        content = self.history_text.get("1.0", tk.END).strip()
        if not content or content == "Sin registros para el filtro actual.":
            messagebox.showwarning("Historial", "No hay contenido visible para copiar.")
            return

        self.root.clipboard_clear()
        self.root.clipboard_append(content)
        self.root.update()
        self.log_event("HISTORIAL", "visible", "INFO", "Contenido copiado al portapapeles")
        self.refresh_history()
        messagebox.showinfo("Historial", "Contenido copiado al portapapeles.")

    def launch_bat(self, bat_name: str, args: str = "", maximized: bool = False) -> None:
        bat_path = os.path.join(self.tools_dir, bat_name)
        if not os.path.isfile(bat_path):
            messagebox.showerror("Archivo", f"No se encontro:\n{bat_path}")
            return

        if maximized:
            cmd = f'start "" /wait cmd /c ""{bat_path}" maximizado"'
        elif args:
            cmd = f'start "" /wait cmd /c ""{bat_path}" {args}"'
        else:
            cmd = f'start "" /wait cmd /c ""{bat_path}""'

        try:
            subprocess.Popen(cmd, cwd=self.tools_dir, shell=True, creationflags=subprocess.CREATE_NO_WINDOW)
            self.log_event("SCRIPT", bat_name, "INFO", f"Lanzado con args: {args or '-'}")
            self.refresh_history()
        except Exception as exc:  # pragma: no cover
            self.log_event("SCRIPT", bat_name, "ERROR", str(exc))
            messagebox.showerror("Ejecucion", f"No se pudo ejecutar {bat_name}.\n\n{exc}")

    def open_docs(self) -> None:
        docs = self._find_first_existing([
            "guia_usuario.html",
            "README.md",
        ])
        if not docs or not os.path.isfile(docs):
            messagebox.showerror("Archivo", "No se encontro guia_usuario.html ni README.md")
            return

        try:
            if sys.platform == "win32":
                # os.startfile() llama directamente a la Shell API de Windows
                # sin lanzar ningún proceso de terminal intermedio
                os.startfile(os.path.abspath(docs))
            else:
                docs_uri = Path(docs).resolve().as_uri()
                webbrowser.open_new_tab(docs_uri)
            doc_name = os.path.basename(docs)
            self.log_event("DOCS", doc_name, "INFO", f"Documentacion abierta: {doc_name}")
            self.refresh_history()
        except Exception as exc:  # pragma: no cover
            doc_name = os.path.basename(docs) or "guia_usuario.html"
            self.log_event("DOCS", doc_name, "ERROR", str(exc))
            messagebox.showerror("Documento", f"No se pudo abrir la documentacion.\n\n{exc}")

    @staticmethod
    def _is_host_port_available(port: int) -> bool:
        if port < 1 or port > 65535:
            return False
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind(("0.0.0.0", port))
            return True
        except OSError:
            return False
        finally:
            sock.close()

    def _get_running_docker_published_ports(self) -> set[int]:
        code, out, _ = self._run(["docker", "ps", "--format", "{{.Ports}}"])
        if code != 0 or not out:
            return set()

        ports: set[int] = set()
        for line in out.splitlines():
            if not line.strip():
                continue
            # Ejemplos:
            # 0.0.0.0:8181->80/tcp, :::8181->80/tcp
            # [::]:8181->80/tcp
            for match in re.finditer(r":(\d+)->", line):
                try:
                    ports.add(int(match.group(1)))
                except ValueError:
                    continue
        return ports

    def _validate_setup_ports_inputs(
        self,
        http_port: str,
        https_port: str,
        db_port: str,
        pma_port: str,
    ) -> tuple[bool, str]:
        raw_map = {
            "HTTP": http_port.strip(),
            "HTTPS": https_port.strip(),
            "MariaDB": db_port.strip(),
            "phpMyAdmin": pma_port.strip(),
        }

        parsed: dict[str, int] = {}
        for label, raw in raw_map.items():
            if not raw:
                return False, f"Completa el puerto {label}."
            try:
                value = int(raw)
            except ValueError:
                return False, f"El puerto {label} debe ser numerico."
            if value < 1 or value > 65535:
                return False, f"El puerto {label} debe estar entre 1 y 65535."
            parsed[label] = value

        values = list(parsed.values())
        if len(set(values)) != len(values):
            return False, "No se permiten puertos repetidos."

        docker_ports = self._get_running_docker_published_ports()
        busy_labels: list[str] = []
        for label, port in parsed.items():
            # En remoto validamos contra puertos publicados del daemon remoto.
            # En local tambien validamos disponibilidad en host cliente.
            is_busy = port in docker_ports
            if not is_busy and self.docker_mode != "remote":
                is_busy = not self._is_host_port_available(port)
            if is_busy:
                busy_labels.append(label)
        if busy_labels:
            prefix = "Puertos en uso en daemon remoto: " if self.docker_mode == "remote" else "Puertos en uso: "
            return False, prefix + ", ".join(busy_labels)

        return True, "Puertos validados y disponibles."

    def _add_password_entry_with_toggle(
        self,
        parent: tk.Misc,
        textvariable: tk.StringVar,
        row: int,
        column: int,
        padx: int | tuple[int, int] = 0,
        pady: int | tuple[int, int] = 0,
        sticky: str = "ew",
    ) -> ttk.Entry:
        wrapper = ttk.Frame(parent)
        wrapper.grid(row=row, column=column, sticky=sticky, padx=padx, pady=pady)
        wrapper.columnconfigure(0, weight=1)

        entry = ttk.Entry(wrapper, textvariable=textvariable, show="*")
        entry.grid(row=0, column=0, sticky="ew")

        toggle_button = ttk.Button(wrapper, text="Ver", width=7)
        toggle_button.grid(row=0, column=1, padx=(6, 0))

        def toggle_password_visibility() -> None:
            hidden = entry.cget("show") == "*"
            entry.configure(show="" if hidden else "*")
            toggle_button.configure(text="Ocultar" if hidden else "Ver")

        toggle_button.configure(command=toggle_password_visibility)
        return entry

    def _open_or_focus_work_tab(self, tab_key: str, title: str) -> ttk.Frame | None:
        if self.tabs is None:
            return None

        existing = self.dynamic_tabs.get(tab_key)
        if existing is not None and existing.winfo_exists():
            self.tabs.select(existing)
            return existing

        frame = ttk.Frame(self.tabs, padding=10)
        self.dynamic_tabs[tab_key] = frame
        self.tabs.add(frame, text=title)
        self.tabs.select(frame)
        return frame

    def _close_work_tab(self, tab_key: str) -> None:
        if self.tabs is None:
            return
        tab = self.dynamic_tabs.get(tab_key)
        if tab is None or not tab.winfo_exists():
            self.dynamic_tabs.pop(tab_key, None)
            return
        try:
            self.tabs.forget(tab)
        except Exception:
            pass
        tab.destroy()
        self.dynamic_tabs.pop(tab_key, None)

    def _update_status_dot(self, *_: object) -> None:
        if self.docker_status_dot is None or not self.docker_status_dot.winfo_exists():
            return
        status = self.status_var.get().lower()
        if "disponible" in status and "no " not in status:
            color = "#10b981"   # green  - available
        elif "no disponible" in status or "no encontrado" in status or "error" in status:
            color = "#ef4444"   # red    - unavailable
        elif "iniciando" in status or "comprobando" in status:
            color = "#f59e0b"   # amber  - in progress
        else:
            color = "#64748b"   # slate  - unknown
        self.docker_status_dot.configure(fg=color)
        self._update_connection_mode_badge()
        self._refresh_observability_panel()

    def _update_connection_mode_badge(self) -> None:
        host = (self.docker_host or "").strip()
        if host.startswith("tcp://"):
            host = host[6:]
        elif host.startswith("http://"):
            host = host[7:]
        elif host.startswith("https://"):
            host = host[8:]

        if self.docker_mode == "remote":
            short_host = host if len(host) <= 34 else f"{host[:31]}..."
            if self.is_compact_layout:
                mode_text = f"Remoto: {short_host or 'sin host'}"
            else:
                mode_text = f"Modo: remoto ({short_host or 'sin host'})"
            fg = "#7f1d1d"
            bg = "#fee2e2"
        else:
            mode_text = "Local" if self.is_compact_layout else "Modo: local"
            fg = "#1e3a8a"
            bg = "#dbeafe"

        self.connection_mode_var.set(mode_text)
        self.mode_state_var.set(mode_text)
        if self.connection_mode_badge is not None and self.connection_mode_badge.winfo_exists():
            self.connection_mode_badge.configure(fg=fg, bg=bg)
        self._refresh_observability_panel()

    def _bind_global_shortcuts(self) -> None:
        def bind_shortcut(sequence: str, action: Callable[[], None]) -> None:
            def handler(_event: object) -> str:
                try:
                    action()
                except Exception:
                    pass
                return "break"

            self.root.bind_all(sequence, handler)

        bind_shortcut("<Control-r>", self.refresh_everything)
        bind_shortcut("<Control-i>", self.open_import_wizard)
        bind_shortcut("<Control-e>", self.open_export_wizard)
        bind_shortcut("<Control-l>", self.open_setup_wizard)
        bind_shortcut("<Control-b>", self._toggle_compact_layout)
        bind_shortcut("<F1>", self.open_docs)
        bind_shortcut("<Control-q>", self.on_close)

    def _schedule_layout_reflow(self, event: tk.Event) -> None:
        if event.widget is not self.root:
            return
        if self._layout_reflow_job is not None:
            try:
                self.root.after_cancel(self._layout_reflow_job)
            except Exception:
                pass
        self._layout_reflow_job = self.root.after(90, self._apply_responsive_layout)

    def _apply_responsive_layout(self) -> None:
        self._layout_reflow_job = None
        width = self.root.winfo_width()
        height = self.root.winfo_height()
        compact = width < 1040 or height < 620
        self._set_compact_layout(compact)

    def _toggle_compact_layout(self) -> None:
        self._set_compact_layout(not self.is_compact_layout)

    def _set_compact_layout(self, compact: bool) -> None:
        if self.is_compact_layout == compact:
            return
        self.is_compact_layout = compact

        if self.sidebar_frame is not None and self.sidebar_frame.winfo_exists():
            self.sidebar_frame.configure(width=84 if compact else 234)

        if self.sidebar_logo_title_label is not None and self.sidebar_logo_title_label.winfo_exists():
            self.sidebar_logo_title_label.configure(text="\u2726" if compact else "\u2726  Shopify")

        if self.sidebar_logo_subtitle_label is not None and self.sidebar_logo_subtitle_label.winfo_exists():
            if compact:
                self.sidebar_logo_subtitle_label.pack_forget()
            else:
                if not self.sidebar_logo_subtitle_label.winfo_manager():
                    self.sidebar_logo_subtitle_label.pack(anchor="w", pady=(3, 0))

        if self.sidebar_shortcuts_frame is not None and self.sidebar_shortcuts_frame.winfo_exists():
            if compact:
                self.sidebar_shortcuts_frame.pack_forget()
            else:
                if not self.sidebar_shortcuts_frame.winfo_manager():
                    self.sidebar_shortcuts_frame.pack(fill="x")

        if self.sidebar_observability_frame is not None and self.sidebar_observability_frame.winfo_exists():
            if compact:
                self.sidebar_observability_frame.pack_forget()
            else:
                if not self.sidebar_observability_frame.winfo_manager():
                    self.sidebar_observability_frame.pack(fill="x", padx=12, pady=(10, 0))

        for btn, full_label, compact_label in self.sidebar_nav_buttons:
            if not btn.winfo_exists():
                continue
            if compact:
                btn.configure(text=compact_label, anchor="center", padx=0)
            else:
                btn.configure(text=full_label, anchor="w", padx=18)

        if self.sidebar_quit_button is not None and self.sidebar_quit_button.winfo_exists():
            if compact:
                self.sidebar_quit_button.configure(text="\u00d7", anchor="center", padx=0)
            else:
                self.sidebar_quit_button.configure(text="\u00d7  Cerrar aplicacion", anchor="w", padx=18)

        if self.sidebar_status_label is not None and self.sidebar_status_label.winfo_exists():
            if compact:
                self.sidebar_status_label.pack_forget()
            else:
                if not self.sidebar_status_label.winfo_manager():
                    self.sidebar_status_label.pack(side="left", padx=(6, 0))

        style = ttk.Style(self.root)
        if compact:
            style.configure("TNotebook.Tab", padding=(10, 6), font=("Segoe UI", 9))
        else:
            style.configure("TNotebook.Tab", padding=(16, 9), font=("Segoe UI", 10))

        self._update_connection_mode_badge()
        self._refresh_observability_panel()

    def _add_work_tab_header(self, parent: ttk.Frame, title: str, tab_key: str) -> None:
        header = tk.Frame(parent, bg="#eff6ff", padx=16, pady=10)
        header.grid(row=0, column=0, columnspan=3, sticky="ew", pady=(0, 14))
        tk.Label(header, text=title, font=("Segoe UI Semibold", 12),
                 fg="#1e40af", bg="#eff6ff").pack(side="left")
        ttk.Button(header, text="Cerrar ×", command=lambda: self._close_work_tab(tab_key),
                   style="Ghost.TButton").pack(side="right")

    def open_setup_wizard(self) -> None:
        if not self.docker_ready():
            messagebox.showerror("Docker", self._docker_unavailable_message())
            return

        self.discovered_lan_hosts = self._discover_lan_hosts()

        window = self._open_or_focus_work_tab("setup", "Crear/Recrear")
        if window is None:
            messagebox.showerror("Interfaz", "No se pudo abrir la pestaña de Crear/Recrear.")
            return

        for child in window.winfo_children():
            child.destroy()

        outer = self._create_scrollable_surface(window, padding=(8, 8))
        outer.columnconfigure(1, weight=1)
        self._add_work_tab_header(outer, "Asistente Crear/Recrear entorno Shopify", "setup")

        shopify_name_var = tk.StringVar(value="shopify-dev1")
        network_var = tk.StringVar(value="shopify-network1")
        shopify_volume_var = tk.StringVar(value="shopifydata1")
        dev_port_var = tk.StringVar(value="9292")
        theme_port_var = tk.StringVar(value="3000")
        ssh_port_var = tk.StringVar(value="2222")
        ip_red_var = tk.StringVar(value=self.discovered_lan_hosts[0] if self.discovered_lan_hosts else "")
        store_url_var = tk.StringVar(value="tu-tienda.myshopify.com")
        # shopify_token_var eliminado, no se usará access token
        theme_name_var = tk.StringVar(value="mi-tema")
        store_password_var = tk.StringVar(value="")
        node_image_var = tk.StringVar(value="node:20-alpine")
        auto_pull_var = tk.BooleanVar(value=True)
        status_var = tk.StringVar(value="Completa la configuracion y pulsa Crear/Recrear.")
        progress_var = tk.DoubleVar(value=0)
        stop_event = threading.Event()

        row = 1
        # Asegurarse de que row esté correctamente incrementado antes de usarlo para el checkbox
        row += 1
        ttk.Label(outer, text="Contenedor Shopify CLI:").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Entry(outer, textvariable=shopify_name_var).grid(row=row, column=1, sticky="ew", pady=4)

        row += 1
        ttk.Label(outer, text="Network Docker:").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Entry(outer, textvariable=network_var).grid(row=row, column=1, sticky="ew", pady=4)

        row += 1
        ttk.Label(outer, text="Volumen datos Shopify:").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Entry(outer, textvariable=shopify_volume_var).grid(row=row, column=1, sticky="ew", pady=4)

        row += 1
        ttk.Label(outer, text="Imagen Node.js:").grid(row=row, column=0, sticky="w", pady=4)
        node_combo = ttk.Combobox(outer, textvariable=node_image_var,
            values=["node:20-alpine"], state="readonly")
        node_combo.grid(row=row, column=1, sticky="ew", pady=4)

        ports_frame = ttk.LabelFrame(outer, text="Puertos host")
        row += 1
        ports_frame.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(8, 4))
        for i in range(4):
            ports_frame.columnconfigure(i, weight=1)

        ttk.Label(ports_frame, text="Dev server (Shopify CLI)").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(ports_frame, textvariable=dev_port_var, width=8).grid(row=0, column=1, sticky="w", padx=6)
        ttk.Label(ports_frame, text="Theme Preview").grid(row=0, column=2, sticky="w", padx=6)
        ttk.Entry(ports_frame, textvariable=theme_port_var, width=8).grid(row=0, column=3, sticky="w", padx=6)
        ttk.Label(ports_frame, text="SSH (Remote-SSH de VS Code)").grid(row=1, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(ports_frame, textvariable=ssh_port_var, width=8).grid(row=1, column=1, sticky="w", padx=6)
        ttk.Label(ports_frame, text="Puerto SSH para conectar VS Code\ndirectamente al contenedor", foreground="#555").grid(row=1, column=2, columnspan=2, sticky="w", padx=6)

        row += 1
        ttk.Label(outer, text="IP en red local:").grid(row=row, column=0, sticky="w", pady=4)
        ip_combo = ttk.Combobox(
            outer,
            textvariable=ip_red_var,
            values=self.discovered_lan_hosts,
            state="readonly" if self.discovered_lan_hosts else "normal",
        )
        ip_combo.grid(row=row, column=1, sticky="ew", pady=4)

        shopify_frame = ttk.LabelFrame(outer, text="Configuracion Shopify")
        row += 1
        shopify_frame.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(8, 4))
        shopify_frame.columnconfigure(1, weight=1)
        shopify_frame.columnconfigure(3, weight=1)

        ttk.Label(shopify_frame, text="URL de la tienda").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(shopify_frame, textvariable=store_url_var).grid(row=0, column=1, columnspan=3, sticky="ew", padx=6)
        ttk.Label(shopify_frame, text="Nombre del tema").grid(row=1, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(shopify_frame, textvariable=theme_name_var).grid(row=1, column=1, sticky="ew", padx=6)

        ttk.Label(shopify_frame, text="Contrasena de la tienda").grid(row=2, column=0, sticky="w", padx=6, pady=6)
        self._add_password_entry_with_toggle(shopify_frame, store_password_var, row=2, column=1, padx=6)
        def _open_password_help() -> None:
            import webbrowser
            import urllib.parse as _up
            url = f"https://{store_url_var.get().strip()}/admin/online_store/preferences"
            webbrowser.open(url)
        ttk.Button(
            shopify_frame, text="? Donde esta",
            command=_open_password_help,
            style="Ghost.TButton",
        ).grid(row=2, column=2, padx=6, pady=6)
        ttk.Label(
            shopify_frame,
            text="Admin Shopify > Configuracion > Tienda online > Preferencias > Proteccion con contrasena",
            style="Muted.TLabel",
            wraplength=420,
        ).grid(row=3, column=0, columnspan=4, sticky="w", padx=6, pady=(0, 6))

        info_frame = ttk.LabelFrame(outer, text="Informacion")
        row += 1
        info_frame.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(8, 4))
        ttk.Label(
            info_frame,
            text=(
                "Se creara un contenedor Docker con Node.js y Shopify CLI listo para desarrollo.\n"
                "La URL de la tienda tiene el formato: mi-tienda.myshopify.com (sin https://)."
            ),
            wraplength=560,
            justify="left",
            style="Muted.TLabel",
        ).pack(padx=8, pady=6, anchor="w")

        row += 1
        ttk.Separator(outer, orient="horizontal").grid(row=row, column=0, columnspan=2, sticky="ew", pady=(8, 10))

        row += 1
        progress_panel, _ = self._build_progress_panel(
            outer,
            "Progreso de creación",
            "El asistente prepara imagen, volumen, red y el contenedor principal por fases.",
            status_var,
            progress_var,
        )
        progress_panel.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(8, 0))

        row += 1
        actions = ttk.Frame(outer)
        actions.grid(row=row, column=0, columnspan=2, sticky="e", pady=(12, 0))
        cancel_button = ttk.Button(actions, text="Cancelar", command=window.destroy)
        cancel_button.pack(side="right")
        stop_button = ttk.Button(
            actions,
            text="Detener",
            state="disabled",
            command=lambda: self._request_import_cancel(status_var, stop_event, stop_button),
        )
        stop_button.pack(side="right", padx=(0, 8))
        run_button = ttk.Button(
            actions,
            text="Crear/Recrear ahora",
            command=lambda: self._run_setup_from_wizard(
                window=window,
                status_var=status_var,
                progress_var=progress_var,
                run_button=run_button,
                cancel_button=cancel_button,
                stop_button=stop_button,
                stop_event=stop_event,
                shopify_container=shopify_name_var.get().strip(),
                network_name=network_var.get().strip(),
                shopify_volume=shopify_volume_var.get().strip(),
                dev_port=dev_port_var.get().strip(),
                theme_port=theme_port_var.get().strip(),
                ssh_port=ssh_port_var.get().strip(),
                ip_red=ip_red_var.get().strip(),
                store_url=store_url_var.get().strip(),
                # shopify_token eliminado
                theme_name=theme_name_var.get().strip(),
                node_image=node_image_var.get().strip(),
                store_password=store_password_var.get(),
                auto_pull=auto_pull_var.get(),
            ),
        )
        run_button.pack(side="right", padx=(0, 8))

        def refresh_ports_validation(*_args: object) -> None:
            try:
                dp = int(dev_port_var.get())
                tp = int(theme_port_var.get())
                sp = int(ssh_port_var.get())
                if len({dp, tp, sp}) != 3:
                    run_button.configure(state="disabled")
                    status_var.set("Los puertos no pueden coincidir.")
                    return
                if not (1 <= dp <= 65535 and 1 <= tp <= 65535 and 1 <= sp <= 65535):
                    run_button.configure(state="disabled")
                    status_var.set("Puertos fuera de rango (1-65535).")
                    return
                if not ip_red_var.get().strip():
                    run_button.configure(state="disabled")
                    status_var.set("Selecciona una IP en red local.")
                    return
                run_button.configure(state="normal")
                status_var.set("Completa la configuracion y pulsa Crear/Recrear.")
            except ValueError:
                run_button.configure(state="disabled")
                status_var.set("Los puertos deben ser numeros enteros.")

        for var in (dev_port_var, theme_port_var, ssh_port_var):
            var.trace_add("write", refresh_ports_validation)
        ip_red_var.trace_add("write", refresh_ports_validation)
        refresh_ports_validation()

    def _run_setup_from_wizard(
        self,
        window: tk.Toplevel,
        status_var: tk.StringVar,
        progress_var: tk.DoubleVar,
        run_button: ttk.Button,
        cancel_button: ttk.Button,
        stop_button: ttk.Button,
        stop_event: threading.Event,
        shopify_container: str,
        network_name: str,
        shopify_volume: str,
        dev_port: str,
        theme_port: str,
        ssh_port: str,
        ip_red: str,
        store_url: str,
        theme_name: str,
        node_image: str,
        store_password: str,
        auto_pull: bool,
    ) -> None:
        shopify_container = self._normalize_docker_resource_name(shopify_container, "shopify-dev1")
        network_name = self._normalize_docker_resource_name(network_name, "shopify-network1")
        shopify_volume = self._normalize_docker_resource_name(shopify_volume, "shopifydata1")

        required_fields = [
            (shopify_container, "Contenedor Shopify CLI"),
            (network_name, "Network Docker"),
            (shopify_volume, "Volumen datos Shopify"),
            (dev_port, "Puerto Dev server"),
            (theme_port, "Puerto Theme Preview"),
            (ssh_port, "Puerto SSH"),
            (store_url, "URL de la tienda"),
            (node_image, "Imagen Node.js"),
        ]
        for value, label in required_fields:
            if not value:
                messagebox.showwarning("Crear/Recrear", f"El campo '{label}' es obligatorio.")
                return

        if node_image != "node:20-alpine":
            messagebox.showwarning("Crear/Recrear", "La imagen Node.js debe ser node:20-alpine.")
            return

        try:
            dev_port_i = int(dev_port)
            theme_port_i = int(theme_port)
            ssh_port_i = int(ssh_port)
        except ValueError:
            messagebox.showwarning("Crear/Recrear", "Los puertos deben ser numeros enteros.")
            return

        ports = [dev_port_i, theme_port_i, ssh_port_i]
        if len(set(ports)) != len(ports):
            messagebox.showwarning("Crear/Recrear", "Los puertos no pueden repetirse.")
            return

        if self.docker_mode == "remote":
            remote_published = self._get_running_docker_published_ports()
            busy_ports = [p for p in ports if p in remote_published]
        else:
            busy_ports = [p for p in ports if not self._is_host_port_available(p)]
        if busy_ports:
            messagebox.showwarning(
                "Crear/Recrear",
                (
                    "Estos puertos ya estan publicados en el daemon remoto: "
                    if self.docker_mode == "remote"
                    else "Estos puertos estan en uso en el host: "
                )
                + ", ".join(str(p) for p in busy_ports),
            )
            return

        existing_containers = [
            cname
            for cname in (shopify_container,)
            if self._container_exists(cname)
        ]

        recreate_existing = True
        if existing_containers:
            existing_text = ", ".join(existing_containers)
            delete_existing = messagebox.askyesno(
                "Crear/Recrear",
                (
                    "Se detectaron contenedores ya creados:\n\n"
                    f"{existing_text}\n\n"
                    "Quieres borrarlos para crearlos de nuevo?"
                ),
            )
            if not delete_existing:
                continue_without_delete = messagebox.askyesno(
                    "Crear/Recrear",
                    (
                        "No se borraran los contenedores existentes.\n\n"
                        "Quieres avanzar con el resto del proceso?\n\n"
                        "Si eliges No, se cancelara la operacion."
                    ),
                )
                if not continue_without_delete:
                    return
                recreate_existing = False

        if recreate_existing:
            warning = (
                "Esta accion destruira el entorno anterior con los mismos nombres.\n\n"
                f"Contenedor: {shopify_container}\n"
                f"Volumen: {shopify_volume}\n"
                f"Network: {network_name}\n\n"
                "Deseas continuar?"
            )
        else:
            warning = (
                "Se conservaran los contenedores existentes y se intentara crear lo que falte.\n\n"
                f"Contenedor objetivo: {shopify_container}\n"
                f"Volumen objetivo: {shopify_volume}\n"
                f"Network objetivo: {network_name}\n\n"
                "Deseas continuar?"
            )
        if not messagebox.askyesno("Confirmar Crear/Recrear", warning):
            return

        run_button.configure(state="disabled")
        cancel_button.configure(state="disabled")
        stop_button.configure(state="normal")
        stop_event.clear()
        progress_var.set(0)
        status_var.set("Iniciando creacion del entorno Shopify...")

        raw_theme_name = (theme_name or "").strip()
        theme_name_for_cli = raw_theme_name or "Horizon"
        theme_dir_name = re.sub(r"[^a-z0-9._-]+", "-", theme_name_for_cli.lower()).strip("-._")
        if not theme_dir_name:
            theme_dir_name = "horizon"
        theme_dir = f"/app/{theme_dir_name}"

        events: queue.Queue[tuple[str, object]] = queue.Queue()
        worker = threading.Thread(
            target=self._run_setup_worker,
            args=(
                events,
                stop_event,
                shopify_container,
                network_name,
                shopify_volume,
                dev_port_i,
                theme_port_i,
                ssh_port_i,
                ip_red,
                store_url,
                # shopify_token eliminado
                theme_name,
                node_image,
                recreate_existing,
                auto_pull,
                store_password,
            ),
            daemon=True,
        )
        worker.start()

        self._poll_setup_worker_queue(
            window=window,
            status_var=status_var,
            progress_var=progress_var,
            run_button=run_button,
            cancel_button=cancel_button,
            stop_button=stop_button,
            events=events,
            shopify_container=shopify_container,
            dev_port=dev_port_i,
            theme_port=theme_port_i,
            ssh_port=ssh_port_i,
            ip_red=ip_red,
            store_url=store_url,
            theme_dir=theme_dir,
            theme_name=theme_name,
        )

    def _run_setup_worker(
        self,
        events: queue.Queue[tuple[str, object]],
        stop_event: threading.Event,
        shopify_container: str,
        network_name: str,
        shopify_volume: str,
        dev_port: int,
        theme_port: int,
        ssh_port: int,
        ip_red: str,
        store_url: str,
        # shopify_token eliminado
        theme_name: str,
        node_image: str,
        recreate_existing: bool,
        auto_pull: bool = True,
        store_password: str = "",
    ) -> None:
        try:
            raw_theme_name = (theme_name or "").strip()
            theme_name_for_cli = raw_theme_name or "Horizon"
            theme_dir_name = re.sub(r"[^a-z0-9._-]+", "-", theme_name_for_cli.lower()).strip("-._")
            if not theme_dir_name:
                theme_dir_name = "horizon"
            theme_dir = f"/app/{theme_dir_name}"

            def check_cancel() -> None:
                if stop_event.is_set():
                    raise RuntimeError("SETUP_CANCELLED_BY_USER")

            def run_checked(args: list[str], error_message: str) -> None:
                code, _out, err = self._run(args)
                if code != 0:
                    raise RuntimeError(err or error_message)

            if recreate_existing:
                events.put(("progress", (8.0, "[1/5] Limpiando instalacion anterior...")))
                if self._container_exists(shopify_container):
                    run_checked(["docker", "rm", "-f", shopify_container],
                                f"No se pudo eliminar contenedor {shopify_container}")
                self._run(["docker", "volume", "rm", shopify_volume])
                self._run(["docker", "network", "rm", network_name])
            else:
                events.put(("progress", (8.0, "[1/5] Revisando recursos existentes (sin borrar)...")))

            check_cancel()
            events.put(("progress", (22.0, "[2/5] Creando network y volumen...")))
            if recreate_existing or not self._network_exists(network_name):
                run_checked(["docker", "network", "create", network_name], "No se pudo crear la network")
            if recreate_existing or not self._volume_exists(shopify_volume):
                run_checked(["docker", "volume", "create", shopify_volume], "No se pudo crear volumen Shopify")

            check_cancel()
            events.put(("progress", (30.0, "[3/5] Comprobando imagen Docker...")))

            # Comprobar si la imagen ya existe localmente
            code_img, out_img, _ = self._run(["docker", "image", "inspect", node_image, "--format", "{{.Id}}"])
            image_present = (code_img == 0 and out_img.strip() != "")

            if not image_present:
                events.put(("progress", (32.0, f"[3/5] Descargando imagen {node_image} (puede tardar varios minutos)...")))

                # docker pull en hilo separado para poder reportar progreso animado
                pull_result: list[tuple[int, str, str]] = []
                pull_done = threading.Event()

                def _do_pull() -> None:
                    r = self._run(["docker", "pull", node_image])
                    pull_result.append(r)
                    pull_done.set()

                pull_thread = threading.Thread(target=_do_pull, daemon=True)
                pull_thread.start()

                pull_tick = 0
                while not pull_done.is_set():
                    if stop_event.is_set():
                        raise RuntimeError("SETUP_CANCELLED_BY_USER")
                    pull_done.wait(timeout=2.0)
                    pull_tick += 1
                    # Progreso animado entre 32% y 58% mientras descarga
                    animated_pct = 32.0 + min(26.0, pull_tick * 0.8)
                    events.put(("progress", (animated_pct, f"[3/5] Descargando imagen {node_image}... ({pull_tick * 2}s)")))

                pull_code, _, pull_err = pull_result[0] if pull_result else (1, "", "Pull no completado")
                if pull_code != 0:
                    raise RuntimeError(
                        f"No se pudo descargar la imagen '{node_image}'.\n\n"
                        f"{pull_err or 'Comprueba el nombre de la imagen y tu conexion a internet.'}"
                    )
                events.put(("progress", (60.0, f"[3/5] Imagen {node_image} descargada correctamente.")))
            else:
                events.put(("progress", (60.0, f"[3/5] Imagen {node_image} ya disponible localmente.")))

            check_cancel()
            events.put(("progress", (62.0, "[3/5] Arrancando contenedor Node.js con Shopify CLI...")))
            if recreate_existing or not self._container_exists(shopify_container):
                # No usar access token nunca

                _store = store_url or "tu-tienda.myshopify.com"

                # Generar el entrypoint.sh como string Python limpio
                # Se copiara al contenedor via docker cp para evitar problemas
                # de escapado de comillas con printf/sh -c
                entrypoint_lines = [
                    "#!/bin/sh",
                    "# Entrypoint generado por Shopify Utilidades",
                    "unset SHOPIFY_CLI_THEME_TOKEN",
                    "unset SHOPIFY_ACCESS_TOKEN",
                    "unset SHOPIFY_FLAG_STORE",
                    "unset SHOPIFY_THEME_NAME",
                    f"STORE={_store}",
                    f"STORE_PASSWORD={store_password}",
                    f"THEME_NAME={theme_name_for_cli}",
                    f"THEME_DIR={theme_dir}",
                    "FLAG_INSTALLED=/app/.shopify_cli_installed",
                    "",
                    "# Instalacion (solo primera vez)",
                    "if [ ! -f \"$FLAG_INSTALLED\" ]; then",
                    "  echo '[entrypoint] Primera vez: instalando dependencias...'",
                    "  apk add --no-cache xdg-utils git curl libstdc++ openssh openssh-server 2>/dev/null || true",
                    "  # Configurar SSH server",
                    "  echo '[entrypoint] Configurando openssh-server...'",
                    "  ssh-keygen -A 2>/dev/null || true",
                    "  mkdir -p /root/.ssh && chmod 700 /root/.ssh",
                    "  touch /root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys",
                    "  # Permitir login root por clave (sin contraseña)",
                    "  sed -i 's/#PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config 2>/dev/null || true",
                    "  sed -i 's/#PubkeyAuthentication.*/PubkeyAuthentication yes/' /etc/ssh/sshd_config 2>/dev/null || true",
                    "  sed -i 's/#PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config 2>/dev/null || true",
                    "  sed -i 's/.*AllowTcpForwarding.*/AllowTcpForwarding yes/' /etc/ssh/sshd_config 2>/dev/null || true",
                    "  echo 'PermitRootLogin yes' >> /etc/ssh/sshd_config",
                    "  echo 'PubkeyAuthentication yes' >> /etc/ssh/sshd_config",
                    "  echo 'PasswordAuthentication no' >> /etc/ssh/sshd_config",
                    "  echo 'AllowTcpForwarding yes' >> /etc/ssh/sshd_config",
                    "  npm install -g --unsafe-perm @shopify/cli@latest",
                    "  mkdir -p \"$THEME_DIR\"",
                    "  touch \"$FLAG_INSTALLED\"",
                    "  echo '[entrypoint] Instalacion completada.'",
                    "fi",
                    "",
                    "# Arrancar sshd en segundo plano (puerto 22 interno)",
                    "if command -v sshd >/dev/null 2>&1; then",
                    "  echo '[entrypoint] Arrancando sshd...'",
                    "  /usr/sbin/sshd 2>/dev/null || sshd 2>/dev/null || true",
                    "fi",
                    "",
                    "# Descarga automatica del tema si no existe todavia",
                    "LIQUID_COUNT=$(find \"$THEME_DIR\" -name '*.liquid' 2>/dev/null | wc -l)",
                    "if [ \"$LIQUID_COUNT\" -eq 0 ] && [ -f /tmp/shopify_auth_ok ]; then",
                    "  echo '[entrypoint] Descargando tema' \"$THEME_NAME\" '...'",
                    "  cd \"$THEME_DIR\"",
                    "  shopify theme pull --store \"$STORE\" --theme \"$THEME_NAME\" --force 2>/dev/null || \\",
                    "  shopify theme pull --store \"$STORE\" --force 2>/dev/null || true",
                    "  echo '[entrypoint] Descarga completada.'",
                    "fi",
                    "",
                    "# Arranque del dev server en bucle",
                    "while true; do",
                    "  LIQUID_COUNT=$(find \"$THEME_DIR\" -name '*.liquid' 2>/dev/null | wc -l)",
                    "  if [ \"$LIQUID_COUNT\" -gt 0 ]; then",
                    "    echo '[entrypoint] Tema encontrado:' \"$THEME_NAME\" '- Arrancando dev server...'",
                    "    cd \"$THEME_DIR\"",
                    "    shopify theme dev --store \"$STORE\" --host 0.0.0.0 --store-password \"$STORE_PASSWORD\" || true",
                    "    echo '[entrypoint] Dev server detenido. Reiniciando en 5s...'",
                    "    sleep 5",
                    "  else",
                    "    echo '[entrypoint] Esperando tema' \"$THEME_NAME\" 'en' \"$THEME_DIR\"",
                    "    sleep 10",
                    "  fi",
                    "done",
                ]
                entrypoint_content = "\n".join(entrypoint_lines) + "\n"

                # Escribir el entrypoint en un archivo temporal local
                # y copiarlo al contenedor via docker cp (sin problemas de escapado)
                import tempfile as _tmpmod
                _fd, _tmp_ep = _tmpmod.mkstemp(prefix="shu_ep_", suffix=".sh")
                try:
                    with os.fdopen(_fd, "w", encoding="utf-8", newline="\n") as _fh:
                        _fh.write(entrypoint_content)
                except Exception:
                    try:
                        os.close(_fd)
                    except OSError:
                        pass
                    _tmp_ep = ""

                # El contenedor arranca con un loop simple que espera
                # al entrypoint.sh real (que se copiara justo despues via docker cp)
                startup_script = (
                    f"mkdir -p {theme_dir} && "
                    "while [ ! -f /app/entrypoint.sh ]; do sleep 1; done && "
                    "chmod +x /app/entrypoint.sh && "
                    "sh /app/entrypoint.sh"
                )

                # Solo pasar variables de entorno compatibles con el CLI (solo shpat_)
                env_args: list[str] = ["-e", "NODE_ENV=development"]
                if store_url:
                    env_args += ["-e", f"SHOPIFY_STORE={store_url}"]
                # Nunca pasar access token como variable de entorno (eliminado)
                # Si existía código que añadía -e SHOPIFY_CLI_THEME_TOKEN o -e SHOPIFY_ACCESS_TOKEN, ya no se añade nada
                if theme_name:
                    env_args += ["-e", f"SHOPIFY_THEME_NAME={theme_name}"]

                run_cmd = (
                    ["docker", "run", "-d",
                     "--name", shopify_container,
                     "--network", network_name,
                     "--restart", "unless-stopped",  # arranca solo al encender Docker
                     "-v", f"{shopify_volume}:/app",
                     "-w", "/app",
                     "-p", f"{dev_port}:9292",
                     "-p", f"{theme_port}:3000",
                     "-p", f"{ssh_port}:22",            # SSH: Remote-SSH de VS Code
                    ] + env_args + [
                     node_image,
                     "sh", "-c", startup_script,
                    ]
                )
                run_checked(run_cmd, "No se pudo iniciar contenedor Shopify")

                # Copiar el entrypoint.sh al contenedor via docker cp
                if _tmp_ep and os.path.exists(_tmp_ep):
                    try:
                        cp_code, _, cp_err = self._run([
                            "docker", "cp", _tmp_ep,
                            f"{shopify_container}:/app/entrypoint.sh"
                        ])
                        if cp_code != 0:
                            events.put(("debug", f"Aviso: no se pudo copiar entrypoint.sh: {cp_err}"))
                    finally:
                        try:
                            os.remove(_tmp_ep)
                        except OSError:
                            pass

            check_cancel()
            def _check_in_container(check_cmd: str) -> bool:
                code_c, _out_c, _err_c = self._run([
                    "docker", "exec", shopify_container, "sh", "-c", check_cmd
                ])
                return code_c == 0

            def _entrypoint_waiting_theme() -> bool:
                code_l, out_l, err_l = self._run(["docker", "logs", "--tail", "200", shopify_container])
                if code_l != 0 and not (out_l or "").strip() and not (err_l or "").strip():
                    return False
                merged = "\n".join([out_l or "", err_l or ""])
                return "[entrypoint] Esperando tema" in merged

            def _install_step(
                label: str,
                check_cmd: str,
                install_cmd: str,
                pct_start: float,
                pct_end: float,
                max_wait_seconds: int = 7200,  # 2 horas por defecto
            ) -> None:
                check_cancel()
                events.put(("progress", (pct_start, f"[4/5] Verificando {label}...")))
                if _check_in_container(check_cmd):
                    events.put(("progress", (pct_end, f"[4/5] {label} ya disponible.")))
                    return
                if label == "Shopify CLI" and _entrypoint_waiting_theme():
                    events.put(("debug", "[READY] entrypoint ya esta esperando tema; Shopify CLI se considera operativo."))
                    events.put(("progress", (pct_end, f"[4/5] {label} operativo (entrypoint).")))
                    return

                events.put(("progress", (pct_start + 0.8, f"[4/5] Instalando {label}...")))
                code_i, out_i, err_i = self._run([
                    "docker", "exec", shopify_container, "sh", "-c", install_cmd
                ])
                if code_i != 0:
                    events.put(("debug", f"Aviso durante instalación de {label}: {err_i or out_i or 'sin detalle'}"))

                # Esperar a que se complete sin timeout fijo - verificar continuamente
                attempt = 0
                pct_step = (pct_end - pct_start) * 0.85
                while True:
                    check_cancel()
                    time.sleep(2)
                    if _check_in_container(check_cmd):
                        events.put(("progress", (pct_end, f"[4/5] {label} instalado correctamente.")))
                        return
                    if label == "Shopify CLI" and _entrypoint_waiting_theme():
                        events.put(("debug", "[READY] entrypoint ya esta esperando tema; se omite verificacion extra de Shopify CLI."))
                        events.put(("progress", (pct_end, f"[4/5] {label} operativo (entrypoint).")))
                        return
                    
                    attempt += 1
                    # Progreso visual animado
                    animated_pct = pct_start + min(pct_step, attempt * 0.5)
                    events.put(("progress", (animated_pct, f"[4/5] Verificando {label}... ({attempt * 2}s)")))
                    
                    # Máximo configurable
                    if attempt * 2 > max_wait_seconds:
                        events.put(("debug", f"Timeout esperando {label} ({max_wait_seconds}s)"))
                        return

            _install_step(
                label="Shopify CLI",
                check_cmd="command -v shopify >/dev/null 2>&1 || [ -x /usr/local/bin/shopify ] || [ -x /usr/bin/shopify ]",
                install_cmd="command -v shopify >/dev/null 2>&1 || npm install -g --unsafe-perm @shopify/cli@latest",
                pct_start=75.0,
                pct_end=80.0,
            )
            _install_step(
                label="OpenSSH Server",
                check_cmd="command -v sshd >/dev/null 2>&1 || [ -x /usr/sbin/sshd ] || [ -x /usr/bin/sshd ]",
                install_cmd=(
                    "if command -v sshd >/dev/null 2>&1; then exit 0; fi; "
                    "if command -v apk >/dev/null 2>&1; then apk add --no-cache openssh openssh-server; "
                    "elif command -v apt-get >/dev/null 2>&1; then apt-get update -qq && apt-get install -y -qq openssh-server; "
                    "elif command -v yum >/dev/null 2>&1; then yum install -y -q openssh-server; fi"
                ),
                pct_start=80.0,
                pct_end=84.0,
            )

            check_cancel()
            events.put(("progress", (84.0, "[4/5] Arrancando subproceso sshd...")))
            self._run([
                "docker", "exec", shopify_container, "sh", "-c",
                "(command -v ssh-keygen >/dev/null 2>&1 && ssh-keygen -A >/dev/null 2>&1 || true); "
                "(mkdir -p /run/sshd >/dev/null 2>&1 || true); "
                "(/usr/sbin/sshd >/dev/null 2>&1 || sshd >/dev/null 2>&1 || true)"
            ])
            events.put(("debug", "[STARTUP] sshd iniciado, verificando..."))
            # Esperar a que sshd esté realmente corriendo (máximo 30 segundos)
            sshd_ready = False
            for attempt in range(30):
                check_cancel()
                time.sleep(1)
                # Verificar: ¿existe el archivo pid o el proceso está vivo?
                check_code, _, _ = self._run([
                    "docker", "exec", shopify_container, "sh", "-c",
                    "[ -f /var/run/sshd.pid ] 2>/dev/null && kill -0 $(cat /var/run/sshd.pid) 2>/dev/null"
                ])
                
                if check_code == 0:
                    events.put(("debug", f"[READY] sshd está corriendo correctamente"))
                    sshd_ready = True
                    break
                
                if attempt % 5 == 0 and attempt > 0:
                    events.put(("debug", f"[WAITING] Esperando sshd: {attempt}s..."))
            
            if sshd_ready:
                events.put(("progress", (87.0, "[4/5] sshd en ejecución.")))
            else:
                events.put(("debug", f"[NOTICE] sshd puede no estar completamente listo, continuando"))
                events.put(("progress", (87.0, "[4/5] sshd en ejecución (parcial).")))

            check_cancel()
            events.put(("progress", (94.0, "[5/5] Configurando entorno y verificando Shopify CLI...")))
            time.sleep(1.0)

            # --- Paso 6 (opcional): Descarga automatica del tema ---
            if auto_pull and store_url:
                check_cancel()
                events.put(("progress", (96.0, "[6/6] Descargando tema desde Shopify...")))

                # Primero necesitamos autenticar. Lanzamos auth login dentro del contenedor
                # y leemos los logs para capturar el codigo y URL de verificacion
                events.put(("debug", "Iniciando autenticacion Shopify para descarga del tema..."))

                # Lanzar auth en background y capturar su output via docker logs
                auth_cmd = (
                    "unset SHOPIFY_CLI_THEME_TOKEN SHOPIFY_ACCESS_TOKEN SHOPIFY_FLAG_STORE SHOPIFY_THEME_NAME; "  # Mantener unset por limpieza, pero nunca se setean
                    "shopify auth login > /tmp/shopify_auth.log 2>&1 &"
                )
                self._run(["docker", "exec", shopify_container, "sh", "-c", auth_cmd])

                # Esperar hasta que aparezca el codigo de verificacion en el log (sin timeout fijo)
                auth_url = ""
                auth_code = ""
                attempt = 0
                while not auth_url:
                    if stop_event.is_set():
                        raise RuntimeError("SETUP_CANCELLED_BY_USER")
                    time.sleep(1.0)
                    code_r, log_out, _ = self._run([
                        "docker", "exec", shopify_container,
                        "sh", "-c", "cat /tmp/shopify_auth.log 2>/dev/null"
                    ])
                    if "activate-with-code" in (log_out or ""):
                        # Extraer URL y codigo
                        import re as _re
                        url_m = _re.search(r'(https://accounts\.shopify\.com/activate-with-code[^\s]+)', log_out)
                        code_m = _re.search(r'verification code:\s*([A-Z0-9]{4}-[A-Z0-9]{4})', log_out)
                        if url_m:
                            auth_url = url_m.group(1)
                        if code_m:
                            auth_code = code_m.group(1)
                        break
                    
                    attempt += 1
                    if attempt % 30 == 0:
                        events.put(("debug", f"Esperando código de auth: {attempt}s"))
                    
                    # Máximo 5 minutos
                    if attempt > 300:
                        events.put(("debug", "Timeout esperando código de auth"))
                        break

                if auth_url:
                    events.put(("debug", f"Codigo: {auth_code}"))
                    events.put(("debug", f"URL: {auth_url}"))
                    auth_ack_event_setup = threading.Event()
                    events.put(("auth_required", (auth_code, auth_url, auth_ack_event_setup)))

                    # Esperar a que el usuario vea el diálogo antes de empezar a sondear login
                    auth_ack_event_setup.wait(timeout=30.0)

                    # Esperar hasta que el login complete (sin timeout fijo)
                    logged_in = False
                    attempt = 0
                    while not logged_in:
                        if stop_event.is_set():
                            raise RuntimeError("SETUP_CANCELLED_BY_USER")
                        time.sleep(1.0)
                        _, log_out2, _ = self._run([
                            "docker", "exec", shopify_container,
                            "sh", "-c", "cat /tmp/shopify_auth.log 2>/dev/null"
                        ])
                        if any(m in (log_out2 or "") for m in ("Logged in", "logged in", "authenticated", "✔")):
                            logged_in = True
                            break
                        
                        attempt += 1
                        if attempt % 30 == 0:
                            events.put(("debug", f"Esperando login: {attempt}s"))
                        
                        # Máximo 10 minutos
                        if attempt > 600:
                            events.put(("debug", "Timeout esperando login (10 min)"))
                            break

                    if logged_in:
                        events.put(("debug", "Autenticacion completada. Descargando tema..."))
                        events.put(("progress", (97.0, f"[6/6] Autenticado. Descargando tema '{theme_name}'...")))

                        # Marcar auth como completada para que el entrypoint lo sepa
                        self._run(["docker", "exec", shopify_container, "sh", "-c", "touch /tmp/shopify_auth_ok"])

                        # Ahora hacer shopify theme pull usando el nombre de tema especificado
                        _theme_flag = f"--theme \"{theme_name}\"" if theme_name else ""
                        pull_cmd = (
                            "unset SHOPIFY_CLI_THEME_TOKEN SHOPIFY_ACCESS_TOKEN SHOPIFY_FLAG_STORE SHOPIFY_THEME_NAME; "  # Mantener unset por limpieza
                            f"cd {theme_dir} && shopify theme pull --store {store_url} "
                            f"{_theme_flag} --force > /tmp/shopify_pull.log 2>&1"
                        )
                        pull_tick = 0
                        pull_done_evt = threading.Event()
                        pull_result_ref: list[tuple[int, str, str]] = []

                        def _do_pull_auto() -> None:
                            r = self._run(["docker", "exec", shopify_container, "sh", "-c", pull_cmd])
                            pull_result_ref.append(r)
                            pull_done_evt.set()

                        threading.Thread(target=_do_pull_auto, daemon=True).start()

                        while not pull_done_evt.is_set():
                            if stop_event.is_set():
                                raise RuntimeError("SETUP_CANCELLED_BY_USER")
                            pull_done_evt.wait(timeout=2.0)
                            pull_tick += 1
                            pct = 97.0 + min(2.0, pull_tick * 0.1)
                            events.put(("progress", (pct, f"[6/6] Descargando tema... ({pull_tick * 2}s)")))

                        pull_code, _, pull_err = pull_result_ref[0] if pull_result_ref else (1, "", "")
                        if pull_code == 0:
                            events.put(("debug", f"Tema descargado correctamente en {theme_dir}"))
                        else:
                            events.put(("debug", f"Aviso: pull retorno codigo {pull_code}: {pull_err}"))
                    else:
                        events.put(("debug", "Tiempo de espera de autenticacion agotado. Descarga manual necesaria."))
                else:
                    events.put(("debug", "No se pudo obtener URL de autenticacion. Descarga manual necesaria."))

            events.put(("done", None))
        except Exception as exc:
            events.put(("debug", f"ERROR en worker: {exc}"))
            events.put(("error", str(exc)))

    def _poll_setup_worker_queue(
        self,
        window: tk.Toplevel,
        status_var: tk.StringVar,
        progress_var: tk.DoubleVar,
        run_button: ttk.Button,
        cancel_button: ttk.Button,
        stop_button: ttk.Button,
        events: queue.Queue[tuple[str, object]],
        shopify_container: str,
        dev_port: int,
        theme_port: int,
        ssh_port: int,
        ip_red: str,
        store_url: str,
        theme_dir: str,
        theme_name: str,
    ) -> None:
        if not window.winfo_exists():
            return

        completed = False
        failed: str | None = None

        while True:
            try:
                kind, payload = events.get_nowait()
            except queue.Empty:
                break

            if kind == "progress":
                value, text = payload  # type: ignore[misc]
                progress_var.set(float(value))
                status_var.set(str(text))
            elif kind == "auth_required":
                auth_code, auth_url, auth_ack_event = payload  # type: ignore[misc]
                status_var.set(f"Abre el navegador y confirma el codigo: {auth_code}")
                self._show_shopify_auth_dialog(auth_code, auth_url, auth_ack_event)
            elif kind == "done":
                completed = True
            elif kind == "error":
                failed = str(payload)

        if completed:
            stop_button.configure(state="disabled")
            progress_var.set(100)
            status_var.set("Entorno Shopify creado correctamente.")
            self.log_event("SETUP", shopify_container, "OK", "Entorno Shopify recreado desde asistente GUI")
            self.refresh_everything()
            access_host = self._access_host_for_urls()
            messagebox.showinfo(
                "Crear/Recrear",
                (
                    "Entorno Shopify creado correctamente.\n\n"
                    f"Dev server:      http://{access_host}:{dev_port}\n"
                    f"Dev server red:  http://{ip_red}:{dev_port}\n"
                    f"SSH (VS Code):   {access_host}:{ssh_port}  ← Remote-SSH\n"
                    f"Tienda: {store_url}\n"
                    f"Contenedor: {shopify_container}\n\n"
                    "El contenedor arranca SOLO al encender Docker.\n\n"
                    "Para editar el código desde VS Code:\n"
                    "  → Pulsa 'Acceso Remoto (SSH)'.\n\n"
                    "UNICA VEZ — descargar el tema (si no lo has hecho):\n"
                    f"  docker exec -it {shopify_container} sh\n"
                    f"  cd {theme_dir}\n"
                    f"  shopify theme pull --store {store_url}\n\n"
                    "Tras descargar el tema, el dev server\n"
                    "arrancara automaticamente en cada reinicio."
                ),
            )
            
            # Mostrar diálogo SSH Remote-SSH (sin crear workspace local)
            self._show_vscode_ssh_setup_dialog(
                shopify_container=shopify_container,
                ssh_port=ssh_port,
                ws_path="",
            )
            ask_import = messagebox.askyesno("Crear/Recrear", "Deseas importar un tema/backup ahora?")
            self._close_work_tab("setup")
            if ask_import:
                self.open_import_wizard()
            
            # Iniciar monitoreo automático DESPUÉS de cerrar todos los diálogos
            if store_url:
                def _start_auto_auth_delayed() -> None:
                    # Esperar 3 segundos para que la UI se haya actualizado completamente
                    time.sleep(3)
                    theme_name_for_display = (theme_name or "").strip() or "Horizon"
                    self._auto_detect_and_auth_waiting_theme(
                        shopify_container, store_url, theme_name_for_display, theme_dir
                    )
                
                threading.Thread(target=_start_auto_auth_delayed, daemon=True).start()
            
            return

        if failed is not None:
            stop_button.configure(state="disabled")
            run_button.configure(state="normal")
            cancel_button.configure(state="normal")
            if failed == "SETUP_CANCELLED_BY_USER":
                status_var.set("Operacion cancelada por el usuario.")
                messagebox.showinfo("Crear/Recrear", "Operacion cancelada por el usuario.")
                return
            self.log_event("SETUP", shopify_container or "global", "ERROR", failed)
            self.refresh_history()
            status_var.set(f"Error: {failed}")
            messagebox.showerror("Crear/Recrear", f"No se pudo completar el entorno Shopify.\n\n{failed}")
            return

        window.after(
            150,
            lambda: self._poll_setup_worker_queue(
                window=window,
                status_var=status_var,
                progress_var=progress_var,
                run_button=run_button,
                cancel_button=cancel_button,
                stop_button=stop_button,
                events=events,
                shopify_container=shopify_container,
                dev_port=dev_port,
                theme_port=theme_port,
                ssh_port=ssh_port,
                ip_red=ip_red,
                store_url=store_url,
                theme_dir=theme_dir,
                theme_name=theme_name,
            ),
        )

    def _show_shopify_auth_dialog(self, auth_code: str, auth_url: str, auth_ack_event: threading.Event) -> None:
        """Muestra el diálogo reutilizable de autenticación Shopify."""
        auth_dlg = tk.Toplevel(self.root)
        auth_dlg.title("Autenticacion Shopify requerida")
        auth_dlg.geometry("520x300")
        auth_dlg.resizable(False, False)
        auth_dlg.grab_set()
        auth_dlg.configure(bg="#f6f6f7")
        tk.Label(auth_dlg, text="Autenticacion con Shopify", font=("Segoe UI Semibold", 13),
                 bg="#f6f6f7", fg="#008060").pack(pady=(18, 4))
        tk.Label(auth_dlg, text=f"Codigo de verificacion:  {auth_code}",
                 font=("Segoe UI Semibold", 12), bg="#f6f6f7", fg="#202223").pack(pady=(4, 8))
        tk.Label(auth_dlg,
                 text="1. Pulsa 'Abrir en navegador' o copia la URL\n"
                      "2. Inicia sesion con tu cuenta Shopify\n"
                      "3. Confirma el codigo mostrado arriba\n"
                      "4. Pulsa 'Continuar' — el tema se descargara automaticamente",
                 font=("Segoe UI", 10), bg="#f6f6f7", fg="#6d7175",
                 justify="left").pack(padx=20, pady=(0, 10))
        url_var = tk.StringVar(value=auth_url)
        url_entry = ttk.Entry(auth_dlg, textvariable=url_var, width=60)
        url_entry.pack(padx=20, pady=(0, 8))
        btn_f = tk.Frame(auth_dlg, bg="#f6f6f7")
        btn_f.pack()

        def _open_browser(u: str = auth_url) -> None:
            import webbrowser
            webbrowser.open(u)

        def _copy_url(u: str = auth_url) -> None:
            self.root.clipboard_clear()
            self.root.clipboard_append(u)

        def _continue_setup(ack: threading.Event = auth_ack_event) -> None:
            ack.set()
            auth_dlg.destroy()

        ttk.Button(btn_f, text="Abrir en navegador", style="Accent.TButton",
                   command=_open_browser).pack(side="left", padx=6)
        ttk.Button(btn_f, text="Copiar URL", command=_copy_url).pack(side="left", padx=6)
        ttk.Button(btn_f, text="Continuar", command=_continue_setup).pack(side="left", padx=6)
        auth_dlg.protocol("WM_DELETE_WINDOW", _continue_setup)
        auth_dlg.wait_window()

    @staticmethod
    def _extract_shopify_auth_challenge(log_text: str) -> tuple[str, str] | None:
        if not (log_text or "").strip():
            return None
        text = re.sub(r"\x1b\[[0-9;]*m", "", log_text)
        low = text.casefold()
        if any(marker in low for marker in ("logged in", "authenticated", "login successful")) or "✔" in text:
            return None
        if "activate-with-code" not in low:
            return None
        if "verification code" not in low and "log in to shopify" not in low:
            return None
        url_matches = re.findall(r"(https://accounts\.shopify\.com/activate-with-code[^\s]+)", text, flags=re.I)
        if not url_matches:
            return None
        # Tomar siempre el ultimo challenge del log para evitar reutilizar un codigo antiguo.
        auth_url = url_matches[-1].strip()

        code_matches = re.findall(
            r"(?:user\s+)?verification\s+code\s*:\s*([A-Z0-9]{4}-[A-Z0-9]{4})",
            text,
            flags=re.I,
        )
        auth_code = code_matches[-1].upper().strip() if code_matches else ""

        # Si no hubo linea explicita de codigo, intentar inferirlo desde la URL.
        if not auth_code:
            code_in_url = re.search(r"user_code%5D=([A-Z0-9]{4}-[A-Z0-9]{4})", auth_url, flags=re.I)
            if code_in_url:
                auth_code = code_in_url.group(1).upper().strip()

        return auth_code, auth_url

    def _start_shopify_auth_and_get_challenge(self, shopify_container: str, wait_seconds: int = 25) -> tuple[str, str] | None:
        """Inicia shopify auth login en background y extrae (codigo, url) desde /tmp/shopify_auth.log."""
        container = (shopify_container or "").strip()
        if not container:
            return None

        launch_cmd = (
            "rm -f /tmp/shopify_auth.log; "
            "shopify auth login > /tmp/shopify_auth.log 2>&1 &"
        )
        self._run(["docker", "exec", container, "sh", "-c", launch_cmd])

        for _ in range(max(3, wait_seconds)):
            time.sleep(1.0)
            code, out, err = self._run([
                "docker", "exec", container, "sh", "-c", "cat /tmp/shopify_auth.log 2>/dev/null || true"
            ])
            if code != 0 and not (out or "").strip() and not (err or "").strip():
                continue
            challenge = self._extract_shopify_auth_challenge("\n".join([out or "", err or ""]))
            if challenge:
                return challenge

        return None

    def _get_shopify_auth_challenge_from_container_logs(self, shopify_container: str, tail_lines: int = 250) -> tuple[str, str] | None:
        """Busca (codigo, url) de login Shopify en docker logs del contenedor."""
        container = (shopify_container or "").strip()
        if not container:
            return None
        code, out, err = self._run(["docker", "logs", "--tail", str(max(50, tail_lines)), container])
        if code != 0 and not (out or "").strip() and not (err or "").strip():
            return None
        return self._extract_shopify_auth_challenge("\n".join([out or "", err or ""]))

    def _schedule_shopify_auth_monitor(self, delay_ms: int | None = None) -> None:
        if delay_ms is None:
            delay_ms = self._shopify_auth_monitor_interval_ms
        if self._shopify_auth_monitor_job is not None:
            return
        if not self.root.winfo_exists():
            return
        self._shopify_auth_monitor_job = self.root.after(delay_ms, self._run_shopify_auth_monitor)

    def _run_shopify_auth_monitor(self) -> None:
        self._shopify_auth_monitor_job = None
        if self._shopify_auth_monitor_running:
            self._schedule_shopify_auth_monitor()
            return

        self._shopify_auth_monitor_running = True

        def _worker() -> None:
            pending: list[tuple[str, str, str]] = []
            try:
                details = self._list_containers_details()
                for name, status, image in details:
                    if not status.lower().startswith("up"):
                        continue
                    haystack = f"{name} {image}".lower()
                    if not any(token in haystack for token in ("shopify", "theme", "node")):
                        continue

                    auth_ok_code, auth_ok_out, auth_ok_err = self._run([
                        "docker", "exec", name, "sh", "-c", "test -f /tmp/shopify_auth_ok && echo OK || true"
                    ])
                    if auth_ok_code == 0 and "OK" in "\n".join([auth_ok_out or "", auth_ok_err or ""]):
                        continue

                    code, out, err = self._run(["docker", "logs", "--tail", "120", name])
                    if code != 0 and not (out or "").strip() and not (err or "").strip():
                        continue
                    challenge = self._extract_shopify_auth_challenge("\n".join([out or "", err or ""]))
                    if not challenge:
                        continue
                    auth_code, auth_url = challenge
                    marker = f"{auth_code}|{auth_url}"
                    if self._shopify_auth_prompt_seen.get(name) == marker:
                        continue
                    self._shopify_auth_prompt_seen[name] = marker
                    pending.append((name, auth_code, auth_url))
            except Exception:
                pending = []

            def _finish() -> None:
                self._shopify_auth_monitor_running = False
                if pending:
                    self._handle_shopify_auth_prompts(pending)
                self._schedule_shopify_auth_monitor()

            if self.root.winfo_exists():
                self.root.after(0, _finish)

        threading.Thread(target=_worker, daemon=True).start()

    def _handle_shopify_auth_prompts(self, prompts: list[tuple[str, str, str]]) -> None:
        if not prompts or self._shopify_auth_dialog_active:
            return
        container, auth_code, auth_url = prompts[0]
        now = time.time()
        last_at = self._shopify_auth_prompt_last_shown_at.get(container, 0.0)
        if now - last_at < self._shopify_auth_prompt_cooldown_sec:
            return
        self._shopify_auth_dialog_active = True
        try:
            self._shopify_auth_prompt_last_shown_at[container] = now
            self.status_var.set(f"Shopify login requerido en {container}.")
            ack = threading.Event()
            self._show_shopify_auth_dialog(auth_code or "----", auth_url, ack)
        finally:
            self._shopify_auth_dialog_active = False

    def _auto_detect_and_auth_waiting_theme(
        self,
        shopify_container: str,
        store_url: str,
        theme_name: str,
        theme_dir: str,
    ) -> bool:
        """
        Monitorea si el contenedor está en estado "Esperando tema".
        Si lo detecta, automáticamente inicia el flujo de autenticación y descarga.
        Retorna True si la autenticación se completó y el tema se descargó.
        """
        try:
            # Monitorear logs para detectar "Esperando tema"
            waiting_detected = False
            attempt = 0
            while not waiting_detected:
                if not self._is_container_running(shopify_container):
                    self.log_event("AUTO_AUTH", shopify_container, "DEBUG", "Contenedor no está corriendo")
                    return False

                auth_ok_code, auth_ok_out, auth_ok_err = self._run([
                    "docker", "exec", shopify_container, "sh", "-c", "test -f /tmp/shopify_auth_ok && echo OK || true"
                ])
                if auth_ok_code == 0 and "OK" in "\n".join([auth_ok_out or "", auth_ok_err or ""]):
                    self.log_event("AUTO_AUTH", shopify_container, "DEBUG", "Auth ya completada; no se requiere nuevo prompt")
                    return True

                code, logs_out, _ = self._run([
                    "docker", "logs", shopify_container
                ])
                if code == 0 and "[entrypoint] Esperando tema" in (logs_out or ""):
                    waiting_detected = True
                    self.log_event("AUTO_AUTH", shopify_container, "DEBUG", "Detectado: Esperando tema")
                    break
                
                attempt += 1
                if attempt % 60 == 0:
                    self.log_event("AUTO_AUTH", shopify_container, "DEBUG", f"Monitoreando: {attempt}s sin 'Esperando tema'")
                time.sleep(1)
                
                # Máximo 60 minutos de espera por seguridad
                if attempt > 3600:
                    self.log_event("AUTO_AUTH", shopify_container, "DEBUG", "Timeout (60 min) esperando 'Esperando tema'")
                    return False
            
            # Esperar 5 segundos más para asegurar que está completamente listo
            time.sleep(5)
            
            # Iniciar autenticación
            auth_cmd = (
                "unset SHOPIFY_CLI_THEME_TOKEN SHOPIFY_ACCESS_TOKEN SHOPIFY_FLAG_STORE SHOPIFY_THEME_NAME; "
                "shopify auth login > /tmp/shopify_auth.log 2>&1 &"
            )
            code_auth, _, err_auth = self._run(["docker", "exec", shopify_container, "sh", "-c", auth_cmd])
            self.log_event("AUTO_AUTH", shopify_container, "DEBUG", f"Auth iniciada: code={code_auth}")
            
            # Esperar a que aparezca el código de verificación (sin timeout fijo)
            auth_url = ""
            auth_code = ""
            attempt = 0
            while not auth_url:
                time.sleep(1)
                code_r, log_out, _ = self._run([
                    "docker", "exec", shopify_container,
                    "sh", "-c", "cat /tmp/shopify_auth.log 2>/dev/null"
                ])
                if "activate-with-code" in (log_out or ""):
                    import re as _re
                    url_m = _re.search(r'(https://accounts\.shopify\.com/activate-with-code[^\s]+)', log_out)
                    code_m = _re.search(r'verification code:\s*([A-Z0-9]{4}-[A-Z0-9]{4})', log_out)
                    if url_m:
                        auth_url = url_m.group(1)
                    if code_m:
                        auth_code = code_m.group(1)
                    self.log_event("AUTO_AUTH", shopify_container, "DEBUG", f"Auth code obtenido: {auth_code}")
                    break
                
                attempt += 1
                if attempt % 30 == 0:
                    self.log_event("AUTO_AUTH", shopify_container, "DEBUG", f"Esperando código de auth: {attempt}s")
                
                # Máximo 5 minutos para obtener el código
                if attempt > 300:
                    self.log_event("AUTO_AUTH", shopify_container, "DEBUG", "Timeout (5 min) esperando código de auth")
                    return False
            
            if not auth_url:
                self.log_event("AUTO_AUTH", shopify_container, "DEBUG", "No se obtuvo URL de auth")
                return False
            
            # Mostrar diálogo de autenticación
            auth_ack_event = threading.Event()
            self._show_shopify_auth_dialog(auth_code, auth_url, auth_ack_event)
            
            # Esperar a que el usuario confirme
            auth_ack_event.wait(timeout=60.0)
            
            # Monitorear el log para verificar que la autenticación se completó (sin timeout fijo)
            logged_in = False
            attempt_wait = 0
            while not logged_in:
                time.sleep(1)
                _, log_out2, _ = self._run([
                    "docker", "exec", shopify_container,
                    "sh", "-c", "cat /tmp/shopify_auth.log 2>/dev/null"
                ])
                if any(m in (log_out2 or "") for m in ("Logged in", "logged in", "authenticated", "✔")):
                    logged_in = True
                    self.log_event("AUTO_AUTH", shopify_container, "DEBUG", f"Auth completada en {attempt_wait}s")
                    break
                
                attempt_wait += 1
                if attempt_wait % 30 == 0:
                    self.log_event("AUTO_AUTH", shopify_container, "DEBUG", f"Esperando confirmación de auth: {attempt_wait}s")
                
                # Máximo 10 minutos para completar el login
                if attempt_wait > 600:
                    self.log_event("AUTO_AUTH", shopify_container, "DEBUG", "Timeout (10 min) esperando confirmación de auth")
                    return False
            
            if not logged_in:
                self.log_event("AUTO_AUTH", shopify_container, "DEBUG", "Auth timeout")
                return False
            
            # Marcar auth como completada
            self._run(["docker", "exec", shopify_container, "sh", "-c", "touch /tmp/shopify_auth_ok"])
            
            # Descargar el tema
            theme_name_for_cli = (theme_name or "").strip() or "Horizon"
            _theme_flag = f"--theme \"{theme_name_for_cli}\"" if theme_name else ""
            pull_cmd = (
                "unset SHOPIFY_CLI_THEME_TOKEN SHOPIFY_ACCESS_TOKEN SHOPIFY_FLAG_STORE SHOPIFY_THEME_NAME; "
                f"cd {theme_dir} && shopify theme pull --store {store_url} "
                f"{_theme_flag} --force > /tmp/shopify_pull.log 2>&1"
            )
            code_pull, pull_out, pull_err = self._run(["docker", "exec", shopify_container, "sh", "-c", pull_cmd])
            
            if code_pull == 0:
                self.log_event("AUTO_AUTH", shopify_container, "OK", "Tema descargado exitosamente")
                return True
            else:
                self.log_event("AUTO_AUTH", shopify_container, "DEBUG", f"Pull error: {pull_err}")
                return False
        except Exception as e:
            self.log_event("AUTO_AUTH", shopify_container, "ERROR", str(e))
            return False

    def _list_containers_details(self) -> list[tuple[str, str, str]]:
        code, out, _ = self._run(["docker", "ps", "-a", "--format", "{{.Names}}|{{.Status}}|{{.Image}}"])
        if code != 0 or not out:
            return []

        result: list[tuple[str, str, str]] = []
        for line in out.splitlines():
            parts = line.split("|", 2)
            if len(parts) < 3:
                continue
            name = parts[0].strip()
            status = parts[1].strip()
            image = parts[2].strip()
            if name:
                result.append((name, status, image))
        return result

    def _is_container_running(self, container: str) -> bool:
        code, out, _ = self._run(["docker", "inspect", "--format", "{{.State.Running}}", container])
        return code == 0 and out.strip().lower() == "true"

    def _container_exists(self, container_name: str) -> bool:
        code, out, _ = self._run(["docker", "ps", "-a", "--format", "{{.Names}}"])
        if code != 0 or not out:
            return False
        target = container_name.strip().lstrip("/").casefold()
        return any(line.strip().lstrip("/").casefold() == target for line in out.splitlines())

    def _network_exists(self, network_name: str) -> bool:
        code, out, _ = self._run(["docker", "network", "ls", "--format", "{{.Name}}"])
        if code != 0 or not out:
            return False
        target = network_name.strip().lstrip("/").casefold()
        return any(line.strip().lstrip("/").casefold() == target for line in out.splitlines())

    def _volume_exists(self, volume_name: str) -> bool:
        code, out, _ = self._run(["docker", "volume", "ls", "--format", "{{.Name}}"])
        if code != 0 or not out:
            return False
        target = volume_name.strip().lstrip("/").casefold()
        return any(line.strip().lstrip("/").casefold() == target for line in out.splitlines())

    @staticmethod
    def _extract_host_port(port_output: str) -> str | None:
        for line in port_output.splitlines():
            match = re.search(r":(\d+)\s*$", line.strip())
            if match:
                return match.group(1)
        return None

    def _access_host_for_urls(self) -> str:
        if self.docker_mode == "remote" and self.docker_host:
            parsed = self._extract_host_port_from_docker_host(self.docker_host)
            if parsed is not None:
                host, _port = parsed
                if ":" in host and not host.startswith("["):
                    return f"[{host}]"
                return host
            ssh_host = self._extract_ssh_host_from_docker_host(self.docker_host)
            if ssh_host:
                if ":" in ssh_host and not ssh_host.startswith("["):
                    return f"[{ssh_host}]"
                return ssh_host
        return "localhost"

    def _detect_shopify_local_url(self, shopify_container: str) -> str | None:
        access_host = self._access_host_for_urls()
        for internal_port in ("8080", "80"):
            code, out, _ = self._run(["docker", "port", shopify_container, internal_port])
            if code == 0 and out.strip():
                port = self._extract_host_port(out)
                if port:
                    return f"http://{access_host}:{port}"

        code, out, _ = self._run(["docker", "port", shopify_container])
        if code == 0 and out.strip():
            port = self._extract_host_port(out)
            if port:
                return f"http://{access_host}:{port}"
        return None

    def _detect_db_credentials(self, db_container: str) -> tuple[str, str]:
        code, out, _ = self._run(["docker", "exec", db_container, "env"])
        if code != 0 or not out:
            return "admin", "admin"

        env_map: dict[str, str] = {}
        for line in out.splitlines():
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            env_map[k.strip()] = v.strip()

        # Prioriza root si existe; si no, usuario normal.
        root_pass = env_map.get("MARIADB_ROOT_PASSWORD") or env_map.get("MYSQL_ROOT_PASSWORD")
        if root_pass:
            return "root", root_pass

        user = env_map.get("MARIADB_USER") or env_map.get("MYSQL_USER") or "admin"
        pwd = env_map.get("MARIADB_PASSWORD") or env_map.get("MYSQL_PASSWORD") or "admin"
        return user, pwd

    def _list_databases(self, db_container: str, db_user: str, db_password: str) -> list[str]:
        u = self._sh_single_quote(db_user)
        p = self._sh_single_quote(db_password)
        list_cmd = (
            "if [ -x /opt/bitnami/mariadb/bin/mariadb ]; then "
            f"/opt/bitnami/mariadb/bin/mariadb -h 127.0.0.1 -u {u} -p{p} -N -e 'SHOW DATABASES;'; "
            "else "
            f"mysql -h 127.0.0.1 -u {u} -p{p} -N -e 'SHOW DATABASES;'; "
            "fi"
        )
        code, out, _ = self._run(
            [
                "docker",
                "exec",
                db_container,
                "sh",
                "-c",
                list_cmd,
            ]
        )
        if code != 0 or not out:
            return []

        ignored = {"information_schema", "performance_schema", "mysql", "sys", "test"}
        return [line.strip() for line in out.splitlines() if line.strip() and line.strip().lower() not in ignored]

    @staticmethod
    def _sh_single_quote(value: str) -> str:
        return "'" + value.replace("'", "'\"'\"'") + "'"

    def _set_import_status(self, status_var: tk.StringVar, window: tk.Toplevel, text: str) -> None:
        status_var.set(text)
        window.update_idletasks()

    def _ensure_running_for_import(self, container: str, role_label: str) -> bool:
        if self._is_container_running(container):
            return True

        if not messagebox.askyesno(
            "Importar",
            f"El contenedor Shopify '{container}' esta apagado.\n\nQuieres arrancarlo ahora?",
        ):
            return False

        code, _, err = self._run(["docker", "start", container])
        if code != 0:
            messagebox.showerror("Importar", err or f"No se pudo arrancar {container}.")
            return False
        return True

    @staticmethod
    def _request_import_cancel(status_var: tk.StringVar, stop_event: threading.Event, stop_button: ttk.Button) -> None:
        stop_event.set()
        stop_button.configure(state="disabled")
        status_var.set("Cancelando... esperando fin del paso actual")

    def _pick_theme_tar_file(self, target_var: tk.StringVar) -> None:
        path = filedialog.askopenfilename(
            title="Seleccionar tema Shopify (.tar)",
            initialdir=self.tools_dir,
            filetypes=[("Archivo TAR", "*.tar"), ("Todos los archivos", "*.*")],
        )
        if path:
            target_var.set(path)

    def _pick_theme_folder(self, target_var: tk.StringVar) -> None:
        path = filedialog.askdirectory(
            title="Seleccionar carpeta del tema Shopify",
            initialdir=self.tools_dir,
        )
        if path:
            target_var.set(path)

    def _pick_export_directory(self, target_var: tk.StringVar) -> None:
        path = filedialog.askdirectory(title="Seleccionar carpeta de salida", initialdir=self.tools_dir)
        if path:
            target_var.set(path)

    @staticmethod
    def _default_export_folder() -> str:
        return os.path.join(os.path.expanduser("~"), "Desktop", "shopify-export")

    def _list_themes_in_tar(self, tar_path: str) -> list[str]:
        """Detecta nombres de tema dentro de un TAR usando pistas de estructura Shopify."""
        if not tar_path or not os.path.isfile(tar_path):
            return []
        if not tarfile.is_tarfile(tar_path):
            return []

        candidates: set[str] = set()
        theme_dirs = {"assets", "config", "layout", "sections", "snippets", "templates", "locales", "blocks"}

        try:
            with tarfile.open(tar_path, "r") as archive:
                for member in archive.getmembers():
                    if not member.isfile():
                        continue
                    entry = member.name.replace("\\", "/").lstrip("./")
                    if not entry:
                        continue

                    if entry.endswith("config/settings_schema.json") or entry.endswith("config/settings_data.json"):
                        base = entry.rsplit("/config/", 1)[0]
                        name = base.split("/")[-1].strip()
                        if name:
                            candidates.add(name)
                        continue

                    if entry.endswith(".liquid"):
                        parts = [p for p in entry.split("/") if p]
                        for idx, part in enumerate(parts):
                            if part in theme_dirs and idx > 0:
                                name = parts[idx - 1].strip()
                                if name:
                                    candidates.add(name)
                                break
        except Exception:
            return []

        return sorted(candidates)

    @staticmethod
    def _folder_looks_like_theme(path: str) -> bool:
        if not os.path.isdir(path):
            return False
        checks = [
            os.path.join(path, "config", "settings_schema.json"),
            os.path.join(path, "config", "settings_data.json"),
        ]
        if any(os.path.isfile(p) for p in checks):
            return True
        for d in ("templates", "sections", "layout", "assets"):
            if os.path.isdir(os.path.join(path, d)):
                return True
        return False

    def _discover_theme_sources_in_folder(self, folder_path: str) -> list[tuple[str, str]]:
        """Devuelve pares (nombre_tema, ruta_carpeta) detectados dentro de una carpeta local."""
        if not folder_path or not os.path.isdir(folder_path):
            return []

        sources: list[tuple[str, str]] = []
        seen: set[str] = set()

        # Caso 1: la carpeta seleccionada ya es un tema.
        if self._folder_looks_like_theme(folder_path):
            name = os.path.basename(os.path.normpath(folder_path)).strip() or "tema"
            key = name.casefold()
            if key not in seen:
                seen.add(key)
                sources.append((name, folder_path))

        # Caso 2: la carpeta contiene varios temas como subcarpetas directas.
        try:
            entries = sorted(os.listdir(folder_path))
        except Exception:
            entries = []
        for entry in entries:
            child = os.path.join(folder_path, entry)
            if not os.path.isdir(child):
                continue
            if entry in {".", "..", ".git", ".github", "node_modules", "tmp"}:
                continue
            if not self._folder_looks_like_theme(child):
                continue
            key = entry.casefold()
            if key in seen:
                continue
            seen.add(key)
            sources.append((entry, child))

        return sources

    def _list_themes_in_folder(self, folder_path: str) -> list[str]:
        return [name for (name, _path) in self._discover_theme_sources_in_folder(folder_path)]

    @staticmethod
    def _build_timestamped_export_folder(base_dir: str) -> str:
        base = os.path.abspath(base_dir)
        stamp = datetime.now().strftime("%Y_%m_%d_%H-%M")
        candidate = os.path.join(base, stamp)
        if not os.path.exists(candidate):
            return candidate
        suffix = 1
        while True:
            c = os.path.join(base, f"{stamp}_{suffix:02d}")
            if not os.path.exists(c):
                return c
            suffix += 1

    def _inspect_theme_source(self, theme_path: str) -> tuple[str, int, bool, bool, list[str]]:
        issues: list[str] = []
        file_count = 0
        has_settings_data = False
        has_settings_schema = False

        if os.path.isdir(theme_path):
            kind = "carpeta"
            for _root, _dirs, files in os.walk(theme_path):
                file_count += len(files)
                if "config/settings_data.json" in [os.path.relpath(os.path.join(_root, f), theme_path).replace("\\", "/") for f in files]:
                    has_settings_data = True
                if "config/settings_schema.json" in [os.path.relpath(os.path.join(_root, f), theme_path).replace("\\", "/") for f in files]:
                    has_settings_schema = True
            if file_count == 0:
                issues.append("La carpeta esta vacia.")
        elif tarfile.is_tarfile(theme_path):
            kind = "tar"
            with tarfile.open(theme_path, "r") as archive:
                entries = [m.name.replace("\\", "/") for m in archive.getmembers() if m.isfile()]
                file_count = len(entries)
                has_settings_data = any(entry.endswith("config/settings_data.json") for entry in entries)
                has_settings_schema = any(entry.endswith("config/settings_schema.json") for entry in entries)
                if not entries:
                    issues.append("El TAR no contiene archivos.")
        else:
            raise ValueError("El tema debe ser una carpeta o un archivo TAR valido.")

        if not has_settings_data:
            issues.append("No se encontro config/settings_data.json.")
        if not has_settings_schema:
            issues.append("No se encontro config/settings_schema.json.")
        return kind, file_count, has_settings_data, has_settings_schema, issues

    def _validate_import_selection(
        self,
        shopify_container: str,
        theme_path: str,
        store_url: str,
        push_mode: str,
        backup_enabled: bool,
        backup_dir: str,
        tar_theme_choice: str = "",
    ) -> tuple[bool, str, list[str]]:
        problems: list[str] = []
        warnings: list[str] = []

        if not shopify_container:
            problems.append("Selecciona un contenedor Shopify.")
        if not theme_path:
            problems.append("Selecciona una carpeta o TAR del tema.")
        if theme_path and not os.path.exists(theme_path):
            problems.append("La ruta del tema no existe.")
        if push_mode == "push" and not store_url:
            problems.append("Indica la URL de la tienda para hacer push.")
        if backup_enabled and not backup_dir:
            problems.append("Selecciona una carpeta para guardar el backup automatico.")

        kind = "-"
        file_count = 0
        has_sd = False
        has_ss = False
        tar_themes: list[str] = []
        folder_themes: list[str] = []
        if theme_path and os.path.exists(theme_path):
            try:
                kind, file_count, has_sd, has_ss, source_issues = self._inspect_theme_source(theme_path)
                warnings.extend(source_issues)
                if kind == "tar":
                    tar_themes = self._list_themes_in_tar(theme_path)
                    if len(tar_themes) > 1 and not (tar_theme_choice or "").strip():
                        problems.append("El TAR contiene varios temas. Selecciona uno en 'Tema dentro del TAR'.")
                    if tar_themes and (tar_theme_choice or "").strip() and tar_theme_choice.strip() not in tar_themes:
                        problems.append("El tema seleccionado no existe dentro del TAR.")
                elif kind == "carpeta":
                    folder_themes = self._list_themes_in_folder(theme_path)
                    if folder_themes and (tar_theme_choice or "").strip() and tar_theme_choice.strip() not in folder_themes:
                        problems.append("El tema activo seleccionado no existe dentro de la carpeta.")
            except Exception as exc:
                problems.append(str(exc))

        if not self.docker_ready():
            problems.append(self._docker_unavailable_message())

        summary_lines = [
            f"Contenedor: {shopify_container or '-'}",
            f"Tema: {theme_path or '-'}",
            f"Tipo detectado: {kind}",
            f"Archivos detectados: {file_count}",
            f"settings_data.json: {'si' if has_sd else 'no'}",
            f"settings_schema.json: {'si' if has_ss else 'no'}",
            f"Modo: {'push' if push_mode == 'push' else 'solo copia local'}",
            f"Backup previo: {'si' if backup_enabled else 'no'}",
        ]
        if kind == "tar":
            summary_lines.append(f"Temas en TAR: {', '.join(tar_themes) if tar_themes else '-'}")
            if tar_theme_choice:
                summary_lines.append(f"Tema TAR seleccionado: {tar_theme_choice}")
        elif kind == "carpeta":
            summary_lines.append(f"Temas en carpeta: {', '.join(folder_themes) if folder_themes else '-'}")
            if tar_theme_choice:
                summary_lines.append(f"Tema activo seleccionado: {tar_theme_choice}")
        if backup_enabled:
            summary_lines.append(f"Destino backup: {backup_dir or '-'}")
        if push_mode == "push":
            summary_lines.append(f"Tienda: {store_url or '-'}")
        if warnings:
            summary_lines.append("")
            summary_lines.append("Avisos:")
            summary_lines.extend(f"- {item}" for item in warnings)
        if problems:
            summary_lines.append("")
            summary_lines.append("Bloqueantes:")
            summary_lines.extend(f"- {item}" for item in problems)

        return not problems, "\n".join(summary_lines), problems + warnings

    def _create_pre_import_backup(self, shopify_container: str, backup_root: str) -> str:
        backup_dir = self._build_timestamped_export_folder(backup_root)
        os.makedirs(backup_dir, exist_ok=True)
        tar_path = os.path.join(backup_dir, f"{shopify_container}-preimport.tar")
        extract_dir = os.path.join(backup_dir, "contenedor")
        os.makedirs(extract_dir, exist_ok=True)

        code, _, err = self._run([
            "docker", "exec", "-u", "root", shopify_container, "sh", "-c",
            "tar chf /tmp/shopify-preimport.tar -C /app . 2>/dev/null || tar chf /tmp/shopify-preimport.tar -C /app ."
        ])
        if code != 0:
            raise RuntimeError(err or "No se pudo empaquetar el contenedor para backup")

        code, _, err = self._run(["docker", "cp", f"{shopify_container}:/tmp/shopify-preimport.tar", tar_path])
        if code != 0:
            raise RuntimeError(err or "No se pudo copiar el backup al equipo")

        try:
            with tarfile.open(tar_path, "r") as archive:
                try:
                    archive.extractall(path=extract_dir, filter="data")
                except TypeError:
                    archive.extractall(path=extract_dir)
        except Exception as exc:
            raise RuntimeError(f"Backup creado, pero no se pudo extraer: {exc}") from exc

        return backup_dir

    def _validate_import_wizard_state(
        self,
        validation_var: tk.StringVar,
        validation_ok_var: tk.BooleanVar,
        import_button: ttk.Button,
        shopify_container: str,
        theme_path: str,
        store_url: str,
        push_mode: str,
        backup_enabled: bool,
        backup_dir: str,
        tar_theme_choice: str,
    ) -> None:
        ok, summary, _details = self._validate_import_selection(
            shopify_container=shopify_container,
            theme_path=theme_path,
            store_url=store_url,
            push_mode=push_mode,
            backup_enabled=backup_enabled,
            backup_dir=backup_dir,
            tar_theme_choice=tar_theme_choice,
        )
        validation_var.set(summary)
        validation_ok_var.set(ok)
        import_button.configure(state="normal" if ok else "disabled")
        if ok:
            self._set_last_action("Validacion de importacion completada")

    def _validate_export_selection(
        self,
        shopify_container: str,
        output_dir: str,
        export_mode: str,
        store_url: str,
        theme_name: str,
    ) -> tuple[bool, str]:
        def _render_theme_selection(raw_selection: str) -> str:
            rendered: list[str] = []
            for token in re.split(r"[,;\n]+", raw_selection or ""):
                item = token.strip().strip('"').strip("'")
                if not item:
                    continue
                if "\t" in item:
                    theme_id, theme_label = item.split("\t", 1)
                    theme_id = theme_id.strip()
                    theme_label = theme_label.strip()
                    if theme_id and theme_label:
                        rendered.append(f"{theme_label} [{theme_id}]")
                        continue
                rendered.append(item)
            return ", ".join(rendered)

        problems: list[str] = []
        if not shopify_container:
            problems.append("Selecciona un contenedor Shopify.")
        if not output_dir:
            problems.append("Selecciona una carpeta de salida.")
        if export_mode == "remote" and not store_url:
            problems.append("Indica la URL de la tienda para el modo remoto.")
        if export_mode == "remote" and not theme_name:
            problems.append("Selecciona uno o varios temas remotos.")

        summary_lines = [
            f"Contenedor: {shopify_container or '-'}",
            f"Carpeta de salida: {output_dir or '-'}",
            f"Modo: {'remoto' if export_mode == 'remote' else 'local'}",
        ]
        if export_mode == "local":
            summary_lines.append(f"Temas locales: {_render_theme_selection(theme_name) or '(todo /app)'}")
        if export_mode == "remote":
            summary_lines.append(f"Tienda: {store_url or '-'}")
            summary_lines.append(f"Temas remotos: {_render_theme_selection(theme_name) or '(ninguno)'}")
        if problems:
            summary_lines.append("")
            summary_lines.append("Bloqueantes:")
            summary_lines.extend(f"- {item}" for item in problems)

        return not problems, "\n".join(summary_lines)

    def _validate_export_wizard_state(
        self,
        validation_var: tk.StringVar,
        validation_ok_var: tk.BooleanVar,
        export_button: ttk.Button,
        shopify_container: str,
        output_dir: str,
        export_mode: str,
        store_url: str,
        theme_name: str,
    ) -> None:
        ok, summary = self._validate_export_selection(
            shopify_container=shopify_container,
            output_dir=output_dir,
            export_mode=export_mode,
            store_url=store_url,
            theme_name=theme_name,
        )
        validation_var.set(summary)
        validation_ok_var.set(ok)
        export_button.configure(state="normal" if ok else "disabled")
        if ok:
            self._set_last_action("Validacion de exportacion completada")

    def _list_container_themes_for_export(self, shopify_container: str) -> list[str]:
        """Devuelve temas detectados en /app usando estructura típica de Shopify."""
        if not shopify_container:
            return []

        if not self._is_container_running(shopify_container):
            return []

        cmd = (
            "for d in /app/*; do "
            "  [ -d \"$d\" ] || continue; "
            "  b=$(basename \"$d\"); "
            "  case \"$b\" in .|..|.git|.github|node_modules|tmp) continue ;; esac; "
            "  echo \"DIR:$b\"; "
            "done; "
            "if [ -f /app/config/settings_schema.json ] || [ -f /app/config/settings_data.json ] || "
            "   [ -d /app/templates ] || [ -d /app/sections ] || [ -d /app/layout ] || [ -d /app/assets ]; then "
            "  echo DIR:app; "
            "fi; "
            "echo HINT:$(printenv THEME_NAME 2>/dev/null || true); "
            "echo HINT:$(printenv SHOPIFY_THEME_NAME 2>/dev/null || true); "
            "echo HINT:$(printenv THEME_DIR 2>/dev/null || true); "
            "if [ -f /app/.active_theme_name ]; then echo HINT:$(cat /app/.active_theme_name 2>/dev/null); fi; "
            "if [ -f /app/.active_theme_dir ]; then echo HINT:$(cat /app/.active_theme_dir 2>/dev/null); fi; "
            "if [ -f /app/entrypoint.sh ]; then echo HINT:$(sed -n 's#^THEME_NAME=##p' /app/entrypoint.sh | head -1); fi; "
            "if [ -f /app/entrypoint.sh ]; then echo HINT:$(sed -n 's#^THEME_DIR=##p' /app/entrypoint.sh | head -1); fi; "
            "true"
        )
        code, out, _ = self._run(["docker", "exec", shopify_container, "sh", "-c", cmd])
        if code != 0 and not (out or "").strip():
            return []

        def _normalize_theme_name(raw_name: str) -> str:
            name = (raw_name or "").strip().strip('"').strip("'")
            if name.startswith("/app/"):
                name = name[5:]
            if name == "/app":
                name = "app"
            if name == ".":
                name = "app"
            # Corrige casos anómalos tipo "horizonhorizon".
            if len(name) % 2 == 0:
                half = len(name) // 2
                if half > 0 and name[:half].casefold() == name[half:].casefold():
                    name = name[:half]
            return name.strip()

        dir_seen: set[str] = set()
        dir_themes: list[str] = []
        hint_themes: list[str] = []
        for raw in (out or "").splitlines():
            line = (raw or "").strip()
            if not line:
                continue
            if line.startswith("DIR:"):
                name = _normalize_theme_name(line[4:])
                if not name:
                    continue
                key = name.casefold()
                if key in dir_seen:
                    continue
                dir_seen.add(key)
                dir_themes.append(name)
            elif line.startswith("HINT:"):
                name = _normalize_theme_name(line[5:])
                if name:
                    hint_themes.append(name)

        allowed = {d.casefold() for d in dir_themes}
        seen_keys: set[str] = set(allowed)
        themes = list(dir_themes)

        for hint in hint_themes:
            key = hint.casefold()
            if key not in allowed:
                continue
            if key in seen_keys:
                continue
            seen_keys.add(key)
            themes.append(hint)

        return themes

    def _list_remote_themes_for_export(self, shopify_container: str, store_url: str) -> list[tuple[str, str]]:
        """Devuelve temas remotos como pares (id, nombre) usando Shopify CLI."""
        if not shopify_container or not store_url:
            return []

        if not self._is_container_running(shopify_container):
            return []

        raw_store = store_url.replace('"', '').replace("'", "").strip()
        if not raw_store:
            return []

        def _store_candidates(raw_value: str) -> list[str]:
            candidates: list[str] = []
            seen: set[str] = set()

            def _add(value: str) -> None:
                item = (value or "").strip().strip("/")
                if not item:
                    return
                key = item.casefold()
                if key not in seen:
                    seen.add(key)
                    candidates.append(item)

            _add(raw_value)
            parsed = urllib.parse.urlparse(raw_value if "://" in raw_value else f"https://{raw_value}")
            host = (parsed.netloc or parsed.path or "").strip()
            host = host.split("/", 1)[0].strip().rstrip("/")
            if host.lower().startswith("www."):
                _add(host[4:])
            _add(host)

            host_no_www = host[4:] if host.lower().startswith("www.") else host
            if host_no_www and not host_no_www.lower().endswith(".myshopify.com"):
                shop_slug = host_no_www.split(".", 1)[0].strip()
                if shop_slug:
                    _add(f"{shop_slug}.myshopify.com")
            return candidates

        def _run_theme_list(command: str) -> tuple[int, str]:
            docker_args = self._build_docker_command(["docker", "exec", shopify_container, "sh", "-c", command])
            if docker_args and docker_args[0].lower() == "docker":
                try:
                    docker_args[0] = self._resolver_comando("docker")
                except Exception:
                    pass

            try:
                process = subprocess.run(
                    docker_args,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    cwd=self.tools_dir,
                    shell=False,
                    env=self._docker_process_env(),
                    creationflags=subprocess.CREATE_NO_WINDOW,
                    timeout=25,
                )
                code = process.returncode
                out = (process.stdout or "").strip()
                err = (process.stderr or "").strip()
                return code, "\n".join([out or "", err or ""]).strip()
            except subprocess.TimeoutExpired:
                return 124, ""
            except Exception:
                return 1, ""

        def _extract_store_candidates_from_auth_status(output: str) -> list[str]:
            discovered: list[str] = []
            seen: set[str] = set()

            def _add(value: str) -> None:
                item = (value or "").strip().strip("/")
                if not item:
                    return
                if "://" in item:
                    parsed_val = urllib.parse.urlparse(item)
                    item = (parsed_val.netloc or parsed_val.path or "").strip().strip("/")
                if not item:
                    return
                key = item.casefold()
                if key in seen:
                    return
                seen.add(key)
                discovered.append(item)

            payload = output.strip()
            if not payload:
                return discovered

            if not (payload.startswith("{") or payload.startswith("[")):
                json_match = re.search(r"(\{[\s\S]*\}|\[[\s\S]*\])", payload)
                if json_match:
                    payload = json_match.group(1).strip()

            try:
                data = json.loads(payload)
                scan: list[object] = []
                if isinstance(data, dict):
                    scan.append(data)
                    for key in ("stores", "shop", "currentStore", "store"):
                        value = data.get(key)
                        if value is not None:
                            scan.append(value)
                elif isinstance(data, list):
                    scan.extend(data)

                for item in scan:
                    if isinstance(item, dict):
                        for key in (
                            "shopDomain",
                            "shop_domain",
                            "myshopifyDomain",
                            "myshopify_domain",
                            "storeDomain",
                            "store_domain",
                            "domain",
                            "hostname",
                            "host",
                            "url",
                            "shop",
                            "store",
                        ):
                            value = item.get(key)
                            if isinstance(value, str):
                                _add(value)
                    elif isinstance(item, str):
                        _add(item)
            except Exception:
                pass

            for line in output.splitlines():
                line_clean = line.strip()
                if not line_clean:
                    continue
                for token in re.findall(r"[a-z0-9][a-z0-9-]*\.myshopify\.com", line_clean, flags=re.IGNORECASE):
                    _add(token)

            return discovered

        raw_output = ""
        code = 1
        store_candidates = _store_candidates(raw_store)
        for store_candidate in store_candidates:
            cmd_with_store = (
                f"shopify theme list --store \"{store_candidate}\" --json < /dev/null"
                f" || shopify theme list --store \"{store_candidate}\" < /dev/null"
            )
            code, raw_output = _run_theme_list(cmd_with_store)
            if raw_output:
                break

        if not raw_output:
            _auth_code, auth_output = _run_theme_list("shopify auth status --json < /dev/null || shopify auth status < /dev/null")
            for auth_store in _extract_store_candidates_from_auth_status(auth_output):
                if auth_store.casefold() in {x.casefold() for x in store_candidates}:
                    continue
                cmd_with_store = (
                    f"shopify theme list --store \"{auth_store}\" --json < /dev/null"
                    f" || shopify theme list --store \"{auth_store}\" < /dev/null"
                )
                code, raw_output = _run_theme_list(cmd_with_store)
                if raw_output:
                    break

        if not raw_output:
            code, raw_output = _run_theme_list("shopify theme list --json < /dev/null || shopify theme list < /dev/null")

        if code != 0 and not raw_output:
            return []

        def _normalize_theme_id(raw_id: object) -> str:
            text = str(raw_id or "").strip()
            if not text:
                return ""
            if text.startswith("gid://"):
                gid_match = re.search(r"(\d+)$", text)
                if gid_match:
                    return gid_match.group(1)
            if re.fullmatch(r"\d+", text):
                return text
            return text

        def _dedupe(items: list[tuple[str, str]]) -> list[tuple[str, str]]:
            seen: set[str] = set()
            deduped: list[tuple[str, str]] = []
            for theme_id, theme_name in items:
                key = theme_id.casefold()
                if key in seen:
                    continue
                seen.add(key)
                deduped.append((theme_id, theme_name))
            return deduped

        parsed: list[tuple[str, str]] = []
        json_payload = raw_output
        if not (json_payload.startswith("{") or json_payload.startswith("[")):
            json_match = re.search(r"(\{[\s\S]*\}|\[[\s\S]*\])", json_payload)
            if json_match:
                json_payload = json_match.group(1).strip()

        if json_payload.startswith("{") or json_payload.startswith("["):
            try:
                payload = json.loads(json_payload)
                candidates: object = payload
                if isinstance(payload, dict):
                    for key in ("themes", "data", "items", "nodes"):
                        if isinstance(payload.get(key), list):
                            candidates = payload[key]
                            break
                if isinstance(candidates, list):
                    for item in candidates:
                        if not isinstance(item, dict):
                            continue
                        theme_id = _normalize_theme_id(
                            item.get("id") or item.get("theme_id") or item.get("legacyResourceId")
                        )
                        theme_name = str(item.get("name") or item.get("title") or item.get("themeName") or "").strip()
                        if theme_id and theme_name:
                            parsed.append((theme_id, theme_name))
            except Exception:
                parsed = []

        if parsed:
            return _dedupe(parsed)

        for raw_line in raw_output.splitlines():
            line = re.sub(r"\x1b\[[0-9;]*[mGKHF]", "", raw_line).strip()
            if not line:
                continue
            if line.lower().startswith("id ") or line.lower().startswith("theme "):
                continue

            hash_match = re.search(r"#\s*(\d{6,})", line)
            if hash_match:
                theme_id = hash_match.group(1)
                theme_name = line[:hash_match.start()].strip()
                theme_name = re.sub(r"^[\-\*\u2022\s]+", "", theme_name)
                theme_name = re.sub(r"\[[^\]]+\]", "", theme_name).strip(" -|:")
                if theme_id and theme_name:
                    parsed.append((theme_id, theme_name))
                    continue

            if "│" in line:
                parts = [piece.strip() for piece in line.split("│") if piece.strip()]
            elif "|" in line:
                parts = [piece.strip() for piece in line.split("|") if piece.strip()]
            else:
                parts = [piece.strip() for piece in line.split() if piece.strip()]

            if len(parts) < 2:
                continue

            theme_id = ""
            theme_name = ""
            for idx, part in enumerate(parts):
                if re.fullmatch(r"\d+", part):
                    theme_id = part
                    if idx + 1 < len(parts):
                        theme_name = " ".join(parts[idx + 1:]).strip()
                    elif idx > 0:
                        theme_name = " ".join(parts[:idx]).strip()
                    break

            if theme_id and theme_name:
                theme_name = re.sub(r"\[[^\]]+\]", "", theme_name).strip(" -|:")
                parsed.append((theme_id, theme_name))

        return _dedupe(parsed)

    def open_export_wizard(self) -> None:
        if not self.docker_ready():
            messagebox.showerror("Docker", self._docker_unavailable_message())
            return

        details = self._list_containers_details()
        if not details:
            messagebox.showwarning("Exportar", "No hay contenedores disponibles.")
            return

        shopify_candidates = [
            name for (name, _status, image) in details
            if any(token in (name + " " + image).lower()
                   for token in ("shopify", "node", "theme"))
        ]
        if not shopify_candidates:
            shopify_candidates = [name for (name, _status, _image) in details]

        window = self._open_or_focus_work_tab("export", "Exportar")
        if window is None:
            messagebox.showerror("Interfaz", "No se pudo abrir la pestaña de Exportar.")
            return

        for child in window.winfo_children():
            child.destroy()

        outer = self._create_scrollable_surface(window, padding=(8, 8))
        outer.columnconfigure(1, weight=1)
        self._add_work_tab_header(outer, "Asistente de exportacion Shopify", "export")

        shopify_container_var = tk.StringVar(value=shopify_candidates[0] if shopify_candidates else "")
        store_url_var   = tk.StringVar(value="tu-tienda.myshopify.com")
        output_dir_var  = tk.StringVar(value=self._default_export_folder())
        export_mode_var = tk.StringVar(value="local")
        status_var      = tk.StringVar(value="Configura los datos y pulsa Exportar.")
        validation_var  = tk.StringVar(value="Pulsa Validar para revisar la configuracion antes de exportar.")
        validation_ok_var = tk.BooleanVar(value=False)
        progress_var    = tk.DoubleVar(value=0)
        stop_event      = threading.Event()

        row = 1
        ttk.Label(outer, text="Contenedor Shopify:").grid(row=row, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Combobox(outer, textvariable=shopify_container_var,
                     values=shopify_candidates, state="readonly").grid(row=row, column=1, sticky="ew", pady=4)

        mode_frame = ttk.LabelFrame(outer, text="Modo de exportacion")
        row += 1
        mode_frame.grid(row=row, column=0, columnspan=3, sticky="ew", pady=(10, 4))
        ttk.Radiobutton(mode_frame,
            text="Solo archivos locales del contenedor  (sin conexion a Shopify)",
            variable=export_mode_var, value="local").pack(anchor="w", padx=8, pady=(6, 2))
        ttk.Radiobutton(mode_frame,
            text="Descargar tema desde Shopify  (shopify theme pull — requiere login)",
            variable=export_mode_var, value="remote").pack(anchor="w", padx=8, pady=(2, 6))

        row += 1
        store_frame = ttk.Frame(outer)
        store_frame.grid(row=row, column=0, columnspan=3, sticky="ew", pady=4)
        store_frame.columnconfigure(1, weight=1)
        store_label = ttk.Label(store_frame, text="URL de la tienda:")
        store_label.grid(row=0, column=0, sticky="w", padx=(0, 8))
        store_entry = ttk.Entry(store_frame, textvariable=store_url_var)
        store_entry.grid(row=0, column=1, sticky="ew")
        store_hint = ttk.Label(store_frame, text="Ejemplo: n44zn1-1u.myshopify.com", style="Muted.TLabel")
        store_hint.grid(row=0, column=2, sticky="w", padx=(8, 0))

        row += 1
        local_theme_frame = ttk.LabelFrame(outer, text="Temas locales del contenedor")
        local_theme_frame.grid(row=row, column=0, columnspan=3, sticky="ew", pady=4)
        local_theme_frame.columnconfigure(0, weight=1)
        local_theme_list = tk.Listbox(
            local_theme_frame,
            selectmode="extended",
            height=4,
            exportselection=False,
        )
        local_theme_list.grid(row=0, column=0, sticky="ew")
        local_theme_scroll = ttk.Scrollbar(local_theme_frame, orient="vertical", command=local_theme_list.yview)
        local_theme_scroll.grid(row=0, column=1, sticky="ns")
        local_theme_list.configure(yscrollcommand=local_theme_scroll.set)
        ttk.Button(
            local_theme_frame,
            text="Actualizar temas locales",
            command=lambda: _refresh_local_themes_for_container(show_feedback=True),
        ).grid(row=1, column=0, sticky="w", pady=(6, 0))

        row += 1
        remote_theme_frame = ttk.LabelFrame(outer, text="Temas remotos de Shopify")
        remote_theme_frame.grid(row=row, column=0, columnspan=3, sticky="ew", pady=4)
        remote_theme_frame.columnconfigure(0, weight=1)
        remote_theme_frame.columnconfigure(1, weight=1)
        remote_theme_tools = ttk.Frame(remote_theme_frame)
        remote_theme_tools.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 6))
        ttk.Button(
            remote_theme_tools,
            text="Listar temas remotos",
            command=lambda: _refresh_remote_themes(show_feedback=True),
        ).pack(side="left")
        ttk.Label(
            remote_theme_tools,
            text="Selecciona uno o varios temas para exportarlos.",
            style="Muted.TLabel",
        ).pack(side="left", padx=(8, 0))
        remote_theme_list = tk.Listbox(
            remote_theme_frame,
            selectmode="extended",
            height=4,
            exportselection=False,
        )
        remote_theme_list.grid(row=1, column=0, sticky="ew")
        remote_theme_scroll = ttk.Scrollbar(remote_theme_frame, orient="vertical", command=remote_theme_list.yview)
        remote_theme_scroll.grid(row=1, column=1, sticky="ns")
        remote_theme_list.configure(yscrollcommand=remote_theme_scroll.set)
        remote_loading_var = tk.StringVar(value="Pulsa 'Listar temas remotos' para cargar themes desde Shopify.")
        ttk.Label(remote_theme_frame, textvariable=remote_loading_var, style="Muted.TLabel").grid(
            row=2, column=0, columnspan=2, sticky="w", pady=(6, 0)
        )

        local_theme_items: list[str] = []
        remote_theme_items: list[tuple[str, str]] = []

        def _apply_local_theme_values(themes: list[str], preferred_csv: str = "") -> None:
            nonlocal local_theme_items
            local_theme_items = list(themes)
            prev = [x.strip() for x in preferred_csv.split(",") if x.strip()]
            prev_cf = {p.casefold() for p in prev}
            local_theme_list.delete(0, "end")
            for t in themes:
                local_theme_list.insert("end", t)
            if not themes:
                return
            selected_any = False
            for idx, t in enumerate(themes):
                if t.casefold() in prev_cf:
                    local_theme_list.selection_set(idx)
                    selected_any = True
            if not selected_any:
                local_theme_list.selection_set(0)

        def _apply_remote_theme_values(themes: list[tuple[str, str]], preferred_csv: str = "") -> None:
            nonlocal remote_theme_items
            remote_theme_items = list(themes)
            prev_ids = []
            for token in re.split(r"[,;\n]+", preferred_csv or ""):
                item = token.strip().strip('"').strip("'")
                if not item:
                    continue
                if "\t" in item:
                    theme_id, _theme_name = item.split("\t", 1)
                    item = theme_id.strip()
                prev_ids.append(item)
            prev_cf = {p.casefold() for p in prev_ids if p}
            remote_theme_list.delete(0, "end")
            for theme_id, theme_name in themes:
                remote_theme_list.insert("end", f"{theme_name} [{theme_id}]")
            if not themes:
                return
            selected_any = False
            for idx, (theme_id, _theme_name) in enumerate(themes):
                if theme_id.casefold() in prev_cf:
                    remote_theme_list.selection_set(idx)
                    selected_any = True
            if not selected_any:
                remote_theme_list.selection_set(0)

        def _compose_export_theme_selection() -> str:
            if export_mode_var.get() == "remote":
                indices = list(remote_theme_list.curselection())
                selected: list[str] = []
                for i in indices:
                    if 0 <= i < len(remote_theme_items):
                        theme_id, theme_name = remote_theme_items[i]
                        if theme_id and theme_name:
                            selected.append(f"{theme_id}\t{theme_name}")
                return ",".join(selected)
            indices = list(local_theme_list.curselection())
            selected = [local_theme_list.get(i).strip() for i in indices if local_theme_list.get(i).strip()]
            if not selected:
                return ""
            return ",".join(selected)

        def _refresh_local_themes_for_container(show_feedback: bool = False) -> None:
            container = shopify_container_var.get().strip()
            themes = self._list_container_themes_for_export(container)

            if not themes and container and self._is_container_running(container):
                code_fb, out_fb, _ = self._run([
                    "docker", "exec", container, "sh", "-c",
                    "for d in /app/*; do [ -d \"$d\" ] && basename \"$d\"; done; "
                    "printenv THEME_NAME 2>/dev/null || true; "
                    "printenv SHOPIFY_THEME_NAME 2>/dev/null || true; "
                    "printenv THEME_DIR 2>/dev/null || true; "
                    "if [ -f /app/.active_theme_name ]; then cat /app/.active_theme_name 2>/dev/null; fi; "
                    "if [ -f /app/.active_theme_dir ]; then cat /app/.active_theme_dir 2>/dev/null; fi; "
                    "true"
                ])
                if code_fb == 0 and (out_fb or "").strip():
                    seen_fb: set[str] = set()
                    parsed: list[str] = []
                    for raw_line in (out_fb or "").splitlines():
                        item = raw_line.strip().strip('"').strip("'")
                        if item.startswith("/app/"):
                            item = item[5:]
                        if item == "/app" or item == ".":
                            item = "app"
                        low = item.lower()
                        if not item:
                            continue
                        if low in {".", "..", ".git", ".github", "node_modules", "tmp", "total"}:
                            continue
                        if item.startswith("drw") or item.startswith("-rw"):
                            continue
                        if low in seen_fb:
                            continue
                        seen_fb.add(low)
                        parsed.append(item)
                    if parsed:
                        themes = parsed

            _apply_local_theme_values(themes, _compose_export_theme_selection())
            if not themes and show_feedback:
                if not container:
                    messagebox.showinfo("Exportar", "Selecciona un contenedor Shopify para cargar los temas locales.")
                elif not self._is_container_running(container):
                    messagebox.showinfo("Exportar", "El contenedor Shopify no esta en ejecucion. Inicialo y vuelve a intentarlo.")
                else:
                    code_dbg, out_dbg, err_dbg = self._run([
                        "docker", "exec", container, "sh", "-c",
                        "echo '--- /app ---'; ls -la /app 2>/dev/null | sed -n '1,40p'; "
                        "echo '--- env tema ---'; "
                        "printenv THEME_NAME 2>/dev/null; "
                        "printenv SHOPIFY_THEME_NAME 2>/dev/null; "
                        "printenv THEME_DIR 2>/dev/null; true"
                    ])
                    detail_lines = ["No se detectaron temas locales en /app del contenedor seleccionado."]
                    if code_dbg == 0 and (out_dbg or "").strip():
                        snippet = "\n".join((out_dbg or "").splitlines()[:16])
                        detail_lines.append("")
                        detail_lines.append("Diagnostico rapido:")
                        detail_lines.append(snippet)
                    elif (err_dbg or "").strip():
                        detail_lines.append("")
                        detail_lines.append(f"Detalle docker exec: {(err_dbg or '').strip()[:300]}")
                    messagebox.showinfo("Exportar", "\n".join(detail_lines))

        def _open_remote_theme_debug_terminal(container: str, store_url: str) -> str | None:
            safe_store = (store_url or "").replace('"', '').replace("'", "").strip()
            parsed = urllib.parse.urlparse(safe_store if "://" in safe_store else f"https://{safe_store}")
            host = (parsed.netloc or parsed.path or "").strip().strip("/")
            host = host.split("/", 1)[0].strip()
            if host.lower().startswith("www."):
                host = host[4:]
            slug = host.split(".", 1)[0].strip() if host else ""
            store_candidates: list[str] = []
            seen_candidates: set[str] = set()

            def _add_store(value: str) -> None:
                item = (value or "").strip().strip("/")
                if not item:
                    return
                key = item.casefold()
                if key in seen_candidates:
                    return
                seen_candidates.add(key)
                store_candidates.append(item)

            _add_store(safe_store)
            _add_store(host)
            if slug:
                _add_store(f"{slug}.myshopify.com")

            def _as_cmdline(args: list[str]) -> str:
                final_args = self._build_docker_command(args)
                if final_args and final_args[0].lower() == "docker":
                    try:
                        final_args[0] = self._resolver_comando("docker")
                    except Exception:
                        pass
                return subprocess.list2cmdline(final_args)

            try:
                stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                log_path = os.path.join(tempfile.gettempdir(), f"shu_remote_theme_debug_{stamp}.log")

                cmd_blocks: list[str] = [
                    _as_cmdline(["docker", "version"]),
                    _as_cmdline(["docker", "info"]),
                    _as_cmdline(["docker", "exec", container, "sh", "-c", "shopify version"]),
                    _as_cmdline(["docker", "exec", container, "sh", "-c", "shopify auth status --json < /dev/null || shopify auth status < /dev/null"]),
                    _as_cmdline(["docker", "exec", container, "sh", "-c", "shopify theme list --json < /dev/null || shopify theme list < /dev/null"]),
                ]
                for candidate in store_candidates:
                    cmd_blocks.append(
                        _as_cmdline([
                            "docker", "exec", container, "sh", "-c",
                            f"shopify theme list --store \"{candidate}\" --json < /dev/null || shopify theme list --store \"{candidate}\" < /dev/null",
                        ])
                    )

                bat_lines = [
                    "@echo off",
                    "setlocal",
                    f"set LOGFILE={log_path}",
                    "echo ================================================== > \"%LOGFILE%\"",
                    "echo Shopify Remote Theme Debug >> \"%LOGFILE%\"",
                    "echo Fecha/Hora: %DATE% %TIME% >> \"%LOGFILE%\"",
                    f"echo Contenedor: {container} >> \"%LOGFILE%\"",
                    f"echo Store ingresado: {safe_store} >> \"%LOGFILE%\"",
                    "echo ================================================== >> \"%LOGFILE%\"",
                    "",
                ]

                if self.docker_mode == "remote" and self.docker_host:
                    bat_lines.extend([
                        f"set DOCKER_HOST={self.docker_host}",
                        "set DOCKER_CONTEXT=",
                        "set DOCKER_TLS_VERIFY=",
                        "set DOCKER_CERT_PATH=",
                        "set DOCKER_TLS=",
                        "echo Docker remoto: %DOCKER_HOST% >> \"%LOGFILE%\"",
                        "echo. >> \"%LOGFILE%\"",
                    ])

                for cmdline in cmd_blocks:
                    bat_lines.extend([
                        f"echo [CMD] {cmdline} >> \"%LOGFILE%\"",
                        f"{cmdline} >> \"%LOGFILE%\" 2>&1",
                        "echo. >> \"%LOGFILE%\"",
                    ])

                bat_lines.extend([
                    "echo ================================================== >> \"%LOGFILE%\"",
                    "echo Fin del diagnostico. >> \"%LOGFILE%\"",
                    "echo ================================================== >> \"%LOGFILE%\"",
                    "type \"%LOGFILE%\"",
                    "echo.",
                    "echo Log guardado en:",
                    "echo %LOGFILE%",
                    "pause",
                ])

                fd_diag, bat_path_diag = tempfile.mkstemp(prefix="shu_theme_diag_", suffix=".bat")
                with os.fdopen(fd_diag, "w", encoding="cp1252", errors="replace") as fh:
                    fh.write("\r\n".join(bat_lines) + "\r\n")

                subprocess.Popen(
                    ["cmd.exe", "/c", "start", "Shopify Theme Debug", "cmd.exe", "/k", bat_path_diag],
                    shell=False,
                    creationflags=getattr(subprocess, "CREATE_NEW_CONSOLE", 0),
                )
                return log_path
            except Exception:
                return None

        def _refresh_remote_themes(show_feedback: bool = False) -> None:
            container = shopify_container_var.get().strip()
            store_url = store_url_var.get().strip()
            if not store_url:
                if show_feedback:
                    messagebox.showinfo("Exportar", "Indica la URL de la tienda para listar los temas remotos.")
                return
            if not container:
                if show_feedback:
                    messagebox.showinfo("Exportar", "Selecciona un contenedor Shopify para ejecutar Shopify CLI.")
                return
            if not self._is_container_running(container):
                if show_feedback:
                    messagebox.showinfo("Exportar", "El contenedor Shopify no esta en ejecucion.")
                return

            remote_loading_var.set("Consultando themes remotos...")
            loading_modal = self._show_loading_modal("Listando temas remotos")

            def _worker() -> None:
                def _show_auth_dialog(auth_code: str, auth_url: str, ack_event: threading.Event) -> None:
                    self._show_shopify_auth_dialog(auth_code, auth_url, ack_event)

                try:
                    themes = self._list_remote_themes_for_export(container, store_url)
                    if themes:
                        self.root.after(0, lambda: _apply_remote_theme_values(themes, _compose_export_theme_selection()))
                        self.root.after(0, lambda: remote_loading_var.set(f"Temas remotos cargados: {len(themes)}"))
                        self.root.after(0, lambda: self._finish_loading_modal(loading_modal, True, auto_close_success_ms=450))
                        return

                    auth_ok_code, auth_ok_out, auth_ok_err = self._run([
                        "docker", "exec", container, "sh", "-c", "test -f /tmp/shopify_auth_ok && echo OK || true"
                    ])
                    auth_marker_present = auth_ok_code == 0 and "OK" in "\n".join([auth_ok_out or "", auth_ok_err or ""])

                    if auth_marker_present:
                        debug_log_path = _open_remote_theme_debug_terminal(container, store_url)
                        self.root.after(
                            0,
                            lambda: remote_loading_var.set(
                                "Shopify CLI ya esta autenticado, pero no se devolvieron temas para esa tienda."
                            ),
                        )
                        if show_feedback:
                            self.root.after(
                                0,
                                lambda: messagebox.showinfo(
                                    "Exportar",
                                    (
                                        "Shopify CLI ya esta autenticado, pero no se encontraron temas remotos para la URL indicada.\n\n"
                                        + (f"Se abrio terminal de diagnostico. Log: {debug_log_path}" if debug_log_path else "No se pudo abrir la terminal de diagnostico.")
                                    ),
                                ),
                            )
                        self.root.after(0, lambda: self._finish_loading_modal(loading_modal, True, auto_close_success_ms=450))
                        return

                    auth_prompt = self._start_shopify_auth_and_get_challenge(container)
                    if not auth_prompt:
                        safe_store = store_url.replace('"', '').replace("'", "")
                        docker_login_args = self._build_docker_command([
                            "docker", "exec", "-it", container, "sh", "-c", "shopify auth login"
                        ])
                        docker_touch_args = self._build_docker_command([
                            "docker", "exec", container, "sh", "-c", "touch /tmp/shopify_auth_ok"
                        ])
                        if docker_login_args and docker_login_args[0].lower() == "docker":
                            try:
                                docker_login_args[0] = self._resolver_comando("docker")
                            except Exception:
                                pass
                        if docker_touch_args and docker_touch_args[0].lower() == "docker":
                            try:
                                docker_touch_args[0] = self._resolver_comando("docker")
                            except Exception:
                                pass
                        docker_login_cmd = subprocess.list2cmdline(docker_login_args)
                        docker_touch_cmd = subprocess.list2cmdline(docker_touch_args)
                        bat_lines = [
                            "@echo off",
                            "echo Iniciando autenticacion Shopify CLI...",
                            "echo.",
                        ]
                        if self.docker_mode == "remote" and self.docker_host:
                            bat_lines.extend([
                                f"set DOCKER_HOST={self.docker_host}",
                                "set DOCKER_CONTEXT=",
                                "set DOCKER_TLS_VERIFY=",
                                "set DOCKER_CERT_PATH=",
                                "set DOCKER_TLS=",
                                "echo Docker remoto: %DOCKER_HOST%",
                                "echo.",
                            ])
                        bat_lines.extend([
                            docker_login_cmd,
                            "IF %ERRORLEVEL% EQU 0 (",
                            f"  {docker_touch_cmd}",
                            ")",
                            "echo.",
                            "echo Si el login fue correcto, vuelve a la app y pulsa 'Listar temas remotos'.",
                            "pause",
                        ])
                        bat_content = "\r\n".join(bat_lines) + "\r\n"
                        launched_terminal = False
                        fd_auth, bat_path_auth = tempfile.mkstemp(prefix="shu_auth_", suffix=".bat")
                        try:
                            with os.fdopen(fd_auth, "w", encoding="cp1252", errors="replace") as fh:
                                fh.write(bat_content)
                            subprocess.Popen(
                                ["cmd.exe", "/c", "start", "Shopify Login", "cmd.exe", "/c", bat_path_auth],
                                shell=False,
                                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                            )
                            launched_terminal = True
                        except Exception:
                            launched_terminal = False

                        if launched_terminal:
                            self.root.after(0, lambda: remote_loading_var.set("Terminal de login abierta. Completa la autenticacion y vuelve a listar temas."))
                            self.root.after(0, lambda: self._finish_loading_modal(loading_modal, True, auto_close_success_ms=450))
                            if show_feedback:
                                self.root.after(
                                    0,
                                    lambda: messagebox.showinfo(
                                        "Exportar",
                                        "Se abrio una terminal para autenticar Shopify CLI.\n\nCompleta el login y luego pulsa 'Listar temas remotos' otra vez.",
                                    ),
                                )
                        else:
                            self.root.after(0, lambda: remote_loading_var.set("No se encontro challenge de login y no se pudo abrir la terminal de autenticacion."))
                            self.root.after(
                                0,
                                lambda: self._finish_loading_modal(
                                    loading_modal,
                                    False,
                                    error_msg="No se pudo abrir la terminal de autenticacion.",
                                ),
                            )
                            if show_feedback:
                                self.root.after(
                                    0,
                                    lambda: messagebox.showinfo(
                                        "Exportar",
                                        "No se pudieron listar temas remotos. Shopify CLI no devolvio challenge y no se pudo abrir terminal de login.",
                                    ),
                                )
                        return

                    auth_code, auth_url = auth_prompt
                    auth_ack = threading.Event()
                    self.root.after(0, lambda: _show_auth_dialog(auth_code or "----", auth_url, auth_ack))
                    if not auth_ack.wait(timeout=600):
                        self.root.after(0, lambda: remote_loading_var.set("Autenticacion cancelada o agotada."))
                        return

                    themes = []
                    for _ in range(3):
                        themes = self._list_remote_themes_for_export(container, store_url)
                        if themes:
                            break
                        time.sleep(1.5)

                    self.root.after(0, lambda: _apply_remote_theme_values(themes, _compose_export_theme_selection()))
                    self.root.after(
                        0,
                        lambda: remote_loading_var.set(
                            f"Temas remotos cargados: {len(themes)}" if themes else "No se encontraron themes remotos tras autenticar."
                        ),
                    )
                    if show_feedback and not themes:
                        debug_log_path = _open_remote_theme_debug_terminal(container, store_url)
                        self.root.after(
                            0,
                            lambda: messagebox.showinfo(
                                "Exportar",
                                (
                                    "Shopify CLI ya autentico, pero no devolvio temas remotos para esa tienda.\n\n"
                                    + (f"Se abrio terminal de diagnostico. Log: {debug_log_path}" if debug_log_path else "No se pudo abrir la terminal de diagnostico.")
                                ),
                            ),
                        )
                    self.root.after(0, lambda: self._finish_loading_modal(loading_modal, True, auto_close_success_ms=450))
                except Exception as exc:
                    self.root.after(0, lambda: remote_loading_var.set(f"Error al listar themes remotos: {exc}"))
                    self.root.after(
                        0,
                        lambda: self._finish_loading_modal(
                            loading_modal,
                            False,
                            error_msg=str(exc),
                        ),
                    )
                    if show_feedback:
                        self.root.after(0, lambda: messagebox.showinfo("Exportar", f"No se pudieron listar temas remotos.\n\n{exc}"))

            threading.Thread(target=_worker, daemon=True).start()

        _apply_local_theme_values(self._list_container_themes_for_export(shopify_container_var.get().strip()))

        def _on_mode_change(*_: object) -> None:
            if export_mode_var.get() == "remote":
                store_frame.grid()
                remote_theme_frame.grid()
                local_theme_frame.grid_remove()
                remote_loading_var.set("Modo remoto activo. Pulsa 'Listar temas remotos'.")
            else:
                store_frame.grid_remove()
                remote_theme_frame.grid_remove()
                local_theme_frame.grid()
                _refresh_local_themes_for_container(show_feedback=False)
            _reset_validation()

        row += 1
        ttk.Label(outer, text="Carpeta destino:").grid(row=row, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Entry(outer, textvariable=output_dir_var).grid(row=row, column=1, sticky="ew", pady=4)
        ttk.Button(outer, text="Examinar",
                   command=lambda: self._pick_export_directory(output_dir_var)).grid(
                   row=row, column=2, padx=(8, 0), pady=4)

        info_frame = ttk.LabelFrame(outer, text="Informacion")
        row += 1
        info_frame.grid(row=row, column=0, columnspan=3, sticky="ew", pady=(8, 4))
        ttk.Label(info_frame,
            text=("Local: empaqueta /app del contenedor en un .tar y lo extrae en la carpeta destino.\n"
                  "Remoto: igual que Local, ademas ejecuta shopify theme pull — se pedira login Shopify\n"
                  "        igual que en Crear/Recrear (URL de verificacion en el navegador)."),
            wraplength=560, justify="left", style="Muted.TLabel").pack(padx=8, pady=6, anchor="w")

        validation_frame = ttk.LabelFrame(outer, text="Resumen y validacion")
        row += 1
        validation_frame.grid(row=row, column=0, columnspan=3, sticky="ew", pady=(8, 4))
        validation_frame.columnconfigure(0, weight=1)
        ttk.Label(validation_frame, textvariable=validation_var, justify="left", wraplength=560).grid(
            row=0, column=0, sticky="ew", padx=8, pady=8
        )

        row += 1
        ttk.Separator(outer, orient="horizontal").grid(
            row=row, column=0, columnspan=3, sticky="ew", pady=(10, 8))
        row += 1
        progress_panel, _ = self._build_progress_panel(
            outer,
            "Progreso de exportación",
            "La exportación empaqueta el contenedor y, en modo remoto, completa la descarga desde Shopify.",
            status_var,
            progress_var,
        )
        progress_panel.grid(row=row, column=0, columnspan=3, sticky="ew", pady=(8, 0))

        row += 1
        actions = ttk.Frame(outer)
        actions.grid(row=row, column=0, columnspan=3, sticky="e", pady=(12, 0))
        cancel_button = ttk.Button(actions, text="Cancelar",
                                   command=lambda: self._close_work_tab("export"))
        cancel_button.pack(side="right")
        stop_button = ttk.Button(actions, text="Detener exportacion", state="disabled",
            command=lambda: self._request_import_cancel(status_var, stop_event, stop_button))
        stop_button.pack(side="right", padx=(0, 8))
        validate_button = ttk.Button(
            actions,
            text="Validar",
            command=lambda: self._validate_export_wizard_state(
                validation_var=validation_var,
                validation_ok_var=validation_ok_var,
                export_button=export_button,
                shopify_container=shopify_container_var.get().strip(),
                output_dir=output_dir_var.get().strip(),
                export_mode=export_mode_var.get(),
                store_url=store_url_var.get().strip(),
                theme_name=_compose_export_theme_selection(),
            ),
        )
        validate_button.pack(side="right", padx=(0, 8))
        export_button = ttk.Button(actions, text="Exportar ahora",
            command=lambda: self._run_export_from_wizard(
                window=window, status_var=status_var, progress_var=progress_var,
                export_button=export_button, cancel_button=cancel_button,
                stop_button=stop_button, stop_event=stop_event,
                shopify_container=shopify_container_var.get().strip(),
                store_url=store_url_var.get().strip(),
                theme_name=_compose_export_theme_selection(),
                output_dir=output_dir_var.get().strip(),
                export_mode=export_mode_var.get(),
            ))
        export_button.configure(state="disabled")
        export_button.pack(side="right", padx=(0, 8))

        self._register_tooltip(validate_button, "Revisa la configuracion y activa el boton Exportar")
        self._register_tooltip(export_button, "Exporta el tema con la configuracion actual")
        self._register_tooltip(stop_button, "Detiene el proceso en el siguiente paso seguro")
        self._register_tooltip(cancel_button, "Cierra el asistente")

        def _reset_validation(*_: object) -> None:
            validation_ok_var.set(False)
            validation_var.set("Pendiente de validacion. Pulsa Validar para revisar la configuracion antes de exportar.")
            export_button.configure(state="disabled")

        def _clear_remote_theme_values() -> None:
            remote_theme_items.clear()
            remote_theme_list.delete(0, "end")

        def _on_container_change(*_: object) -> None:
            _reset_validation()
            if export_mode_var.get() == "remote":
                _clear_remote_theme_values()
            _refresh_local_themes_for_container(show_feedback=False)

        def _on_store_url_change(*_: object) -> None:
            _reset_validation()
            if export_mode_var.get() == "remote":
                _clear_remote_theme_values()

        shopify_container_var.trace_add("write", _on_container_change)
        store_url_var.trace_add("write", _on_store_url_change)
        output_dir_var.trace_add("write", _reset_validation)
        export_mode_var.trace_add("write", _on_mode_change)

        local_theme_list.bind("<<ListboxSelect>>", lambda *_: _reset_validation())
        remote_theme_list.bind("<<ListboxSelect>>", lambda *_: _reset_validation())

        _on_mode_change()

    def _run_export_from_wizard(
        self,
        window: ttk.Frame,
        status_var: tk.StringVar,
        progress_var: tk.DoubleVar,
        export_button: ttk.Button,
        cancel_button: ttk.Button,
        stop_button: ttk.Button,
        stop_event: threading.Event,
        shopify_container: str,
        store_url: str,
        theme_name: str,
        output_dir: str,
        export_mode: str,
    ) -> None:
        ok, summary = self._validate_export_selection(
            shopify_container=shopify_container,
            output_dir=output_dir,
            export_mode=export_mode,
            store_url=store_url,
            theme_name=theme_name,
        )
        if not ok:
            messagebox.showwarning("Exportar", "La validacion previa tiene problemas. Revisa el resumen antes de continuar.\n\n" + summary)
            return

        output_dir = output_dir.strip().strip('"')
        final_output_dir = self._build_timestamped_export_folder(output_dir)

        mode_desc = ("Solo archivos locales" if export_mode == "local"
                     else f"Descarga desde Shopify ({store_url}) + archivos locales")
        if not messagebox.askyesno("Confirmar exportacion",
            f"Contenedor: {shopify_container}\nModo: {mode_desc}\nCarpeta: {final_output_dir}\n\nConfirmas?"):
            return

        if not self._ensure_running_for_import(shopify_container, "Shopify"):
            return

        export_button.configure(state="disabled")
        cancel_button.configure(state="disabled")
        stop_button.configure(state="normal")
        stop_event.clear()
        progress_var.set(0)
        status_var.set("Iniciando exportacion...")

        events: queue.Queue[tuple[str, object]] = queue.Queue()
        worker = threading.Thread(
            target=self._run_export_worker,
            args=(events, stop_event, shopify_container, store_url, theme_name, final_output_dir, export_mode),
            daemon=True,
        )
        worker.start()

        self._poll_export_worker_queue(
            window=window, status_var=status_var, progress_var=progress_var,
            export_button=export_button, cancel_button=cancel_button,
            stop_button=stop_button, events=events,
            shopify_container=shopify_container, final_output_dir=final_output_dir,
        )

    def _run_export_worker(
        self,
        events: queue.Queue[tuple[str, object]],
        stop_event: threading.Event,
        shopify_container: str,
        store_url: str,
        theme_name: str,
        output_dir: str,
        export_mode: str,
    ) -> None:
        try:
            os.makedirs(output_dir, exist_ok=True)

            raw_selection = (theme_name or "").strip()
            selected_themes: list[tuple[str, str]] = []
            remote_mode = export_mode == "remote"
            for token in re.split(r"[,;\n]+", raw_selection):
                item = token.strip().strip('"').strip("'")
                item = item.replace("\\", "/")
                if not item:
                    continue
                if remote_mode and "\t" in item:
                    theme_id, theme_label = item.split("\t", 1)
                    theme_id = theme_id.strip()
                    theme_label = theme_label.strip()
                    if theme_id and theme_label and theme_id.casefold() not in {x[0].casefold() for x in selected_themes}:
                        selected_themes.append((theme_id, theme_label))
                    continue
                if item.startswith("/app/"):
                    item = item[5:]
                if item in {"/app", "app", "."}:
                    item = ""
                item = item.lstrip("/")
                if ".." in item:
                    item = ""
                if item and item.casefold() not in {x[0].casefold() for x in selected_themes}:
                    selected_themes.append((item, item))

            primary_theme = selected_themes[0][0] if selected_themes else ""

            def _safe_name(raw_name: str, fallback: str = "theme") -> str:
                cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "-", (raw_name or "").lower()).strip("-._")
                return cleaned or fallback

            def check_cancel() -> None:
                if stop_event.is_set():
                    raise RuntimeError("EXPORT_CANCELLED_BY_USER")

            def dbg(msg: str) -> None:
                events.put(("debug", msg))

            def strip_ansi(text: str) -> str:
                import re as _r
                text = _r.sub(r'\x1b\[[0-9;]*[mGKHF]', '', text)
                text = _r.sub(r'â[\x80-\xbf][\x80-\xbf]', '', text)
                return text.strip()

            local_dir = os.path.join(output_dir, "local")
            os.makedirs(local_dir, exist_ok=True)

            # PASO 1: exportar archivos locales
            themes_for_local: list[tuple[str, str]] = selected_themes if (export_mode == "local" and selected_themes) else [("", "app")]
            if not themes_for_local:
                themes_for_local = [("", "app")]

            total_local = len(themes_for_local)
            for idx, (theme_item, theme_label) in enumerate(themes_for_local, start=1):
                check_cancel()
                source_suffix = theme_item if theme_item and theme_item != "app" else ""
                source = f"/app/{source_suffix}" if source_suffix else "/app"
                label = theme_label or theme_item or "app"
                safe_label = _safe_name(label, "app")
                pct_pack = 5.0 + (idx - 1) * (35.0 / max(1, total_local))
                pct_copy = pct_pack + (18.0 / max(1, total_local))
                events.put(("progress", (pct_pack, f"[1/2] Empaquetando tema {idx}/{total_local}: {label}...")))
                dbg(f"Empaquetando {source} del contenedor...")

                remote_tar = f"/tmp/shopify-export-{idx}.tar"
                code, _, err = self._run([
                    "docker", "exec", "-u", "root", shopify_container, "sh", "-c",
                    f"if [ -d \"{source}\" ]; then "
                    f"  tar chf {remote_tar} -C \"{source}\" . 2>/dev/null || tar chf {remote_tar} -C \"{source}\" .; "
                    "else "
                    f"  tar chf {remote_tar} -C /app . 2>/dev/null || tar chf {remote_tar} -C /app .; "
                    "fi"
                ])
                if code != 0:
                    raise RuntimeError(err or f"No se pudo empaquetar el tema {label}")

                events.put(("progress", (pct_copy, f"[1/2] Copiando tema {idx}/{total_local} al equipo local...")))
                local_tar_name = "shopify-local.tar" if (idx == 1 and total_local == 1 and safe_label == "app") else f"shopify-local-{safe_label}.tar"
                local_tar = os.path.join(output_dir, local_tar_name)
                code, _, err = self._run(["docker", "cp", f"{shopify_container}:{remote_tar}", local_tar])
                if code != 0:
                    raise RuntimeError(err or f"No se pudo copiar el TAR del tema {label}")

                extract_target = os.path.join(local_dir, safe_label) if total_local > 1 or safe_label != "app" else local_dir
                os.makedirs(extract_target, exist_ok=True)
                try:
                    with tarfile.open(local_tar, "r") as tar:
                        try:
                            tar.extractall(path=extract_target, filter="data")
                        except TypeError:
                            tar.extractall(path=extract_target)
                    dbg(f"Tema {label} extraido en: {extract_target}")
                except Exception as ex:
                    dbg(f"Aviso al extraer TAR {label}: {ex}")

            events.put(("progress", (50.0, "[1/2] Archivos locales exportados.")))

            if export_mode != "remote":
                events.put(("done", output_dir))
                return

            # PASO 2: login + theme pull via terminal interactiva
            remote_targets = selected_themes
            if not remote_targets:
                raise RuntimeError("Selecciona uno o varios temas remotos antes de exportar.")
            check_cancel()
            events.put(("progress", (52.0, f"[2/2] Abriendo terminal para login y descarga de {len(remote_targets)} tema(s)...")))
            dbg("Limpiando sesion anterior...")
            self._run(["docker", "exec", shopify_container, "sh", "-c",
                "rm -f /tmp/shopify_pull_ok /tmp/shopify_pull_fail 2>/dev/null || true; "
                "mkdir -p /tmp/shopify-pull-export"])

            safe_store = store_url.replace('"', '').replace("'", "")

            auth_ok_code, auth_ok_out, auth_ok_err = self._run([
                "docker", "exec", shopify_container, "sh", "-c", "test -f /tmp/shopify_auth_ok && echo OK || true"
            ])
            has_auth_marker = auth_ok_code == 0 and "OK" in "\n".join([auth_ok_out or "", auth_ok_err or ""])

            if has_auth_marker:
                events.put(("progress", (56.0, "[2/2] Sesion Shopify detectada. Descargando sin pedir login...")))
                dbg("Sesion Shopify existente detectada (/tmp/shopify_auth_ok).")
                total_remote = len(remote_targets)
                for idx, (theme_id, theme_label) in enumerate(remote_targets, start=1):
                    check_cancel()
                    safe_label = _safe_name(theme_label, f"theme-{idx}")
                    if theme_id and theme_id.casefold() not in safe_label.casefold():
                        safe_label = f"{safe_label}-{_safe_name(theme_id, theme_id)}"
                    pct_remote = 56.0 + (idx - 1) * (18.0 / max(1, total_remote))
                    events.put(("progress", (pct_remote, f"[2/2] Descargando tema remoto {idx}/{total_remote}: {theme_label}...")))
                    pull_cmd_direct = (
                        f"mkdir -p /tmp/shopify-pull-export/{safe_label} "
                        f"&& cd /tmp/shopify-pull-export/{safe_label} "
                        f"&& shopify theme pull --store {safe_store} --theme {theme_id} --force"
                    )
                    pull_code, pull_out, pull_err = self._run([
                        "docker", "exec", shopify_container, "sh", "-c", pull_cmd_direct
                    ])
                    if pull_code != 0:
                        raise RuntimeError(
                            f"No se pudo descargar el tema remoto '{theme_label}' (ID {theme_id}).\n\n"
                            f"Detalle: {(pull_err or pull_out or 'sin detalle')[:400]}"
                        )

                self._run(["docker", "exec", shopify_container, "sh", "-c", "touch /tmp/shopify_pull_ok"])
                pull_ok = True
                pull_fail = False
            else:
                events.put(("progress", (52.0, f"[2/2] Abriendo terminal para login y descarga de {len(remote_targets)} tema(s)...")))
                dbg("No hay sesion Shopify previa; se abrira terminal de login.")

            def _bat_cmd(args: list[str]) -> str:
                final_args = self._build_docker_command(args)
                if final_args and final_args[0].lower() == "docker":
                    try:
                        final_args[0] = self._resolver_comando("docker")
                    except Exception:
                        pass
                return subprocess.list2cmdline(final_args)

            if not has_auth_marker:
                login_cmd = _bat_cmd(["docker", "exec", "-it", shopify_container, "shopify", "auth", "login"])
                auth_marker_cmd = _bat_cmd(["docker", "exec", shopify_container, "sh", "-c", "touch /tmp/shopify_auth_ok"])
                pull_fail_cmd = _bat_cmd(["docker", "exec", shopify_container, "sh", "-c", "touch /tmp/shopify_pull_fail"])
                bat_lines = [
                    "@echo off",
                    "echo Iniciando login con Shopify CLI...",
                    "echo.",
                    login_cmd,
                    "SET LOGIN_CODE=%ERRORLEVEL%",
                    "IF %LOGIN_CODE% NEQ 0 (",
                    f"    {pull_fail_cmd}",
                    "    echo.",
                    "    echo ERROR: Login fallido o cancelado.",
                    "    echo Puedes cerrar esta ventana.",
                    "    pause",
                    "    exit /b 1",
                    ")",
                    auth_marker_cmd,
                    "echo.",
                ]
                if self.docker_mode == "remote" and self.docker_host:
                    bat_lines[1:1] = [
                        f"set DOCKER_HOST={self.docker_host}",
                        "set DOCKER_CONTEXT=",
                        "set DOCKER_TLS_VERIFY=",
                        "set DOCKER_CERT_PATH=",
                        "set DOCKER_TLS=",
                        "echo Docker remoto: %DOCKER_HOST%",
                        "echo.",
                    ]
                for idx, (theme_id, theme_label) in enumerate(remote_targets, start=1):
                    safe_label = _safe_name(theme_label, f"theme-{idx}")
                    if theme_id and theme_id.casefold() not in safe_label.casefold():
                        safe_label = f"{safe_label}-{_safe_name(theme_id, theme_id)}"
                    pull_cmd = _bat_cmd([
                        "docker", "exec", shopify_container, "sh", "-c",
                        f"mkdir -p /tmp/shopify-pull-export/{safe_label} && cd /tmp/shopify-pull-export/{safe_label} && shopify theme pull --store {safe_store} --theme {theme_id} --force"
                    ])
                    bat_lines.extend([
                        f"echo Descargando tema {idx}/{len(remote_targets)}...",
                        pull_cmd,
                        "IF %ERRORLEVEL% NEQ 0 (",
                        f"    echo ERROR: No se pudo descargar el tema #{idx}.",
                        f"    {pull_fail_cmd}",
                        "    echo.",
                        "    pause",
                        "    exit /b 1",
                        ")",
                    ])
                bat_lines.extend([
                    _bat_cmd(["docker", "exec", shopify_container, "sh", "-c", "touch /tmp/shopify_pull_ok"]),
                    "echo.",
                    "echo Descarga finalizada. Puedes cerrar esta ventana.",
                    "pause",
                ])
                bat_content = "\r\n".join(bat_lines) + "\r\n"

                import tempfile as _tmp
                fd, bat_path = _tmp.mkstemp(prefix="shu_export_", suffix=".bat")
                try:
                    with os.fdopen(fd, "w", encoding="cp1252", errors="replace") as fh:
                        fh.write(bat_content)
                except Exception:
                    try:
                        os.close(fd)
                    except OSError:
                        pass

                try:
                    subprocess.Popen(
                        ["cmd.exe", "/c", "start", "Shopify Export", "cmd.exe", "/c", bat_path],
                        shell=False,
                        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                    )
                    dbg(f"Terminal abierta: shopify auth login + theme pull --store {safe_store}")
                except Exception as exc:
                    raise RuntimeError(f"No se pudo abrir la terminal: {exc}")

                events.put(("export_terminal_opened", shopify_container))

                # Esperar señal de exito o fallo
                pull_ok = False
                pull_fail = False
                for tick in range(600):
                    check_cancel()
                    time.sleep(1.0)
                    rc_ok, _, _   = self._run(["docker", "exec", shopify_container,
                        "sh", "-c", "test -f /tmp/shopify_pull_ok"])
                    rc_fail, _, _ = self._run(["docker", "exec", shopify_container,
                        "sh", "-c", "test -f /tmp/shopify_pull_fail"])
                    # Comprobacion alternativa: archivos .liquid = pull exitoso
                    rc_liq, out_liq, _ = self._run(["docker", "exec", shopify_container,
                        "sh", "-c", "find /tmp/shopify-pull-export -name '*.liquid' 2>/dev/null | head -1"])
                    has_liquid = rc_liq == 0 and bool((out_liq or "").strip())

                    if rc_ok == 0 or has_liquid:
                        pull_ok = True
                        dbg("Descarga completada.")
                        self._run(["docker", "exec", shopify_container,
                            "sh", "-c", "rm -f /tmp/shopify_pull_fail 2>/dev/null || true"])
                        break
                    if rc_fail == 0 and not has_liquid:
                        pull_fail = True
                        dbg("Login/descarga fallido.")
                        break
                    if tick % 20 == 0 and tick > 0:
                        pct = 55.0 + min(18.0, tick * 0.03)
                        events.put(("progress", (pct, f"[2/2] Esperando login y descarga... ({tick}s)")))

            if pull_fail:
                raise RuntimeError(
                    "Login o descarga fallidos en la terminal.\n\n"
                    "Revisa la ventana de terminal para ver el error.\n\n"
                    "Los archivos LOCALES si se exportaron correctamente.")
            if not pull_ok:
                raise RuntimeError(
                    "Timeout esperando la descarga (10 minutos).\n\n"
                    "Los archivos LOCALES si se exportaron correctamente.")

            dbg("Copiando tema descargado al equipo...")
            events.put(("progress", (75.0, "[2/2] Descarga completada. Copiando al equipo...")))
            pull_dir = os.path.join(output_dir, "remote")
            os.makedirs(pull_dir, exist_ok=True)
            self._run(["docker", "exec", shopify_container, "sh", "-c",
                "tar chf /tmp/shopify-pull.tar -C /tmp/shopify-pull-export . 2>/dev/null || true"])
            pull_tar = os.path.join(output_dir, "shopify-pull.tar")
            rc_cp, _, _ = self._run(["docker", "cp",
                f"{shopify_container}:/tmp/shopify-pull.tar", pull_tar])
            if rc_cp == 0 and os.path.isfile(pull_tar):
                try:
                    with tarfile.open(pull_tar, "r") as tar:
                        try:
                            tar.extractall(path=pull_dir, filter="data")
                        except TypeError:
                            tar.extractall(path=pull_dir)
                    dbg(f"Tema remoto extraido en: {pull_dir}")
                except Exception as ex:
                    dbg(f"Aviso al extraer: {ex}")

            events.put(("done", output_dir))
        except Exception as exc:
            events.put(("debug", f"ERROR: {exc}"))
            events.put(("error", str(exc)))

    def _poll_export_worker_queue(
        self,
        window: ttk.Frame,
        status_var: tk.StringVar,
        progress_var: tk.DoubleVar,
        export_button: ttk.Button,
        cancel_button: ttk.Button,
        stop_button: ttk.Button,
        events: queue.Queue[tuple[str, object]],
        shopify_container: str,
        final_output_dir: str,
        debug_window: "tk.Toplevel | None" = None,
        debug_text: "tk.Text | None" = None,
    ) -> None:
        completed_output: str | None = None
        failed: str | None = None
        latest_progress: tuple[float, str] | None = None
        debug_lines: list[str] = []
        processed = 0

        while processed < 120:
            try:
                kind, payload = events.get_nowait()
            except queue.Empty:
                break
            processed += 1

            if kind == "progress":
                value, text = payload  # type: ignore[misc]
                latest_progress = (float(value), str(text))
            elif kind == "debug":
                debug_lines.append(str(payload))
            elif kind == "export_terminal_opened":
                status_var.set("Terminal abierta — sigue los pasos en la ventana negra...")
            elif kind == "done":
                completed_output = str(payload)
            elif kind == "error":
                failed = str(payload)

        for line in debug_lines:
            self._append_import_debug(debug_window, debug_text, line)

        if latest_progress is not None:
            progress_var.set(latest_progress[0])
            status_var.set(latest_progress[1])

        if completed_output is not None:
            stop_button.configure(state="disabled")
            progress_var.set(100)
            status_var.set("Exportacion completada correctamente.")
            self.log_event("EXPORT", shopify_container, "OK", f"Exportado en {completed_output}")
            self.refresh_history()
            messagebox.showinfo("Exportar",
                f"Exportacion completada.\n\nCarpeta: {completed_output}\n\n"
                "  local/   — archivos del contenedor\n"
                "  remote/  — tema descargado desde Shopify (si modo remoto)")
            return

        if failed is not None:
            stop_button.configure(state="disabled")
            export_button.configure(state="normal")
            cancel_button.configure(state="normal")
            if failed == "EXPORT_CANCELLED_BY_USER":
                status_var.set("Exportacion cancelada.")
                messagebox.showinfo("Exportar", "Exportacion cancelada por el usuario.")
                return
            self.log_event("EXPORT", shopify_container, "ERROR", failed)
            self.refresh_history()
            status_var.set(f"Error: {failed[:80]}")
            messagebox.showerror("Exportar", f"La exportacion fallo.\n\n{failed}")
            return

        delay = 40 if processed >= 120 else 150
        self.root.after(delay, lambda: self._poll_export_worker_queue(
            window=window, status_var=status_var, progress_var=progress_var,
            export_button=export_button, cancel_button=cancel_button,
            stop_button=stop_button, events=events,
            shopify_container=shopify_container, final_output_dir=final_output_dir,
            debug_window=debug_window, debug_text=debug_text,
        ))

    def open_import_wizard(self) -> None:
        if not self.docker_ready():
            messagebox.showerror("Docker", self._docker_unavailable_message())
            return

        details = self._list_containers_details()
        if not details:
            messagebox.showwarning("Importar", "No hay contenedores disponibles.")
            return

        shopify_candidates = [
            name for (name, _status, image) in details
            if any(token in (name + " " + image).lower()
                   for token in ("shopify", "node", "theme"))
        ]
        if not shopify_candidates:
            shopify_candidates = [name for (name, _status, _image) in details]

        window = self._open_or_focus_work_tab("import", "Importar")
        if window is None:
            messagebox.showerror("Interfaz", "No se pudo abrir la pestaña de Importar.")
            return

        for child in window.winfo_children():
            child.destroy()

        outer = self._create_scrollable_surface(window, padding=(8, 8))
        outer.columnconfigure(1, weight=1)
        self._add_work_tab_header(outer, "Asistente de importacion Shopify", "import")

        shopify_container_var = tk.StringVar(value=shopify_candidates[0] if shopify_candidates else "")
        theme_path_var        = tk.StringVar(value="")
        tar_theme_var         = tk.StringVar(value="")
        store_url_var         = tk.StringVar(value="tu-tienda.myshopify.com")
        push_mode_var         = tk.StringVar(value="none")   # "none" | "push"
        status_var            = tk.StringVar(value="Configura los datos y pulsa Importar.")
        validation_var        = tk.StringVar(value="Pulsa Validar para revisar el tema antes de importar.")
        backup_enabled_var    = tk.BooleanVar(value=True)
        backup_dir_var        = tk.StringVar(value=os.path.join(self._default_export_folder(), "backups"))
        validation_ok_var     = tk.BooleanVar(value=False)
        show_debug_var        = tk.BooleanVar(value=False)
        progress_var          = tk.DoubleVar(value=0)
        stop_event            = threading.Event()

        row = 1
        ttk.Label(outer, text="Contenedor Shopify:").grid(row=row, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Combobox(outer, textvariable=shopify_container_var,
                     values=shopify_candidates, state="readonly").grid(row=row, column=1, sticky="ew", pady=4)

        row += 1
        ttk.Label(outer, text="Carpeta / TAR del tema:").grid(row=row, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Entry(outer, textvariable=theme_path_var).grid(row=row, column=1, sticky="ew", pady=4)
        pick_frame = ttk.Frame(outer)
        pick_frame.grid(row=row, column=2, padx=(8, 0), pady=4)
        ttk.Button(pick_frame, text="Archivo TAR",
                   command=lambda: self._pick_theme_tar_file(theme_path_var)).pack(side="left")
        ttk.Button(pick_frame, text="Carpeta",
                   command=lambda: self._pick_theme_folder(theme_path_var)).pack(side="left", padx=(6, 0))

        row += 1
        tar_theme_label = ttk.Label(outer, text="Tema a activar:")
        tar_theme_label.grid(row=row, column=0, sticky="w", padx=(0, 8), pady=4)
        tar_theme_combo = ttk.Combobox(outer, textvariable=tar_theme_var, values=[], state="readonly")
        tar_theme_combo.grid(row=row, column=1, sticky="ew", pady=4)
        tar_theme_hint = ttk.Label(
            outer,
            text="(si hay varios temas en TAR o carpeta, elige cual queda activo)",
            style="Muted.TLabel",
        )
        tar_theme_hint.grid(row=row, column=2, sticky="w", padx=(8, 0), pady=4)

        # ── Modo de subida ───────────────────────────────────────────────────
        push_frame = ttk.LabelFrame(outer, text="Despues de copiar al contenedor")
        row += 1
        push_frame.grid(row=row, column=0, columnspan=3, sticky="ew", pady=(10, 4))

        ttk.Radiobutton(
            push_frame,
            text="Solo copiar archivos al contenedor  (sin subir a Shopify)",
            variable=push_mode_var, value="none",
        ).pack(anchor="w", padx=8, pady=(6, 2))
        ttk.Radiobutton(
            push_frame,
            text="Copiar + subir tema a Shopify  (shopify theme push — requiere login)",
            variable=push_mode_var, value="push",
        ).pack(anchor="w", padx=8, pady=(2, 6))

        # ── Tienda (solo visible si push) ────────────────────────────────────
        row += 1
        store_label = ttk.Label(outer, text="URL de la tienda:")
        store_label.grid(row=row, column=0, sticky="w", padx=(0, 8), pady=4)
        store_entry = ttk.Entry(outer, textvariable=store_url_var)
        store_entry.grid(row=row, column=1, sticky="ew", pady=4)

        # ── Info ─────────────────────────────────────────────────────────────
        info_frame = ttk.LabelFrame(outer, text="Que hace este asistente")
        row += 1
        info_frame.grid(row=row, column=0, columnspan=3, sticky="ew", pady=(8, 4))
        ttk.Label(
            info_frame,
            text=(
                "1. Copia el tema (carpeta o TAR) a /app/<tema> del contenedor.\n"
                "2. Ejecuta npm install en la carpeta del tema.\n"
                "3. Opcionalmente abre una terminal para hacer shopify auth login\n"
                "   y shopify theme push (igual que el export, requiere login interactivo)."
            ),
            wraplength=560, justify="left", style="Muted.TLabel",
        ).pack(padx=8, pady=6, anchor="w")

        validation_frame = ttk.LabelFrame(outer, text="Resumen y validacion")
        row += 1
        validation_frame.grid(row=row, column=0, columnspan=3, sticky="ew", pady=(8, 4))
        validation_frame.columnconfigure(0, weight=1)
        ttk.Label(validation_frame, textvariable=validation_var, justify="left", wraplength=560).grid(
            row=0, column=0, sticky="ew", padx=8, pady=(8, 4)
        )

        backup_row = ttk.Frame(validation_frame)
        backup_row.grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 8))
        backup_row.columnconfigure(1, weight=1)
        ttk.Checkbutton(
            backup_row,
            text="Crear backup automatico del contenedor antes de importar",
            variable=backup_enabled_var,
        ).grid(row=0, column=0, columnspan=3, sticky="w")
        ttk.Label(backup_row, text="Carpeta backup:").grid(row=1, column=0, sticky="w", pady=(6, 0))
        ttk.Entry(backup_row, textvariable=backup_dir_var).grid(row=1, column=1, sticky="ew", padx=(6, 6), pady=(6, 0))
        ttk.Button(backup_row, text="Examinar",
                   command=lambda: self._pick_export_directory(backup_dir_var)).grid(row=1, column=2, sticky="e", pady=(6, 0))

        row += 1
        progress_panel, _ = self._build_progress_panel(
            outer,
            "Progreso de importación",
            "La importación valida el tema, crea backup y luego copia o publica los archivos.",
            status_var,
            progress_var,
        )
        progress_panel.grid(row=row, column=0, columnspan=3, sticky="ew", pady=(8, 0))

        row += 1
        actions = ttk.Frame(outer)
        actions.grid(row=row, column=0, columnspan=3, sticky="e", pady=(12, 0))
        cancel_button = ttk.Button(actions, text="Cancelar", command=window.destroy)
        cancel_button.pack(side="right")
        stop_button = ttk.Button(
            actions, text="Detener importacion", state="disabled",
            command=lambda: self._request_import_cancel(status_var, stop_event, stop_button),
        )
        stop_button.pack(side="right", padx=(0, 8))
        validate_button = ttk.Button(
            actions,
            text="Validar",
            command=lambda: self._validate_import_wizard_state(
                validation_var=validation_var,
                validation_ok_var=validation_ok_var,
                import_button=import_button,
                shopify_container=shopify_container_var.get().strip(),
                theme_path=theme_path_var.get().strip(),
                store_url=store_url_var.get().strip(),
                push_mode=push_mode_var.get(),
                backup_enabled=backup_enabled_var.get(),
                backup_dir=backup_dir_var.get().strip(),
                tar_theme_choice=tar_theme_var.get().strip(),
            ),
        )
        validate_button.pack(side="right", padx=(0, 8))
        import_button = ttk.Button(
            actions, text="Importar ahora",
            command=lambda: self._run_import_from_wizard(
                window=window,
                status_var=status_var,
                progress_var=progress_var,
                import_button=import_button,
                cancel_button=cancel_button,
                stop_button=stop_button,
                stop_event=stop_event,
                shopify_container=shopify_container_var.get().strip(),
                theme_path=theme_path_var.get().strip(),
                tar_theme_choice=tar_theme_var.get().strip(),
                store_url=store_url_var.get().strip(),
                push_mode=push_mode_var.get(),
                backup_enabled=backup_enabled_var.get(),
                backup_dir=backup_dir_var.get().strip(),
                show_debug=show_debug_var.get(),
            ),
            state="disabled",
        )
        import_button.pack(side="right", padx=(0, 8))

        row += 1
        ttk.Separator(outer, orient="horizontal").grid(
            row=row, column=0, columnspan=3, sticky="ew", pady=(10, 8))

        row += 1
        ttk.Checkbutton(
            outer,
            text="Mostrar log de debug (consola detallada)",
            variable=show_debug_var,
        ).grid(row=row, column=0, columnspan=3, sticky="w", pady=(0, 4))

        self._register_tooltip(validate_button, "Revisa el tema antes de importar y activa el boton Importar")
        self._register_tooltip(import_button, "Importa el tema al contenedor")
        self._register_tooltip(stop_button, "Detiene el proceso en el siguiente paso seguro")
        self._register_tooltip(cancel_button, "Cierra el asistente")
        self._register_tooltip(backup_enabled_var, "Crear una copia de seguridad antes de modificar el contenedor")

        def _reset_validation(*_: object) -> None:
            validation_ok_var.set(False)
            validation_var.set("Pendiente de validacion. Pulsa Validar para revisar el tema antes de importar.")
            import_button.configure(state="disabled")

        def _refresh_tar_themes(*_: object) -> None:
            path = theme_path_var.get().strip()
            themes: list[str] = []
            if path and os.path.isfile(path) and tarfile.is_tarfile(path):
                themes = self._list_themes_in_tar(path)
            elif path and os.path.isdir(path):
                themes = self._list_themes_in_folder(path)

            if themes:
                tar_theme_combo.configure(values=themes)
                if len(themes) == 1:
                    tar_theme_var.set(themes[0])
                elif tar_theme_var.get().strip() not in themes:
                    tar_theme_var.set("")
                tar_theme_label.grid()
                tar_theme_combo.grid()
                tar_theme_hint.grid()
            else:
                tar_theme_combo.configure(values=[])
                tar_theme_var.set("")
                tar_theme_label.grid_remove()
                tar_theme_combo.grid_remove()
                tar_theme_hint.grid_remove()
            _reset_validation()

        theme_path_var.trace_add("write", _refresh_tar_themes)
        tar_theme_var.trace_add("write", _reset_validation)
        store_url_var.trace_add("write", _reset_validation)
        push_mode_var.trace_add("write", _reset_validation)
        backup_enabled_var.trace_add("write", _reset_validation)
        backup_dir_var.trace_add("write", _reset_validation)

        def _on_push_mode_change(*_: object) -> None:
            if push_mode_var.get() == "push":
                store_label.grid()
                store_entry.grid()
            else:
                store_label.grid_remove()
                store_entry.grid_remove()
            _reset_validation()

        push_mode_var.trace_add("write", _on_push_mode_change)
        _on_push_mode_change()
        _refresh_tar_themes()

    def _run_import_from_wizard(
        self,
        window: ttk.Frame,
        status_var: tk.StringVar,
        progress_var: tk.DoubleVar,
        import_button: ttk.Button,
        cancel_button: ttk.Button,
        stop_button: ttk.Button,
        stop_event: threading.Event,
        shopify_container: str,
        theme_path: str,
        tar_theme_choice: str,
        store_url: str,
        push_mode: str,   # "none" | "push"
        backup_enabled: bool = False,
        backup_dir: str = "",
        show_debug: bool = False,
    ) -> None:
        ok, summary, issues = self._validate_import_selection(
            shopify_container=shopify_container,
            theme_path=theme_path,
            store_url=store_url,
            push_mode=push_mode,
            backup_enabled=backup_enabled,
            backup_dir=backup_dir,
            tar_theme_choice=tar_theme_choice,
        )
        if not ok:
            messagebox.showwarning("Importar", "La validacion previa tiene problemas. Revisa el resumen antes de continuar.\n\n" + summary)
            return

        mode_desc = (
            "Solo copiar archivos al contenedor"
            if push_mode == "none"
            else f"Copiar + shopify theme push a {store_url}"
        )
        if not messagebox.askyesno(
            "Confirmar importacion",
            f"Contenedor: {shopify_container}\n"
            f"Tema: {theme_path}\n"
            f"Tema activo: {tar_theme_choice or '(auto)'}\n"
            f"Modo: {mode_desc}\n"
            f"Backup automatico: {'si' if backup_enabled else 'no'}\n\n"
            "Confirmas la importacion?",
        ):
            return

        if not self._ensure_running_for_import(shopify_container, "Shopify"):
            return

        import_button.configure(state="disabled")
        cancel_button.configure(state="disabled")
        stop_button.configure(state="normal")
        stop_event.clear()
        progress_var.set(0)
        status_var.set("Iniciando importacion...")

        debug_win, debug_txt = (None, None)
        if show_debug:
            try:
                debug_win, debug_txt = self._open_import_debug_console(window)
            except Exception as e:
                print(f"No se pudo abrir consola debug: {e}")

        events: queue.Queue[tuple[str, object]] = queue.Queue()
        worker = threading.Thread(
            target=self._run_import_worker,
            args=(events, stop_event, shopify_container, theme_path, tar_theme_choice, store_url, push_mode, backup_enabled, backup_dir),
            daemon=True,
        )
        worker.start()

        self._poll_import_worker_queue_simple(
            window=window,
            status_var=status_var,
            progress_var=progress_var,
            import_button=import_button,
            cancel_button=cancel_button,
            stop_button=stop_button,
            events=events,
            shopify_container=shopify_container,
            debug_window=debug_win,
            debug_text=debug_txt,
        )

    def _run_import_worker(
        self,
        events: queue.Queue[tuple[str, object]],
        stop_event: threading.Event,
        shopify_container: str,
        theme_path: str,
        tar_theme_choice: str,
        store_url: str,
        push_mode: str,   # "none" | "push"
        backup_enabled: bool,
        backup_dir: str,
    ) -> None:
        try:
            imported_targets: list[tuple[str, str]] = []
            is_tar_input = tarfile.is_tarfile(theme_path) if os.path.isfile(theme_path) else False
            if is_tar_input:
                if tar_theme_choice:
                    import_theme_name = tar_theme_choice.strip()
                else:
                    tar_themes = self._list_themes_in_tar(theme_path)
                    import_theme_name = tar_themes[0] if tar_themes else os.path.splitext(os.path.basename(theme_path))[0].strip()
                    if len(tar_themes) > 1 and not tar_theme_choice:
                        raise RuntimeError("El TAR contiene varios temas. Selecciona uno antes de importar.")
            elif os.path.isfile(theme_path):
                import_theme_name = os.path.splitext(os.path.basename(theme_path))[0].strip()
            else:
                import_theme_name = os.path.basename(os.path.normpath(theme_path)).strip()
            if not import_theme_name:
                import_theme_name = "horizon"

            theme_dir_name = re.sub(r"[^a-z0-9._-]+", "-", import_theme_name.lower()).strip("-._")
            if not theme_dir_name:
                theme_dir_name = "horizon"
            theme_container_path = f"/app/{theme_dir_name}"

            def check_cancel() -> None:
                if stop_event.is_set():
                    raise RuntimeError("IMPORT_CANCELLED_BY_USER")

            def dbg(msg: str) -> None:
                events.put(("debug", msg))

            if backup_enabled:
                events.put(("progress", (2.0, "[0/3] Creando backup automatico antes de importar...")))
                dbg(f"Backup automatico habilitado. Destino: {backup_dir}")
                backup_path = self._create_pre_import_backup(shopify_container, backup_dir)
                dbg(f"Backup creado en: {backup_path}")
                self.log_event("BACKUP", shopify_container, "INFO", f"Backup previo creado en {backup_path}")
            else:
                dbg("Backup automatico desactivado por el usuario.")

            # ── PASO 1: Copiar tema al contenedor ────────────────────────────
            events.put(("progress", (5.0, "[1/3] Validando y copiando tema al contenedor...")))

            is_tar = tarfile.is_tarfile(theme_path) if os.path.isfile(theme_path) else False
            is_dir = os.path.isdir(theme_path)
            dbg(f"=== INICIO IMPORT ===")
            dbg(f"  tema detectado: {import_theme_name}")
            dbg(f"  ruta destino: {theme_container_path}")
            dbg(f"  theme_path recibido: {theme_path}")
            dbg(f"  es TAR: {is_tar}")
            dbg(f"  es DIR: {is_dir}")
            dbg(f"  existe: {os.path.exists(theme_path)}")
            if is_dir:
                try:
                    contenido_raiz = os.listdir(theme_path)
                    dbg(f"  contenido raiz ({len(contenido_raiz)} items): {contenido_raiz[:30]}")
                    for item in contenido_raiz:
                        full = os.path.join(theme_path, item)
                        if os.path.isdir(full):
                            sub = os.listdir(full)
                            dbg(f"    DIR  {item}/ ({len(sub)} items): {sub[:15]}")
                        else:
                            sz = os.path.getsize(full)
                            dbg(f"    FILE {item} ({sz} bytes)")
                except Exception as e:
                    dbg(f"  Error listando contenido: {e}")
            elif is_tar:
                try:
                    sz = os.path.getsize(theme_path)
                    dbg(f"  tamaño TAR: {sz} bytes")
                except Exception:
                    pass
            dbg(f"  push_mode: {push_mode}")
            dbg(f"  store_url: {store_url}")
            dbg(f"  shopify_container: {shopify_container}")

            if is_tar:
                dbg(f"Copiando TAR al contenedor: {theme_path}")
                remote_tar = "/tmp/shopify-import.tar"
                code, _, err = self._run(["docker", "cp", theme_path, f"{shopify_container}:{remote_tar}"])
                if code != 0:
                    raise RuntimeError(err or "No se pudo copiar el TAR al contenedor")

                events.put(("progress", (30.0, "[1/3] Extrayendo TAR en el contenedor...")))
                safe_choice = import_theme_name.replace("'", "'\"'\"'")
                extract_cmd = (
                    "rm -rf /tmp/wpu-import-src && mkdir -p /tmp/wpu-import-src; "
                    f"tar xf {remote_tar} -C /tmp/wpu-import-src 2>/dev/null || exit 2; "
                    f"src_dir=$(find /tmp/wpu-import-src -type d -name '{safe_choice}' 2>/dev/null | head -1); "
                    "if [ -z \"$src_dir\" ]; then "
                    "  src_dir=$(find /tmp/wpu-import-src -type d -path '*/config' 2>/dev/null | sed 's#/config$##' | head -1); "
                    "fi; "
                    "[ -n \"$src_dir\" ] || exit 3; "
                    f"mkdir -p {theme_container_path}; "
                    f"rm -rf {theme_container_path}/* {theme_container_path}/.[!.]* 2>/dev/null; "
                    f"(cd \"$src_dir\" && tar cf - .) | (cd {theme_container_path} && tar xf -); "
                    f"rm -f {remote_tar}; rm -rf /tmp/wpu-import-src"
                )
                dbg(f"Ejecutando extract_cmd: {extract_cmd}")
                code, out, err = self._run([
                    "docker", "exec", "-u", "root", shopify_container, "sh", "-c", extract_cmd
                ])
                dbg(f"extract resultado: code={code}, stdout={out[:500] if out else ''}, stderr={err[:500] if err else ''}")
                if code != 0:
                    raise RuntimeError(err or "No se pudo extraer el TAR en el contenedor")
                imported_targets.append((import_theme_name, theme_container_path))

            elif is_dir:
                sources = self._discover_theme_sources_in_folder(theme_path)
                if not sources:
                    sources = [(import_theme_name, theme_path)]

                total_sources = len(sources)
                dbg(f"Temas detectados en carpeta para importar: {[name for name, _ in sources]}")
                for idx, (source_name, source_path) in enumerate(sources, start=1):
                    source_dir_name = re.sub(r"[^a-z0-9._-]+", "-", source_name.lower()).strip("-._") or "horizon"
                    source_container_path = f"/app/{source_dir_name}"
                    dbg(f"Empaquetando carpeta tema [{idx}/{total_sources}]: {source_name} -> {source_container_path}")

                    fd, tmp_tar = tempfile.mkstemp(prefix="shu_theme_", suffix=".tar")
                    os.close(fd)
                    remote_tar = f"/tmp/wpu-import-{idx}.tar"
                    try:
                        with tarfile.open(tmp_tar, "w") as tar:
                            tar.add(source_path, arcname=source_dir_name)
                        pct = 12.0 + (idx / max(1, total_sources)) * 25.0
                        events.put(("progress", (pct, f"[1/3] Copiando tema {idx}/{total_sources} al contenedor...")))
                        code, _, err = self._run(["docker", "cp", tmp_tar, f"{shopify_container}:{remote_tar}"])
                        if code != 0:
                            raise RuntimeError(err or f"No se pudo copiar el tema {source_name} al contenedor")
                        tar_cmd = (
                            f"mkdir -p {source_container_path}; "
                            f"rm -rf {source_container_path}/* {source_container_path}/.[!.]* 2>/dev/null; "
                            f"cd /app && tar xf {remote_tar} && rm -f {remote_tar}"
                        )
                        code, out, err = self._run([
                            "docker", "exec", "-u", "root", shopify_container, "sh", "-c", tar_cmd
                        ])
                        dbg(f"copiar {source_name}: code={code}, stdout={out[:300] if out else ''}, stderr={err[:300] if err else ''}")
                        if code != 0:
                            raise RuntimeError(err or f"No se pudo extraer el tema {source_name} en el contenedor")
                        imported_targets.append((source_name, source_container_path))
                    finally:
                        try:
                            os.remove(tmp_tar)
                        except OSError:
                            pass

                if imported_targets:
                    selected = (tar_theme_choice or "").strip()
                    selected_cf = selected.casefold()
                    chosen = imported_targets[0]
                    for item in imported_targets:
                        if item[0].casefold() == selected_cf:
                            chosen = item
                            break
                    import_theme_name, theme_container_path = chosen
            else:
                raise RuntimeError("El archivo de tema debe ser una carpeta o un TAR.")

            if not imported_targets:
                imported_targets.append((import_theme_name, theme_container_path))

            dbg(f"Tema copiado en el contenedor: {theme_container_path}")
            # Listar contenido del contenedor tras extracción
            ls_code, ls_out, ls_err = self._run([
                "docker", "exec", shopify_container, "sh", "-c",
                f"echo '=== {theme_container_path} ==='; ls -la {theme_container_path}/; "
                f"echo '=== subdirs ==='; for d in {theme_container_path}/*/; do echo \"$d: $(ls $d | wc -l) archivos\"; done; "
                f"echo '=== .liquid count ==='; find {theme_container_path} -name '*.liquid' | wc -l; "
                f"echo '=== .json count ==='; find {theme_container_path} -name '*.json' | wc -l"
            ])
            dbg(f"Contenido contenedor tras extracción:\n{ls_out}")
            # --- AUTO-FIX DE SETTINGS_DATA.JSON ---
            events.put(("progress", (45.0, "[Auto-Fix] Validando settings_data.json...")))
            auto_fix_js = r"""
const fs = require('fs');
console.log('[AUTOFIX-V4] iniciando');
try {
    const dataPath = '__THEME_CONTAINER_PATH__/config/settings_data.json';
    const schemaPath = '__THEME_CONTAINER_PATH__/config/settings_schema.json';
  if (!fs.existsSync(dataPath) || !fs.existsSync(schemaPath)) { console.log('[AUTOFIX-V4] archivos no encontrados'); process.exit(0); }

  const strip = (s) => s.replace(/\\"|"(?:\\"|[^"])*"|(\/\/.*|\/\*[\s\S]*?\*\/)/g, (m, g) => g ? "" : m);
  const data = JSON.parse(strip(fs.readFileSync(dataPath, 'utf8')));
  const schema = JSON.parse(strip(fs.readFileSync(schemaPath, 'utf8')));

  // Construir mapa de reglas del schema
  const rules = {};
  for (const group of schema) {
    if (group.settings) for (const s of group.settings) { if (s.id) rules[s.id] = s; }
    if (group.blocks) for (const b of group.blocks) { if (b.settings) for (const s of b.settings) { if (s.id) rules[s.id] = s; } }
  }
  console.log('[AUTOFIX-V4] schema rules cargadas: ' + Object.keys(rules).length);

  let modified = false;
  const log = [];

  // Recopilar targets: current (si objeto) y cada preset
  const targets = [];
  if (data.current && typeof data.current === 'object') targets.push(data.current);
  if (data.presets && typeof data.presets === 'object') {
    for (const p in data.presets) {
      if (typeof data.presets[p] === 'object') targets.push(data.presets[p]);
    }
  }
  console.log('[AUTOFIX-V4] targets a parchear: ' + targets.length);

  function patchTarget(obj) {
    if (!obj || typeof obj !== 'object') return;
    if (Array.isArray(obj)) { obj.forEach(item => patchTarget(item)); return; }

    for (const k in obj) {
      const val = obj[k];

      // Recurse into nested objects (sections, blocks, etc.)
      if (val && typeof val === 'object') { patchTarget(val); continue; }

      // Solo parchear si hay regla de schema para esta key
      if (!rules[k]) continue;
      const r = rules[k];

      // --- Tipo numerico (number / range): parsear, clampear, alinear step ---
      if (r.type === 'number' || r.type === 'range') {
        let numVal = (typeof val === 'string') ? Number(val.replace(/[^0-9.\-]/g, '')) || 0
                   : (typeof val === 'number') ? val : 0;

        const hasMin = r.min !== undefined;
        const hasMax = r.max !== undefined;
        const minV = hasMin ? Number(r.min) : null;
        const maxV = hasMax ? Number(r.max) : null;
        const stepV = r.step !== undefined ? Number(r.step) : null;

        // Clamp min
        if (hasMin && numVal < minV) {
          log.push('[FIX] ' + k + ': ' + numVal + ' -> min ' + minV);
          numVal = minV;
        }
        // Clamp max
        if (hasMax && numVal > maxV) {
          log.push('[FIX] ' + k + ': ' + numVal + ' -> max ' + maxV);
          numVal = maxV;
        }
        // Alinear al step
        if (stepV && stepV > 0) {
          const base = hasMin ? minV : 0;
          let stepped = Math.round((numVal - base) / stepV) * stepV + base;
          // Re-clamp tras alinear (el redondeo puede sacarlo de rango)
          if (hasMin && stepped < minV) stepped = minV;
          if (hasMax && stepped > maxV) {
            // Bajar al step valido mas cercano por debajo del max
            stepped = Math.floor((maxV - base) / stepV) * stepV + base;
          }
          if (numVal !== stepped) {
            log.push('[FIX] ' + k + ': ' + numVal + ' -> step ' + stepped);
            numVal = stepped;
          }
        }

        // Shopify CLI espera STRINGS en settings_data.json, incluso para numeros
        const finalVal = String(numVal);
        if (obj[k] !== finalVal) {
          log.push('[FIX] ' + k + ': ' + JSON.stringify(obj[k]) + ' -> "' + finalVal + '"');
          obj[k] = finalVal;
          modified = true;
        }
        continue;
      }

      // --- Tipo texto/select/radio/color/html: debe ser string ---
      if ((r.type === 'text' || r.type === 'select' || r.type === 'radio' || r.type === 'color' || r.type === 'html') && typeof val === 'number') {
        obj[k] = String(val);
        modified = true;
        log.push('[FIX] ' + k + ': number -> "' + obj[k] + '"');
        continue;
      }

      // --- Tipo checkbox: debe ser boolean o string "true"/"false" ---
      if (r.type === 'checkbox' && typeof val !== 'boolean' && val !== 'true' && val !== 'false') {
        obj[k] = val ? 'true' : 'false';
        modified = true;
        log.push('[FIX] ' + k + ': ' + JSON.stringify(val) + ' -> "' + obj[k] + '"');
      }
    }
  }

  for (const t of targets) patchTarget(t);

  if (log.length > 0) console.log(log.join('\\n'));
  console.log('[AUTOFIX-V4] modified=' + modified);
  if (modified) fs.writeFileSync(dataPath, JSON.stringify(data, null, 2));
  console.log('[AUTOFIX-V4] fin ok');
} catch(e) { console.error('[AUTOFIX-V4] ERROR:', e.message); }
"""
            auto_fix_js = auto_fix_js.replace("__THEME_CONTAINER_PATH__", theme_container_path)
            import tempfile as _af_tmp
            fd_af, af_path = _af_tmp.mkstemp(prefix="shu_af_", suffix=".js")
            try:
                with os.fdopen(fd_af, "w", encoding="utf-8") as fh:
                    fh.write(auto_fix_js)
                self._run(["docker", "cp", af_path, f"{shopify_container}:/tmp/autofix.js"])
                af_code, af_out, af_err = self._run(["docker", "exec", shopify_container, "node", "/tmp/autofix.js"])
                dbg(f"AutoFix resultado: code={af_code}, stdout={af_out[:500] if af_out else ''}, stderr={af_err[:500] if af_err else ''}")
            except Exception as e:
                dbg(f"Error en autofix: {e}")
            finally:
                try:
                    os.remove(af_path)
                except OSError:
                    pass

            events.put(("progress", (50.0, "[2/3] Instalando dependencias npm...")))

            # ── PASO 2: npm install ──────────────────────────────────────────
            check_cancel()
            npm_cmd = f"cd {theme_container_path} && npm install 2>/dev/null || true"
            npm_done = threading.Event()
            npm_result: list[tuple[int, str, str]] = []
            def _do_npm() -> None:
                npm_result.append(self._run(["docker", "exec", shopify_container, "sh", "-c", npm_cmd]))
                npm_done.set()
            threading.Thread(target=_do_npm, daemon=True).start()
            tick = 0
            while not npm_done.is_set():
                check_cancel()
                npm_done.wait(timeout=2.0)
                tick += 1
                pct = 50.0 + min(20.0, tick * 0.5)
                events.put(("progress", (pct, f"[2/3] npm install... ({tick * 2}s)")))
            dbg(f"npm install resultado: {npm_result[0][0] if npm_result else 'n/a'}")
            events.put(("progress", (70.0, "[2/3] npm install completado.")))

            events.put(("progress", (88.0, "[3/3] Activando tema importado...")))
            dbg(f"Activando tema importado: {import_theme_name} ({theme_container_path})")
            safe_theme_name = (
                import_theme_name.replace("\\", "\\\\")
                .replace("&", "\\&")
                .replace("|", "\\|")
                .replace('"', '\\"')
                .replace("$", "\\$")
            )
            activate_cmd = (
                f"mkdir -p {theme_container_path}; "
                f"printf '%s' '{theme_container_path}' > /app/.active_theme_dir; "
                "if [ -f /app/entrypoint.sh ]; then "
                f"  sed -i \"s|^THEME_NAME=.*$|THEME_NAME={safe_theme_name}|\" /app/entrypoint.sh 2>/dev/null || true; "
                f"  sed -i \"s|^THEME_DIR=.*$|THEME_DIR={theme_container_path}|\" /app/entrypoint.sh 2>/dev/null || true; "
                "fi"
            )
            act_code, act_out, act_err = self._run([
                "docker", "exec", "-u", "root", shopify_container, "sh", "-c", activate_cmd
            ])
            dbg(f"activar tema: code={act_code}, stdout={act_out[:300] if act_out else ''}, stderr={act_err[:300] if act_err else ''}")

            if push_mode == "none":
                events.put(("progress", (92.0, "[3/3] Reiniciando contenedor para aplicar tema activo...")))
                dbg("Reiniciando contenedor para aplicar tema activo...")
                rs_code, rs_out, rs_err = self._run(["docker", "restart", shopify_container])
                dbg(f"restart resultado: code={rs_code}, stdout={rs_out}, stderr={rs_err}")
                dbg("=== IMPORT LOCAL FINALIZADO ===")
                events.put(("done", theme_container_path))
                events.put(("workspace_ready", (shopify_container, theme_path)))
                return

            # ── PASO 3: theme push via terminal interactiva ──────────────────
            check_cancel()
            events.put(("progress", (72.0, "[3/3] Abriendo terminal para login y theme push...")))
            dbg("Limpiando sesion anterior...")
            self._run(["docker", "exec", shopify_container, "sh", "-c",
                "rm -f /tmp/shopify_push_ok /tmp/shopify_push_fail 2>/dev/null || true"])

            safe_store = store_url.replace('"', '').replace("'", "")
            push_inner = (
                f"cd {theme_container_path}"
                f" && shopify theme push --store {safe_store}"
                f" && touch /tmp/shopify_push_ok"
                f" || touch /tmp/shopify_push_fail"
            )

            def _bat_cmd(args: list[str]) -> str:
                final_args = self._build_docker_command(args)
                if final_args and final_args[0].lower() == "docker":
                    try:
                        final_args[0] = self._resolver_comando("docker")
                    except Exception:
                        pass
                return subprocess.list2cmdline(final_args)

            login_cmd = _bat_cmd(["docker", "exec", "-it", shopify_container, "shopify", "auth", "login"])
            auth_marker_cmd = _bat_cmd(["docker", "exec", shopify_container, "sh", "-c", "touch /tmp/shopify_auth_ok"])
            auth_check_cmd = _bat_cmd(["docker", "exec", shopify_container, "sh", "-c", "test -f /tmp/shopify_auth_ok"])
            push_fail_cmd = _bat_cmd(["docker", "exec", shopify_container, "sh", "-c", "touch /tmp/shopify_push_fail"])
            bat_lines = [
                "@echo off",
                "echo Verificando sesion Shopify CLI...",
                "echo.",
                auth_check_cmd,
                "SET AUTH_MARKER=%ERRORLEVEL%",
                "IF %AUTH_MARKER% NEQ 0 (",
                "    echo Iniciando login con Shopify CLI...",
                "    echo.",
                f"    {login_cmd}",
                "    SET LOGIN_CODE=%ERRORLEVEL%",
                "    IF %LOGIN_CODE% NEQ 0 (",
                f"        {push_fail_cmd}",
                "        echo.",
                "        echo ERROR: Login fallido o cancelado.",
                "        echo Puedes cerrar esta ventana.",
                "        pause",
                "        exit /b 1",
                "    )",
                f"    {auth_marker_cmd}",
                ") ELSE (",
                "    echo Sesion Shopify existente detectada. Se omite login.",
                ")",
                "echo.",
                "echo Login completado. Subiendo tema a Shopify...",
                "echo.",
                _bat_cmd(["docker", "exec", "-it", shopify_container, "sh", "-c", push_inner]),
                "echo.",
                "echo Subida finalizada. Puedes cerrar esta ventana.",
                "pause",
            ]
            if self.docker_mode == "remote" and self.docker_host:
                bat_lines[1:1] = [
                    f"set DOCKER_HOST={self.docker_host}",
                    "set DOCKER_CONTEXT=",
                    "set DOCKER_TLS_VERIFY=",
                    "set DOCKER_CERT_PATH=",
                    "set DOCKER_TLS=",
                    "echo Docker remoto: %DOCKER_HOST%",
                    "echo.",
                ]
            bat_content = "\r\n".join(bat_lines) + "\r\n"

            import tempfile as _tmp
            fd2, bat_path = _tmp.mkstemp(prefix="shu_push_", suffix=".bat")
            try:
                with os.fdopen(fd2, "w", encoding="cp1252", errors="replace") as fh:
                    fh.write(bat_content)
            except Exception:
                try:
                    os.close(fd2)
                except OSError:
                    pass

            try:
                subprocess.Popen(
                    ["cmd.exe", "/c", "start", "Shopify Push", "cmd.exe", "/c", bat_path],
                    shell=False,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                dbg(f"Terminal abierta: shopify auth login + theme push --store {safe_store}")
            except Exception as exc:
                raise RuntimeError(f"No se pudo abrir la terminal: {exc}")

            events.put(("push_terminal_opened", shopify_container))

            # Esperar señal de exito o fallo
            push_ok = False
            push_fail = False
            for tick2 in range(600):
                check_cancel()
                time.sleep(1.0)
                rc_ok, _, _  = self._run(["docker", "exec", shopify_container,
                    "sh", "-c", "test -f /tmp/shopify_push_ok"])
                rc_fail, _, _ = self._run(["docker", "exec", shopify_container,
                    "sh", "-c", "test -f /tmp/shopify_push_fail"])
                if rc_ok == 0:
                    push_ok = True
                    dbg("theme push completado.")
                    break
                if rc_fail == 0:
                    push_fail = True
                    dbg("theme push fallido.")
                    break
                if tick2 % 20 == 0 and tick2 > 0:
                    events.put(("progress", (72.0 + min(20.0, tick2 * 0.03),
                        f"[3/3] Esperando push en la terminal... ({tick2}s)")))

            if push_fail:
                raise RuntimeError(
                    "Login o theme push fallidos en la terminal.\n\n"
                    "Revisa la ventana de terminal para ver el error."
                )
            if not push_ok:
                raise RuntimeError("Timeout esperando el theme push (10 minutos).")

            events.put(("progress", (95.0, "[3/3] Reiniciando contenedor para aplicar tema activo...")))
            dbg("Reiniciando contenedor tras push para aplicar tema activo...")
            rs_code, rs_out, rs_err = self._run(["docker", "restart", shopify_container])
            dbg(f"restart resultado: code={rs_code}, stdout={rs_out}, stderr={rs_err}")

            events.put(("done", theme_container_path))
        except Exception as exc:
            events.put(("debug", f"ERROR: {exc}"))
            events.put(("error", str(exc)))

    def _poll_import_worker_queue_simple(
        self,
        window: ttk.Frame,
        status_var: tk.StringVar,
        progress_var: tk.DoubleVar,
        import_button: ttk.Button,
        cancel_button: ttk.Button,
        stop_button: ttk.Button,
        events: queue.Queue[tuple[str, object]],
        shopify_container: str,
        debug_window: "tk.Toplevel | None" = None,
        debug_text: "tk.Text | None" = None,
    ) -> None:
        completed: str | None = None
        failed: str | None = None
        latest_progress: tuple[float, str] | None = None
        debug_lines: list[str] = []
        processed = 0
        workspace_payload: tuple | None = None

        while processed < 120:
            try:
                kind, payload = events.get_nowait()
            except queue.Empty:
                break
            processed += 1

            if kind == "progress":
                value, text = payload  # type: ignore[misc]
                latest_progress = (float(value), str(text))
            elif kind == "debug":
                debug_lines.append(str(payload))
            elif kind == "workspace_ready":
                workspace_payload = payload  # type: ignore[assignment]
            elif kind == "push_terminal_opened":
                status_var.set("Terminal abierta — sigue los pasos en la ventana negra...")
                info_dlg = tk.Toplevel(self.root)
                info_dlg.title("Login Shopify — terminal abierta")
                info_dlg.geometry("500x240")
                info_dlg.resizable(False, False)
                info_dlg.grab_set()
                info_dlg.configure(bg="#f6f6f7")
                tk.Label(info_dlg, text="Sigue los pasos en la terminal",
                         font=("Segoe UI Semibold", 13), bg="#f6f6f7", fg="#008060").pack(pady=(18, 6))
                tk.Label(info_dlg,
                         text="Se ha abierto una ventana de terminal.\n\n"
                              "Pasos:\n"
                              "  1. Sigue las instrucciones de Shopify CLI (login)\n"
                              "  2. Abre la URL en el navegador y confirma el codigo\n"
                              "  3. El theme push se ejecutara automaticamente tras el login\n"
                              "  4. Cuando la terminal diga 'Subida finalizada',\n"
                              "     esta app terminara automaticamente.\n\n"
                              "NO cierres la terminal hasta que termine.",
                         font=("Segoe UI", 10), bg="#f6f6f7", fg="#6d7175",
                         justify="left").pack(padx=20, pady=(0, 14))
                ttk.Button(info_dlg, text="Entendido", command=info_dlg.destroy).pack()
                info_dlg.wait_window()
            elif kind == "done":
                completed = str(payload)
            elif kind == "error":
                failed = str(payload)

        for line in debug_lines:
            self._append_import_debug(debug_window, debug_text, line)

        if latest_progress is not None:
            progress_var.set(latest_progress[0])
            status_var.set(latest_progress[1])

        if completed is not None:
            stop_button.configure(state="disabled")
            progress_var.set(100)
            status_var.set("Importacion completada correctamente.")
            self.log_event("IMPORT", shopify_container, "OK",
                           f"Tema importado en {completed}")
            self.refresh_history()
            msg = f"Importacion completada.\n\nTema en el contenedor: {completed}"
            messagebox.showinfo("Importar", msg)
            return

        if failed is not None:
            stop_button.configure(state="disabled")
            import_button.configure(state="normal")
            cancel_button.configure(state="normal")
            if failed == "IMPORT_CANCELLED_BY_USER":
                status_var.set("Importacion cancelada.")
                messagebox.showinfo("Importar", "Importacion cancelada por el usuario.")
                return
            self.log_event("IMPORT", shopify_container, "ERROR", failed)
            self.refresh_history()
            status_var.set(f"Error: {failed[:80]}")
            messagebox.showerror("Importar", f"La importacion fallo.\n\n{failed}")
            return

        delay = 40 if processed >= 120 else 150
        self.root.after(delay, lambda: self._poll_import_worker_queue_simple(
            window=window,
            status_var=status_var,
            progress_var=progress_var,
            import_button=import_button,
            cancel_button=cancel_button,
            stop_button=stop_button,
            events=events,
            shopify_container=shopify_container,
            debug_window=debug_window,
            debug_text=debug_text,
        ))

    # ── Debug console helpers ─────────────────────────────────────────────────

    @staticmethod
    def _debug_timestamp() -> str:
        return datetime.now().strftime("%H:%M:%S")

    def _append_import_debug(self, debug_window: "tk.Toplevel | None", debug_text: "tk.Text | None", message: str) -> None:
        if debug_window is None or debug_text is None:
            return
        try:
            if not debug_window.winfo_exists() or not debug_text.winfo_exists():
                return
        except Exception:
            return
        line = f"[{self._debug_timestamp()}] {message.strip()}\n"
        debug_text.configure(state="normal")
        debug_text.insert("end", line)
        debug_text.see("end")
        debug_text.configure(state="disabled")

    def _open_import_debug_console(self, parent: tk.Misc) -> "tuple[tk.Toplevel, tk.Text]":
        debug_window = tk.Toplevel(parent)
        debug_window.title("Debug importacion")
        debug_window.geometry("900x360")
        try:
            debug_window.transient(parent)  # type: ignore[arg-type]
        except Exception:
            pass

        body = ttk.Frame(debug_window, padding=8)
        body.pack(fill="both", expand=True)
        body.columnconfigure(0, weight=1)
        body.rowconfigure(0, weight=1)

        debug_text = tk.Text(
            body, wrap="word", bg="#0b1220", fg="#cbd5e1",
            insertbackground="#cbd5e1", relief="flat", borderwidth=1,
            font=("Consolas", 10),
        )
        debug_text.grid(row=0, column=0, sticky="nsew")

        y_scroll = ttk.Scrollbar(body, orient="vertical", command=debug_text.yview)
        y_scroll.grid(row=0, column=1, sticky="ns")
        debug_text.configure(yscrollcommand=y_scroll.set)

        actions = ttk.Frame(body)
        actions.grid(row=1, column=0, columnspan=2, sticky="e", pady=(8, 0))
        ttk.Button(actions, text="Copiar log",
                   command=lambda: self._copy_debug_text_to_clipboard(debug_window, debug_text)).pack(side="right", padx=(0, 8))
        ttk.Button(actions, text="Limpiar",
                   command=lambda: self._clear_debug_text(debug_text)).pack(side="right")

        self._append_import_debug(debug_window, debug_text, "Consola de debug iniciada.")
        return debug_window, debug_text

    @staticmethod
    def _clear_debug_text(debug_text: tk.Text) -> None:
        try:
            if not debug_text.winfo_exists():
                return
        except Exception:
            return
        debug_text.configure(state="normal")
        debug_text.delete("1.0", "end")
        debug_text.configure(state="disabled")

    def _copy_debug_text_to_clipboard(self, debug_window: tk.Toplevel, debug_text: tk.Text) -> None:
        try:
            if not debug_window.winfo_exists() or not debug_text.winfo_exists():
                return
        except Exception:
            return
        content = debug_text.get("1.0", "end").strip()
        if not content:
            messagebox.showinfo("Debug", "No hay contenido para copiar.", parent=debug_window)
            return
        debug_window.clipboard_clear()
        debug_window.clipboard_append(content)
        messagebox.showinfo("Debug", "Log copiado al portapapeles.", parent=debug_window)

    # ── VS Code Remote-SSH automático ────────────────────────────────────────

    def _get_ssh_key_path(self) -> str:
        """Devuelve la ruta de la clave privada SSH del usuario actual (id_ed25519 o id_rsa)."""
        ssh_dir = os.path.join(os.path.expanduser("~"), ".ssh")
        for name in ("id_ed25519", "id_rsa"):
            candidate = os.path.join(ssh_dir, name)
            if os.path.isfile(candidate):
                return candidate
        # Preferir ed25519 para crear si no existe ninguna
        return os.path.join(ssh_dir, "id_ed25519")

    def _generate_ssh_key_if_needed(self) -> tuple[bool, str, str]:
        """
        Genera un par de claves SSH ed25519 si no existe ninguna.
        Devuelve (ok, private_key_path, mensaje).
        """
        key_path = self._get_ssh_key_path()
        pub_path = key_path + ".pub"
        if os.path.isfile(key_path) and os.path.isfile(pub_path):
            return True, key_path, f"Clave existente: {key_path}"
        # Crear directorio ~/.ssh si no existe
        ssh_dir = os.path.dirname(key_path)
        os.makedirs(ssh_dir, exist_ok=True)
        if sys.platform == "win32":
            try:
                subprocess.run(
                    ["icacls", ssh_dir, "/inheritance:r",
                     "/grant:r", f"{os.environ.get('USERNAME', 'User')}:(OI)(CI)F"],
                    capture_output=True, timeout=10,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
            except Exception:
                pass
        try:
            result = subprocess.run(
                ["ssh-keygen", "-t", "ed25519", "-f", key_path, "-N", "", "-C", "shopify-utilidades"],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode != 0:
                return False, key_path, result.stderr or "ssh-keygen falló"
            return True, key_path, f"Clave generada: {key_path}"
        except FileNotFoundError:
            return False, key_path, "ssh-keygen no encontrado. Instala OpenSSH client."
        except Exception as exc:
            return False, key_path, str(exc)

    def _read_public_key(self, private_key_path: str) -> str:
        """Lee el contenido de la clave pública (.pub)."""
        pub_path = private_key_path + ".pub"
        try:
            with open(pub_path, "r", encoding="utf-8") as f:
                return f.read().strip()
        except Exception:
            return ""

    def _install_pubkey_in_container(self, container: str, pubkey: str) -> tuple[bool, str]:
        """
        Instala la clave pública en /root/.ssh/authorized_keys del contenedor
        usando docker exec (no necesita SSH todavía).
        """
        # Escapar la clave para usarla en sh -c
        safe_key = pubkey.replace("'", "'\"'\"'")
        cmd = (
            "mkdir -p /root/.ssh && "
            "chmod 700 /root/.ssh && "
            f"echo '{safe_key}' >> /root/.ssh/authorized_keys && "
            "sort -u /root/.ssh/authorized_keys -o /root/.ssh/authorized_keys && "
            "chmod 600 /root/.ssh/authorized_keys && "
            "echo OK"
        )
        code, out, err = self._run(["docker", "exec", container, "sh", "-c", cmd])
        if code != 0 or "OK" not in (out or ""):
            return False, err or "No se pudo instalar la clave pública"
        return True, "Clave pública instalada en el contenedor"

    def _ensure_sshd_running(self, container: str, timeout_sec: int = 60) -> tuple[bool, str]:
        """
        Asegura que sshd esté corriendo dentro del contenedor.
        Usa 'ps' en lugar de 'pgrep' porque pgrep no está disponible en node:alpine.
        Si openssh no está instalado aún, lo instala via docker exec y luego arranca sshd.
        Reintenta hasta timeout_sec segundos.
        """
        _CHECK = "ps 2>/dev/null | grep -v grep | grep -q sshd && echo RUNNING || echo STOPPED"

        def _is_running() -> bool:
            _, out, _ = self._run(["docker", "exec", container, "sh", "-c", _CHECK])
            return "RUNNING" in (out or "")

        # Comprobar si ya está activo (puede que el entrypoint lo haya arrancado)
        if _is_running():
            return True, "sshd ya estaba corriendo"

        # Intentar arrancar sshd (puede que ya esté instalado pero no corriendo)
        self._run(["docker", "exec", container, "sh", "-c",
                   "/usr/sbin/sshd 2>/dev/null || sshd 2>/dev/null || true"])
        if _is_running():
            return True, "sshd arrancado correctamente"

        # openssh todavía no instalado → instalarlo ahora via docker exec
        install_cmd = (
            "apk add --no-cache openssh openssh-server 2>/dev/null && "
            "ssh-keygen -A 2>/dev/null || true && "
            "mkdir -p /root/.ssh && chmod 700 /root/.ssh && "
            "touch /root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys && "
            "{ grep -q 'PermitRootLogin yes' /etc/ssh/sshd_config 2>/dev/null || "
            "  echo 'PermitRootLogin yes' >> /etc/ssh/sshd_config; } && "
            "{ grep -q 'PubkeyAuthentication yes' /etc/ssh/sshd_config 2>/dev/null || "
            "  echo 'PubkeyAuthentication yes' >> /etc/ssh/sshd_config; } && "
            "{ grep -q 'PasswordAuthentication no' /etc/ssh/sshd_config 2>/dev/null || "
            "  echo 'PasswordAuthentication no' >> /etc/ssh/sshd_config; } && "
            "/usr/sbin/sshd 2>/dev/null || sshd 2>/dev/null || true"
        )
        self._run(["docker", "exec", container, "sh", "-c", install_cmd])

        # Reintentar hasta timeout_sec con polling cada 3 s
        deadline = time.time() + timeout_sec
        attempt = 0
        while time.time() < deadline:
            if _is_running():
                return True, f"sshd arrancado (intento {attempt + 1})"
            time.sleep(3)
            attempt += 1

        return False, f"sshd no responde tras {timeout_sec}s"

    def _read_ssh_config_entry(self, host_alias: str) -> tuple[bool, str, int]:
        """Lee HostName y Port para un alias en ~/.ssh/config."""
        ssh_dir = os.path.join(os.path.expanduser("~"), ".ssh")
        config_path = os.path.join(ssh_dir, "config")
        if not os.path.isfile(config_path):
            return False, "", 0

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
        except Exception:
            return False, "", 0

        in_target = False
        found = False
        host = ""
        port = 22

        for raw in lines:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue

            lower = line.lower()
            if lower.startswith("host "):
                aliases = line[5:].split()
                in_target = host_alias in aliases
                if in_target:
                    found = True
                continue

            if not in_target:
                continue

            parts = line.split(None, 1)
            if len(parts) != 2:
                continue
            key, value = parts[0].lower(), parts[1].strip()
            if key == "hostname":
                host = value
            elif key == "port":
                try:
                    port = int(value)
                except Exception:
                    pass

        if not found:
            return False, "", 0
        return True, host, port

    def _write_ssh_config_entry(
        self,
        host_alias: str,
        hostname: str,
        ssh_port: int,
        key_path: str,
    ) -> tuple[bool, str]:
        """
        Escribe (o actualiza) una entrada en ~/.ssh/config para el host dado.
        Si ya existe un bloque 'Host <alias>' lo sustituye.
        """
        ssh_dir = os.path.join(os.path.expanduser("~"), ".ssh")
        os.makedirs(ssh_dir, exist_ok=True)
        config_path = os.path.join(ssh_dir, "config")

        new_block = (
            f"Host {host_alias}\n"
            f"  HostName {hostname}\n"
            f"  Port {ssh_port}\n"
            f"  User root\n"
            f"  IdentityFile {key_path}\n"
            f"  StrictHostKeyChecking no\n"
            f"  ServerAliveInterval 60\n"
        )

        try:
            existing = ""
            if os.path.isfile(config_path):
                with open(config_path, "r", encoding="utf-8") as f:
                    existing = f.read()

            # Eliminar bloque antiguo con el mismo alias si existe
            import re as _re
            pattern = rf"(?m)^Host {re.escape(host_alias)}\s*\n(?:[ \t]+.*\n)*"
            cleaned = _re.sub(pattern, "", existing)

            # Añadir nuevo bloque al final
            final = cleaned.rstrip("\n") + "\n\n" + new_block
            with open(config_path, "w", encoding="utf-8") as f:
                f.write(final)

            return True, f"~/.ssh/config actualizado (alias: {host_alias})"
        except Exception as exc:
            return False, str(exc)

    def _show_vscode_ssh_setup_dialog(
        self,
        shopify_container: str,
        ssh_port: int,
        ws_path: str,
    ) -> None:
        """
        Diálogo que pregunta si el usuario quiere configurar acceso VS Code via SSH.
        Si acepta:
          1. Genera clave SSH en el PC si no existe
          2. Copia la clave pública al contenedor via docker exec
          3. Escribe ~/.ssh/config con el host configurado
          4. Detecta si es local (127.0.0.1) o remoto (IP del docker_host)
          5. Ofrece botón "Abrir en VS Code" al terminar
        """
        # Determinar hostname: local o IP remota
        is_remote = self.docker_mode == "remote" and bool(self.docker_host)
        if is_remote:
            parsed = self._extract_host_port_from_docker_host(self.docker_host)
            hostname = parsed[0] if parsed else (self._extract_ssh_host_from_docker_host(self.docker_host) or "127.0.0.1")
        else:
            hostname = "127.0.0.1"

        host_alias = shopify_container
        existing_cfg, existing_host, existing_port = self._read_ssh_config_entry(host_alias)
        if existing_cfg:
            if existing_host:
                hostname = existing_host
            if existing_port > 0:
                ssh_port = existing_port

        dlg = tk.Toplevel(self.root)
        dlg.title("Configurar acceso VS Code via SSH")
        dlg.resizable(False, False)
        dlg.transient(self.root)
        dlg.grab_set()
        dlg.configure(bg="#f6f6f7")

        self.root.update_idletasks()
        x = self.root.winfo_x() + (self.root.winfo_width() - 560) // 2
        y = self.root.winfo_y() + (self.root.winfo_height() - 520) // 2
        dlg.geometry(f"560x520+{x}+{y}")

        # ── Encabezado ────────────────────────────────────────────────────────
        hdr = tk.Frame(dlg, bg="#0f766e", padx=20, pady=14)
        hdr.pack(fill="x", side="top")
        tk.Label(hdr, text="🔑  VS Code Remote-SSH automático",
                 font=("Segoe UI Semibold", 13), bg="#0f766e", fg="#ffffff").pack(anchor="w")
        tk.Label(hdr, text="Edita el código directamente en el contenedor desde VS Code",
                 font=("Segoe UI", 9), bg="#0f766e", fg="#99f6e4").pack(anchor="w", pady=(2, 0))

        # ── Botones (abajo) ───────────────────────────────────────────────────
        btn_frame = tk.Frame(dlg, bg="#e2e8f0", padx=20, pady=12)
        btn_frame.pack(fill="x", side="bottom")
        tk.Frame(dlg, bg="#d1d5db", height=1).pack(fill="x", side="bottom")

        # ── Cuerpo ────────────────────────────────────────────────────────────
        body = tk.Frame(dlg, bg="#f6f6f7", padx=20, pady=12)
        body.pack(fill="both", expand=True, side="top")

        tk.Label(body, text="¿Qué va a hacer esta configuración?",
                 font=("Segoe UI Semibold", 10), bg="#f6f6f7", fg="#202223").pack(anchor="w", pady=(0, 6))

        if existing_cfg:
            steps_text = (
                f"Se detectó configuración SSH existente para este contenedor:\n"
                f"  Host {host_alias}\n"
                f"    HostName  {hostname}\n"
                f"    Port      {ssh_port}\n\n"
                "Si pulsas continuar sin cambios manuales, se abrirá VS Code directamente.\n"
                "Si modificas HostName o Port, se actualizará ~/.ssh/config y luego se abrirá VS Code."
            )
        else:
            steps_text = (
                f"1.  Generar clave SSH en tu PC  (si no existe ya)\n"
                f"2.  Instalar la clave pública en el contenedor\n"
                f"    via docker exec  (sin necesitar SSH todavía)\n"
                f"3.  Escribir ~/.ssh/config  con el host configurado:\n"
                f"      Host {host_alias}\n"
                f"        HostName  {hostname}\n"
                f"        Port      {ssh_port}\n"
                f"        User      root\n"
                f"4.  Abrir VS Code directamente con Remote-SSH"
            )
        info_box = tk.Frame(body, bg="#e0f2fe", padx=12, pady=10)
        info_box.pack(fill="x", pady=(0, 10))
        steps_view = tk.Text(
            info_box,
            height=7,
            font=("Consolas", 9),
            bg="#e0f2fe",
            fg="#0c4a6e",
            relief="flat",
            borderwidth=0,
            highlightthickness=0,
            wrap="word",
        )
        steps_view.pack(fill="x")
        steps_view.insert("1.0", steps_text)
        steps_view.configure(state="disabled")

        # ── Host y puerto editables ───────────────────────────────────────────
        hostname_var = tk.StringVar(value=hostname)
        port_var = tk.StringVar(value=str(ssh_port))
        if existing_cfg:
            tk.Label(body,
                     text="Configuración existente detectada: puedes modificar HostName/Port manualmente.",
                     font=("Segoe UI", 9), bg="#f6f6f7", fg="#92400e").pack(anchor="w", pady=(0, 6))
        elif not is_remote:
            tk.Label(body,
                     text="Modo local detectado → por defecto se usará 127.0.0.1",
                     font=("Segoe UI", 9), bg="#f6f6f7", fg="#059669").pack(anchor="w", pady=(0, 6))

        inputs = tk.Frame(body, bg="#f6f6f7")
        inputs.pack(fill="x", pady=(4, 10))
        tk.Label(inputs, text="HostName:", font=("Segoe UI", 9), bg="#f6f6f7", fg="#6d7175").grid(row=0, column=0, sticky="w")
        ttk.Entry(inputs, textvariable=hostname_var, font=("Consolas", 10)).grid(row=0, column=1, sticky="ew", padx=(10, 0))
        tk.Label(inputs, text="Port:", font=("Segoe UI", 9), bg="#f6f6f7", fg="#6d7175").grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(inputs, textvariable=port_var, font=("Consolas", 10), width=12).grid(row=1, column=1, sticky="w", padx=(10, 0), pady=(8, 0))
        inputs.columnconfigure(1, weight=1)

        # ── Estado y progreso ─────────────────────────────────────────────────
        status_lbl = tk.Label(body, text="", font=("Segoe UI", 9),
                              bg="#f6f6f7", fg="#008060", wraplength=510, justify="left")
        status_lbl.pack(anchor="w")

        log_text = tk.Text(body, height=5, font=("Consolas", 8),
                           bg="#1e293b", fg="#94a3b8", relief="flat",
                           state="disabled", wrap="word")
        log_text.pack(fill="x", pady=(6, 0))

        def _log(msg: str) -> None:
            log_text.configure(state="normal")
            log_text.insert("end", msg + "\n")
            log_text.see("end")
            log_text.configure(state="disabled")
            dlg.update_idletasks()

        result: dict = {"done": False, "success": False}

        def do_setup() -> None:
            setup_btn.configure(state="disabled")
            skip_btn.configure(state="disabled")
            h = hostname_var.get().strip() or hostname
            p_raw = port_var.get().strip() or str(ssh_port)
            try:
                p = int(p_raw)
                if not (1 <= p <= 65535):
                    raise ValueError("range")
            except Exception:
                status_lbl.configure(text="✘  Puerto SSH inválido (1-65535).", fg="#dc2626")
                setup_btn.configure(state="normal")
                skip_btn.configure(state="normal")
                return

            same_as_existing = existing_cfg and h == (existing_host or "") and p == existing_port

            if same_as_existing:
                status_lbl.configure(text="Configuración existente sin cambios. Conectando a VS Code...", fg="#0f766e")
                _log(f"[INFO] SSH | {shopify_container} | Configuración existente detectada, sin cambios manuales.")
                _log(f"[INFO] SSH | {shopify_container} | Abriendo VS Code con alias '{host_alias}'...")
                self.log_event("SSH", shopify_container, "INFO", "Configuración SSH existente reutilizada (sin cambios).")
                dlg.after(120, lambda: _open_vscode_ssh(host_alias, ws_path))
                return

            if existing_cfg:
                status_lbl.configure(text="Actualizando configuración SSH...", fg="#f59e0b")
                _log(f"[INFO] SSH | {shopify_container} | Verificando ~/.ssh/config...")

                def _worker_update_only() -> None:
                    ok, key_path, msg = self._generate_ssh_key_if_needed()
                    dlg.after(0, lambda m=msg: _log(f"  {m}"))
                    if not ok:
                        dlg.after(0, lambda m=msg: (
                            status_lbl.configure(text=f"✘  Error: {m}", fg="#dc2626"),
                            setup_btn.configure(state="normal"),
                            skip_btn.configure(state="normal"),
                        ))
                        return

                    ok4, msg4 = self._write_ssh_config_entry(host_alias, h, p, key_path)
                    dlg.after(0, lambda m=msg4: _log(f"  {m}"))
                    if not ok4:
                        dlg.after(0, lambda m=msg4: (
                            status_lbl.configure(text=f"✘  {m}", fg="#dc2626"),
                            setup_btn.configure(state="normal"),
                            skip_btn.configure(state="normal"),
                        ))
                        return

                    self.log_event("SSH-SETUP", shopify_container, "OK",
                                   f"Remote-SSH actualizado manualmente: {host_alias} → {h}:{p}")
                    dlg.after(0, lambda: (
                        status_lbl.configure(text="✔  Configuración actualizada. Abriendo VS Code...", fg="#059669"),
                        _log("✔  Configuración completada."),
                    ))
                    dlg.after(150, lambda: _open_vscode_ssh(host_alias, ws_path))

                threading.Thread(target=_worker_update_only, daemon=True).start()
                return

            status_lbl.configure(text="Configurando acceso SSH...", fg="#f59e0b")
            _log(f"[INFO] SSH | {shopify_container} | Verificando openssh-server en el contenedor...")

            # Ejecutar en hilo para no bloquear la UI (sshd puede tardar ~60s)
            def _worker() -> None:
                # Paso 1: generar clave
                dlg.after(0, lambda: _log("[INFO] SSH | Clave SSH local: verificando/generando..."))
                ok, key_path, msg = self._generate_ssh_key_if_needed()
                dlg.after(0, lambda m=msg: _log(f"  {m}"))
                if not ok:
                    dlg.after(0, lambda m=msg: (
                        status_lbl.configure(text=f"✘  Error: {m}", fg="#dc2626"),
                        setup_btn.configure(state="normal"),
                        skip_btn.configure(state="normal"),
                    ))
                    return

                pubkey = self._read_public_key(key_path)
                if not pubkey:
                    dlg.after(0, lambda: (
                        status_lbl.configure(text="✘  No se pudo leer la clave pública.", fg="#dc2626"),
                        setup_btn.configure(state="normal"),
                        skip_btn.configure(state="normal"),
                    ))
                    return

                # Paso 2: instalar clave en contenedor
                dlg.after(0, lambda: _log(f"[INFO] SSH | {shopify_container} | Inyectando clave pública en el contenedor..."))
                ok2, msg2 = self._install_pubkey_in_container(shopify_container, pubkey)
                dlg.after(0, lambda m=msg2: _log(f"  {m}"))
                if not ok2:
                    dlg.after(0, lambda m=msg2: (
                        status_lbl.configure(text=f"✘  {m}", fg="#dc2626"),
                        setup_btn.configure(state="normal"),
                        skip_btn.configure(state="normal"),
                    ))
                    return

                # Paso 2b: asegurar que sshd esté corriendo (instala openssh si hace falta)
                dlg.after(0, lambda: (
                    _log(f"[INFO] SSH | {shopify_container} | Configurando y arrancando sshd..."),
                    status_lbl.configure(text="Instalando/arrancando sshd... por favor espera.", fg="#f59e0b"),
                ))
                ok3, msg3 = self._ensure_sshd_running(shopify_container, timeout_sec=90)
                dlg.after(0, lambda m=msg3: _log(f"  {m}"))
                if not ok3:
                    # sshd no arrancó pero seguimos — ~/.ssh/config ya es útil para cuando arranque
                    dlg.after(0, lambda m=msg3: _log(f"  ⚠  {m}"))
                else:
                    dlg.after(0, lambda: _log(f"[INFO] SSH | {shopify_container} | sshd corriendo en el contenedor."))

                dlg.after(0, lambda: _log(f"[INFO] SSH | {shopify_container} | Detectando puerto SSH del contenedor..."))
                dlg.after(0, lambda: _log(f"[INFO] SSH | {shopify_container} | SSH accesible en {h}:{p}"))

                # Paso 3: escribir ~/.ssh/config
                dlg.after(0, lambda: _log(f"[INFO] SSH | {shopify_container} | Verificando ~/.ssh/config..."))
                ok4, msg4 = self._write_ssh_config_entry(host_alias, h, p, key_path)
                dlg.after(0, lambda m=msg4: _log(f"  {m}"))
                if not ok4:
                    dlg.after(0, lambda m=msg4: (
                        status_lbl.configure(text=f"✘  {m}", fg="#dc2626"),
                        setup_btn.configure(state="normal"),
                        skip_btn.configure(state="normal"),
                    ))
                    return

                self.log_event("SSH-SETUP", shopify_container, "OK",
                               f"Remote-SSH configurado: {host_alias} → {h}:{p}")

                result["done"] = True
                result["success"] = True
                result["key_path"] = key_path
                result["host_alias"] = host_alias

                if ok3:
                    final_msg = f"✔  Listo. Conecta desde VS Code con:\n   ssh-remote+{host_alias}"
                    final_color = "#059669"
                else:
                    final_msg = (
                        f"⚠  ~/.ssh/config configurado, pero sshd aún no responde.\n"
                        f"   Espera 1-2 min (primera instalación) y pulsa 'Abrir en VS Code'."
                    )
                    final_color = "#92400e"

                dlg.after(0, lambda: (
                    status_lbl.configure(text=final_msg, fg=final_color),
                    _log("✔  Configuración completada."),
                    setup_btn.configure(
                        text="🗒  Abrir en VS Code (Remote-SSH)",
                        state="normal",
                        command=lambda: _open_vscode_ssh(host_alias, ws_path),
                    ),
                    skip_btn.configure(text="Cerrar", state="normal", command=dlg.destroy),
                ))

            threading.Thread(target=_worker, daemon=True).start()

        def _open_vscode_ssh(alias: str, ws: str) -> None:
            """Abre VS Code conectado via Remote-SSH al contenedor."""
            vscode_uri = f"vscode://vscode-remote/ssh-remote+{alias}/app"
            try:
                if sys.platform == "win32":
                    subprocess.Popen(
                        ["cmd", "/c", "start", "", vscode_uri],
                        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                    )
                elif sys.platform == "darwin":
                    subprocess.Popen(["open", vscode_uri])
                else:
                    subprocess.Popen(["xdg-open", vscode_uri])
            except Exception:
                pass
            dlg.destroy()

        def do_skip() -> None:
            dlg.destroy()

        setup_btn = ttk.Button(
            btn_frame,
            text=("▶  Continuar y abrir VS Code" if existing_cfg else "✔  Configurar SSH y abrir VS Code"),
            style="Accent.TButton",
            command=do_setup,
        )
        setup_btn.pack(side="left")

        skip_btn = ttk.Button(
            btn_frame,
            text="Saltar (lo haré manualmente)",
            style="Ghost.TButton",
            command=do_skip,
        )
        skip_btn.pack(side="left", padx=(10, 0))

        dlg.protocol("WM_DELETE_WINDOW", do_skip)

    # ── Configuración automática DOCKER_HOST en PC cliente ───────────────────

    def _show_docker_host_setup_dialog(self, shopify_container: str, ws_path: str) -> None:
        """
        Muestra un diálogo que pregunta si el usuario quiere configurar DOCKER_HOST
        en este PC para que VS Code pueda conectarse al contenedor remoto.
        Si acepta, aplica la variable de entorno de forma permanente (Windows registry)
        y opcionalmente abre el workspace en VS Code.
        """
        dh = (self.docker_host or "").strip()
        is_remote = self.docker_mode == "remote" and bool(dh)

        # En modo local solo preguntamos si quiere abrir VS Code, sin configurar nada
        if not is_remote:
            if ws_path and messagebox.askyesno(
                "VS Code Workspace",
                f"Workspace VS Code creado:\n{ws_path}\n\n¿Abrirlo en VS Code ahora?",
            ):
                self._open_vscode_workspace(ws_path)
            return

        dlg = tk.Toplevel(self.root)
        dlg.title("Configurar VS Code para edición remota en vivo")
        dlg.resizable(False, False)
        dlg.transient(self.root)
        dlg.grab_set()
        dlg.configure(bg="#f6f6f7")

        self.root.update_idletasks()
        x = self.root.winfo_x() + (self.root.winfo_width() - 540) // 2
        y = self.root.winfo_y() + (self.root.winfo_height() - 520) // 2
        dlg.geometry(f"540x520+{x}+{y}")

        # ── Encabezado (top) ──────────────────────────────────────────────────
        hdr = tk.Frame(dlg, bg="#0f766e", padx=20, pady=14)
        hdr.pack(fill="x", side="top")
        tk.Label(
            hdr,
            text="⚡  Configuración VS Code Remoto",
            font=("Segoe UI Semibold", 13),
            bg="#0f766e",
            fg="#ffffff",
        ).pack(anchor="w")
        tk.Label(
            hdr,
            text="Para que VS Code se conecte al contenedor Docker remoto",
            font=("Segoe UI", 9),
            bg="#0f766e",
            fg="#99f6e4",
        ).pack(anchor="w", pady=(2, 0))

        # ── Botones (declarados ANTES de body para que pack los reserve abajo) ──
        btn_frame = tk.Frame(dlg, bg="#e2e8f0", padx=20, pady=12)
        btn_frame.pack(fill="x", side="bottom")
        tk.Frame(dlg, bg="#d1d5db", height=1).pack(fill="x", side="bottom")

        # ── Cuerpo ────────────────────────────────────────────────────────────
        body = tk.Frame(dlg, bg="#f6f6f7", padx=20, pady=12)
        body.pack(fill="both", expand=True, side="top")

        # ── Explicación ───────────────────────────────────────────────────────
        tk.Label(
            body,
            text="¿Qué va a hacer esta configuración?",
            font=("Segoe UI Semibold", 10),
            bg="#f6f6f7",
            fg="#202223",
        ).pack(anchor="w", pady=(0, 6))

        steps_text = (
            f"1.  Establecer  DOCKER_HOST = {dh}\n"
            f"    en las variables de entorno del sistema (permanente).\n\n"
            f"2.  VS Code usará ese DOCKER_HOST para conectarse\n"
            f"    al contenedor  '{shopify_container}'  en el servidor remoto.\n\n"
            f"3.  Cualquier cambio que hagas en VS Code ocurrirá\n"
            f"    directamente dentro del contenedor  →  live reload\n"
            f"    automático en Shopify en 1-2 segundos."
        )
        info_box = tk.Frame(body, bg="#e0f2fe", padx=12, pady=10, relief="flat")
        info_box.pack(fill="x", pady=(0, 8))
        tk.Label(
            info_box,
            text=steps_text,
            font=("Segoe UI", 9),
            bg="#e0f2fe",
            fg="#0c4a6e",
            justify="left",
        ).pack(anchor="w")

        # ── Valor de DOCKER_HOST editable ─────────────────────────────────────
        tk.Label(
            body,
            text="Valor de DOCKER_HOST (puedes editarlo):",
            font=("Segoe UI", 9),
            bg="#f6f6f7",
            fg="#6d7175",
        ).pack(anchor="w")

        dh_var = tk.StringVar(value=dh)
        dh_entry = ttk.Entry(body, textvariable=dh_var, font=("Consolas", 10))
        dh_entry.pack(fill="x", pady=(4, 12))

        # ── Checkbox abrir VS Code ────────────────────────────────────────────
        open_vscode_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            body,
            text="Abrir el workspace en VS Code al terminar",
            variable=open_vscode_var,
        ).pack(anchor="w", pady=(0, 4))

        restart_note = tk.Label(
            body,
            text="⚠  Es posible que necesites reiniciar VS Code para que tome el nuevo DOCKER_HOST.",
            font=("Segoe UI", 8),
            bg="#f6f6f7",
            fg="#92400e",
            wraplength=490,
            justify="left",
        )
        restart_note.pack(anchor="w", pady=(0, 4))

        result = {"applied": False}
        status_lbl = tk.Label(body, text="", font=("Segoe UI", 9), bg="#f6f6f7", fg="#059669")
        status_lbl.pack(anchor="w")

        # ── Botones (ya declarados arriba, solo definimos las funciones aquí) ──

        def do_apply() -> None:
            host_value = dh_var.get().strip()
            if not host_value:
                messagebox.showwarning("DOCKER_HOST", "El valor no puede estar vacío.", parent=dlg)
                return

            apply_btn.configure(state="disabled")
            skip_btn.configure(state="disabled")
            status_lbl.configure(text="Aplicando...", fg="#f59e0b")
            dlg.update_idletasks()

            ok, msg = self._apply_docker_host_env(host_value)

            if ok:
                result["applied"] = True
                status_lbl.configure(
                    text=f"✔  DOCKER_HOST configurado: {host_value}",
                    fg="#059669",
                )
                self.log_event("VSCODE", shopify_container, "OK", f"DOCKER_HOST configurado: {host_value}")
                dlg.update_idletasks()
                dlg.after(800, lambda: _finish(host_value))
            else:
                status_lbl.configure(text=f"✘  Error: {msg}", fg="#dc2626")
                apply_btn.configure(state="normal")
                skip_btn.configure(state="normal")

        def _finish(host_value: str) -> None:
            dlg.destroy()
            messagebox.showinfo(
                "DOCKER_HOST configurado",
                f"Variable de entorno establecida:\n\n"
                f"DOCKER_HOST = {host_value}\n\n"
                f"Esta configuración es permanente.\n"
                f"Si VS Code estaba abierto, ciérralo y vuelve a abrirlo\n"
                f"para que tome el nuevo valor.",
            )
            if open_vscode_var.get() and ws_path:
                self._open_vscode_workspace(ws_path)

        def do_skip() -> None:
            dlg.destroy()
            # Aunque no configure, pregunta si abrir VS Code
            if ws_path and messagebox.askyesno(
                "VS Code Workspace",
                f"Workspace creado:\n{ws_path}\n\n¿Abrirlo en VS Code ahora?\n\n"
                f"(Recuerda configurar DOCKER_HOST manualmente si es necesario)",
            ):
                self._open_vscode_workspace(ws_path)

        apply_btn = ttk.Button(
            btn_frame,
            text="✔  Configurar DOCKER_HOST automáticamente",
            style="Accent.TButton",
            command=do_apply,
        )
        apply_btn.pack(side="left")

        skip_btn = ttk.Button(
            btn_frame,
            text="Saltar (lo haré manualmente)",
            style="Ghost.TButton",
            command=do_skip,
        )
        skip_btn.pack(side="left", padx=(10, 0))

        dlg.protocol("WM_DELETE_WINDOW", do_skip)

    def _apply_docker_host_env(self, host_value: str) -> tuple[bool, str]:
        """
        Establece DOCKER_HOST como variable de entorno permanente del sistema.
        - Windows: escribe en el registro (HKLM\\System\\CurrentControlSet\\Control\\Session Manager\\Environment)
          y notifica a todas las ventanas del cambio via WM_SETTINGCHANGE.
        - Linux/Mac: añade línea al /etc/environment o ~/.bashrc como fallback.
        Devuelve (True, "") si tuvo éxito o (False, mensaje_error) si falló.
        """
        if sys.platform == "win32":
            return self._apply_docker_host_env_windows(host_value)
        return self._apply_docker_host_env_unix(host_value)

    def _apply_docker_host_env_windows(self, host_value: str) -> tuple[bool, str]:
        """Escribe DOCKER_HOST en el registro de Windows (variables de sistema)."""
        ps_script = f"""
$ErrorActionPreference = 'Stop'
try {{
    # Escribir en variables de sistema (requiere admin) o de usuario como fallback
    $regPathSystem = 'HKLM:\\System\\CurrentControlSet\\Control\\Session Manager\\Environment'
    $regPathUser   = 'HKCU:\\Environment'

    $written = $false
    try {{
        Set-ItemProperty -Path $regPathSystem -Name 'DOCKER_HOST' -Value '{host_value}' -Type String
        $written = $true
        Write-Output 'SYSTEM'
    }} catch {{
        # Sin privilegios de admin, usar variables de usuario
        if (-not (Test-Path $regPathUser)) {{ New-Item -Path $regPathUser -Force | Out-Null }}
        Set-ItemProperty -Path $regPathUser -Name 'DOCKER_HOST' -Value '{host_value}' -Type String
        $written = $true
        Write-Output 'USER'
    }}

    # Notificar a todas las ventanas del cambio de entorno
    if ($written) {{
        Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;
public class WinEnv {{
    [DllImport("user32.dll", SetLastError=true)]
    public static extern IntPtr SendMessageTimeout(
        IntPtr hWnd, uint Msg, UIntPtr wParam, string lParam,
        uint fuFlags, uint uTimeout, out UIntPtr lpdwResult);
    public static readonly IntPtr HWND_BROADCAST = new IntPtr(0xffff);
    public const uint WM_SETTINGCHANGE = 0x001A;
    public const uint SMTO_ABORTIFHUNG = 0x0002;
}}
"@
        $result = [UIntPtr]::Zero
        [WinEnv]::SendMessageTimeout(
            [WinEnv]::HWND_BROADCAST,
            [WinEnv]::WM_SETTINGCHANGE,
            [UIntPtr]::Zero,
            'Environment',
            [WinEnv]::SMTO_ABORTIFHUNG,
            5000,
            [ref]$result
        ) | Out-Null
    }}
    exit 0
}} catch {{
    Write-Error $_.Exception.Message
    exit 1
}}
""".strip()

        try:
            proc = subprocess.run(
                [
                    "powershell.exe",
                    "-NoProfile",
                    "-ExecutionPolicy", "Bypass",
                    "-Command", ps_script,
                ],
                capture_output=True,
                text=True,
                timeout=15,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            if proc.returncode == 0:
                scope = "sistema" if "SYSTEM" in (proc.stdout or "") else "usuario"
                return True, f"Escrito en registro de Windows ({scope})"
            err = (proc.stderr or proc.stdout or "Error desconocido").strip()
            return False, err
        except subprocess.TimeoutExpired:
            return False, "Timeout ejecutando PowerShell"
        except Exception as exc:
            return False, str(exc)

    def _apply_docker_host_env_unix(self, host_value: str) -> tuple[bool, str]:
        """Escribe DOCKER_HOST en /etc/environment (Linux/Mac) o ~/.bashrc como fallback."""
        line = f'DOCKER_HOST="{host_value}"'
        # Intentar /etc/environment (sistema)
        try:
            env_file = "/etc/environment"
            with open(env_file, "r", encoding="utf-8") as f:
                content = f.read()
            # Reemplazar línea existente o añadir nueva
            import re as _re
            if _re.search(r"^DOCKER_HOST=", content, _re.MULTILINE):
                content = _re.sub(r"^DOCKER_HOST=.*$", line, content, flags=_re.MULTILINE)
            else:
                content = content.rstrip("\n") + f"\n{line}\n"
            with open(env_file, "w", encoding="utf-8") as f:
                f.write(content)
            return True, f"Escrito en {env_file}"
        except PermissionError:
            pass
        except Exception as exc:
            return False, str(exc)

        # Fallback: ~/.bashrc y ~/.zshrc del usuario actual
        written_to: list[str] = []
        for rc_file in (
            os.path.expanduser("~/.bashrc"),
            os.path.expanduser("~/.zshrc"),
            os.path.expanduser("~/.profile"),
        ):
            try:
                if not os.path.exists(rc_file):
                    continue
                with open(rc_file, "r", encoding="utf-8") as f:
                    rc_content = f.read()
                import re as _re
                export_line = f'export DOCKER_HOST="{host_value}"'
                if _re.search(r"export DOCKER_HOST=", rc_content):
                    rc_content = _re.sub(
                        r"export DOCKER_HOST=.*$", export_line, rc_content, flags=_re.MULTILINE
                    )
                else:
                    rc_content = rc_content.rstrip("\n") + f"\n{export_line}\n"
                with open(rc_file, "w", encoding="utf-8") as f:
                    f.write(rc_content)
                written_to.append(rc_file)
            except Exception:
                continue

        if written_to:
            return True, f"Escrito en {', '.join(written_to)} (requiere nueva terminal)"
        return False, "Sin permisos para escribir /etc/environment ni archivos de shell"

    # ── VS Code Workspace Colaborativo ───────────────────────────────────────
 
    # Carpeta del tema dentro del contenedor (parametrizable en el futuro)
    CONTAINER_THEME_PATH = "/app/horizon"
 
    def _create_vscode_workspace(
        self,
        shopify_container: str,
        theme_local_path: str = "",
        workspace_dir: str = "",
    ) -> str:
        """
        Crea un .code-workspace de VS Code para el tema Shopify.
 
        MODO LOCAL
        ----------
        VS Code se adjunta directamente al contenedor Docker local vía
        "Dev Containers: Attach to Running Container".  Cualquier guardado
        en /app/horizon es detectado al instante por `shopify theme dev`.
 
        MODO REMOTO COLABORATIVO
        ------------------------
        Todos los programadores que abran este .code-workspace se conectan
        al mismo contenedor (vía SSH o Docker TCP) → editan los mismos
        archivos en /app/horizon → `shopify theme dev --live-reload` propaga
        el cambio al navegador de cualquiera que tenga abierta la preview.
 
        Flujo completo:
          PC-A  guarda templates/index.liquid
            → archivo cambia en el contenedor remoto
              → shopify theme dev detecta el cambio (inotify)
                → live-reload en TODOS los navegadores con la preview abierta
        """
        try:
            return self._create_collaborative_workspace(
                shopify_container=shopify_container,
                theme_local_path=theme_local_path,
                workspace_dir=workspace_dir,
            )
        except Exception as exc:
            import traceback as _tb
            self.log_event(
                "WORKSPACE", shopify_container, "ERROR",
                f"No se pudo crear workspace: {exc}\n{_tb.format_exc()}",
            )
            return ""
 
    def _create_collaborative_workspace(
        self,
        shopify_container: str,
        theme_local_path: str = "",
        workspace_dir: str = "",
    ) -> str:
        """
        Genera los artefactos del workspace colaborativo:
          1. <container>.code-workspace     – abre en VS Code
          2. .devcontainer/devcontainer.json – define el entorno
          3. README-colaboracion.md          – guía para nuevos programadores
          4. open_workspace.ps1 / .sh        – solo en modo TCP remoto
 
        LÓGICA DE URI POR MODO (corrige el error ENOPRO):
        ─────────────────────────────────────────────────
        LOCAL →  attached-container+HEX  (Dev Containers local, funciona siempre)
        TCP   →  NO se pone uri en folders; el devcontainer.json lleva "dockerHost"
                 y se genera un script open_workspace que setea DOCKER_HOST antes
                 de abrir VS Code (único modo fiable con Docker TCP remoto)
        SSH   →  ssh-remote+user@host    (Remote-SSH resuelve el path en el host)
        """
        # ── Directorio de salida ──────────────────────────────────────────────
        if workspace_dir and os.path.isdir(workspace_dir):
            ws_dir = workspace_dir
        elif theme_local_path and os.path.isdir(theme_local_path):
            ws_dir = os.path.dirname(theme_local_path.rstrip("\\/")) or theme_local_path
        else:
            ws_dir = self.app_dir
 
        ws_path        = os.path.join(ws_dir, f"{shopify_container}.code-workspace")
        container_path = self.CONTAINER_THEME_PATH
 
        is_remote = (self.docker_mode == "remote" and bool(self.docker_host))
        dh        = self.docker_host or ""
 
        # ── Determinar modo y construir metadatos ─────────────────────────────
        if is_remote and dh.startswith("ssh://"):
            # ── Modo SSH ──────────────────────────────────────────────────────
            ssh_target = dh[6:]          # user@host
            # SSH Remote resuelve el path directamente en el host remoto
            container_folder_uri = f"vscode-remote://ssh-remote+{ssh_target}{container_path}"
            remote_access_note   = (
                f"SSH: {ssh_target}  →  {container_path}  "
                f"(contenedor {shopify_container})\n"
                "Instala 'Remote - SSH' en VS Code y abre este archivo."
            )
            docker_prefix     = f"docker -H {dh}"
            devcontainer_type = "ssh"
            devcontainer_host = ssh_target
            server_addr       = ssh_target.split("@")[-1]
            # La folder usa la URI ssh-remote
            folder_entry: dict = {
                "name": f"[SSH] {shopify_container} {container_path}  ← EDITA AQUÍ",
                "uri":  container_folder_uri,
            }
 
        elif is_remote:
            # ── Modo TCP remoto ───────────────────────────────────────────────
            # CORRECCIÓN DEL ERROR ENOPRO:
            # vscode-remote://attached-container+HEX SOLO funciona con Docker LOCAL.
            # Con TCP remoto NO existe URI directa soportada por VS Code.
            # Solución: la folder apunta a una ruta LOCAL vacía (el devcontainer.json
            # contiene "dockerHost" para que Dev Containers sepa dónde conectarse),
            # y se genera un script que abre VS Code con DOCKER_HOST ya seteado.
            parsed     = self._extract_host_port_from_docker_host(dh)
            remote_ip  = parsed[0] if parsed else dh
            remote_access_note = (
                f"Docker TCP remoto: {remote_ip}\n"
                f"Contenedor: {shopify_container}  →  {container_path}\n"
                f"USA el script open_workspace.ps1 (Windows) o open_workspace.sh\n"
                f"para abrir VS Code. Ese script setea DOCKER_HOST={dh}\n"
                f"antes de abrir, que es lo que Dev Containers necesita."
            )
            docker_prefix     = f"docker -H {dh}"
            devcontainer_type = "tcp"
            devcontainer_host = dh
            server_addr       = remote_ip
            # Sin "uri" → VS Code abre el workspace sin intentar resolver
            # ningún sistema de archivos remoto hasta que Dev Containers
            # lo gestiona a través del devcontainer.json con "dockerHost"
            folder_entry = {
                "name": f"[TCP {remote_ip}] {shopify_container} {container_path}  ← EDITA AQUÍ",
                "path": ws_dir.replace("\\", "/"),   # carpeta local donde está el workspace
            }
 
        else:
            # ── Modo local ────────────────────────────────────────────────────
            container_hex = shopify_container.encode().hex()
            container_folder_uri = (
                f"vscode-remote://attached-container+{container_hex}{container_path}"
            )
            remote_access_note = (
                f"Docker local\n"
                f"Contenedor: {shopify_container}  →  {container_path}\n"
                "Instala 'Remote - Containers' en VS Code."
            )
            docker_prefix     = "docker"
            devcontainer_type = "local"
            devcontainer_host = ""
            server_addr       = "localhost"
            folder_entry = {
                "name": f"[LOCAL] {shopify_container} {container_path}  ← EDITA AQUÍ",
                "uri":  container_folder_uri,
            }
 
        # ── Folders del workspace ─────────────────────────────────────────────
        folders: list[dict] = []
 
        if theme_local_path and os.path.isdir(theme_local_path):
            theme_base_name = os.path.basename(theme_local_path.rstrip('\\/'))
            folders.append({
                "name": f"Tema local – {theme_base_name}",
                "path": theme_local_path.replace("\\", "/"),
            })
 
        folders.append(folder_entry)
 
        # ── Settings del workspace ────────────────────────────────────────────
        workspace_settings: dict = {
            "files.associations": {
                "*.liquid": "liquid",
                "*.json": "jsonc",
            },
            "editor.formatOnSave": False,
            "editor.tabSize": 2,
            "editor.rulers": [120],
            "emmet.includeLanguages": {"liquid": "html"},
            "[liquid]": {
                "editor.defaultFormatter": "sissel.shopify-liquid",
                "editor.wordWrap": "on",
            },
            # ── Auto-guardar: shopify theme dev detecta cambios al instante ──
            "files.autoSave": "afterDelay",
            "files.autoSaveDelay": 500,
            "files.exclude": {
                "**/node_modules": True,
                "**/.git": True,
            },
            "search.exclude": {"**/node_modules": True},
            # ── Live Share: mostrar cursores y presencia de otros ──────────
            "liveshare.showInStatusBar": "always",
            "liveshare.guestApprovalRequired": False,
            "liveshare.anonymousGuestApproval": "accept",
            # ── Metadatos internos (ignorados por VS Code) ─────────────────
            "_shopify_container": shopify_container,
            "_docker_mode": self.docker_mode,
            "_docker_host": dh if is_remote else "local",
            "_remote_note": remote_access_note,
            "_live_reload": True,
            "_colaborativo": is_remote,
        }
 
        # ── Extensiones recomendadas ──────────────────────────────────────────
        # NOTA: ms-vsliveshare.vsliveshare es la CLAVE para colaboración en tiempo
        #       real. Con Live Share activo, un programador comparte la sesión y
        #       todos los demás ven y editan los mismos archivos simultáneamente.
        extensions: dict = {
            "recommendations": [
                "ms-vscode-remote.remote-containers",   # Dev Containers
                "ms-vscode-remote.remote-ssh",          # SSH Remote
                "ms-vscode-remote.vscode-remote-extensionpack",
                "ms-vsliveshare.vsliveshare",           # ← LIVE SHARE (clave)
                "sissel.shopify-liquid",                # Sintaxis Liquid
                "Shopify.theme-check-vscode",           # Linter oficial
                "bradlc.vscode-tailwindcss",
                "esbenp.prettier-vscode",
                "streetsidesoftware.code-spell-checker",
                "eamodio.gitlens",
            ],
        }
 
        # ── Tareas VS Code ────────────────────────────────────────────────────
        tasks: dict = {
            "version": "2.0.0",
            "tasks": [
                {
                    "label": "Shopify: Reiniciar theme dev (live reload)",
                    "type": "shell",
                    "command": (
                        f"{docker_prefix} exec {shopify_container} "
                        "sh -c \"pkill -f 'shopify theme dev' || true; "
                        f"cd {container_path} && shopify theme dev --live-reload\""
                    ),
                    "group": "build",
                    "presentation": {"reveal": "always"},
                    "problemMatcher": [],
                },
                {
                    "label": "Shopify: Ver logs en tiempo real",
                    "type": "shell",
                    "command": f"{docker_prefix} logs -f {shopify_container}",
                    "group": "test",
                    "presentation": {"reveal": "always", "panel": "new"},
                    "problemMatcher": [],
                },
                {
                    "label": "Shopify: Abrir shell en contenedor",
                    "type": "shell",
                    "command": f"{docker_prefix} exec -it {shopify_container} sh",
                    "group": "test",
                    "presentation": {"reveal": "always", "panel": "new"},
                    "problemMatcher": [],
                },
                {
                    "label": "Shopify: theme push (subir a la tienda)",
                    "type": "shell",
                    "command": (
                        f"{docker_prefix} exec -it {shopify_container} "
                        f"sh -c \"cd {container_path} && shopify theme push\""
                    ),
                    "group": "build",
                    "presentation": {"reveal": "always", "panel": "new"},
                    "problemMatcher": [],
                },
                {
                    "label": "Shopify: theme pull (descargar desde la tienda)",
                    "type": "shell",
                    "command": (
                        f"{docker_prefix} exec -it {shopify_container} "
                        f"sh -c \"cd {container_path} && shopify theme pull --force\""
                    ),
                    "group": "build",
                    "presentation": {"reveal": "always", "panel": "new"},
                    "problemMatcher": [],
                },
                # ── Live Share ──────────────────────────────────────────────
                {
                    "label": "Live Share: Iniciar sesión colaborativa",
                    "type": "shell",
                    # Abre el panel de Live Share en VS Code
                    "command": "workbench.action.liveShare.start",
                    "group": "test",
                    "presentation": {"reveal": "always"},
                    "problemMatcher": [],
                },
            ],
        }
 
        workspace_content: dict = {
            "folders": folders,
            "settings": workspace_settings,
            "extensions": extensions,
            "tasks": tasks,
            "launch": {"version": "0.2.0", "configurations": []},
        }
 
        with open(ws_path, "w", encoding="utf-8") as _f:
            json.dump(workspace_content, _f, indent=2, ensure_ascii=False)
 
        # ── Generar .devcontainer/devcontainer.json ───────────────────────────
        devcontainer_dir  = os.path.join(ws_dir, ".devcontainer")
        os.makedirs(devcontainer_dir, exist_ok=True)
        devcontainer_path = os.path.join(devcontainer_dir, "devcontainer.json")
 
        devcontainer = self._generate_devcontainer_json(
            shopify_container = shopify_container,
            container_path    = container_path,
            devcontainer_type = devcontainer_type,
            devcontainer_host = devcontainer_host,
            extensions        = extensions["recommendations"],
        )
 
        with open(devcontainer_path, "w", encoding="utf-8") as _f:
            json.dump(devcontainer, _f, indent=2, ensure_ascii=False)
 
        # ── Generar scripts open_workspace (SOLO modo TCP remoto) ────────────
        # En modo TCP no basta con hacer doble clic en el .code-workspace porque
        # VS Code se lanza sin DOCKER_HOST seteado → Dev Containers no encuentra
        # el daemon → error ENOPRO.  Los scripts setean DOCKER_HOST y LUEGO
        # abren VS Code, que es el único flujo fiable con TCP remoto.
        if devcontainer_type == "tcp" and devcontainer_host:
            ws_basename = os.path.basename(ws_path)
 
            # PowerShell (Windows)
            ps1_path = os.path.join(ws_dir, "open_workspace.ps1")
            ps1_content = f"""# Abre el workspace de Shopify apuntando al Docker remoto TCP
# Doble clic (o ejecutar en PowerShell) – NO hace falta ejecutar como admin
 
$env:DOCKER_HOST = '{devcontainer_host}'
Write-Host "DOCKER_HOST -> $env:DOCKER_HOST"
 
$wsPath = Join-Path $PSScriptRoot '{ws_basename}'
$codePaths = @(
    (Get-Command code -ErrorAction SilentlyContinue)?.Source,
    "$env:LocalAppData\\Programs\\Microsoft VS Code\\Code.exe",
    "$env:ProgramFiles\\Microsoft VS Code\\Code.exe"
) | Where-Object {{ $_ -and (Test-Path $_) }} | Select-Object -First 1
 
if ($codePaths) {{
    Start-Process -FilePath $codePaths -ArgumentList "`"$wsPath`""
}} else {{
    Write-Warning "VS Code no encontrado. Abre manualmente: $wsPath"
    Start-Sleep 3
}}
"""
            with open(ps1_path, "w", encoding="utf-8-sig") as _f:
                _f.write(ps1_content)
 
            # Bash (Linux / macOS)
            sh_path = os.path.join(ws_dir, "open_workspace.sh")
            sh_content = f"""#!/usr/bin/env bash
# Abre el workspace de Shopify apuntando al Docker remoto TCP
export DOCKER_HOST='{devcontainer_host}'
echo "DOCKER_HOST -> $DOCKER_HOST"
 
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
code "$SCRIPT_DIR/{ws_basename}"
"""
            with open(sh_path, "w", encoding="utf-8") as _f:
                _f.write(sh_content)
            try:
                import stat as _stat
                os.chmod(sh_path, os.stat(sh_path).st_mode | _stat.S_IXUSR | _stat.S_IXGRP)
            except Exception:
                pass
 
        # ── Generar README de colaboración ────────────────────────────────────
        readme_path = os.path.join(ws_dir, "README-colaboracion.md")
        self._write_collaboration_readme(
            readme_path       = readme_path,
            shopify_container = shopify_container,
            container_path    = container_path,
            server_addr       = server_addr,
            is_remote         = is_remote,
            devcontainer_type = devcontainer_type,
            dh                = dh,
            ws_path           = ws_path,
        )
 
        return ws_path
 
    # ── Helpers del workspace ─────────────────────────────────────────────────
 
    def _generate_devcontainer_json(
        self,
        shopify_container: str,
        container_path: str,
        devcontainer_type: str,           # "local" | "tcp" | "ssh"
        devcontainer_host: str,           # vacío si local
        extensions: list[str],
    ) -> dict:
        """
        Genera el devcontainer.json correcto para cada modo de conexión.
 
        MODO LOCAL y TCP
        ─────────────────
        Usa "dockerComposeFile" inexistente con "service" para que Dev Containers
        detecte el contenedor ya en marcha (modo "attach").
        En TCP añade "dockerHost" → Dev Containers apunta al daemon remoto
        sin que el usuario tenga que configurar DOCKER_HOST manualmente.
        (Soportado desde ms-vscode-remote.remote-containers v0.300.0)
 
        MODO SSH
        ────────
        Remote-SSH gestiona la conexión al host; devcontainer.json es estándar.
        """
        vscode_settings = {
            "terminal.integrated.defaultProfile.linux": "sh",
            "files.autoSave": "afterDelay",
            "files.autoSaveDelay": 500,
            "liveshare.guestApprovalRequired": False,
        }
 
        base: dict = {
            "name": f"Shopify – {shopify_container}",
            # "attachContainer" le dice a Dev Containers que se adjunte a un
            # contenedor EXISTENTE en lugar de crear uno nuevo.
            "attachContainer": shopify_container,
            "workspaceFolder": container_path,
            "shutdownAction": "none",
            "remoteUser": "root",
            "customizations": {
                "vscode": {
                    "extensions": extensions,
                    "settings": vscode_settings,
                },
            },
            "forwardPorts": [9292, 3000],
            "portsAttributes": {
                "9292": {"label": "Shopify Dev Server", "onAutoForward": "notify"},
                "3000": {"label": "Theme Preview",      "onAutoForward": "notify"},
            },
        }
 
        if devcontainer_type == "tcp" and devcontainer_host:
            # CLAVE: "dockerHost" le dice a Dev Containers dónde está el daemon.
            # Sin este campo, con Docker TCP remoto, Dev Containers intenta usar
            # el socket local y no encuentra el contenedor → error ENOPRO.
            base["dockerHost"] = devcontainer_host
 
        elif devcontainer_type == "ssh" and devcontainer_host:
            base["remoteEnv"] = {"SHOPIFY_SSH_HOST": devcontainer_host}
 
        return base
 
    def _write_collaboration_readme(
        self,
        readme_path: str,
        shopify_container: str,
        container_path: str,
        server_addr: str,
        is_remote: bool,
        devcontainer_type: str,
        dh: str,
        ws_path: str,
    ) -> None:
        """
        Escribe README-colaboracion.md con todas las instrucciones necesarias
        para que un nuevo programador se incorpore al workspace en <5 minutos.
 
        CAMBIO: se guarda con utf-8-sig (BOM) para que Windows lo muestre bien.
        """
        ws_basename = os.path.basename(ws_path)
 
        lines = [
            f"# Workspace colaborativo – {shopify_container}",
            "",
            "## Arquitectura del flujo en vivo",
            "",
            "```",
            f"PC-A (tú)          PC-B (compañero)   PC-N ...",
            "   |                     |                 |",
            "   +----------+----------+-----------------+",
            "              |",
            "              v",
            f"  Docker: contenedor  {shopify_container}",
            f"  {container_path}   ← archivos del tema",
            "  shopify theme dev --live-reload",
            "    → detecta cualquier cambio de archivo",
            "    → propaga live-reload al navegador",
            "```",
            "",
            "## Opción A – Remote Containers (todos en el mismo contenedor)",
            "",
            "Cada programador abre el MISMO contenedor Docker con VS Code.",
            "Los archivos están en el contenedor → cualquier guardado es inmediato.",
            "",
            "### 1. Instalar extensiones",
            "```bash",
            "code --install-extension ms-vscode-remote.vscode-remote-extensionpack",
            "code --install-extension sissel.shopify-liquid",
            "code --install-extension Shopify.theme-check-vscode",
            "```",
            "",
            "### 2. Abrir el workspace",
            "```bash",
            f'code "{ws_basename}"',
            "```",
            'VS Code mostrará "Reopen in Container" → pulsa **Yes**.',
            "",
        ]
 
        # Instrucciones adicionales según modo
        if is_remote and devcontainer_type == "tcp":
            lines += [
                "### 3. ⚠️ IMPORTANTE – Cómo abrir el workspace en modo TCP remoto",
                "",
                "> **NO hagas doble clic en el `.code-workspace` directamente.**",
                "> VS Code se abriría sin `DOCKER_HOST` seteado y Dev Containers",
                "> no encontraría el contenedor → error ENOPRO.",
                "",
                "**Usa siempre el script incluido:**",
                "",
                "```powershell",
                "# Windows: doble clic en open_workspace.ps1",
                "# o desde PowerShell:",
                ".\\open_workspace.ps1",
                "```",
                "```bash",
                "# Linux / macOS:",
                "./open_workspace.sh",
                "```",
                "",
                "El script setea `DOCKER_HOST` y luego abre VS Code.",
                "Dev Containers usará el `dockerHost` del `devcontainer.json`",
                f"(`{dh}`) para conectarse al daemon remoto automáticamente.",
                "",
                "**Alternativa manual** (si prefieres no usar el script):",
                "```powershell",
                f'$env:DOCKER_HOST = "{dh}"',
                f'code "{os.path.basename(ws_path)}"',
                "```",
                "",
            ]
        elif is_remote and devcontainer_type == "ssh":
            lines += [
                "### 3. Configurar SSH (~/.ssh/config)",
                "```",
                "Host shopify-dev",
                f"  HostName {server_addr}",
                "  User root",
                "  IdentityFile ~/.ssh/id_rsa",
                "  ServerAliveInterval 60",
                "```",
                "",
            ]
 
        lines += [
            "---",
            "",
            "## Opción B – VS Live Share (el anfitrión comparte la sesión)",
            "",
            "Live Share permite que los invitados vean y editen archivos en tiempo",
            "real **sin necesitar acceso directo a Docker**.",
            "",
            "1. El anfitrión abre el workspace y pulsa **Live Share** en la barra inferior.",
            "2. Copia el enlace de invitación y compártelo por Slack/Teams/etc.",
            "3. Los invitados abren el enlace → quedan conectados al mismo editor.",
            "4. Cualquier guardado del anfitrión (o de un invitado con permisos) llega",
            "   al contenedor y `shopify theme dev` recarga la preview al instante.",
            "",
            "```bash",
            "# Instalar Live Share (solo si no aparece la barra inferior):",
            "code --install-extension ms-vsliveshare.vsliveshare",
            "```",
            "",
            "---",
            "",
            "## URLs de preview",
            "",
            f"| Servicio          | URL                          |",
            f"|-------------------|------------------------------|",
            f"| Dev server        | `http://{server_addr}:9292`  |",
            f"| Theme preview     | `http://{server_addr}:3000`  |",
            "",
            "## Tareas disponibles  (`Ctrl+Shift+P` → *Run Task*)",
            "",
            "| Tarea | Descripción |",
            "|-------|-------------|",
            "| Reiniciar theme dev | Reinicia el servidor con `--live-reload` |",
            "| Ver logs en tiempo real | `docker logs -f` del contenedor |",
            "| Abrir shell en contenedor | Acceso directo a `sh` |",
            "| theme push | Sube el tema a la tienda Shopify |",
            "| theme pull | Descarga el tema desde la tienda |",
            "| Live Share: Iniciar sesión | Abre el panel de Live Share |",
            "",
            "## Notas importantes",
            "",
            f"- Los archivos del tema viven **dentro** del contenedor (`{container_path}`).",
            "- El contenedor tiene `--restart unless-stopped`: arranca automáticamente",
            "  con Docker, sin intervención manual.",
            "- Para pair-programming visual usa **Live Share** (Opción B).",
            "- Con Remote Containers (Opción A) todos editan literalmente el mismo",
            "  directorio → sin conflictos de sincronización.",
        ]
 
        # FIX: guardar con utf-8-sig (BOM) → correcto en el Bloc de notas de Windows
        with open(readme_path, "w", encoding="utf-8-sig") as _f:
            _f.write("\n".join(lines) + "\n")
 
    def _open_vscode_workspace(self, ws_path: str) -> None:
        """
        Abre el workspace en VS Code.
 
        MEJORA: busca el ejecutable en las rutas habituales de Windows, Linux y macOS,
        incluyendo instalaciones Flatpak y Snap.
        """
        if not ws_path or not os.path.isfile(ws_path):
            return
 
        # 1. 'code' en PATH (válido en todos los OS si VS Code está bien instalado)
        try:
            result = subprocess.run(
                ["code", ws_path],
                capture_output=True,
                timeout=8,
            )
            if result.returncode == 0:
                return
        except Exception:
            pass
 
        # 2. Rutas conocidas de Windows
        win_paths = [
            os.path.join(
                os.environ.get("LocalAppData", ""),
                "Programs", "Microsoft VS Code", "Code.exe",
            ),
            os.path.join(
                os.environ.get("ProgramFiles", ""),
                "Microsoft VS Code", "Code.exe",
            ),
            os.path.join(
                os.environ.get("ProgramFiles(x86)", ""),
                "Microsoft VS Code", "Code.exe",
            ),
        ]
 
        # 3. Rutas conocidas de Linux / macOS
        unix_paths = [
            "/usr/bin/code",
            "/usr/local/bin/code",
            "/snap/bin/code",                                  # Snap
            "/var/lib/flatpak/exports/bin/com.visualstudio.code",  # Flatpak system
            os.path.expanduser("~/.local/share/flatpak/exports/bin/com.visualstudio.code"),
            "/Applications/Visual Studio Code.app/Contents/Resources/app/bin/code",  # macOS
        ]
 
        for vscode_exe in win_paths + unix_paths:
            if vscode_exe and os.path.isfile(vscode_exe):
                try:
                    subprocess.Popen(
                        [vscode_exe, ws_path],
                        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                    )
                    return
                except Exception:
                    pass
 
        # 4. Último recurso: os.startfile (solo Windows)
        try:
            os.startfile(ws_path)  # type: ignore[attr-defined]
        except Exception:
            pass

def main() -> None:
    if sys.platform != "win32":
        print("Esta app esta pensada para Windows.")

    root = tk.Tk()
    app = ShopifyUtilitiesApp(root)
    root.mainloop()


if __name__ == "__main__":
    helper_exit_code = _run_helper_cli_from_argv(sys.argv)
    if helper_exit_code is None:
        main()
    else:
        raise SystemExit(helper_exit_code)