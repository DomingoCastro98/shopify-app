# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['shopify_utilidades_app.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('docker_bin/docker.exe', 'docker_bin'),
        ('guia_usuario.html', '.'),
        ('shopify_basket.ico', '.'),
    ] + [],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='Shopify Utilidades',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    icon=r'C:\Users\domin\Desktop\Shopify_App\shopify-app\shopify_basket.ico',
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
