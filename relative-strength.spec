# -*- mode: python ; coding: utf-8 -*-

block_cipher = None

a = Analysis(['relative-strength.py'],
             binaries=[],
             datas=[('config.yaml', '.'), ('config_private.yaml', '.', 'DATA')],  # Optional private config
             hiddenimports=[],  # Remove scipy imports unless confirmed needed
             hookspath=[],
             runtime_hooks=[],
             excludes=['scipy'],  # Exclude scipy unless used
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          [],
          exclude_binaries=True,
          name='relative-strength',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=False,  # Disable UPX for testing, re-enable if stable
          console=True )
coll = COLLECT(exe,
               a.binaries,
               a.zipfiles,
               a.datas,
               strip=False,
               upx=False,  # Disable UPX for testing
               upx_exclude=[],
               name='relative-strength')
