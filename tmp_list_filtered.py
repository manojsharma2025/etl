from pathlib import Path
p=Path('data/filtered')
if p.exists():
    for f in sorted(p.iterdir()):
        print(f.name, f.stat().st_size)
else:
    print('data/filtered does not exist')
