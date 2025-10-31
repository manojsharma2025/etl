import sys
sys.path.insert(0, r'c:\attomdataapps\src')
from utils.config_loader import ConfigLoader
import ftplib

cfg = ConfigLoader('config/config.json')
ftp_cfg = cfg.get_ftp_config()
host = ftp_cfg.get('host')
user = ftp_cfg.get('username') or ''
passwd = ftp_cfg.get('password') or ''
use_ftps = ftp_cfg.get('use_ftps', True)
timeout = int(ftp_cfg.get('timeout', 15))

print(f"Connecting to FTP host={host} user={'(set)' if user else '(empty)'} use_ftps={use_ftps} timeout={timeout}")

def try_list(ftp, pattern):
    try:
        result = ftp.nlst(pattern)
        print(f"NLST returned {len(result)} entries for pattern {pattern}")
        for i, r in enumerate(result[:200]):
            print(i+1, r)
    except Exception as e:
        print('NLST error:', e)

try:
    if use_ftps:
        try:
            ftp = ftplib.FTP_TLS(host=host, timeout=timeout)
            ftp.login(user, passwd)
            ftp.prot_p()
            print('Connected via FTPS')
        except Exception as e:
            print('FTPS failed:', e)
            print('Falling back to FTP')
            ftp = ftplib.FTP(host=host, timeout=timeout)
            ftp.login(user, passwd)
            print('Connected via FTP')
    else:
        ftp = ftplib.FTP(host=host, timeout=timeout)
        ftp.login(user, passwd)
        print('Connected via FTP')

    print('PWD before listing:', ftp.pwd())
    try_list(ftp, '*TAXASSESSOR*')
    # Also raw listing of root first 50
    try:
        files = ftp.nlst()
        print('\nFull listing count:', len(files))
        print('First 50 entries:')
        for i, f in enumerate(files[:50]):
            print(i+1, f)
    except Exception as e:
        print('Full listing error:', e)

    ftp.quit()
except Exception as e:
    print('Connection/listing failed:', e)
