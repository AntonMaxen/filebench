from pathlib import Path

def get_suffixes(filename):
    suffixes = Path(filename).suffixes
    if len(suffixes) == 1:
        suffix_dict = {'filetype': suffixes[0], 'compression': None}
    elif len(suffixes) > 1:
        suffix_dict = {'filetype': suffixes[0], 'compression': suffixes[-1]}
    
    return suffix_dict 