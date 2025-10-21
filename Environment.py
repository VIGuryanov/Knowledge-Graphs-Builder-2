import os

class Environment:
    _domain = os.environ.get("DOMAIN")
    if _domain[-1] == '/':
        _domain = _domain[:-1]
    
    _scheme = f"{_domain}/scheme"
    # _predicate = f'{_domain}/math#predicate'
    # _entity = f'{_domain}/math#entity'