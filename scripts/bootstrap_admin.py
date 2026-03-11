from __future__ import annotations

from app.auth.bootstrap import init_auth_schema


if __name__ == '__main__':
    init_auth_schema()
    print('admin bootstrap completed')
