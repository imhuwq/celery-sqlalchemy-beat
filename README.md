# celery-sqlalchemy-beat
Celery beat backed on sqlalchemy database

With this util you will be able to add and remove periodic task to(from) celery dynamiclly.

This util is inspired by [celerybeat-mongo](https://github.com/zmap/celerybeat-mongo).

TODO:
Celery beat checks and updates database at a high frequency,  which may concern IO perfermance.
Redis may be a better choice.
