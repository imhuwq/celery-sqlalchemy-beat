from celery import Celery

app = Celery("tasks")
app.conf.broker_url = 'redis://localhost:6379/0'
app.conf.result_backend = 'redis://localhost:6379/0'
app.conf.timezone = 'Asia/Shanghai'


@app.task(name="test")
def test(what):
    with open(what, 'a') as f:
        f.write('lala')
        f.write("\n")
