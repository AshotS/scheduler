Simple usage
----

```python
task1 = Task().every(10).seconds.do(print, "I'm Task1")
task2 = Task().every(5).seconds.do(print, "I'm Task2")
task3 = Task().every(1).seconds.do(print, "I'm Task3")

s = Scheduler()

s.add_task(task1)
s.add_task(task2)
s.add_task(task3)
s.start()
```
