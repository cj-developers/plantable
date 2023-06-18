import json

from plantable.app import Event

with open("test1.json", "r") as f:
    test1 = json.load(f)

with open("test2.json", "r") as f:
    test2 = json.load(f)

with open("test3.json", "r") as f:
    test3 = json.load(f)

event1 = Event(**test1)
event2 = Event(**test2)
event3 = Event(**test3)

print(event2)
