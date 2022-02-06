# baichuan_go_worker

作为百川的 go worker 使用

# 1 发起消息
```
import json

from xlib import db
from xlib.mq import Queue

# Redis connection
baichuan_connection = db.my_caches["baichuan"]
# Queue
mq_queue=Queue("testjob", connection=baichuan_connection)

# 消息入队列
msg_dict = {"Length": 10, "Width": 5}
msg_data = json.dumps(msg_dict)
msg_obj=mq_queue.enqueue(msg_data)
print msg_obj.id
```

# 2 消费消息

```
go run main.go
```

