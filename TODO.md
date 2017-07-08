# TODO

- [ ] AutoReconnectError in `_try_requeue_current_job` may result in weird behavior
- [ ] Handle exceptions in worker main_loop
- [ ] Per Job HEARTBEAT_TIMEOUT
- [ ] Think about Job cancelling (maybe remove cancel_job method and cancel jobs simply deleting them)
