Load balancer written in Python. Can handle multiple requests using concurrency.
Project makes use of socket and threading modules

You can create simple http servers for testing using similar command:
```bash
python -m http.server 8080 --directory server8080
```
To invoke curl to make concurrent requests use this command:
```bash
curl --parallel --parallel-immediate --parallel-max 3 --config urls.txt
```