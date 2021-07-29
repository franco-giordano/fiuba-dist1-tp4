# fiuba-dist1-tp4

## Bootstraping

1. Place CSVs in `~/fiuba-dist1-tp4` (or change mount point in `run-cli.sh`)
2. Build RabbitMQ image
```bash
cd base-images
./build.sh
```
3. Run
```bash
make rabbit-up
# wait for rabbit to start...

# start the system
make nodes-up

# upload csvs
./run-cli.sh
```
