# fiuba-dist1-tp4

## Bootstraping

Place CSVs in a /sources folder

```bash
make rabbit-up
# wait for rabbit to start...

# start the system
make nodes-up

# upload csvs
./run-cli.sh
```