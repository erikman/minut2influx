# Download data from Minut.com to Influx database

[Minut's Point](https://minut.com/) is Smart Home Sensor that checks temperature, humidity,
pressure and other readings.

This progam downloads historical data from Minut's API and stores it in an influx database. You
will need a Minut developer account in addition to your regular Minut account for this to work,
see https://www.minut.com/developers/ on how to request a developer account.

The current downloaded state is saved so you can run this script from a cron-job to only fetch new
values.

## Example

Assumes a running influx database on localhost without authentication and with an empty "minut"
database.

```bash
./minut2influx --state-path=./state.json --minut-username=user --minut-password=secret \
  --minut-client-id=0123456789abcdef --minut-client-secret=01234456789abcdef
Uploading... (24%)
Uploading... (48%)
Uploading... (71%)
Uploading... (95%)
```
