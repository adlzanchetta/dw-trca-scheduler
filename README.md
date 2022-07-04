# Self-notes and about

## Testing

To launch a *one-off dyno* that executes the ```harvest_realtime_from_api_to_mongodb.py``` Python script direcly from Heroku servers inside the ```my-app``` app, execute the following command in the local terminal:

    $ heroku login
    $ heroku run --app=my-app -- "python harvest_realtime_from_api_to_mongodb.py"
