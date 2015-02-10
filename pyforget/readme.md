# This is the Python implementation of Forget Table

Written by [Mike Dewar](http://twitter.com/mikedewar) and [Micha Gorelick](http://micha.gd/).

To install run `pip install forgettable`.

To start the service run `forgettable --port=8080` which will start the wrapper. Note that you will need a Redis database running locally on port 6379. Forget Table will write into db 2 by default. 

Upon recieving an event, to increment a bin in a distribution call 

    localhost:8080/incr?key=colours&bin=red

where `colours` is the name of the distribution and `red` is the name of the bin you want to increment.
The distribution and bin will be created if they don't already exist. 

To query the whole distribution call

    localhost:8080/dist?key=colours

This will return a JSON blob with the normalised distribution in it. To query a specific bin of a distribution call 

    localhost:8080/bin?key=colours&bin=blue

